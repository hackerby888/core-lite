#pragma once
// Page-aware KangarooTwelve for Windows lazy-committed contract-state regions.
//
// Why: on Windows the demand-zero contract-state reserves are MEM_RESERVE (commit-on-write via the VEH in
// overload.h) so the node's commit charge tracks the written footprint (~2 GB) instead of the full ~16 GB of
// reserves — Linux/macOS get this for free via overcommit + the shared zero page. The per-tick state digest,
// though, READS the whole effective state span. A plain KangarooTwelve(state, size) would fault every
// untouched page, the VEH would commit it, and the commit charge would balloon right back to the full reserve.
//
// KangarooTwelvePaged hashes the SAME logical bytes without touching reserved pages: it walks the region with
// VirtualQuery and feeds the hash the real bytes of committed (written) runs and synthesized zeros for reserved
// (never-written, therefore all-zero) runs. The result is bit-identical to KangarooTwelve over the full buffer
// — verified by tools/k12paged_test.cpp — so the cross-platform contract-state digest is unchanged.
//
// Must be included AFTER kangaroo_twelve.h (uses its KangarooTwelve_F / *_Absorb / permute) and after <windows.h>.

#ifdef _WIN32
#include <cstdint>   // uintptr_t

// Cached VirtualQuery run so a multi-GB reserved region costs one query, not one per 8 KB chunk.
struct K12PagedSrc {
    const unsigned char* base;
    uintptr_t runBase, runEnd;   // [runBase, runEnd) — the current contiguous same-state run
    bool runCommitted;
    bool valid;
};

// Absorb [base+off, base+off+len) into `node`, sourcing reserved (uncommitted) pages as zeros without faulting
// them in. `len` is always <= K12_chunkSize (8192) here — the K12 driver only ever absorbs input a chunk at a
// time — so ZBUF need only cover one chunk. KangarooTwelve_F_Absorb is a stateful sponge absorb (byteIOIndex
// persists), so feeding a run in consecutive sub-absorbs is identical to one contiguous absorb.
static void KangarooTwelve_F_AbsorbPaged(KangarooTwelve_F* node, K12PagedSrc* s,
                                         unsigned long long off, unsigned int len)
{
    static const unsigned char ZBUF[K12_chunkSize] = { 0 };
    while (len)
    {
        const unsigned char* p = s->base + off;
        uintptr_t pa = (uintptr_t)p;
        if (!s->valid || pa < s->runBase || pa >= s->runEnd)
        {
            MEMORY_BASIC_INFORMATION mbi;
            if (VirtualQuery((const void*)p, &mbi, sizeof(mbi)))
            {
                s->runBase = (uintptr_t)mbi.BaseAddress;
                s->runEnd = (uintptr_t)mbi.BaseAddress + (uintptr_t)mbi.RegionSize;
                // MEM_COMMIT pages hold real (written, or execution-read) state; MEM_RESERVE/MEM_FREE are zero.
                s->runCommitted = (mbi.State == MEM_COMMIT);
                s->valid = true;
            }
            else
            {
                // query failed: read the real bytes (safe — never under-hashes written state)
                s->runBase = pa; s->runEnd = pa + len; s->runCommitted = true; s->valid = true;
            }
        }
        uintptr_t avail = s->runEnd - pa;
        unsigned int take = (avail < (uintptr_t)len) ? (unsigned int)avail : len;   // <= len <= K12_chunkSize
        if (s->runCommitted) KangarooTwelve_F_Absorb(node, p, take);
        else                 KangarooTwelve_F_Absorb(node, ZBUF, take);
        off += take;
        len -= take;
    }
}

// Page-aware twin of KangarooTwelve (src/kangaroo_twelve.h). Structurally IDENTICAL — only the two input-absorb
// sites are routed through KangarooTwelve_F_AbsorbPaged (offset-tracked) instead of advancing an `input` ptr.
// Keep in lockstep with KangarooTwelve if that ever changes.
static void KangarooTwelvePaged(const unsigned char* base, unsigned int inputByteLen,
                                unsigned char* output, unsigned int outputByteLen)
{
    KangarooTwelve_F queueNode;
    KangarooTwelve_F finalNode;
    unsigned int blockNumber, queueAbsorbedLen;

    K12PagedSrc src; src.base = base; src.valid = false; src.runBase = 0; src.runEnd = 0; src.runCommitted = false;
    unsigned long long off = 0;

    setMem(&finalNode, sizeof(KangarooTwelve_F), 0);
    const unsigned int len = inputByteLen ^ ((K12_chunkSize ^ inputByteLen) & -(K12_chunkSize < inputByteLen));
    KangarooTwelve_F_AbsorbPaged(&finalNode, &src, off, len);
    off += len;
    inputByteLen -= len;
    if (len == K12_chunkSize && inputByteLen)
    {
        blockNumber = 1;
        queueAbsorbedLen = 0;
        finalNode.state[finalNode.byteIOIndex] ^= 0x03;
        if (++finalNode.byteIOIndex == K12_rateInBytes)
        {
            KeccakP1600_Permute_12rounds(finalNode.state);
            finalNode.byteIOIndex = 0;
        }
        else
        {
            finalNode.byteIOIndex = (finalNode.byteIOIndex + 7) & ~7;
        }

        while (inputByteLen > 0)
        {
            const unsigned int len = K12_chunkSize ^ ((inputByteLen ^ K12_chunkSize) & -(inputByteLen < K12_chunkSize));
            setMem(&queueNode, sizeof(KangarooTwelve_F), 0);
            KangarooTwelve_F_AbsorbPaged(&queueNode, &src, off, len);
            off += len;
            inputByteLen -= len;
            if (len == K12_chunkSize)
            {
                ++blockNumber;
                queueNode.state[queueNode.byteIOIndex] ^= K12_suffixLeaf;
                queueNode.state[K12_rateInBytes - 1] ^= 0x80;
                KeccakP1600_Permute_12rounds(queueNode.state);
                queueNode.byteIOIndex = K12_capacityInBytes;
                KangarooTwelve_F_Absorb(&finalNode, queueNode.state, K12_capacityInBytes);
            }
            else
            {
                queueAbsorbedLen = len;
            }
        }

        if (queueAbsorbedLen)
        {
            if (++queueNode.byteIOIndex == K12_rateInBytes)
            {
                KeccakP1600_Permute_12rounds(queueNode.state);
                queueNode.byteIOIndex = 0;
            }
            if (++queueAbsorbedLen == K12_chunkSize)
            {
                ++blockNumber;
                queueAbsorbedLen = 0;
                queueNode.state[queueNode.byteIOIndex] ^= K12_suffixLeaf;
                queueNode.state[K12_rateInBytes - 1] ^= 0x80;
                KeccakP1600_Permute_12rounds(queueNode.state);
                queueNode.byteIOIndex = K12_capacityInBytes;
                KangarooTwelve_F_Absorb(&finalNode, queueNode.state, K12_capacityInBytes);
            }
        }
        else
        {
            setMem(queueNode.state, sizeof(queueNode.state), 0);
            queueNode.byteIOIndex = 1;
            queueAbsorbedLen = 1;
        }
    }
    else
    {
        if (len == K12_chunkSize)
        {
            blockNumber = 1;
            finalNode.state[finalNode.byteIOIndex] ^= 0x03;
            if (++finalNode.byteIOIndex == K12_rateInBytes)
            {
                KeccakP1600_Permute_12rounds(finalNode.state);
                finalNode.byteIOIndex = 0;
            }
            else
            {
                finalNode.byteIOIndex = (finalNode.byteIOIndex + 7) & ~7;
            }

            setMem(queueNode.state, sizeof(queueNode.state), 0);
            queueNode.byteIOIndex = 1;
            queueAbsorbedLen = 1;
        }
        else
        {
            blockNumber = 0;
            if (++finalNode.byteIOIndex == K12_rateInBytes)
            {
                KeccakP1600_Permute_12rounds(finalNode.state);
                finalNode.state[0] ^= 0x07;
            }
            else
            {
                finalNode.state[finalNode.byteIOIndex] ^= 0x07;
            }
        }
    }

    if (blockNumber)
    {
        if (queueAbsorbedLen)
        {
            blockNumber++;
            queueNode.state[queueNode.byteIOIndex] ^= K12_suffixLeaf;
            queueNode.state[K12_rateInBytes - 1] ^= 0x80;
            KeccakP1600_Permute_12rounds(queueNode.state);
            KangarooTwelve_F_Absorb(&finalNode, queueNode.state, K12_capacityInBytes);
        }
        unsigned int n = 0;
        for (unsigned long long v = --blockNumber; v && (n < sizeof(unsigned long long)); ++n, v >>= 8)
        {
        }
        unsigned char encbuf[sizeof(unsigned long long) + 1 + 2];
        for (unsigned int i = 1; i <= n; ++i)
        {
            encbuf[i - 1] = (unsigned char)(blockNumber >> (8 * (n - i)));
        }
        encbuf[n] = (unsigned char)n;
        encbuf[++n] = 0xFF;
        encbuf[++n] = 0xFF;
        KangarooTwelve_F_Absorb(&finalNode, encbuf, ++n);
        finalNode.state[finalNode.byteIOIndex] ^= 0x06;
    }
    finalNode.state[K12_rateInBytes - 1] ^= 0x80;
    KeccakP1600_Permute_12rounds(finalNode.state);
    copyMem(output, finalNode.state, outputByteLen);
}

#endif // _WIN32

#pragma once

// Hash lazy Windows regions without committing untouched logical-zero pages.
// Include after kangaroo_twelve.h and windows.h.

#ifdef _WIN32
#include <cstdint>

// Cache the current VirtualQuery run across adjacent K12 chunks.
struct K12PagedSrc
{
    const unsigned char* base;
    uintptr_t runBase;
    uintptr_t runEnd;
    bool runCommitted;
    bool valid;
};

// Absorb committed bytes and synthesize zeros for reserved pages.
static void KangarooTwelve_F_AbsorbPaged(
    KangarooTwelve_F* node,
    K12PagedSrc* source,
    unsigned long long offset,
    unsigned int length)
{
    static const unsigned char zeroBuffer[K12_chunkSize] = { 0 };
    while (length)
    {
        const unsigned char* current = source->base + offset;
        const uintptr_t currentAddress = (uintptr_t)current;
        if (!source->valid
            || currentAddress < source->runBase
            || currentAddress >= source->runEnd)
        {
            MEMORY_BASIC_INFORMATION memoryInfo;
            if (VirtualQuery((const void*)current, &memoryInfo, sizeof(memoryInfo)))
            {
                source->runBase = (uintptr_t)memoryInfo.BaseAddress;
                source->runEnd =
                    (uintptr_t)memoryInfo.BaseAddress + (uintptr_t)memoryInfo.RegionSize;
                source->runCommitted = memoryInfo.State == MEM_COMMIT;
                source->valid = true;
            }
            else
            {
                // Fall back to real bytes so a query failure cannot omit written state.
                source->runBase = currentAddress;
                source->runEnd = currentAddress + length;
                source->runCommitted = true;
                source->valid = true;
            }
        }

        const uintptr_t available = source->runEnd - currentAddress;
        const unsigned int absorbedLength = available < (uintptr_t)length
            ? (unsigned int)available
            : length;
        if (source->runCommitted)
        {
            KangarooTwelve_F_Absorb(node, current, absorbedLength);
        }
        else
        {
            KangarooTwelve_F_Absorb(node, zeroBuffer, absorbedLength);
        }
        offset += absorbedLength;
        length -= absorbedLength;
    }
}

// Keep this structure aligned with the canonical KangarooTwelve implementation.
static void KangarooTwelvePaged(
    const unsigned char* base,
    unsigned int inputByteLen,
    unsigned char* output,
    unsigned int outputByteLen)
{
    KangarooTwelve_F queueNode;
    KangarooTwelve_F finalNode;
    unsigned int blockNumber;
    unsigned int queueAbsorbedLen;

    K12PagedSrc source{base, 0, 0, false, false};
    unsigned long long offset = 0;

    setMem(&finalNode, sizeof(KangarooTwelve_F), 0);
    const unsigned int initialLength =
        inputByteLen ^ ((K12_chunkSize ^ inputByteLen) & -(K12_chunkSize < inputByteLen));
    KangarooTwelve_F_AbsorbPaged(&finalNode, &source, offset, initialLength);
    offset += initialLength;
    inputByteLen -= initialLength;
    if (initialLength == K12_chunkSize && inputByteLen)
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
            const unsigned int chunkLength =
                K12_chunkSize
                ^ ((inputByteLen ^ K12_chunkSize) & -(inputByteLen < K12_chunkSize));
            setMem(&queueNode, sizeof(KangarooTwelve_F), 0);
            KangarooTwelve_F_AbsorbPaged(&queueNode, &source, offset, chunkLength);
            offset += chunkLength;
            inputByteLen -= chunkLength;
            if (chunkLength == K12_chunkSize)
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
                queueAbsorbedLen = chunkLength;
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
        if (initialLength == K12_chunkSize)
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

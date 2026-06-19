#pragma once

// Auto dirty-page tracking for SwapVirtualMemory cache pools (Linux). A freshly loaded slot is armed
// read-only; the first write faults and is caught by the SIGSEGV fast path here (marks the slot dirty,
// restores write access), so eviction can skip the writeback for slots never modified since load.
// SwapVM arms its own slots; this header holds the global pieces the handler needs: toggle, registry,
// fast path. Gated by gSwapDirtyTrackEnabled.

#if defined(__linux__)

#include <sys/mman.h>
#include <unistd.h>
#include <atomic>

// Set by --swap-dirty-track. Off => pools are never armed read-only => no faults, no tracking.
inline bool gSwapDirtyTrackEnabled = false;

namespace SwapDirtyTrack
{
    struct Pool
    {
        unsigned char** basePtr;        // &currentPage of the VM: *basePtr is the live pool base, or
                                        // NULL while the pool is being reallocated across an epoch reset
        unsigned long long slotStride;  // page-aligned bytes between slots (a multiple of the OS page size)
        int numSlots;                   // numCachePage + 1
        volatile unsigned char* dirty;  // per-slot dirty bytes (a stable VM-member array)
    };

    inline constexpr int MAX_POOLS = 16;
    inline Pool gPools[MAX_POOLS];
    inline std::atomic<int> gPoolCount{0};

    // Registered once per SwapVM at init. Keyed by &currentPage so the entry follows the pool's realloc
    // on epoch reset (and reads NULL, skipped, mid-realloc). Built before any slot is armed, so the
    // fault path reads it lock-free.
    inline void registerPool(unsigned char** basePtr, unsigned long long slotStride, int numSlots, volatile unsigned char* dirty)
    {
        int idx = gPoolCount.fetch_add(1, std::memory_order_acq_rel);
        if (idx >= MAX_POOLS)
            return; // more SwapVM instances than expected; tracking simply won't cover the overflow
        gPools[idx].basePtr = basePtr;
        gPools[idx].slotStride = slotStride;
        gPools[idx].numSlots = numSlots;
        gPools[idx].dirty = dirty;
    }

    // SIGSEGV fast path (called from signalHandler). If addr is in a registered pool, mark the slot
    // dirty, restore write access, return true (kernel retries the store); else false -> real crash
    // path. Async-signal-safe: a byte store + mprotect over an immutable registry.
    inline bool tryMarkDirty(void* addr)
    {
        if (!gSwapDirtyTrackEnabled)
            return false;
        unsigned char* a = (unsigned char*)addr;
        const int n = gPoolCount.load(std::memory_order_acquire);
        for (int i = 0; i < n; i++)
        {
            Pool& p = gPools[i];
            unsigned char* base = *p.basePtr;
            if (base == nullptr)
                continue;
            unsigned char* end = base + p.slotStride * (unsigned long long)p.numSlots;
            if (a >= base && a < end)
            {
                const int slot = (int)(((unsigned long long)(a - base)) / p.slotStride);
                p.dirty[slot] = 1;
                mprotect(base + (unsigned long long)slot * p.slotStride, p.slotStride, PROT_READ | PROT_WRITE);
                return true;
            }
        }
        return false;
    }
}

#else // !__linux__ : tracking unavailable; the flag exists but stays false.

inline bool gSwapDirtyTrackEnabled = false;

#endif

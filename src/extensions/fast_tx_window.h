#pragma once

#include "network_messages/transactions.h"
#include "platform/memory_util.h"
#include "platform/concurrency.h"
#include "public_settings.h"
#include "kangaroo_twelve.h"
#include "optimizations/opt_config.h"

// Lock-light future-transaction window for AUX nodes: stages broadcast txs for ticks
// (currentTick, currentTick + FAST_TX_WINDOW_TICKS] and answers digest lookups in O(1).
// Replaces PendingTxsPool on AUX, which never builds ticks and so needs no priority/eviction.
// Trades RAM for speed: flat pre-allocated slabs, one spinlock per tick, open-addressing digest
// hash, no global lock and no Collection. Slabs self-clear via a modulo ring + ownerTick tag, so
// the window slides for free as the node advances (no explicit advance step).
class FastTxWindow
{
protected:
    static constexpr unsigned int W = FAST_TX_WINDOW_TICKS;
    static constexpr unsigned int SLOTS = NUMBER_OF_TRANSACTIONS_PER_TICK;     // max txs kept per tick
    static constexpr unsigned int HASH_SLOTS = 2 * NUMBER_OF_TRANSACTIONS_PER_TICK; // 50% max load (N is 2^k)
    static constexpr unsigned int HASH_EMPTY = 0xFFFFFFFFu;

    inline static volatile char slabLock[W];
    inline static unsigned int slabOwnerTick[W];   // tick the slab currently holds (0 = none)
    inline static unsigned int slabCount[W];       // txs stored in the slab

    inline static m256i* digests = nullptr;          // [W * SLOTS]
    inline static unsigned char* bodies = nullptr;   // [W * SLOTS * MAX_TRANSACTION_SIZE]
    inline static unsigned int* hashIndex = nullptr; // [W * HASH_SLOTS] open addressing: digest -> tx index

    inline static m256i* digestPtr(unsigned int slab, unsigned int i) { return &digests[slab * SLOTS + i]; }
    inline static unsigned char* bodyPtr(unsigned int slab, unsigned int i) { return bodies + (slab * SLOTS + i) * MAX_TRANSACTION_SIZE; }
    inline static unsigned int* hashAt(unsigned int slab, unsigned int h) { return &hashIndex[slab * HASH_SLOTS + h]; }

public:
    // Window holds future ticks only: (currentTick, currentTick + W].
    inline static bool inWindow(unsigned int tick, unsigned int currentTick)
    {
        return tick > currentTick && tick <= currentTick + W;
    }

    // Reset a slab to hold a new tick (caller holds slabLock[slab]).
    inline static void resetSlab(unsigned int slab, unsigned int tick)
    {
        slabOwnerTick[slab] = tick;
        slabCount[slab] = 0;
        setMem(hashAt(slab, 0), HASH_SLOTS * sizeof(unsigned int), 0xFF); // all HASH_EMPTY
    }

public:
    static unsigned long long getSize()
    {
        return (unsigned long long)W * SLOTS * sizeof(m256i)
             + (unsigned long long)W * SLOTS * MAX_TRANSACTION_SIZE
             + (unsigned long long)W * HASH_SLOTS * sizeof(unsigned int);
    }

    static bool init()
    {
        if (!allocPoolWithErrorLog(L"FastTxWindow::digests", (unsigned long long)W * SLOTS * sizeof(m256i), (void**)&digests, __LINE__)
            || !allocPoolWithErrorLog(L"FastTxWindow::bodies", (unsigned long long)W * SLOTS * MAX_TRANSACTION_SIZE, (void**)&bodies, __LINE__)
            || !allocPoolWithErrorLog(L"FastTxWindow::hashIndex", (unsigned long long)W * HASH_SLOTS * sizeof(unsigned int), (void**)&hashIndex, __LINE__))
        {
            return false;
        }
        setMem(digests, (unsigned long long)W * SLOTS * sizeof(m256i), 0);
        setMem(bodies, (unsigned long long)W * SLOTS * MAX_TRANSACTION_SIZE, 0);
        setMem(hashIndex, (unsigned long long)W * HASH_SLOTS * sizeof(unsigned int), 0xFF); // all HASH_EMPTY
        setMem((void*)slabLock, sizeof(slabLock), 0);
        setMem(slabOwnerTick, sizeof(slabOwnerTick), 0);
        setMem(slabCount, sizeof(slabCount), 0);
        return true;
    }

    static void deinit()
    {
        if (digests) freePool(digests);
        if (bodies) freePool(bodies);
        if (hashIndex) freePool(hashIndex);
    }

    // Stage a broadcast tx. Computes the full-tx digest internally. Dedups; drops when the tick's slots
    // are full or the tick is outside the window. Returns true if newly stored.
    static bool add(const Transaction* tx, unsigned int currentTick)
    {
        const unsigned int tick = tx->tick;
        if (!inWindow(tick, currentTick))
            return false;
        const unsigned int slab = tick % W;
        const unsigned int txSize = tx->totalSize();
        m256i digest;
        KangarooTwelve(tx, txSize, &digest, sizeof(m256i));

        bool added = false;
        ACQUIRE(slabLock[slab]);
        if (slabOwnerTick[slab] != tick)
            resetSlab(slab, tick);

        unsigned int h = (unsigned int)digest.u64._0 & (HASH_SLOTS - 1);
        unsigned int idx;
        bool dup = false;
        while ((idx = *hashAt(slab, h)) != HASH_EMPTY)
        {
            if (*digestPtr(slab, idx) == digest) { dup = true; break; }
            h = (h + 1) & (HASH_SLOTS - 1);
        }
        if (!dup && slabCount[slab] < SLOTS)
        {
            const unsigned int newIdx = slabCount[slab];
            *digestPtr(slab, newIdx) = digest;
            copyMem(bodyPtr(slab, newIdx), (void*)tx, txSize);
            *hashAt(slab, h) = newIdx;
            slabCount[slab] = newIdx + 1;
            added = true;
        }
        RELEASE(slabLock[slab]);
        return added;
    }

    // O(1) lookup of a staged tx by its full-tx digest. Returns a pointer into the slab body, valid
    // while `tick` stays in the window (true throughout single-tick processing). nullptr if absent.
    static const Transaction* lookup(unsigned int tick, const m256i& digest, unsigned int currentTick)
    {
        if (!inWindow(tick, currentTick))
            return nullptr;
        const unsigned int slab = tick % W;
        const Transaction* result = nullptr;
        ACQUIRE(slabLock[slab]);
        if (slabOwnerTick[slab] == tick)
        {
            unsigned int h = (unsigned int)digest.u64._0 & (HASH_SLOTS - 1);
            unsigned int idx;
            while ((idx = *hashAt(slab, h)) != HASH_EMPTY)
            {
                if (*digestPtr(slab, idx) == digest) { result = (const Transaction*)bodyPtr(slab, idx); break; }
                h = (h + 1) & (HASH_SLOTS - 1);
            }
        }
        RELEASE(slabLock[slab]);
        return result;
    }
};

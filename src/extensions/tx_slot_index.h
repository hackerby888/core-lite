#pragma once

// O(1) digest->slot index for the next tick's tickData.
// Replaces the linear scan in processBroadcastTransaction(). Separate from the
// (disabled) tick_storage digest hashmap: covers only a small window of upcoming
// ticks, so RAM cost is a few MB, not epoch-wide.

#include <atomic>
#include <cstdint>

namespace TxSlotIndex
{
static constexpr unsigned int W = 8;                                            // upcoming ticks indexed at once (store targets tick+1; 8 = slack)
static constexpr unsigned int HASH_SLOTS = 2 * NUMBER_OF_TRANSACTIONS_PER_TICK; // 8192, 50% max load, 2^k
static constexpr unsigned int HASH_MASK = HASH_SLOTS - 1;
static constexpr uint16_t EMPTY_SLOT = 0xFFFF;

struct Buffer
{
    std::atomic<uint32_t> readyTick{0xFFFFFFFFu}; // tick this table indexes; published last (release)
    volatile char buildLock{0};
    m256i key[HASH_SLOTS];
    uint16_t slot[HASH_SLOTS];
};
static Buffer gBuf[W];

// K12 output is uniform, so low bits make a fine hash.
static inline unsigned int hashOf(const m256i& d)
{
    return (unsigned int)(*(const unsigned long long*)d.m256i_u8) & HASH_MASK;
}

// Builds the table for one tick from its tickData digests. Idempotent; first accept wins.
static inline void build(uint32_t tick, const m256i* digests)
{
    Buffer& b = gBuf[tick & (W - 1)];
    if (b.readyTick.load(std::memory_order_acquire) == tick) return; // already built (dup tickData)
    LockGuard guard(b.buildLock);
    if (b.readyTick.load(std::memory_order_relaxed) == tick) return;
    b.readyTick.store(0xFFFFFFFFu, std::memory_order_release);       // mark not-ready while filling
    for (unsigned int i = 0; i < HASH_SLOTS; i++) b.slot[i] = EMPTY_SLOT;
    for (unsigned int i = 0; i < NUMBER_OF_TRANSACTIONS_PER_TICK; i++)
    {
        if (isZero(digests[i])) continue;
        unsigned int p = hashOf(digests[i]);
        while (b.slot[p] != EMPTY_SLOT) p = (p + 1) & HASH_MASK;
        b.key[p] = digests[i];
        b.slot[p] = (uint16_t)i;
    }
    b.readyTick.store(tick, std::memory_order_release);             // publish
}

// -2 = index not ready (caller must linear-fallback); -1 = definitively absent; >=0 = slot.
static inline int lookup(uint32_t tick, const m256i& digest)
{
    Buffer& b = gBuf[tick & (W - 1)];
    if (b.readyTick.load(std::memory_order_acquire) != tick) return -2;
    unsigned int p = hashOf(digest);
    while (b.slot[p] != EMPTY_SLOT)
    {
        if (b.key[p] == digest) return (int)b.slot[p];
        p = (p + 1) & HASH_MASK;
    }
    return -1;
}
}

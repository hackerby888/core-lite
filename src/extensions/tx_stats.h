#pragma once

// Lock-free counters for broadcast txs seen by processBroadcastTransaction().
// Updated from many tickProcessor threads; all ops are atomic-relaxed (approximate is fine).

#include <atomic>
#include <cstdint>

namespace TxStats
{
// Ring of recent per-target-tick counters; power-of-two for mask indexing.
static constexpr unsigned int RING = 4096;
static constexpr unsigned int RING_MASK = RING - 1;

// 64-byte aligned to avoid false sharing between concurrent tick buckets.
struct alignas(64) TickSlot
{
    std::atomic<uint32_t> tick{0};      // target tick this slot counts
    std::atomic<uint32_t> received{0};  // valid broadcast txs seen for that tick
    std::atomic<uint32_t> stored{0};    // txs newly written into the tickData slot
};

static TickSlot gRing[RING];

static std::atomic<uint64_t> gTotalReceived{0};  // every packet entering the handler
static std::atomic<uint64_t> gTotalValid{0};     // passed checkValidity()
static std::atomic<uint64_t> gTotalStored{0};    // written into a next-tick slot
static std::atomic<uint32_t> gLastTick{0};       // newest target tick observed

// Claims the ring slot for tick t, resetting counters on first touch of a new tick value.
static inline TickSlot& slotFor(uint32_t t)
{
    TickSlot& s = gRing[t & RING_MASK];
    uint32_t cur = s.tick.load(std::memory_order_relaxed);
    if (cur != t && s.tick.compare_exchange_strong(cur, t, std::memory_order_relaxed))
    {
        s.received.store(0, std::memory_order_relaxed);
        s.stored.store(0, std::memory_order_relaxed);
    }
    return s;
}

// Every broadcast tx packet entering processBroadcastTransaction().
static inline void onReceive()
{
    gTotalReceived.fetch_add(1, std::memory_order_relaxed);
}

// A tx that passed checkValidity(), bucketed by its target tick.
static inline void onValid(uint32_t targetTick)
{
    gTotalValid.fetch_add(1, std::memory_order_relaxed);
    gLastTick.store(targetTick, std::memory_order_relaxed);
    slotFor(targetTick).received.fetch_add(1, std::memory_order_relaxed);
}

// A tx newly written into the next-tick tickData transaction slot.
static inline void onStored(uint32_t targetTick)
{
    gTotalStored.fetch_add(1, std::memory_order_relaxed);
    slotFor(targetTick).stored.fetch_add(1, std::memory_order_relaxed);
}
}

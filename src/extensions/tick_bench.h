#pragma once

// Per-phase timing for processTick(). Lock-free TSC accumulators, published via HTTP.
// One measurement per phase per tick, so atomic contention is negligible.

#include <atomic>
#include <cstdint>

namespace TickBench
{
enum Phase : unsigned int
{
    TICK_TOTAL = 0,             // whole processTick()
    BEGIN_TICK,                 // contract BEGIN_TICK
    PRESCAN_SOLUTIONS,          // scan tickData for solution txs
    PROCESS_SOLUTIONS,          // score engine task queue
    PROCESS_TXS,                // spectrum backup + transaction execution loop
    ORACLE,                     // subscription queries + timeouts
    END_TICK,                   // contract END_TICK
    DIGEST_SPECTRUM,            // spectrum Merkle digest
    DIGEST_UNIVERSE_COMPUTER,   // universe + computer digests
    PHASE_COUNT
};

static const char* const kPhaseName[PHASE_COUNT] = {
    "tickTotal", "beginTick", "prescanSolutions", "processSolutions",
    "processTxs", "oracle", "endTick", "digestSpectrum", "digestUniverseComputer"
};

struct alignas(64) PhaseStat
{
    std::atomic<uint64_t> count{0};
    std::atomic<uint64_t> sumTsc{0};
    std::atomic<uint64_t> maxTsc{0};
    std::atomic<uint64_t> lastTsc{0};
};
static PhaseStat gStat[PHASE_COUNT];

static inline void addDelta(unsigned int phase, unsigned long long dtTsc)
{
    PhaseStat& s = gStat[phase];
    s.count.fetch_add(1, std::memory_order_relaxed);
    s.sumTsc.fetch_add(dtTsc, std::memory_order_relaxed);
    s.lastTsc.store(dtTsc, std::memory_order_relaxed);
    uint64_t cur = s.maxTsc.load(std::memory_order_relaxed); // racy max, good enough
    while (dtTsc > cur && !s.maxTsc.compare_exchange_weak(cur, dtTsc, std::memory_order_relaxed)) {}
}

static inline void add(unsigned int phase, unsigned long long startTsc, unsigned long long endTsc)
{
    if (endTsc < startTsc) return; // discard TSC wrap / cross-core skew
    addDelta(phase, endTsc - startTsc);
}

static inline void reset()
{
    for (unsigned int p = 0; p < PHASE_COUNT; p++)
    {
        gStat[p].count.store(0, std::memory_order_relaxed);
        gStat[p].sumTsc.store(0, std::memory_order_relaxed);
        gStat[p].maxTsc.store(0, std::memory_order_relaxed);
        gStat[p].lastTsc.store(0, std::memory_order_relaxed);
    }
}

// RAII span. ctor/dtor must run on the same thread (true inside processTick()).
struct Scope
{
    unsigned int phase;
    unsigned long long start;
    Scope(unsigned int p) : phase(p), start(__rdtsc()) {}
    ~Scope() { add(phase, start, __rdtsc()); }
};
}

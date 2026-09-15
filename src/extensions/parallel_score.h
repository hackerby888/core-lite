#pragma once

// Splits one bpp9000 score() call across worker threads: every worker owns an engine loaded from
// the same task, the primary's currentANN is copied in per step, batches are claimed dynamically.
// Bit-exact: windows are independent and INFINITE_ERROR is absorbing, so partial sums compose.
// Opt-in per walk through Scope; tick-path walks preempt precompute walks at step granularity.

#ifdef LITE_PARALLEL_SCORE

#include "platform/concurrency.h"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#endif
#if defined(__linux__)
#include <sched.h>
#endif

namespace LiteParallelScore
{
enum ScopeClass
{
    None = 0,
    TickPath = 1,
    Precompute = 2,
};

struct Stats
{
    int threads;
    unsigned long long walksTickPath;
    unsigned long long walksPrecompute;
    unsigned long long walksSerial;
    unsigned long long stepsParallel;
    unsigned long long stepsPriorityWait;
    unsigned long long stepsSerialFallback;
};

inline int currentPid()
{
#if defined(_WIN32)
    return (int)_getpid();
#else
    return (int)getpid();
#endif
}

template<typename Engine>
struct Pool
{
#if defined(__AVX512F__)
    static constexpr unsigned long long BATCH_WINDOWS = Engine::SIMD_WINDOWS;
#else
    static constexpr unsigned long long BATCH_WINDOWS = Engine::SIMD_LANES;
#endif
    static constexpr unsigned long long WINDOW_COUNT = Engine::numberOfWindows;
    static constexpr int BATCH_COUNT = (int)((WINDOW_COUNT + BATCH_WINDOWS - 1) / BATCH_WINDOWS);
    // Batches per claim. 1 = best tail balance; raise only if the per-call setup shows in a profile.
    static constexpr int CLAIM_CHUNK = 1;
    static constexpr int MAX_PARTICIPANTS = BATCH_COUNT / 2;

    struct alignas(64) Slot
    {
        std::unique_ptr<Engine> engine;
        unsigned int localSum = 0;
    };

    // One cache line each: the round protocol hammers these from every worker.
    alignas(64) volatile int generation = 0;
    alignas(64) volatile int nextBatch = 0;
    alignas(64) volatile int abort = 0;
    alignas(64) volatile int done = 0;
    alignas(64) volatile int armedCount = 0;
    alignas(64) volatile int busy = 0;
    alignas(64) volatile int priorityActive = 0;
    alignas(64) volatile int stopping = 0;

    std::vector<Slot> slots;
    int workerCount = 0;
    int startPid = 0;
    // Heap so a fork child can abandon the handles without running ~thread on joinable ghosts.
    std::vector<std::thread>* threads = nullptr;

    std::atomic<unsigned long long> walksTickPath{ 0 };
    std::atomic<unsigned long long> walksPrecompute{ 0 };
    std::atomic<unsigned long long> walksSerial{ 0 };
    std::atomic<unsigned long long> stepsParallel{ 0 };
    std::atomic<unsigned long long> stepsPriorityWait{ 0 };   // precompute walks that waited for the tick path
    std::atomic<unsigned long long> stepsSerialFallback{ 0 };

    static inline Pool* sActive = nullptr;
    static inline thread_local ScopeClass tlClass = None;
    static inline thread_local int tlScopeDepth = 0;

    Pool() = default;
    Pool(const Pool&) = delete;
    Pool& operator=(const Pool&) = delete;

    ~Pool()
    {
        stop();
    }

    bool isAvailable() const
    {
        return threads != nullptr && workerCount > 0 && startPid == currentPid() && stopping == 0;
    }

    int participants() const
    {
        return threads != nullptr ? workerCount + 1 : 0;
    }

    static bool hookThunk(Engine& primary, unsigned int& out)
    {
        Pool* pool = sActive;
        if (pool == nullptr)
        {
            return false;
        }
        return pool->tryScore(primary, out);
    }

    // Threads inherit the main thread's single-core pin (Overload::initializeUefi); undo it here.
    static void resetAffinity()
    {
#if defined(__linux__)
        cpu_set_t all;
        CPU_ZERO(&all);
        for (int cpu = 0; cpu < CPU_SETSIZE; cpu++)
        {
            CPU_SET(cpu, &all);
        }
        sched_setaffinity(0, sizeof(all), &all);
#endif
    }

    bool start(int requestedParticipants, const unsigned char* topoBlock, const unsigned char* dataBlock)
    {
        if (threads != nullptr)
        {
            return true;
        }
        int n = requestedParticipants;
        if (n > MAX_PARTICIPANTS)
        {
            n = MAX_PARTICIPANTS;
        }
        if (n < 2)
        {
            return false;
        }

        ATOMIC_STORE32(generation, 0);
        ATOMIC_STORE32(nextBatch, 0);
        ATOMIC_STORE32(abort, 0);
        ATOMIC_STORE32(done, 0);
        ATOMIC_STORE32(armedCount, 0);
        ATOMIC_STORE32(busy, 0);
        ATOMIC_STORE32(priorityActive, 0);
        ATOMIC_STORE32(stopping, 0);

        workerCount = n - 1;
        slots = std::vector<Slot>((size_t)workerCount);
        for (int w = 0; w < workerCount; w++)
        {
            slots[w].engine = std::make_unique<Engine>();
            slots[w].engine->initMemory();
            if (!slots[w].engine->loadTaskFromMemory(topoBlock, dataBlock))
            {
                slots.clear();
                workerCount = 0;
                return false;
            }
        }

        startPid = currentPid();
        threads = new std::vector<std::thread>();
        threads->reserve((size_t)workerCount);
        for (int w = 0; w < workerCount; w++)
        {
            threads->emplace_back([this, w]() { workerLoop(w); });
        }
        sActive = this;
        Engine::parallelScoreHook = &hookThunk;
        return true;
    }

    void stop()
    {
        if (threads == nullptr)
        {
            return;
        }
        // Hold busy so no round can start once stopping is visible; busy stays taken until start().
        while (ATOMIC_CAS32(busy, 1, 0) != 0)
        {
            _mm_pause();
        }
        ATOMIC_STORE32(stopping, 1);
        for (auto& t : *threads)
        {
            t.join();
        }
        delete threads;
        threads = nullptr;
        if (sActive == this)
        {
            sActive = nullptr;
            Engine::parallelScoreHook = nullptr;
        }
        slots.clear();
        workerCount = 0;
    }

    // Only the calling thread survives fork(): the handles point at threads that do not exist, so
    // they are abandoned (join hangs, ~thread terminates) and the pool is rebuilt.
    void restartAfterPromote(int requestedParticipants, const unsigned char* topoBlock, const unsigned char* dataBlock)
    {
        threads = nullptr;
        slots.clear();
        workerCount = 0;
        tlClass = None;
        tlScopeDepth = 0;
        start(requestedParticipants, topoBlock, dataBlock);
    }

    unsigned int claimLoop(Engine& engine, int g)
    {
        unsigned int sum = 0;
        for (;;)
        {
            if (ATOMIC_LOAD32(abort) != 0 || (int)ATOMIC_LOAD32(generation) != g)
            {
                break;
            }
            const int first = ATOMIC_ADD32(nextBatch, CLAIM_CHUNK);
            if (first >= BATCH_COUNT)
            {
                break;
            }
            const unsigned long long windowBegin = (unsigned long long)first * BATCH_WINDOWS;
            unsigned long long windowEnd = (unsigned long long)(first + CLAIM_CHUNK) * BATCH_WINDOWS;
            if (windowEnd > WINDOW_COUNT)
            {
                windowEnd = WINDOW_COUNT;
            }
            const unsigned int r = engine.scoreSIMD(windowBegin, windowEnd, &abort);
            if (r == Engine::INFINITE_ERROR)
            {
                ATOMIC_STORE32(abort, 1);
                break;
            }
            sum += r;
        }
        return sum;
    }

    void workerLoop(int slotIndex)
    {
        resetAffinity();
        Slot& slot = slots[(size_t)slotIndex];
        int seen = 0;
        for (;;)
        {
            const int g = (int)ATOMIC_LOAD32(generation);
            if (g != seen)
            {
                seen = g;
                slot.localSum = claimLoop(*slot.engine, g);
                ATOMIC_ADD32(done, 1);
                continue;
            }
            if (ATOMIC_LOAD32(stopping) != 0)
            {
                break;
            }
            if (ATOMIC_LOAD32(armedCount) > 0)
            {
                _mm_pause();
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::microseconds(50));
            }
        }
    }

    // One score() call: copy the primary's LUT out, run one claim round with the caller as a
    // participant, sum. false = caller runs the serial kernel itself.
    bool tryScore(Engine& primary, unsigned int& out)
    {
        const ScopeClass cls = tlClass;
        if (cls == None)
        {
            if (tlScopeDepth > 0)
            {
                stepsSerialFallback++;
            }
            return false;
        }

        while (ATOMIC_CAS32(busy, 1, 0) != 0)
        {
            if (ATOMIC_LOAD32(stopping) != 0)
            {
                return false;
            }
            _mm_pause();
        }

        const int workers = workerCount;
        for (int w = 0; w < workers; w++)
        {
            copyMem(&slots[(size_t)w].engine->currentANN, &primary.currentANN, sizeof(primary.currentANN));
        }
        ATOMIC_STORE32(nextBatch, 0);
        ATOMIC_STORE32(abort, 0);
        ATOMIC_STORE32(done, 0);
        const int g = (int)((unsigned int)ATOMIC_ADD32(generation, 1) + 1u);

        unsigned int total = claimLoop(primary, g);

        while ((int)ATOMIC_LOAD32(done) < workers)
        {
            _mm_pause();
        }
        for (int w = 0; w < workers; w++)
        {
            total += slots[(size_t)w].localSum;
        }
        out = ATOMIC_LOAD32(abort) != 0 ? Engine::INFINITE_ERROR : total;
        stepsParallel++;
        ATOMIC_STORE32(busy, 0);
        return true;
    }

    Stats stats() const
    {
        Stats s;
        s.threads = participants();
        s.walksTickPath = walksTickPath.load(std::memory_order_relaxed);
        s.walksPrecompute = walksPrecompute.load(std::memory_order_relaxed);
        s.walksSerial = walksSerial.load(std::memory_order_relaxed);
        s.stepsParallel = stepsParallel.load(std::memory_order_relaxed);
        s.stepsPriorityWait = stepsPriorityWait.load(std::memory_order_relaxed);
        s.stepsSerialFallback = stepsSerialFallback.load(std::memory_order_relaxed);
        return s;
    }

    // One walk. Arms the pool so workers spin instead of sleep; TickPath also raises the priority
    // flag that parks precompute steps. Inert when no usable pool exists or when nested.
    struct Scope
    {
        Pool* pool = nullptr;
        ScopeClass cls = None;

        explicit Scope(ScopeClass requested)
        {
            tlScopeDepth++;
            Pool* p = sActive;
            if (p == nullptr)
            {
                return;
            }
            if (tlScopeDepth != 1 || !p->isAvailable())
            {
                p->walksSerial++;
                return;
            }
            // A precompute walk waits here, before the caller takes its engine-slot lock, until no
            // tick-path walk is active; once started it shares rounds rather than parking mid-walk.
            if (requested == Precompute)
            {
                bool waited = false;
                while (ATOMIC_LOAD32(p->priorityActive) != 0 && ATOMIC_LOAD32(p->stopping) == 0)
                {
                    waited = true;
                    _mm_pause();
                }
                if (waited)
                {
                    p->stepsPriorityWait++;
                }
            }
            pool = p;
            cls = requested;
            tlClass = requested;
            ATOMIC_ADD32(p->armedCount, 1);
            if (requested == TickPath)
            {
                ATOMIC_ADD32(p->priorityActive, 1);
                p->walksTickPath++;
            }
            else
            {
                p->walksPrecompute++;
            }
        }

        ~Scope()
        {
            if (pool != nullptr)
            {
                tlClass = None;
                if (cls == TickPath)
                {
                    ATOMIC_ADD32(pool->priorityActive, -1);
                }
                ATOMIC_ADD32(pool->armedCount, -1);
            }
            tlScopeDepth--;
        }

        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;
    };
};
}

#ifndef LITE_PARALLEL_SCORE_POOL_ONLY
// Node glue: one pool over the node's bpp9000 engine type, fed from the embedded task.

#include "platform/processor_count.h"

#include <cstdio>

namespace LiteParallelScore
{
using NodePool = Pool<score_engine::ScoreBpp9000T>;
inline NodePool gPool;
inline int gRequestedParticipants = -1;

using Scope = NodePool::Scope;

inline void configure(int participants)
{
    gRequestedParticipants = participants;
}

inline int resolvedParticipants()
{
    if (gRequestedParticipants >= 0)
    {
        return gRequestedParticipants;
    }
    int n = (int)totalProcessorCount() / 2;
    if (n < 2)
    {
        n = 2;
    }
    if (n > NodePool::MAX_PARTICIPANTS)
    {
        n = NodePool::MAX_PARTICIPANTS;
    }
    return n;
}

// Same pins loadBpp9000Task applies to the task file, so "same task as the primary" is checked, not assumed.
inline bool embeddedTaskBlocks(const unsigned char*& topoBlock, const unsigned char*& dataBlock)
{
    const unsigned int N = (unsigned int)BPP9000_NUMBER_OF_INPUT_NEURONS;
    const unsigned int M = (unsigned int)BPP9000_NUMBER_OF_OUTPUT_NEURONS;
    const unsigned int P = (unsigned int)BPP9000_POPULATION_THRESHOLD;
    const unsigned int K = (unsigned int)BPP9000_NUMBER_OF_NEIGHBORS;
    const unsigned long long T = BPP9000_SEQUENCE_LENGTH;

    const unsigned long long topoBytes = score_task_file::topologyBytes(N, M, P, K);
    const unsigned long long dataBytes = score_task_file::dataBytes(N, M, T);
    const unsigned long long headerBytes = sizeof(score_task_file::TaskFileHeader);
    if (headerBytes + topoBytes + dataBytes > BPP9000_TASK_SIZE)
    {
        return false;
    }
    topoBlock = BPP9000_TASK_BYTES + headerBytes;
    dataBlock = topoBlock + topoBytes;

    unsigned char topoHash[32];
    unsigned char dataHash[32];
    KangarooTwelve(topoBlock, (unsigned int)topoBytes, topoHash, 32);
    KangarooTwelve(dataBlock, (unsigned int)dataBytes, dataHash, 32);
    return *(const m256i*)topoHash == *(const m256i*)BPP9000_TOPOLOGY_HASH
        && *(const m256i*)dataHash == *(const m256i*)BPP9000_DATA_HASH;
}

inline void start()
{
    const int n = resolvedParticipants();
    if (n < 2)
    {
        return;
    }
    const unsigned char* topoBlock = nullptr;
    const unsigned char* dataBlock = nullptr;
    if (!embeddedTaskBlocks(topoBlock, dataBlock))
    {
        fprintf(stderr, "[parallel-score] embedded task does not match the pinned hashes, staying serial\n");
        fflush(stderr);
        return;
    }
    if (!gPool.start(n, topoBlock, dataBlock))
    {
        fprintf(stderr, "[parallel-score] worker pool failed to start, staying serial\n");
        fflush(stderr);
        return;
    }
    fprintf(stderr, "[parallel-score] %d participants per ant score step\n", gPool.participants());
    fflush(stderr);
}

inline void stop()
{
    gPool.stop();
}

inline void restartAfterPromote()
{
    const unsigned char* topoBlock = nullptr;
    const unsigned char* dataBlock = nullptr;
    const int n = embeddedTaskBlocks(topoBlock, dataBlock) ? resolvedParticipants() : 0;
    gPool.restartAfterPromote(n, topoBlock, dataBlock);
}

inline Stats stats()
{
    return gPool.stats();
}
}
#endif // LITE_PARALLEL_SCORE_POOL_ONLY

#else

namespace LiteParallelScore
{
enum ScopeClass
{
    None = 0,
    TickPath = 1,
    Precompute = 2,
};

struct Stats
{
    int threads;
    unsigned long long walksTickPath;
    unsigned long long walksPrecompute;
    unsigned long long walksSerial;
    unsigned long long stepsParallel;
    unsigned long long stepsPriorityWait;
    unsigned long long stepsSerialFallback;
};

struct Scope
{
    explicit Scope(ScopeClass) {}
};

inline void configure(int) {}
inline void start() {}
inline void stop() {}
inline void restartAfterPromote() {}
inline Stats stats() { return Stats{}; }
}

#endif // LITE_PARALLEL_SCORE

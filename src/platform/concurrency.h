#pragma once

#include <lib/platform_common/qintrin.h>
#include <atomic>
#include <cstdio>

#ifdef __linux__
#define _byteswap_ulong(x) bswap_32(x)
#define _InterlockedExchange8(target, val) __atomic_exchange_n(target, val, __ATOMIC_SEQ_CST)
#define _InterlockedIncrement64(target) __atomic_add_fetch(target, 1, __ATOMIC_SEQ_CST)
#define _InterlockedAnd64(target, val) __atomic_fetch_and(target, val, __ATOMIC_SEQ_CST)
#define _InterlockedExchange(target, val) __atomic_exchange_n(target, val, __ATOMIC_SEQ_CST)
#define _InterlockedExchange64(target, val) __atomic_exchange_n(target, val, __ATOMIC_SEQ_CST)
static long long _InterlockedCompareExchange64(volatile long long *target, long long exchange, long long comparand) {
    __atomic_compare_exchange_n(target, &comparand, exchange, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    return comparand;
}
static char _InterlockedCompareExchange8(volatile char *target, char exchange, char comparand) {
    __atomic_compare_exchange_n(target, &comparand, exchange, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    return comparand;
}
static long _InterlockedCompareExchange(volatile long *target, long exchange, long comparand) {
    __atomic_compare_exchange_n(target, &comparand, exchange, 0, __ATOMIC_SEQ_CST, __ATOMIC_SEQ_CST);
    return comparand;
}
#define _InterlockedExchangeAdd64(target, val) __atomic_fetch_add(target, val, __ATOMIC_SEQ_CST)
#define _interlockedadd64 _InterlockedExchangeAdd64
#define _InterlockedDecrement(target) __atomic_sub_fetch(target, 1, __ATOMIC_SEQ_CST)
#define _InterlockedIncrement(target) __atomic_add_fetch(target, 1, __ATOMIC_SEQ_CST)
#endif

// ---- Fork-eligibility census (tick fork-rollback) ------------------------------------------------
// Counts node locks each thread holds (via ACQUIRE/RELEASE/TRY_ACQUIRE + SmartMutex in fork_census.h).
// bspForkPoint verifies nobody but itself holds one before fork() (else it skips the fork and runs the
// tick strict, no crash) -> the child never inherits a held lock. Global slots, not thread_local
// pointers, so a short-lived thread can't dangle the gate read. ACQUIRE_WITHOUT_DEBUG_LOGGING is raw.
inline thread_local int tlLockSlot = -1;   // this thread's census slot; -1 until first acquire

namespace ForkCensus
{
    // Headroom for every thread that can hold a node lock: BSP + APs + per-socket workers + RPC
    // handlers (detached, one per unix conn -> can flood). 64 B/slot, so 2048 = 128 KB static.
    inline constexpr int MAX_THREADS = 2048;
    struct alignas(64) Slot   // 64-byte aligned: no false sharing between threads' depth counters
    {
        std::atomic<int> depth{ 0 };
        std::atomic<const char*> what{ nullptr };
        std::atomic<int> live{ 0 };   // 0 = free/reusable, 1 = owned by a live thread
    };
    inline Slot gSlots[MAX_THREADS];
    inline std::atomic<int> gCount{ 0 };   // high-water of ever-claimed slots (<= MAX_THREADS)
    // Latched if a thread ever can't get a slot (>MAX_THREADS live). Makes overflow fail-SAFE: the gate
    // then treats the census as unreliable and degrades to strict (never forks with an undercount).
    inline std::atomic<bool> gOverflow{ false };

    // Free this thread's slot at exit (slot memory is global, so the gate never reads dead storage).
    struct Unreg
    {
        ~Unreg()
        {
            if (tlLockSlot < 0) return;
            gSlots[tlLockSlot].depth.store(0, std::memory_order_relaxed);
            gSlots[tlLockSlot].what.store(nullptr, std::memory_order_relaxed);
            gSlots[tlLockSlot].live.store(0, std::memory_order_release);   // free for reuse
        }
    };
    inline thread_local Unreg tlUnreg;

    inline void claimSlot()
    {
        for (;;)
        {
            int n = gCount.load(std::memory_order_acquire);
            for (int i = 0; i < n; i++) // reuse a slot freed by an exited thread
            {
                int e = 0;
                if (gSlots[i].live.compare_exchange_strong(e, 1, std::memory_order_acq_rel))
                {
                    tlLockSlot = i;
                    (void)&tlUnreg;
                    return;
                }
            }
            int i = gCount.fetch_add(1, std::memory_order_acq_rel);
            if (i >= MAX_THREADS)   // more lock-holding threads than slots: latch overflow, fail safe (not silent)
            {
                gCount.fetch_sub(1, std::memory_order_acq_rel);
                if (!gOverflow.exchange(true, std::memory_order_acq_rel))
                {
                    fprintf(stderr, "[FORKCENSUS] slot overflow (>%d lock-holding threads) -> forks degrade to strict\n", MAX_THREADS);
                    fflush(stderr);
                }
                return;
            }
            int e = 0;
            if (gSlots[i].live.compare_exchange_strong(e, 1, std::memory_order_acq_rel))
            { tlLockSlot = i; (void)&tlUnreg; return; }
            // a reuse-scan claimed our fresh index in the tiny window above; retry
        }
    }

    inline void enter(const char* what)
    {
        if (tlLockSlot < 0) claimSlot();
        if (tlLockSlot < 0) return;   // registry full (>MAX_THREADS live): best-effort, this thread uncounted
        gSlots[tlLockSlot].what.store(what, std::memory_order_relaxed);
        gSlots[tlLockSlot].depth.fetch_add(1, std::memory_order_relaxed);
    }
    inline void leave()
    {
        if (tlLockSlot >= 0) gSlots[tlLockSlot].depth.fetch_sub(1, std::memory_order_relaxed);
    }

    // Held depth across all slots but the caller's own (excludes the BSP's deliberate fork-time holds).
    inline int sumExceptSelf()
    {
        if (gOverflow.load(std::memory_order_acquire)) return MAX_THREADS;   // unreliable -> force the gate to skip the fork
        int self = tlLockSlot;
        int n = gCount.load(std::memory_order_acquire);
        if (n > MAX_THREADS) n = MAX_THREADS;
        int s = 0;
        for (int i = 0; i < n; i++)
            if (i != self) s += gSlots[i].depth.load(std::memory_order_relaxed);
        return s < 0 ? 0 : s;
    }
    inline const char* offenderName()
    {
        if (gOverflow.load(std::memory_order_acquire)) return "fork-census-slot-overflow";
        int self = tlLockSlot;
        int n = gCount.load(std::memory_order_acquire);
        if (n > MAX_THREADS) n = MAX_THREADS;
        for (int i = 0; i < n; i++)
            if (i != self && gSlots[i].depth.load(std::memory_order_relaxed) > 0)
                return gSlots[i].what.load(std::memory_order_relaxed);
        return nullptr;
    }
}

inline void forkCensusEnter(const char* what) { ForkCensus::enter(what); }
inline void forkCensusLeave() { ForkCensus::leave(); }
inline int forkCensusSumExcept() { return ForkCensus::sumExceptSelf(); }
inline const char* forkCensusOffender() { return ForkCensus::offenderName(); }

// Gates the fork-eligibility enforcement in bspForkPoint (counting itself is always on, ~free).
// Disable with --no-fork-census.
inline bool gForkCensus = true;

// Acquire lock, may block
#define ACQUIRE_WITHOUT_DEBUG_LOGGING(lock) while (_InterlockedCompareExchange8(&lock, 1, 0)) _mm_pause()

#ifdef NDEBUG

// Acquire lock, may block
#define ACQUIRE(lock) do { ACQUIRE_WITHOUT_DEBUG_LOGGING(lock); forkCensusEnter(#lock " @ " __FILE__); } while (0)

#else

// Emit output if waiting long
class BusyWaitingTracker
{
    unsigned long long mStartTsc;
    unsigned long long mNextReportTscDelta;
    const char* mExpr;
    const char* mFile;
    unsigned int mLine;
    bool mTotalWaitTimeReport;
public:
    BusyWaitingTracker(const char* expr, const char* file, unsigned int line);
    ~BusyWaitingTracker();
    void pause();
};

// Acquire lock, may block and may log if it is blocked for a long time
#define ACQUIRE(lock) \
    do { \
        if (_InterlockedCompareExchange8(&lock, 1, 0)) { \
            BusyWaitingTracker bwt(#lock, __FILE__, __LINE__); \
            while (_InterlockedCompareExchange8(&lock, 1, 0)) \
                bwt.pause(); \
        } \
        forkCensusEnter(#lock " @ " __FILE__); \
    } while (0)

#endif

// Try to acquire lock and return if successful (without blocking)
#define TRY_ACQUIRE(lock) (_InterlockedCompareExchange8(&lock, 1, 0) == 0 ? (forkCensusEnter(#lock " @ " __FILE__), true) : false)

// Release lock
#ifdef _MSC_VER
#define RELEASE(lock) do { forkCensusLeave(); lock = 0; } while (0)
#else
#define RELEASE(lock) do { forkCensusLeave(); __atomic_store_n(&lock, 0, __ATOMIC_RELEASE); } while (0)
#endif

// Create an object of this class to lock until the end of the life-time of this object.
// Usually used on stack for making sure that the lock is released, no matter which way the function is left.
struct LockGuard
{
    LockGuard(volatile char& lock) : _lock(lock)
    {
        ACQUIRE(_lock);
    }

    ~LockGuard()
    {
        RELEASE(_lock);
    }

    volatile char& _lock;
};


#ifdef NDEBUG

// Begin waiting loop (with short expected waiting time). Outputs to debug.log if waiting long and NDEBUG isn't defined.
#define BEGIN_WAIT_WHILE(condition) \
    while (condition) {

// End waiting loop, corresponding to BEGIN_WAIT_WHILE().
#define END_WAIT_WHILE() _mm_pause(); }

#else

// Begin waiting loop (with short expected waiting time). Outputs to debug.log if waiting long and NDEBUG isn't defined.
#define BEGIN_WAIT_WHILE(condition) \
    if (condition) { \
        BusyWaitingTracker bwt(#condition, __FILE__, __LINE__); \
        while (condition) {

// End waiting loop, corresponding to BEGIN_WAIT_WHILE().
#define END_WAIT_WHILE() bwt.pause(); } }

#endif


// Waiting loop with short expected waiting time. Outputs to debug.log if waiting long and NDEBUG isn't defined.
#define WAIT_WHILE(condition) \
    BEGIN_WAIT_WHILE(condition) \
    END_WAIT_WHILE()

#define ATOMIC_STORE8(target, val) _InterlockedExchange8(&target, val)
// long in windows is 32bits
#ifdef _MSC_VER
static_assert(sizeof(long) == 4, "Size of long for _InterlockedExchange is 4 bytes");
#define ATOMIC_STORE32(target, val) _InterlockedExchange((volatile long*)&target, val)
#else
#define ATOMIC_STORE32(target, val) _InterlockedExchange((volatile int*)&target, val)
#endif
#define ATOMIC_INC64(target) _InterlockedIncrement64(&target)
#define ATOMIC_AND64(target, val) _InterlockedAnd64(&target, val)
#define ATOMIC_STORE64(target, val) _InterlockedExchange64(&target, val)
#define ATOMIC_LOAD64(target) _InterlockedCompareExchange64(&target, 0, 0)
#define ATOMIC_ADD64(target, val) _InterlockedExchangeAdd64(&target, val)
#define ATOMIC_MAX64(target, val) atomicMax64(&target, val)

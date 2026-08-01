#pragma once

// Tick fork-rollback (AUX wrong-solution path): fork on BSP, child keeps networking thread,
// re-spawns AP loops on promote. Linux-only (fork/pipe/_exit); #else inert stubs.

#ifdef __linux__

#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <signal.h>
#include <cstdio>
#include <ctime>
#include <atomic>
#include "extensions/tick_fork_control.h"

// Fork-path diagnostics: fprintf/stderr is fork-safe (no log-subsystem locks/buffers).
static inline void tickForkLog(const char* msg)
{
    fprintf(stderr, "[FORK] %s (pid=%d tick=%u)\n", msg, (int)getpid(), (unsigned)system.tick);
    fflush(stderr);
}

// Benchmark helpers (--fork-bench): monotonic ns + parent RSS from /proc/self/status.
static inline long long tickForkNowNs()
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (long long)t.tv_sec * 1000000000LL + t.tv_nsec;
}
// Monotonic ms for timeouts (the ns above is for bench precision).
static inline long long tickForkNowMs() { return tickForkNowNs() / 1'000'000LL; }
static inline long tickForkRssKb()
{
    FILE* f = fopen("/proc/self/status", "r");
    if (!f)
        return -1;
    char line[256];
    long kb = -1;
    while (fgets(line, sizeof(line), f))
    {
        if (sscanf(line, "VmRSS: %ld kB", &kb) == 1)
            break;
    }
    fclose(f);
    return kb;
}
// Set by maybeForkBeforeTick / bspForkPoint; consumed by verdict to report one fork's cost.
inline long long gForkWindowStartNs = 0;
inline long long gForkQuiesceNs = 0;     // quiesceNetworking() duration (BSP)
inline long long gForkSyscallNs = 0;     // fork() syscall duration (BSP)
inline long gForkRssBeforeKb = 0;        // parent RSS just before fork

namespace tickFork
{
    inline std::atomic<bool> gForkRequest{ false };  // tickProcessor -> BSP: fork now
    inline std::atomic<pid_t> gChildPid{ -2 };       // BSP -> tickProcessor: child pid (>=0) / -1 fail / -2 idle
    inline int gPipe[2] = { -1, -1 };                // verdict channel: parent writes [1], child reads [0]
    inline std::atomic<bool> gIsForkChild{ false };  // set in the promoted child

    enum class WindowState { Idle, Checkpointing, Live, Retiring };
    inline std::atomic<int> gWinState{ (int)WindowState::Idle };

    inline WindowState winState()
    {
        return (WindowState)gWinState.load(std::memory_order_acquire);
    }
    inline void setWinState(WindowState s)
    {
        gWinState.store((int)s, std::memory_order_release);
    }

    // Only ticks carrying a mining-solution tx can mismatch quorum.
    inline bool tickHasSolution(unsigned int tick)
    {
        TickData td;
        ts.tickData.acquireLock();
        const TickData* src = ts.tickData.getByTickIfNotEmpty(tick);
        if (src)
            copyMem(&td, src, sizeof(TickData));
        ts.tickData.releaseLock();
        if (!src)
            return false;

        auto offsets = ts.tickTransactionOffsets.getByTickInCurrentEpoch(tick);
        for (unsigned int i = 0; i < NUMBER_OF_TRANSACTIONS_PER_TICK; i++)
        {
            if (isZero(td.transactionDigests[i]) || !offsets[i])
                continue;
            Transaction* t = ts.tickTransactions(offsets[i]);
            if (!t->checkValidity())
                continue;
            if (MiningSolutionTransaction::isSolutionTransaction(t))
                return true;
        }
        return false;
    }

    // Checkpoint-and-replay: k ticks share one fork (amortizes the O(RSS) fork). The checkpoint child
    // stays alive across the window; match keeps it, mismatch rewinds+replays strict, staleness commits.
    inline unsigned int gForkWindowK = 64;
    inline unsigned int gCheckpointTick = 0;
    // Deadline to hand work to the BSP and quiesce fork writers before a fork / shadow commit.
    inline constexpr int gForkQuiesceTimeoutMs = 5'000;

    // BSP-owned request-processor barrier for a consistent fork snapshot / shadow commit.
    inline bool parkRequestProcessors(int timeoutMs)
    {
        unsigned long long idlePhase = gForkParkPhase.load(std::memory_order_acquire);
        if ((idlePhase & 1) || !gForkParkPhase.compare_exchange_strong(idlePhase, idlePhase + 1, std::memory_order_acq_rel))
        {
            return false;
        }
        const unsigned long long parkPhase = idlePhase + 1;
        long long deadline = tickForkNowMs() + timeoutMs;
        for (;;)
        {
            bool allParked = true;
            for (int i = 0; i < nRequestProcessorIDs; i++)
            {
                const unsigned long long processorNumber = requestProcessorIDs[i];
                if (gForkParkPhaseByProcessor[processorNumber].load(std::memory_order_acquire) != parkPhase)
                {
                    allParked = false;
                    break;
                }
            }
            if (allParked)
                return true;

            flushAsyncFileIOBuffer(0);
            if (tickForkNowMs() > deadline)
                return false;
            std::this_thread::yield();
        }
    }
    inline void unparkRequestProcessors()
    {
        unsigned long long parkPhase = gForkParkPhase.load(std::memory_order_acquire);
        while ((parkPhase & 1) && !gForkParkPhase.compare_exchange_weak(parkPhase, parkPhase + 1, std::memory_order_acq_rel))
        {
        }
    }

    struct RequestProcessorPark
    {
        bool requested = false;

        bool acquire(int timeoutMs)
        {
            requested = true;
            return parkRequestProcessors(timeoutMs);
        }

        void release()
        {
            if (requested)
            {
                unparkRequestProcessors();
                requested = false;
            }
        }

        ~RequestProcessorPark()
        {
            release();
        }
    };

    struct WriterQuiesce
    {
        RequestProcessorPark requestProcessors;
        bool rpcLocked = false;

        bool acquire(int timeoutMs)
        {
            const long long deadline = tickForkNowMs() + timeoutMs;
            if (!requestProcessors.acquire(timeoutMs))
                return false;

#if !defined(NO_RPC)
            while (!gRpcDispatchLock.try_lock())
            {
                flushAsyncFileIOBuffer(0);
                if (tickForkNowMs() >= deadline)
                    return false;
                std::this_thread::yield();
            }
            rpcLocked = true;
#endif
            return true;
        }

        void release()
        {
#if !defined(NO_RPC)
            if (rpcLocked)
            {
                gRpcDispatchLock.unlock();
                rpcLocked = false;
            }
#endif
            requestProcessors.release();
        }

        ~WriterQuiesce()
        {
            release();
        }
    };

    // Establish a fresh checkpoint at system.tick. The tick thread waits here while the BSP parks
    // writers, drains VM IO, arms the shadow, and forks.
    inline void establishCheckpoint()
    {
        if (gForkBench)
        {
            gForkWindowStartNs = tickForkNowNs();
            gForkRssBeforeKb = tickForkRssKb();
        }
        tickForkLog("checkpoint -> request BSP fork");
        setWinState(WindowState::Checkpointing);
        ForkStats::onForkRequested();

        // If the fork cannot be set up, score this tick strict so the optimistic pass cannot diverge
        // from quorum: no child is needed and the no-fork verdict won't stall.
        if (pipe(gPipe) != 0)
        {
            gReRunStrict = true;
            gReRunStrictUntilTick = (unsigned)system.tick;
            ForkStats::onForkSkipped(ForkStats::PIPE_FAIL, (unsigned)system.tick, "");
            tickForkLog("pipe() failed -> scoring this tick strict (no fork)");
            setWinState(WindowState::Idle);
            return;
        }
        // O_CLOEXEC: a fork+exec helper must not hold the write end open — child needs EOF on parent crash.
        fcntl(gPipe[0], F_SETFD, fcntl(gPipe[0], F_GETFD) | FD_CLOEXEC);
        fcntl(gPipe[1], F_SETFD, fcntl(gPipe[1], F_GETFD) | FD_CLOEXEC);
        gChildPid = -2;
        gForkRequest = true;                        // BSP forks at its loop-top
        // Before the BSP claims this request, a stalled main loop requires supervisor recovery.
        // After it claims the request, wait through the non-cancellable fork critical section.
        long long forkDeadlineMs = tickForkNowMs() + 30'000;
        while (gChildPid == -2)
        {
            // The timeout may cancel only before the BSP claims the request. Once claimed, exiting
            // here could race a live fork just as cancelling a running shadow commit would.
            if (tickForkNowMs() > forkDeadlineMs
                && gForkRequest.load(std::memory_order_acquire))
            {
                tickForkLog("BSP fork handoff stalled -> fatal exit for supervisor restart");
                _exit(70);
            }
            std::this_thread::yield();
        }

        if (gChildPid < 0)                          // fork failed
        {
            close(gPipe[0]);
            close(gPipe[1]);
            gPipe[0] = gPipe[1] = -1;
            gReRunStrict = true;
            gReRunStrictUntilTick = (unsigned)system.tick;
            tickForkLog("fork() failed -> scoring this tick strict (no fork)");
            setWinState(WindowState::Idle);
            return;
        }
        close(gPipe[0]);
        gCheckpointTick = (unsigned)system.tick;
        ForkStats::onForkOk();
        tickForkLog("parent: checkpoint forked, optimistic processTick ahead");
        setWinState(WindowState::Live);
    }

    inline void reapCheckpointChild()
    {
        const pid_t child = gChildPid.load();
        if (child <= 0)
        {
            tickForkLog("checkpoint child missing before commit -> supervisor restart");
            _exit(70);
        }
        if (!tickForkControl::writeRetireCommand(gPipe[1]))
            kill(child, SIGKILL);
        while (waitpid(child, nullptr, 0) < 0 && errno == EINTR)
        {
        }
        close(gPipe[1]);
        gPipe[1] = -1;
        gChildPid = -2;
    }

    // Close out a window that completed with no mismatch. The waiting tick thread is already stopped;
    // the BSP owns the remaining writer barrier, VM drain, and commit.
    inline void retireCheckpoint()
    {
        if (winState() != WindowState::Live)
        {
            return;
        }
        tickForkLog("window complete -> commit shadow + reap checkpoint");
        setWinState(WindowState::Retiring);
        if (!tickForkControl::gBspRetireHandoff.requestAndWait(gForkQuiesceTimeoutMs))
        {
            // Tell the child to promote — its state is at the checkpoint, it replays the window strict.
            char tag = tickForkControl::promoteTag;
            unsigned int target = (unsigned)system.tick;
            write(gPipe[1], &tag, 1);
            write(gPipe[1], &target, sizeof(target));
            tickForkLog("FATAL: swap writers did not quiesce before commit -> child promoted");
            _exit(70);
        }
    }

    // A deliberate node shutdown must not look like a parent crash to the checkpoint child.
    // The BSP sets shutDownNode before releasing its writer barrier, then acknowledges this request.
    inline void retireCheckpointForShutdown()
    {
        if (winState() == WindowState::Live)
        {
            tickForkLog("graceful shutdown -> commit shadow + reap checkpoint");
            setWinState(WindowState::Retiring);
        }
        for (;;)
        {
            if (tickForkControl::gBspRetireHandoff.requestAndWait(
                    gForkQuiesceTimeoutMs, true))
            {
                shutDownNode = 1;
                return;
            }
            tickForkLog("graceful shutdown retirement failed -> retrying without promoting child");
        }
    }

    // tickProcessor, before processTick(system.tick). Maintains the checkpoint window.
    inline void maybeForkBeforeTick(unsigned long long processorNumber)
    {
        (void)processorNumber;
        if (gReRunStrict)
            return;
        if (forceVerifySolutions)
            return;

        if (isMainMode())
        {
            if (winState() == WindowState::Live)
            {
                retireCheckpoint();
            }
            return;
        }

        if (isLastTickInEpoch())
        {
            if (winState() == WindowState::Live)
            {
                retireCheckpoint();
            }
            return;
        }

        if (gForkForceRollbackEvery && (unsigned)system.tick % gForkForceRollbackEvery == 0)
        {
            if (winState() != WindowState::Live)
            {
                establishCheckpoint();
            }
            return;
        }

        switch (winState())
        {
        case WindowState::Idle:
            if (!gForkForceFork && !tickHasSolution(system.tick))
            {
                return;
            }
            establishCheckpoint();
            break;

        case WindowState::Live:
            if ((unsigned)system.tick - gCheckpointTick < gForkWindowK)
            {
                return;
            }
            retireCheckpoint();
            if (gForkForceFork || tickHasSolution(system.tick))
            {
                establishCheckpoint();
            }
            break;

        case WindowState::Checkpointing:
        case WindowState::Retiring:
            break;
        }
    }

    // At the quorum compare. Returns true if the checkpoint window handled this tick.
    inline bool verdict(bool mismatch, const m256i& quorumSpectrumDigest, unsigned long long processorNumber)
    {
        (void)quorumSpectrumDigest;
        (void)processorNumber;
        if (winState() != WindowState::Live)
            return false;

        if (gForkForceMatch)
            mismatch = false;      // test: exercise the keep-checkpoint path
        if (gForkForceMismatch)
            mismatch = true;       // test: force rewind (parent _exit + child replays)
        if (gForkForceRollbackEvery && (unsigned)system.tick % gForkForceRollbackEvery == 0)
            mismatch = true;       // test: periodic forced rollback
        if (gShadowPoisoned.load(std::memory_order_acquire))
            mismatch = true;       // shadow I/O failed -> replay strict from pristine

        if (gForkBench)
        {
            long long windowNs = tickForkNowNs() - gForkWindowStartNs;
            long rssNow = tickForkRssKb();
            fprintf(stderr,
                "[FORK-BENCH] tick=%u %s ckpt=%u window=%.2fms quiesce=%.2fms fork()=%.3fms "
                "rss: before=%ldMB after=%ldMB cow_delta=%ldMB\n",
                (unsigned)system.tick, mismatch ? "MISMATCH" : "MATCH", gCheckpointTick,
                windowNs / 1e6, gForkQuiesceNs / 1e6, gForkSyscallNs / 1e6,
                gForkRssBeforeKb / 1024, rssNow / 1024, (rssNow - gForkRssBeforeKb) / 1024);
            fflush(stderr);
        }

        ForkStats::onVerdict(mismatch);

        if (!mismatch)
        {
            // Keep the checkpoint for the rest of the window; committed + reaped at staleness.
            if (gForkBench)
            {
                tickForkLog("verdict MATCH: keep checkpoint (window)");
            }
            return true;
        }

        // Mismatch: leave shadowing active and exit. The child waits for pipe EOF before purging the
        // registered shadow directories, so a late compressed write can never fall through to real.
        tickForkLog("verdict MISMATCH: rewind to checkpoint + parent _exit");
        unsigned int target = (unsigned)system.tick;
        const char tag = tickForkControl::promoteTag;
        ssize_t w = write(gPipe[1], &tag, 1);
        (void)w;
        w = write(gPipe[1], &target, sizeof(target));
        (void)w;
        _exit(0);
    }
}

#else  // !__linux__ : fork-based rollback is Linux-only; inert stubs keep qubic.cpp building.

#include <atomic>
namespace tickFork
{
    inline std::atomic<bool> gIsForkChild{ false };
    inline std::atomic<bool> gForkRequest{ false };
    inline void maybeForkBeforeTick(unsigned long long) {}
    inline bool verdict(bool mismatch, const m256i& quorumSpectrumDigest, unsigned long long processorNumber)
    {
        (void)mismatch;
        (void)quorumSpectrumDigest;
        (void)processorNumber;
        return false;
    }
}

#endif // __linux__

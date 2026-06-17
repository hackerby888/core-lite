#pragma once

// Fork-on-BSP child-promote tick rollback (AUX wrong-solution path); disk side in disk_shadow.h.
// The fork is on the BSP so the child keeps the networking/dispatch thread; it re-spawns the AP
// loops on promote. Linux-only (fork/pipe/_exit); #else gives inert stubs for the shared TU.

#ifdef __linux__

#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
#include <cstdio>
#include <ctime>
#include <atomic>

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
static inline long tickForkRssKb()
{
    FILE* f = fopen("/proc/self/status", "r");
    if (!f) return -1;
    char line[256];
    long kb = -1;
    while (fgets(line, sizeof(line), f))
        if (sscanf(line, "VmRSS: %ld kB", &kb) == 1) break;
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

    // Only ticks carrying a mining-solution tx can mismatch quorum.
    inline bool tickHasSolution(unsigned int tick)
    {
        TickData td;
        ts.tickData.acquireLock();
        const TickData* src = ts.tickData.getByTickIfNotEmpty(tick);
        if (src) copyMem(&td, src, sizeof(TickData));
        ts.tickData.releaseLock();
        if (!src) return false;

        auto offsets = ts.tickTransactionOffsets.getByTickInCurrentEpoch(tick);
        for (unsigned int i = 0; i < NUMBER_OF_TRANSACTIONS_PER_TICK; i++)
        {
            if (isZero(td.transactionDigests[i]) || !offsets[i]) continue;
            Transaction* t = ts.tickTransactions(offsets[i]);
            if (!t->checkValidity()) continue;
            if (MiningSolutionTransaction::isSolutionTransaction(t))
                return true;
        }
        return false;
    }

    // Checkpoint-and-replay: k ticks share one fork (amortizes the O(RSS) fork). The checkpoint child
    // stays alive across the window; match keeps it, mismatch rewinds+replays strict, staleness commits.
    inline unsigned int gForkWindowK = 32;
    inline unsigned int gCheckpointTick = 0;

    // Establish a fresh checkpoint at system.tick: park request procs, pipe, arm shadow, ask BSP to fork.
    inline void establishCheckpoint()
    {
        if (gForkBench) { gForkWindowStartNs = tickForkNowNs(); gForkRssBeforeKb = tickForkRssKb(); }
        tickForkLog("checkpoint -> request BSP fork");
        gForkParked.store(0, std::memory_order_release);
        gForkParkGen.fetch_add(1, std::memory_order_acq_rel);   // open a new park generation
        gForkQuiesceRequest = true;
        // Bounded: a wedged request processor must not hang the fork. On timeout, score this tick strict.
        long long parkDeadline = tickForkNowNs() + 5000000000LL;
        while (gForkParked.load(std::memory_order_acquire) < nRequestProcessorIDs)
        {
            if (tickForkNowNs() > parkDeadline)
            {
                gForkQuiesceRequest = false;
                gReRunStrict = true; gReRunStrictUntilTick = (unsigned)system.tick;
                tickForkLog("park barrier timeout -> scoring this tick strict (no fork)");
                return;
            }
            std::this_thread::yield();
        }

        // If the fork cannot be set up, score this tick strict so the optimistic pass cannot diverge
        // from quorum: no child is needed and the no-fork verdict won't stall.
        if (pipe(gPipe) != 0)
        {
            gForkQuiesceRequest = false;
            gReRunStrict = true; gReRunStrictUntilTick = (unsigned)system.tick;
            tickForkLog("pipe() failed -> scoring this tick strict (no fork)");
            return;
        }
        gShadow.arm();                              // parent disk writes -> shadow for the whole window
        gChildPid = -2;
        gForkRequest = true;                        // BSP forks at its loop-top
        // The BSP handoff is synchronous and fast; only a wedged/dead main loop stalls here. Reclaiming
        // on timeout would race a late fork into a rogue promoted child, so treat a true stall as fatal
        // and let the supervisor restart the node from its snapshot.
        long long forkDeadline = tickForkNowNs() + 30000000000LL;
        while (gChildPid == -2)
        {
            if (tickForkNowNs() > forkDeadline)
            {
                tickForkLog("BSP fork handoff stalled -> fatal exit for supervisor restart");
                _exit(70);
            }
            std::this_thread::yield();
        }

        if (gChildPid < 0)                          // fork failed
        {
            gShadow.discard();
            close(gPipe[0]); close(gPipe[1]); gPipe[0] = gPipe[1] = -1;
            gForkQuiesceRequest = false;
            gReRunStrict = true; gReRunStrictUntilTick = (unsigned)system.tick;
            tickForkLog("fork() failed -> scoring this tick strict (no fork)");
            return;
        }
        close(gPipe[0]);                            // parent keeps the write end across the window
        gForkQuiesceRequest = false;                // release request processors
        gCheckpointTick = (unsigned)system.tick;
        tickForkLog("parent: checkpoint forked, optimistic processTick ahead");
    }

    // Close out a window that completed with no mismatch (we are still alive): commit its diverted
    // disk writes into the real files and reap the checkpoint child.
    inline void retireCheckpoint()
    {
        tickForkLog("window complete -> commit shadow + reap checkpoint");
        gShadow.commit();
        kill(gChildPid.load(), SIGKILL);
        int st; waitpid(gChildPid.load(), &st, 0);
        close(gPipe[1]); gPipe[1] = -1;
        gChildPid = -2;
    }

    // tickProcessor, before processTick(system.tick). Maintains the checkpoint window.
    inline void maybeForkBeforeTick(unsigned long long processorNumber)
    {
        (void)processorNumber;
        if (gReRunStrict) return;                   // replaying the window strict: never fork
        if (forceVerifySolutions) return;           // -fv = strict-all mode: no trick/rollback/fork
        if (isMainMode()) return;

        // Last tick of the epoch: verdict() is skipped here (same gate as main's reprocess path) and the
        // next tick runs beginEpoch. Never let a window span the boundary — retire any live checkpoint
        // and don't open one. An unresolved checkpoint would carry into the new epoch, forcing a promoted
        // child to replay strict across beginEpoch (which blocks on the operator clean-memory flag).
        if (isLastTickInEpoch())
        {
            if (gChildPid >= 0) retireCheckpoint();
            return;
        }

        // Test: force a single-tick fork + rollback every N ticks. Retire any live window first so the
        // forced mismatch (in verdict) rewinds exactly this tick, back to the tick-1 state.
        if (gForkForceRollbackEvery && (unsigned)system.tick % gForkForceRollbackEvery == 0)
        {
            if (gChildPid >= 0) retireCheckpoint();
            establishCheckpoint();
            return;
        }

        // Cheap common path: a live checkpoint still covers this tick -> reuse it, no fork.
        if (gChildPid >= 0 && (unsigned)system.tick - gCheckpointTick < gForkWindowK) return;

        // Window aged out with no mismatch (still alive) -> commit it and drop the child.
        if (gChildPid >= 0) retireCheckpoint();

        // Only solution ticks can diverge from quorum, so only they need a checkpoint.
        if (!gForkForceFork && !tickHasSolution(system.tick)) return;

        establishCheckpoint();
    }

    // At the quorum compare. Returns true if the checkpoint window handled this tick.
    inline bool verdict(bool mismatch, const m256i& quorumSpectrumDigest, unsigned long long processorNumber)
    {
        (void)quorumSpectrumDigest; (void)processorNumber;
        if (gChildPid < 0) return false;            // no checkpoint (non-solution tick, or mid-replay)

        if (gForkForceMatch) mismatch = false;      // test: exercise the keep-checkpoint path
        if (gForkForceMismatch) mismatch = true;    // test: force rewind (parent _exit + child replays)
        if (gForkForceRollbackEvery && (unsigned)system.tick % gForkForceRollbackEvery == 0) mismatch = true;  // test: periodic forced rollback
        if (gShadowPoisoned.load(std::memory_order_acquire)) mismatch = true;  // shadow I/O failed -> replay strict from pristine

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

        if (!mismatch)
        {
            // Keep the checkpoint for the rest of the window; committed + reaped at staleness.
            if (gForkBench)
            {
                tickForkLog("verdict MATCH: keep checkpoint (window)");
            }
            return true;
        }

        // Mismatch: rewind to the checkpoint and replay [gCheckpointTick, system.tick] strict (target
        // sent to the child); discard the window's diverts so the child reads pristine pages.
        tickForkLog("verdict MISMATCH: rewind to checkpoint + parent _exit");
        gShadow.discard();
        unsigned int target = (unsigned)system.tick;
        const char tag = 'P';
        ssize_t w = write(gPipe[1], &tag, 1); (void)w;
        w = write(gPipe[1], &target, sizeof(target)); (void)w;
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
    inline bool verdict(bool, const m256i&, unsigned long long) { return false; }
}

#endif // __linux__

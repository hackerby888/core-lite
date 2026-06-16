#pragma once

// Fork-on-BSP child-promote tick rollback (AUX wrong-solution path); the only rollback path.
// Disk side: disk_shadow.h.
//
// The fork is taken on the BSP (main-loop thread) so the child inherits the thread that drives
// networking + contract dispatch; the child re-spawns only the simple AP loops on promotion.
//
// tickProcessor side (here): before processTick of a solution-bearing AUX tick it parks the request
// processors, arms the disk shadow, and asks the BSP to fork. At the quorum compare it issues the
// verdict: match -> commit shadow + kill the child; mismatch -> hand off to the child + _exit.
// BSP side (bspForkPoint / tickForkChildPromote) lives in qubic.cpp where spawnAPs is visible.
//
// fork()/pipe()/_exit are POSIX: this rollback is Linux-only. On other platforms the #else block
// provides inert stubs so the shared qubic.cpp translation unit still compiles.

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
            if (isZero(t->destinationPublicKey)
                && t->amount >= MiningSolutionTransaction::minAmount()
                && t->inputType == MiningSolutionTransaction::transactionType())
                return true;
        }
        return false;
    }

    // tickProcessor, before processTick(system.tick). Requests a BSP fork on a risky tick.
    inline void maybeForkBeforeTick(unsigned long long processorNumber)
    {
        (void)processorNumber;
        if (gReRunStrict) return;                   // this is the re-run tick: do not fork again
        if (forceVerifySolutions) return;           // -fv = strict-all mode: no trick/rollback/fork
        if (isMainMode()) return;
        if (!gForkForceFork && !tickHasSolution(system.tick)) return;  // force: fork clean ticks too

        if (gForkBench) { gForkWindowStartNs = tickForkNowNs(); gForkRssBeforeKb = tickForkRssKb(); }
        tickForkLog("solution tick -> request BSP fork");
        gForkParked.store(0, std::memory_order_release);
        gForkParkGen.fetch_add(1, std::memory_order_acq_rel);   // open a new park generation
        gForkQuiesceRequest = true;
        while (gForkParked.load(std::memory_order_acquire) < nRequestProcessorIDs)
            std::this_thread::yield();

        // If the fork cannot be set up, score this tick strict (like the re-run) so the optimistic
        // pass cannot diverge from quorum: no child is needed and the no-fork verdict won't stall.
        if (pipe(gPipe) != 0)
        {
            gForkQuiesceRequest = false;
            gReRunStrict = true;
            tickForkLog("pipe() failed -> scoring this tick strict (no fork)");
            return;
        }
        gShadow.arm();                              // parent disk writes -> shadow; child resets on promote
        gChildPid = -2;
        gForkRequest = true;                        // BSP forks at its loop-top
        while (gChildPid == -2) std::this_thread::yield();

        if (gChildPid < 0)                          // fork failed
        {
            gShadow.discard();
            close(gPipe[0]); close(gPipe[1]); gPipe[0] = gPipe[1] = -1;
            gForkQuiesceRequest = false;
            gReRunStrict = true;
            tickForkLog("fork() failed -> scoring this tick strict (no fork)");
            return;
        }
        close(gPipe[0]);                            // parent keeps the write end
        gForkQuiesceRequest = false;                // release request processors
        tickForkLog("parent: child forked, optimistic processTick ahead");
    }

    // At the quorum compare. Returns true if fork handled this tick (skip legacy reprocess).
    inline bool verdict(bool mismatch, const m256i& quorumSpectrumDigest, unsigned long long processorNumber)
    {
        (void)quorumSpectrumDigest; (void)processorNumber;
        if (gChildPid < 0) return false;            // no live child (no fork this tick, or we are the re-run)

        if (gForkForceMatch) mismatch = false;      // test: exercise the commit + kill-child path
        if (gForkForceMismatch) mismatch = true;    // test: force promote (parent _exit + child takes over)

        if (gForkBench)
        {
            long long windowNs = tickForkNowNs() - gForkWindowStartNs;
            long rssNow = tickForkRssKb();
            fprintf(stderr,
                "[FORK-BENCH] tick=%u %s window=%.2fms quiesce=%.2fms fork()=%.3fms "
                "rss: before=%ldMB after=%ldMB cow_delta=%ldMB\n",
                (unsigned)system.tick, mismatch ? "MISMATCH" : "MATCH",
                windowNs / 1e6, gForkQuiesceNs / 1e6, gForkSyscallNs / 1e6,
                gForkRssBeforeKb / 1024, rssNow / 1024, (rssNow - gForkRssBeforeKb) / 1024);
            fflush(stderr);
        }

        if (!mismatch)
        {
            tickForkLog("verdict MATCH: commit shadow + kill child");
            gShadow.commit();
            kill(gChildPid.load(), SIGKILL);
            int st; waitpid(gChildPid.load(), &st, 0);
            close(gPipe[1]); gPipe[1] = -1;
            gChildPid = -2;
            return true;
        }

        // Hand off to the child donor and die without committing; the child reads pristine disk.
        // Discard our optimistic /s diverts first: the child forked BEFORE these page writes, so its
        // COW shadow bookkeeping is empty and its purgeOrphans cannot see them. Real page files were
        // never touched (writes diverted to /s), so removing /s is safe and prevents orphan buildup.
        tickForkLog("verdict MISMATCH: promote child + parent _exit");
        gShadow.discard();
        const char tag = 'P';
        ssize_t w = write(gPipe[1], &tag, 1);
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
    inline bool verdict(bool, const m256i&, unsigned long long) { return false; }
}

#endif // __linux__

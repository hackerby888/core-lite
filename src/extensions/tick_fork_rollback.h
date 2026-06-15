#pragma once

// Fork-on-BSP child-promote tick rollback (AUX wrong-solution path). Behind gRollbackMode==Fork;
// inert by default. Disk side: disk_shadow.h.
//
// The fork is taken on the BSP (main-loop thread) so the child inherits the thread that drives
// networking + contract dispatch; the child re-spawns only the simple AP loops on promotion.
//
// tickProcessor side (here): before processTick of a solution-bearing AUX tick it parks the request
// processors, arms the disk shadow, and asks the BSP to fork. At the quorum compare it issues the
// verdict: match -> commit shadow + kill the child; mismatch -> hand off to the child + _exit.
// BSP side (bspForkPoint / tickForkChildPromote) lives in qubic.cpp where spawnAPs is visible.

#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
#include <cstdio>

// Fork-path diagnostics: fprintf/stderr is fork-safe (no log-subsystem locks/buffers).
static inline void tickForkLog(const char* msg)
{
    fprintf(stderr, "[FORK] %s (pid=%d tick=%u)\n", msg, (int)getpid(), (unsigned)system.tick);
    fflush(stderr);
}

namespace tickFork
{
    inline volatile bool gForkRequest = false;  // tickProcessor -> BSP: fork now
    inline volatile pid_t gChildPid = -2;       // BSP -> tickProcessor: child pid (>=0) / -1 fail / -2 idle
    inline int gPipe[2] = { -1, -1 };           // verdict channel: parent writes [1], child reads [0]
    inline volatile bool gIsForkChild = false;  // set in the promoted child

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
        if (gRollbackMode != RollbackMode::Fork) return;
        if (isMainMode()) return;
        if (!tickHasSolution(system.tick)) return;

        tickForkLog("solution tick -> request BSP fork");
        gForkParked.store(0, std::memory_order_release);
        gForkQuiesceRequest = true;
        while (gForkParked.load(std::memory_order_acquire) < nRequestProcessorIDs)
            std::this_thread::yield();

        if (pipe(gPipe) != 0) { gForkQuiesceRequest = false; return; }
        gShadow.arm();                              // parent disk writes -> shadow; child resets on promote
        gChildPid = -2;
        gForkRequest = true;                        // BSP forks at its loop-top
        while (gChildPid == -2) std::this_thread::yield();

        if (gChildPid < 0)                          // fork failed
        {
            gShadow.discard();
            close(gPipe[0]); close(gPipe[1]); gPipe[0] = gPipe[1] = -1;
            gForkQuiesceRequest = false;
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

        if (!mismatch)
        {
            tickForkLog("verdict MATCH: commit shadow + kill child");
            gShadow.commit();
            kill(gChildPid, SIGKILL);
            int st; waitpid(gChildPid, &st, 0);
            close(gPipe[1]); gPipe[1] = -1;
            gChildPid = -2;
            return true;
        }

        // Hand off to the child donor and die without committing; the child reads pristine disk.
        tickForkLog("verdict MISMATCH: promote child + parent _exit");
        const char tag = 'P';
        ssize_t w = write(gPipe[1], &tag, 1);
        (void)w;
        _exit(0);
    }
}

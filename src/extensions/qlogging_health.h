#pragma once

// Per-tick self-check of the on-chain logging read paths (mapTxToLogId /
// mapLogIdToBufferIndex / logBuffer). Detects the "log_range all 0s" and
// "logId mismatch / bad bytes" corruption seen by log readers; counts surface
// on /v1/logging-health and the main status line.

#include <atomic>

namespace Qlogging
{
static bool gEnabled = true;

// Bound per-tick getBlobInfo calls; this runs on the tick critical path.
static constexpr unsigned int CALL_BUDGET = 128;

// kind: 1=zero-range 2=logId-mismatch 3=bad-bytes
static std::atomic<unsigned long long> gTicksChecked{0};
static std::atomic<unsigned long long> gZeroRangeTicks{0};
static std::atomic<unsigned long long> gLogIdMismatch{0};
static std::atomic<unsigned long long> gBadLogBytes{0};
static std::atomic<unsigned int> gLastBadTick{0};
static std::atomic<unsigned int> gLastBadKind{0};

static void markBad(unsigned int tick, unsigned int kind)
{
    gLastBadTick.store(tick, std::memory_order_relaxed);
    gLastBadKind.store(kind, std::memory_order_relaxed);
}

#if ENABLED_LOGGING
// Resolve one logId the way a RequestLog response would, then cross-check the
// stored header against the tick that claims it. kind out: 2 mismatch, 3 bad bytes.
static bool checkLogId(unsigned long long id, unsigned int tick, unsigned int& outKind)
{
    qLogger::BlobInfo bi = logger.logBuf.getBlobInfo(id); // runs verifyLog (logId + size)
    if (bi.startIndex == -1 && bi.length == -1)
    {
        outKind = 2;
        return false;
    }
    char hdr[LOG_HEADER_SIZE];
    setMem(hdr, sizeof(hdr), 0);
    logger.logBuf.getMany(hdr, bi.startIndex, LOG_HEADER_SIZE);
    const unsigned short hEpoch = *((unsigned short*)hdr);
    const unsigned int hTick = *((unsigned int*)(hdr + 2));
    const unsigned long long hSize = qLogger::getLogSize(hdr);
    if (qLogger::getLogId(hdr) != id
        || hTick != tick
        || hEpoch != system.epoch
        || bi.length <= 0
        || (long long)(hSize + LOG_HEADER_SIZE) != bi.length
        || hSize + LOG_HEADER_SIZE >= RequestResponseHeader::max_size)
    {
        outKind = 3;
        return false;
    }
    return true;
}

// Revalidate the logs this tick just committed. Call right after
// logger.updateTick(T), in the same tick-processor thread that ran _commit,
// so it reads its own freshly-committed state with no race.
static void validateTick(unsigned int T)
{
    if (!gEnabled) return;
    if (T < logger.tickBegin || T > logger.lastUpdatedTick) return;
    gTicksChecked.fetch_add(1, std::memory_order_relaxed);

    qLogger::TickBlobInfo tb;
    logger.tx.getTickLogIdInfo(&tb, T);

    bool allZero = true;
    bool zeroSeen = false;
    unsigned int calls = 0;
    for (int i = 0; i < LOG_TX_PER_TICK; i++)
    {
        const long long from = tb.fromLogId[i];
        const long long len = tb.length[i];
        if (!(from == 0 && len == 0)) allZero = false;

        // length 0 is never legitimate (addLogId sets >=1; empty slot is -1).
        if (len == 0) { zeroSeen = true; continue; }
        if (from < 0 || len < 0) continue; // empty / default

        if (calls < CALL_BUDGET)
        {
            unsigned int kind = 0;
            if (!checkLogId((unsigned long long)from, T, kind))
            {
                if (kind == 2) gLogIdMismatch.fetch_add(1, std::memory_order_relaxed);
                else gBadLogBytes.fetch_add(1, std::memory_order_relaxed);
                markBad(T, kind);
            }
            calls++;
        }
        if (len > 1 && calls < CALL_BUDGET)
        {
            unsigned int kind = 0;
            if (!checkLogId((unsigned long long)(from + len - 1), T, kind))
            {
                if (kind == 2) gLogIdMismatch.fetch_add(1, std::memory_order_relaxed);
                else gBadLogBytes.fetch_add(1, std::memory_order_relaxed);
                markBad(T, kind);
            }
            calls++;
        }
    }

    // A real empty tick reads back all -1/-1; every slot 0/0 means the VM page
    // returned zeros instead of the committed ranges -- the "all 0s" report.
    if (allZero) zeroSeen = true;
    if (zeroSeen)
    {
        gZeroRangeTicks.fetch_add(1, std::memory_order_relaxed);
        markBad(T, 1);
    }
}
#else
static inline void validateTick(unsigned int) {}
#endif

static const char* kindName(unsigned int kind)
{
    switch (kind)
    {
    case 1: return "zero-range";
    case 2: return "logId-mismatch";
    case 3: return "bad-bytes";
    default: return "none";
    }
}

static void appendStatus(CHAR16* message)
{
    if (!gEnabled) return;
    const unsigned long long zr = gZeroRangeTicks.load(std::memory_order_relaxed);
    const unsigned long long mm = gLogIdMismatch.load(std::memory_order_relaxed);
    const unsigned long long by = gBadLogBytes.load(std::memory_order_relaxed);
    if (zr == 0 && mm == 0 && by == 0)
    {
        appendText(message, L" LogHC ok");
        return;
    }
    appendText(message, L" LogHC zr=");
    appendNumber(message, zr, TRUE);
    appendText(message, L" mm=");
    appendNumber(message, mm, TRUE);
    appendText(message, L" by=");
    appendNumber(message, by, TRUE);
    appendText(message, L" @");
    appendNumber(message, gLastBadTick.load(std::memory_order_relaxed), FALSE);
}
}

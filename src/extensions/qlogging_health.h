#pragma once

// Integrity self-check of the on-chain logging read paths (mapTxToLogId /
// mapLogIdToBufferIndex / logBuffer). Runs at two points: per-tick right after
// commit, and again on the request handlers right before data is returned to a
// peer (catches corruption that was valid at processTick but surfaces malformed
// on a later request). Detects the "log_range all 0s" and "logId mismatch /
// bad bytes" reports; counts surface on /v1/logging-health and the status line.

#include <atomic>

namespace Qlogging
{
static bool gEnabled = true;

// Bound getBlobInfo calls per scan (tick critical path / serve path).
static constexpr unsigned int CALL_BUDGET = 128;

// kind: 1=zero-range 2=logId-mismatch 3=bad-bytes
static std::atomic<unsigned long long> gTicksChecked{0};
static std::atomic<unsigned long long> gZeroRangeTicks{0};
static std::atomic<unsigned long long> gLogIdMismatch{0};
static std::atomic<unsigned long long> gBadLogBytes{0};
static std::atomic<unsigned int> gLastBadTick{0};
static std::atomic<unsigned int> gLastBadKind{0};

// Serve-path counters: anomalies in data we were about to return to a peer.
static std::atomic<unsigned long long> gServeChecks{0};
static std::atomic<unsigned long long> gServeZeroRange{0};
static std::atomic<unsigned long long> gServeLogIdMismatch{0};
static std::atomic<unsigned long long> gServeBadBytes{0};
static std::atomic<unsigned int> gLastServeBadTick{0};

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

struct ScanResult { bool zero; unsigned int mismatch; unsigned int badBytes; };

// Inspect a tick's per-tx ranges (the RespondAllLogIdRangesFromTick / TickBlobInfo
// shape): all-zero page, length==0 slots, and per-id header cross-check.
static ScanResult scanRanges(const long long* fromLogId, const long long* length, unsigned int T)
{
    ScanResult r{ false, 0, 0 };
    bool allZero = true;
    unsigned int calls = 0;
    for (int i = 0; i < LOG_TX_PER_TICK; i++)
    {
        const long long from = fromLogId[i];
        const long long len = length[i];
        if (!(from == 0 && len == 0)) allZero = false;

        // length 0 is never legitimate (addLogId sets >=1; empty slot is -1).
        if (len == 0) { r.zero = true; continue; }
        if (from < 0 || len < 0) continue; // empty / default

        if (calls < CALL_BUDGET)
        {
            unsigned int kind = 0;
            if (!checkLogId((unsigned long long)from, T, kind)) { if (kind == 2) r.mismatch++; else r.badBytes++; }
            calls++;
        }
        if (len > 1 && calls < CALL_BUDGET)
        {
            unsigned int kind = 0;
            if (!checkLogId((unsigned long long)(from + len - 1), T, kind)) { if (kind == 2) r.mismatch++; else r.badBytes++; }
            calls++;
        }
    }
    // A real empty tick reads back all -1/-1; every slot 0/0 means the page
    // returned zeros instead of the committed ranges -- the "all 0s" report.
    if (allZero) r.zero = true;
    return r;
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

    ScanResult r = scanRanges(tb.fromLogId, tb.length, T);
    if (r.mismatch) { gLogIdMismatch.fetch_add(r.mismatch, std::memory_order_relaxed); markBad(T, 2); }
    if (r.badBytes) { gBadLogBytes.fetch_add(r.badBytes, std::memory_order_relaxed); markBad(T, 3); }
    if (r.zero) { gZeroRangeTicks.fetch_add(1, std::memory_order_relaxed); markBad(T, 1); }
}

// Serve-path hooks: validate the bytes/ranges a log request is about to return.
// Run on request-processor threads; only read the VM (as the handler already
// does) + bump atomics, so they are reentrant and lock-free.
static void onServeTickRanges(unsigned int T, const qLogger::TickBlobInfo* resp)
{
    if (!gEnabled) return;
    gServeChecks.fetch_add(1, std::memory_order_relaxed);
    ScanResult r = scanRanges(resp->fromLogId, resp->length, T);
    if (r.mismatch) gServeLogIdMismatch.fetch_add(r.mismatch, std::memory_order_relaxed);
    if (r.badBytes) gServeBadBytes.fetch_add(r.badBytes, std::memory_order_relaxed);
    if (r.zero) gServeZeroRange.fetch_add(1, std::memory_order_relaxed);
    if (r.zero || r.mismatch || r.badBytes) gLastServeBadTick.store(T, std::memory_order_relaxed);
}

static void onServeTxRange(unsigned int T, long long fromLogId, long long length)
{
    if (!gEnabled) return;
    gServeChecks.fetch_add(1, std::memory_order_relaxed);
    if (length == 0) { gServeZeroRange.fetch_add(1, std::memory_order_relaxed); gLastServeBadTick.store(T, std::memory_order_relaxed); return; }
    if (fromLogId < 0 || length < 0) return; // -1 empty / -3 not generated
    bool bad = false;
    unsigned int kind = 0;
    if (!checkLogId((unsigned long long)fromLogId, T, kind))
    { if (kind == 2) gServeLogIdMismatch.fetch_add(1, std::memory_order_relaxed); else gServeBadBytes.fetch_add(1, std::memory_order_relaxed); bad = true; }
    if (length > 1 && !checkLogId((unsigned long long)(fromLogId + length - 1), T, kind))
    { if (kind == 2) gServeLogIdMismatch.fetch_add(1, std::memory_order_relaxed); else gServeBadBytes.fetch_add(1, std::memory_order_relaxed); bad = true; }
    if (bad) gLastServeBadTick.store(T, std::memory_order_relaxed);
}

// Walk the exact buffer assembled for a RequestLog response: each entry's header
// logId must run fromID..toID and its size must fit. Catches a torn/garbled
// logBuffer page that the per-tick check already passed at commit time.
static void onServeLogBytes(unsigned long long fromID, unsigned long long toID, const char* buf, long long len)
{
    if (!gEnabled) return;
    gServeChecks.fetch_add(1, std::memory_order_relaxed);
    long long off = 0;
    unsigned long long expect = fromID;
    bool bad = false;
    while (off + (long long)LOG_HEADER_SIZE <= len)
    {
        const char* h = buf + off;
        const unsigned long long hId = qLogger::getLogId(h);
        const unsigned long long hSize = qLogger::getLogSize(h);
        const unsigned long long entry = (unsigned long long)LOG_HEADER_SIZE + hSize;
        if (hId != expect
            || hSize + LOG_HEADER_SIZE >= RequestResponseHeader::max_size
            || off + (long long)entry > len)
        { bad = true; break; }
        off += (long long)entry;
        expect++;
    }
    if (!bad && (off != len || expect != toID + 1)) bad = true;
    if (bad) gServeBadBytes.fetch_add(1, std::memory_order_relaxed);
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
    const unsigned long long sv = gServeZeroRange.load(std::memory_order_relaxed)
        + gServeLogIdMismatch.load(std::memory_order_relaxed)
        + gServeBadBytes.load(std::memory_order_relaxed);
    if (zr == 0 && mm == 0 && by == 0 && sv == 0)
    {
        appendText(message, L" LogHC ok");
        return;
    }
    unsigned int badTick = gLastBadTick.load(std::memory_order_relaxed);
    const unsigned int serveTick = gLastServeBadTick.load(std::memory_order_relaxed);
    if (serveTick > badTick) badTick = serveTick;
    appendText(message, L" LogHC zr=");
    appendNumber(message, zr, TRUE);
    appendText(message, L" mm=");
    appendNumber(message, mm, TRUE);
    appendText(message, L" by=");
    appendNumber(message, by, TRUE);
    appendText(message, L" sv=");
    appendNumber(message, sv, TRUE);
    appendText(message, L" @");
    appendNumber(message, badTick, FALSE);
}
}

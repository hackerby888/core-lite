#pragma once

// Integrity self-check of the on-chain logging read paths (mapTxToLogId /
// mapLogIdToBufferIndex / logBuffer). Runs at two points: per-tick right after
// commit, and again on the request handlers right before data is returned to a
// peer (catches corruption that was valid at processTick but surfaces malformed
// on a later request). Detects the "log_range all 0s" and "logId mismatch /
// bad bytes" reports; counts surface on /v1/logging-health and the status line.

#include <atomic>
#include <cstdio>

namespace Qlogging
{
static bool gEnabled = false;

// Every distinct malformed tick is kept here for later investigation: a bounded
// in-memory ring (queried via /v1/logging-bad-ticks) plus an append to an on-disk
// file in the node working dir so the record survives a restart.
static constexpr unsigned int BAD_RING = 4096;
static const char* BAD_TICK_FILE = "logging_health_bad_ticks.log";
struct BadTickRec { unsigned int tick; unsigned int kindMask; unsigned int sourceMask; unsigned long long seq; };
static BadTickRec gBadTicks[BAD_RING];
static std::atomic<unsigned int> gBadStored{0};   // distinct ticks held in the ring
static std::atomic<unsigned long long> gBadEvents{0}; // total flag events (incl. repeats)
static std::atomic<unsigned int> gBadDropped{0};  // distinct ticks beyond ring capacity
static std::atomic_flag gBadLock = ATOMIC_FLAG_INIT;

// Bound getBlobInfo calls per scan (tick critical path / serve path).
static constexpr unsigned int CALL_BUDGET = 128;

// kind: 1=zero-range 2=logId-mismatch 3=bad-bytes 4=duplicate-logId 5=logId-gap
static std::atomic<unsigned long long> gTicksChecked{0};
static std::atomic<unsigned long long> gZeroRangeTicks{0};
static std::atomic<unsigned long long> gLogIdMismatch{0};
static std::atomic<unsigned long long> gBadLogBytes{0};
static std::atomic<unsigned long long> gLogIdDup{0};
static std::atomic<unsigned long long> gLogIdGap{0};
static std::atomic<unsigned int> gLastBadTick{0};
static std::atomic<unsigned int> gLastBadKind{0};

// Cross-tick cursor: the logId the next committed tick must start at (global ids
// are dense/sequential). Ordered with the per-tick commit; atomic for visibility.
static std::atomic<unsigned long long> gExpectedNextLogId{0};
static std::atomic<unsigned int> gSeqLastTick{0};

// Serve-path counters: anomalies in data we were about to return to a peer.
static std::atomic<unsigned long long> gServeChecks{0};
static std::atomic<unsigned long long> gServeZeroRange{0};
static std::atomic<unsigned long long> gServeLogIdMismatch{0};
static std::atomic<unsigned long long> gServeBadBytes{0};
static std::atomic<unsigned long long> gServeLogIdDup{0};
static std::atomic<unsigned long long> gServeLogIdGap{0};
static std::atomic<unsigned int> gLastServeBadTick{0};

static void markBad(unsigned int tick, unsigned int kind)
{
    gLastBadTick.store(tick, std::memory_order_relaxed);
    gLastBadKind.store(kind, std::memory_order_relaxed);
}

static void badLock() { while (gBadLock.test_and_set(std::memory_order_acquire)) {} }
static void badUnlock() { gBadLock.clear(std::memory_order_release); }

// Append a newly-seen malformed tick to the on-disk record (called under gBadLock,
// once per distinct tick). Best-effort: a failed open just skips persistence.
static void appendBadTickFile(const BadTickRec& rec)
{
    FILE* f = fopen(BAD_TICK_FILE, "a");
    if (!f) return;
    const char* src = (rec.sourceMask == 3) ? "both" : ((rec.sourceMask & 2) ? "serve" : "commit");
    fprintf(f, "tick=%u kindMask=0x%02x source=%s epoch=%u seq=%llu\n",
            rec.tick, rec.kindMask, src, (unsigned)system.epoch, rec.seq);
    fclose(f);
}

// Record a malformed tick. Dedups by tick (accumulating the kind/source bitmask)
// so the ring holds every distinct bad tick. Thread-safe (tick + serve threads).
static void recordBad(unsigned int tick, unsigned int kind, bool serve)
{
    const unsigned long long ev = gBadEvents.fetch_add(1, std::memory_order_relaxed) + 1;
    const unsigned int kbit = 1u << kind;
    const unsigned int sbit = serve ? 2u : 1u;
    badLock();
    unsigned int n = gBadStored.load(std::memory_order_relaxed);
    for (unsigned int i = 0; i < n; i++)
    {
        if (gBadTicks[i].tick == tick)
        {
            gBadTicks[i].kindMask |= kbit;
            gBadTicks[i].sourceMask |= sbit;
            badUnlock();
            return;
        }
    }
    if (n < BAD_RING)
    {
        gBadTicks[n] = BadTickRec{ tick, kbit, sbit, ev };
        appendBadTickFile(gBadTicks[n]);
        gBadStored.store(n + 1, std::memory_order_relaxed);
    }
    else
    {
        gBadDropped.fetch_add(1, std::memory_order_relaxed);
    }
    badUnlock();
}

static void flagCommit(unsigned int tick, unsigned int kind) { markBad(tick, kind); recordBad(tick, kind, false); }
static void flagServe(unsigned int tick, unsigned int kind) { gLastServeBadTick.store(tick, std::memory_order_relaxed); recordBad(tick, kind, true); }

// Snapshot the bad-tick ring into out[0..max) for the HTTP reader. Returns count copied.
static unsigned int copyBadTicks(BadTickRec* out, unsigned int max)
{
    badLock();
    unsigned int n = gBadStored.load(std::memory_order_relaxed);
    if (n > max) n = max;
    for (unsigned int i = 0; i < n; i++) out[i] = gBadTicks[i];
    badUnlock();
    return n;
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

struct ScanResult { bool zero; bool dup; bool gap; unsigned int mismatch; unsigned int badBytes; long long minId; long long maxEnd; };

// Inspect a tick's per-tx ranges (the RespondAllLogIdRangesFromTick / TickBlobInfo
// shape): all-zero page, length==0 slots, per-id header cross-check, and that the
// non-empty ranges tile a contiguous logId span with no overlap (a logId in two
// slots) or gap -- Sum(length) must equal (maxEnd - minId).
static ScanResult scanRanges(const long long* fromLogId, const long long* length, unsigned int T)
{
    ScanResult r{ false, false, false, 0, 0, -1, -1 };
    bool allZero = true;
    unsigned int calls = 0;
    unsigned long long sumLen = 0;
    for (int i = 0; i < LOG_TX_PER_TICK; i++)
    {
        const long long from = fromLogId[i];
        const long long len = length[i];
        if (!(from == 0 && len == 0)) allZero = false;

        // length 0 is never legitimate (addLogId sets >=1; empty slot is -1).
        if (len == 0) { r.zero = true; continue; }
        if (from < 0 || len < 0) continue; // empty / default

        sumLen += (unsigned long long)len;
        if (r.minId < 0 || from < r.minId) r.minId = from;
        if (from + len > r.maxEnd) r.maxEnd = from + len;

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
    // Valid ranges densely tile [minId, maxEnd): Sum(length) == span. More than
    // the span means a logId is claimed twice (overlap); less means the ids are
    // not linear -- a gap inside the tick.
    if (r.minId >= 0)
    {
        const unsigned long long span = (unsigned long long)(r.maxEnd - r.minId);
        if (sumLen > span) r.dup = true;
        else if (sumLen < span) r.gap = true;
    }
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
    if (r.mismatch) { gLogIdMismatch.fetch_add(r.mismatch, std::memory_order_relaxed); flagCommit(T, 2); }
    if (r.badBytes) { gBadLogBytes.fetch_add(r.badBytes, std::memory_order_relaxed); flagCommit(T, 3); }
    if (r.zero) { gZeroRangeTicks.fetch_add(1, std::memory_order_relaxed); flagCommit(T, 1); }
    if (r.dup) { gLogIdDup.fetch_add(1, std::memory_order_relaxed); flagCommit(T, 4); }
    if (r.gap) { gLogIdGap.fetch_add(1, std::memory_order_relaxed); flagCommit(T, 5); }

    // Cross-tick uniqueness + linearity: the global logId space is dense, so this
    // tick must begin exactly where the previous tick's ids ended. A lower start
    // means a logId was reused (overlap); a higher start means the sequence is not
    // linear -- a gap. Only compare when we checked T-1 right before (so the cursor
    // is trustworthy) and not at an epoch reset where logId restarts at 0; otherwise
    // just seed the cursor. (Calls are ordered by updateTick, one per tick; atomics
    // carry the value across processor threads.)
    const unsigned int prevTick = gSeqLastTick.load(std::memory_order_relaxed);
    const bool contiguous = (prevTick != 0) && (T == prevTick + 1) && (T != logger.tickBegin);
    unsigned long long expected = gExpectedNextLogId.load(std::memory_order_relaxed);
    if (r.minId >= 0)
    {
        if (contiguous)
        {
            if ((unsigned long long)r.minId < expected) { gLogIdDup.fetch_add(1, std::memory_order_relaxed); flagCommit(T, 4); }
            else if ((unsigned long long)r.minId > expected) { gLogIdGap.fetch_add(1, std::memory_order_relaxed); flagCommit(T, 5); }
        }
        expected = (unsigned long long)r.maxEnd;
    }
    gExpectedNextLogId.store(expected, std::memory_order_relaxed);
    gSeqLastTick.store(T, std::memory_order_relaxed);
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
    if (r.dup) gServeLogIdDup.fetch_add(1, std::memory_order_relaxed);
    if (r.gap) gServeLogIdGap.fetch_add(1, std::memory_order_relaxed);
    if (r.zero) flagServe(T, 1);
    if (r.mismatch) flagServe(T, 2);
    if (r.badBytes) flagServe(T, 3);
    if (r.dup) flagServe(T, 4);
    if (r.gap) flagServe(T, 5);
}

static void onServeTxRange(unsigned int T, long long fromLogId, long long length)
{
    if (!gEnabled) return;
    gServeChecks.fetch_add(1, std::memory_order_relaxed);
    if (length == 0) { gServeZeroRange.fetch_add(1, std::memory_order_relaxed); flagServe(T, 1); return; }
    if (fromLogId < 0 || length < 0) return; // -1 empty / -3 not generated
    unsigned int kind = 0;
    if (!checkLogId((unsigned long long)fromLogId, T, kind))
    { if (kind == 2) gServeLogIdMismatch.fetch_add(1, std::memory_order_relaxed); else gServeBadBytes.fetch_add(1, std::memory_order_relaxed); flagServe(T, kind); }
    if (length > 1 && !checkLogId((unsigned long long)(fromLogId + length - 1), T, kind))
    { if (kind == 2) gServeLogIdMismatch.fetch_add(1, std::memory_order_relaxed); else gServeBadBytes.fetch_add(1, std::memory_order_relaxed); flagServe(T, kind); }
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
    case 4: return "duplicate-logId";
    case 5: return "logId-gap";
    default: return "none";
    }
}

static void appendStatus(CHAR16* message)
{
    if (!gEnabled) return;
    const unsigned long long zr = gZeroRangeTicks.load(std::memory_order_relaxed);
    const unsigned long long mm = gLogIdMismatch.load(std::memory_order_relaxed);
    const unsigned long long by = gBadLogBytes.load(std::memory_order_relaxed);
    const unsigned long long dp = gLogIdDup.load(std::memory_order_relaxed);
    const unsigned long long gp = gLogIdGap.load(std::memory_order_relaxed);
    const unsigned long long sv = gServeZeroRange.load(std::memory_order_relaxed)
        + gServeLogIdMismatch.load(std::memory_order_relaxed)
        + gServeBadBytes.load(std::memory_order_relaxed)
        + gServeLogIdDup.load(std::memory_order_relaxed)
        + gServeLogIdGap.load(std::memory_order_relaxed);
    if (zr == 0 && mm == 0 && by == 0 && dp == 0 && gp == 0 && sv == 0)
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
    appendText(message, L" dp=");
    appendNumber(message, dp, TRUE);
    appendText(message, L" gp=");
    appendNumber(message, gp, TRUE);
    appendText(message, L" sv=");
    appendNumber(message, sv, TRUE);
    appendText(message, L" @");
    appendNumber(message, badTick, FALSE);
}
}

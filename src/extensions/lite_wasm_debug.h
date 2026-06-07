#pragma once
// WASM contract debug trace (testnet dev tool). A ring of per-call records — input/output heads, a
// state-before/after snapshot (diff), the QPI host-calls the contract made, the trap reason, and exec
// time — captured by liteWasmDispatch ONLY when the runtime toggle is on (off by default => a single
// atomic-bool check, zero overhead). Exposed via the dyn debug-trace RPC; browsed by `qinit debug`.
// Per-call entry is built thread-local on the dispatch stack (host-calls append same-thread, no lock);
// only the final commit copies it into the shared ring under a spinlock. Gated LITE_DYNAMIC_CONTRACTS.
#ifdef LITE_DYNAMIC_CONTRACTS
#include <atomic>
#include <string>
#include <vector>
#include <algorithm>

#ifndef LITE_WASM_TRACE_RING
#define LITE_WASM_TRACE_RING  256u           // entries kept
#endif
#ifndef LITE_WASM_TRACE_HEAD
#define LITE_WASM_TRACE_HEAD  256u           // bytes of input/output captured
#endif
#ifndef LITE_WASM_TRACE_STATE
#define LITE_WASM_TRACE_STATE 4096u          // bytes of state-before/after captured (diff cap)
#endif

struct LiteWasmHostCall { const char* name; std::string detail; };   // a side-effect QPI call the contract made

struct LiteWasmTraceEntry {
    unsigned long long seq = 0;              // monotonic; client polls with ?since=<seq>
    unsigned int  tick = 0, idx = 0;
    unsigned short it = 0; unsigned char kind = 0;   // 0 fn / 1 proc / 2 sysproc
    bool ok = true, used = false;
    m256i invocator = m256i::zero();         // caller (procedures only)
    long long invocationReward = 0;
    unsigned int inSize = 0, outSize = 0, stateSize = 0;
    bool stateTruncated = false;             // state larger than the snapshot cap
    unsigned char inHead[LITE_WASM_TRACE_HEAD] = {};
    unsigned char outHead[LITE_WASM_TRACE_HEAD] = {};
    unsigned char stateBefore[LITE_WASM_TRACE_STATE] = {};
    unsigned char stateAfter[LITE_WASM_TRACE_STATE] = {};
    unsigned long long execNs = 0;
    std::string trap;
    std::vector<LiteWasmHostCall> hostCalls;
};

static std::atomic<bool> g_liteWasmDebug{ false };
static inline bool liteWasmDebugEnabled() { return g_liteWasmDebug.load(std::memory_order_relaxed); }

static LiteWasmTraceEntry g_liteWasmTrace[LITE_WASM_TRACE_RING];
static volatile long      g_liteWasmTraceLock = 0;
static unsigned int       g_liteWasmTraceHead = 0;
static unsigned long long g_liteWasmTraceSeq  = 0;

static inline void liteWasmTraceAcquire() { while (__sync_lock_test_and_set(&g_liteWasmTraceLock, 1)) {} }
static inline void liteWasmTraceRelease() { __sync_lock_release(&g_liteWasmTraceLock); }

// Append a side-effect to the call's (thread-local) entry. Same thread as dispatch -> no lock. Capped.
static inline void liteWasmTraceHostCall(LiteWasmTraceEntry* e, const char* name, const std::string& detail) {
    if (e && e->hostCalls.size() < 64) e->hostCalls.push_back({ name, detail });
}

// Publish a finished entry into the shared ring (copy under the spinlock; assigns the seq).
static inline void liteWasmTraceCommit(LiteWasmTraceEntry& te) {
    liteWasmTraceAcquire();
    te.seq = ++g_liteWasmTraceSeq; te.used = true;
    g_liteWasmTrace[g_liteWasmTraceHead % LITE_WASM_TRACE_RING] = te;
    g_liteWasmTraceHead++;
    liteWasmTraceRelease();
}

// Snapshot recent entries (seq > since), oldest-first, at most `limit`. For the debug-trace RPC.
static inline std::vector<LiteWasmTraceEntry> liteWasmTraceSnapshot(unsigned long long since, unsigned int limit) {
    std::vector<LiteWasmTraceEntry> out;
    liteWasmTraceAcquire();
    for (unsigned int i = 0; i < LITE_WASM_TRACE_RING; i++)
        if (g_liteWasmTrace[i].used && g_liteWasmTrace[i].seq > since) out.push_back(g_liteWasmTrace[i]);
    liteWasmTraceRelease();
    std::sort(out.begin(), out.end(), [](const LiteWasmTraceEntry& a, const LiteWasmTraceEntry& b) { return a.seq < b.seq; });
    if (out.size() > limit) out.erase(out.begin(), out.end() - limit);
    return out;
}

static inline void liteWasmTraceClear() {
    liteWasmTraceAcquire();
    for (unsigned int i = 0; i < LITE_WASM_TRACE_RING; i++) g_liteWasmTrace[i].used = false;
    g_liteWasmTraceHead = 0;
    liteWasmTraceRelease();
}

// hex of the first n bytes of p (for trace details / RPC).
static inline std::string liteWasmHex(const void* p, unsigned int n) {
    if (!p) return "null";
    static const char* h = "0123456789abcdef";
    const unsigned char* b = (const unsigned char*)p; std::string s; s.reserve(n * 2);
    for (unsigned int i = 0; i < n; i++) { s += h[b[i] >> 4]; s += h[b[i] & 15]; }
    return s;
}
#endif // LITE_DYNAMIC_CONTRACTS

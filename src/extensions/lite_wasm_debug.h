#pragma once
// WASM contract debug trace (testnet dev tool). A ring of per-call records — input/output heads, the
// FULL state diff (only the changed bytes), the QPI host-calls, the trap reason, exec time — captured by
// liteWasmDispatch ONLY when the runtime toggle is on (off by default => a single atomic-bool check, zero
// overhead). Exposed via the dyn debug-trace RPC; browsed by `qinit debug`.
//
// Full-state diff WITHOUT a shadow copy: writes are single-threaded (always processTick), so for a write
// call we mprotect the contract's state region read-only, let the contract fault on each written page (a
// SIGSEGV handler saves the page's pre-write bytes once + unprotects it), then diff saved-before vs current
// for the dirtied pages. RAM = one fixed ~16MB before-page pool (reused), not O(stateSize). The handler is
// installed on enable (saving the prior boost-stacktrace handler) and chains to it for non-state faults;
// it's inert when no write call is tracking. Gated LITE_DYNAMIC_CONTRACTS (testnet only).
#ifdef LITE_DYNAMIC_CONTRACTS
#include <atomic>
#include <string>
#include <vector>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <signal.h>
#include <unistd.h>
#include <sys/mman.h>

#ifndef LITE_WASM_TRACE_RING
#define LITE_WASM_TRACE_RING  256u           // entries kept
#endif
#ifndef LITE_WASM_TRACE_HEAD
#define LITE_WASM_TRACE_HEAD  256u           // bytes of input/output captured
#endif
#ifndef LITE_WASM_DIRTY_MAX
#define LITE_WASM_DIRTY_MAX   4096u          // pages tracked per call (= 16MB change cap @4K pages)
#endif

struct LiteWasmHostCall { const char* name; std::string detail; };           // a side-effect QPI call
struct LiteWasmStateRegion { unsigned int off; std::string before, after; }; // a changed byte run in state

struct LiteWasmTraceEntry {
    unsigned long long seq = 0;
    unsigned int  tick = 0, idx = 0;
    unsigned short it = 0; unsigned char kind = 0;   // 0 fn / 1 proc / 2 sysproc
    bool ok = true, used = false;
    m256i invocator = m256i::zero();                 // caller (procedures only)
    long long invocationReward = 0;
    unsigned int inSize = 0, outSize = 0, stateSize = 0;
    bool stateTruncated = false;                      // diff hit the per-call change cap
    unsigned char inHead[LITE_WASM_TRACE_HEAD] = {};
    unsigned char outHead[LITE_WASM_TRACE_HEAD] = {};
    unsigned long long execNs = 0;
    std::string trap;
    std::vector<LiteWasmStateRegion> stateDiff;       // FULL-state diff: only the changed byte runs
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

static inline void liteWasmTraceHostCall(LiteWasmTraceEntry* e, const char* name, const std::string& detail) {
    if (e && e->hostCalls.size() < 64) e->hostCalls.push_back({ name, detail });
}
static inline void liteWasmTraceCommit(LiteWasmTraceEntry& te) {
    liteWasmTraceAcquire();
    te.seq = ++g_liteWasmTraceSeq; te.used = true;
    g_liteWasmTrace[g_liteWasmTraceHead % LITE_WASM_TRACE_RING] = te;
    g_liteWasmTraceHead++;
    liteWasmTraceRelease();
}
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

static inline std::string liteWasmHex(const void* p, unsigned int n) {
    if (!p) return "null";
    static const char* h = "0123456789abcdef";
    const unsigned char* b = (const unsigned char*)p; std::string s; s.reserve(n * 2);
    for (unsigned int i = 0; i < n; i++) { s += h[b[i] >> 4]; s += h[b[i] & 15]; }
    return s;
}

// ---- dirty-page state tracking (write calls are single-threaded -> thread_local, lock-free) -------------
static long g_liteWasmPageSize = 4096;
static struct sigaction g_liteWasmOldSegv;
static bool g_liteWasmSegvInstalled = false;
static thread_local bool           t_liteWasmDirtyActive = false;
static thread_local unsigned char* t_liteWasmProtLo = nullptr;
static thread_local unsigned char* t_liteWasmProtHi = nullptr;
static thread_local unsigned char* t_liteWasmDirtyBefore = nullptr;          // pool: LITE_WASM_DIRTY_MAX * pageSize
static thread_local unsigned char* t_liteWasmDirtyPages[LITE_WASM_DIRTY_MAX];
static thread_local unsigned int   t_liteWasmDirtyCount = 0;
static thread_local bool           t_liteWasmDirtyTrunc = false;

static void liteWasmSegv(int sig, siginfo_t* info, void* uc) {
    unsigned char* fa = (unsigned char*)info->si_addr;
    if (t_liteWasmDirtyActive && fa >= t_liteWasmProtLo && fa < t_liteWasmProtHi) {
        unsigned char* page = (unsigned char*)((uintptr_t)fa & ~(uintptr_t)(g_liteWasmPageSize - 1));
        if (t_liteWasmDirtyCount < LITE_WASM_DIRTY_MAX && t_liteWasmDirtyBefore) {   // save pre-write bytes once
            memcpy(t_liteWasmDirtyBefore + (size_t)t_liteWasmDirtyCount * g_liteWasmPageSize, page, g_liteWasmPageSize);
            t_liteWasmDirtyPages[t_liteWasmDirtyCount++] = page;
        } else t_liteWasmDirtyTrunc = true;
        mprotect(page, g_liteWasmPageSize, PROT_READ | PROT_WRITE);              // let the write proceed
        return;
    }
    // not a tracked state fault -> chain to the prior (boost stacktrace) handler
    if ((g_liteWasmOldSegv.sa_flags & SA_SIGINFO) && g_liteWasmOldSegv.sa_sigaction) { g_liteWasmOldSegv.sa_sigaction(sig, info, uc); return; }
    if (g_liteWasmOldSegv.sa_handler && g_liteWasmOldSegv.sa_handler != SIG_DFL && g_liteWasmOldSegv.sa_handler != SIG_IGN) { g_liteWasmOldSegv.sa_handler(sig); return; }
    signal(sig, SIG_DFL); raise(sig);
}

// install once on first enable (save the boot-time handler); never uninstalled (inert when not tracking).
static inline void liteWasmDebugSetEnabled(bool on) {
    if (on && !g_liteWasmSegvInstalled) {
        g_liteWasmPageSize = sysconf(_SC_PAGESIZE); if (g_liteWasmPageSize <= 0) g_liteWasmPageSize = 4096;
        struct sigaction sa; memset(&sa, 0, sizeof(sa));
        sa.sa_sigaction = liteWasmSegv; sa.sa_flags = SA_SIGINFO | SA_RESTART; sigemptyset(&sa.sa_mask);
        sigaction(SIGSEGV, &sa, &g_liteWasmOldSegv);
        g_liteWasmSegvInstalled = true;
    }
    g_liteWasmDebug.store(on, std::memory_order_relaxed);
}

// before a WRITE call: protect the state region RO so the contract's writes fault (= dirty-page capture).
static inline void liteWasmDirtyBegin(unsigned char* stateStart, unsigned int stateSize) {
    if (!t_liteWasmDirtyBefore) t_liteWasmDirtyBefore = (unsigned char*)malloc((size_t)LITE_WASM_DIRTY_MAX * g_liteWasmPageSize);
    if (!t_liteWasmDirtyBefore || !stateStart || !stateSize) return;
    t_liteWasmDirtyCount = 0; t_liteWasmDirtyTrunc = false;
    t_liteWasmProtLo = stateStart;   // page-aligned (g_wasmState is alignas(page) in lite_wasm_tu.h)
    t_liteWasmProtHi = (unsigned char*)(((uintptr_t)(stateStart + stateSize) + g_liteWasmPageSize - 1) & ~(uintptr_t)(g_liteWasmPageSize - 1));
    if (mprotect(t_liteWasmProtLo, t_liteWasmProtHi - t_liteWasmProtLo, PROT_READ) == 0) t_liteWasmDirtyActive = true;
}

// after the call: restore RW + build the changed-byte diff (clipped to the real state range) into the entry.
static inline void liteWasmDirtyEnd(LiteWasmTraceEntry& te, unsigned char* stateStart, unsigned int stateSize) {
    if (!t_liteWasmDirtyActive) return;
    t_liteWasmDirtyActive = false;
    mprotect(t_liteWasmProtLo, t_liteWasmProtHi - t_liteWasmProtLo, PROT_READ | PROT_WRITE);
    te.stateTruncated = t_liteWasmDirtyTrunc;
    unsigned char* sEnd = stateStart + stateSize;
    for (unsigned int i = 0; i < t_liteWasmDirtyCount && te.stateDiff.size() < 256; i++) {
        unsigned char* page = t_liteWasmDirtyPages[i];
        const unsigned char* before = t_liteWasmDirtyBefore + (size_t)i * g_liteWasmPageSize;
        unsigned char* lo = page > stateStart ? page : stateStart;            // clip page to [state, state+size)
        unsigned char* hi = (page + g_liteWasmPageSize) < sEnd ? (page + g_liteWasmPageSize) : sEnd;
        for (unsigned char* p = lo; p < hi;) {
            unsigned int bi = (unsigned int)(p - page);
            if (before[bi] != *p) {
                unsigned char* q = p;
                while (q < hi && before[(unsigned int)(q - page)] != *q) q++;
                te.stateDiff.push_back({ (unsigned int)(p - stateStart), liteWasmHex(before + bi, (unsigned int)(q - p)), liteWasmHex(p, (unsigned int)(q - p)) });
                p = q;
            } else p++;
        }
    }
}
#endif // LITE_DYNAMIC_CONTRACTS

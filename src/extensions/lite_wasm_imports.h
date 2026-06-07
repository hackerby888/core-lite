#pragma once
// Node-side wasm import table (WASM_CONTRACTS.md §13.5): one wasm import per LiteHostServices member, each
// forwarding to the host's g_liteHostServices vtable. ctx is bound per-call out-of-band
// (LiteWasmCallCtx via exec_env user_data, Stage-2 ABI #1); pointers cross as i32 linear-mem offsets,
// converted with addr_app_to_native (the general marshalling pattern). acquireScratch returns a linear-mem
// offset, not a native ptr (ABI #2). Module name = "lhost"; the contract side (lite_wasm_tu.h) imports it.
#ifdef LITE_WASM_CONTRACTS
#include "wasm_export.h"
#include "extensions/lite_wasm_debug.h"   // trace ring + liteWasmTraceHostCall (debug toggle, off by default)

// Per-call binding, set by liteWasmDispatch (step 3) before entering the contract: the QpiContext the QPI
// backends need, the contract's scratch arena (acquireScratch bump-allocates offsets within it), and the
// debug trace entry (non-null only while the debug toggle is on) so effectful imports record host-calls.
struct LiteWasmCallCtx {
    const void* ctx;        // QPI::QpiContext* (g_liteHostServices casts it back)
    uint32_t arenaBase;     // linear-mem offset of the per-call scratch arena
    uint32_t arenaBump;
    uint32_t arenaEnd;
    void* trace = nullptr;  // LiteWasmTraceEntry* for this call (debug); nullptr when debug off
};

static inline LiteWasmCallCtx* liteWasmCC(wasm_exec_env_t e) { return (LiteWasmCallCtx*)wasm_runtime_get_user_data(e); }
static inline void* liteWasmA2N(wasm_exec_env_t e, uint32_t off) {
    return off ? wasm_runtime_addr_app_to_native(wasm_runtime_get_module_inst(e), off) : nullptr;
}
#define LWC LiteWasmCallCtx* cc = liteWasmCC(e)
#define A2N(off) liteWasmA2N(e, (off))
// record a side-effect into the call's debug trace (no-op unless debug is on for this call)
#define LWTRACE(nm, det) do { if (cc && cc->trace) liteWasmTraceHostCall((LiteWasmTraceEntry*)cc->trace, nm, det); } while (0)

// --- infra (no ctx) ---
static void     w_beginFn(wasm_exec_env_t e, uint32_t id) { (void)e; g_liteHostServices.beginFn(id); }
static void     w_endFn(wasm_exec_env_t e, uint32_t id) { (void)e; g_liteHostServices.endFn(id); }
static void     w_markDirty(wasm_exec_env_t e, uint32_t ci) { (void)e; g_liteHostServices.markDirty(ci); }
static void     w_pauseLog(wasm_exec_env_t e) { (void)e; g_liteHostServices.pauseLog(); }
static void     w_resumeLog(wasm_exec_env_t e) { (void)e; g_liteHostServices.resumeLog(); }
static uint32_t w_acquireScratch(wasm_exec_env_t e, uint64_t size, uint32_t initZero) {
    LWC; uint32_t n = (uint32_t)((size + 7) & ~7ull);
    if (!cc || cc->arenaBump + n > cc->arenaEnd) {
        // arena exhausted: trap loudly (caught + logged by the dispatch trap handler) rather than return
        // offset 0, which the contract treats as a valid ptr and writes to — a silent, wrong result.
        wasm_runtime_set_exception(wasm_runtime_get_module_inst(e), "lhost: scratch arena exhausted");
        return 0;
    }
    uint32_t off = cc->arenaBump; cc->arenaBump += n;
    if (initZero) setMem(A2N(off), n, 0);
    return off;                                              // offset == ptr in wasm32 (ABI #2)
}
static void     w_releaseScratch(wasm_exec_env_t e, uint32_t off) { (void)e; (void)off; } // bump arena freed per call
static void     w_logBytes(wasm_exec_env_t e, uint32_t ci, uint32_t type, uint32_t msgOff, uint32_t size) {
    LWC; LWTRACE("log", "type=" + std::to_string(type) + " " + std::to_string(size) + "B");
    g_liteHostServices.logBytes(ci, (unsigned char)type, A2N(msgOff), size);
}
static void     w_k12(wasm_exec_env_t e, uint32_t inOff, uint32_t len, uint32_t outOff) {
    g_liteHostServices.k12(A2N(inOff), len, A2N(outOff));
}

// --- QPI backends (ctx bound via user_data) ---
static int64_t  w_transfer(wasm_exec_env_t e, uint32_t d, int64_t a) { LWC; LWTRACE("transfer", liteWasmHex(A2N(d), 8) + ".. " + std::to_string(a)); return g_liteHostServices.transfer(cc->ctx, A2N(d), a); }
static int64_t  w_transferTyped(wasm_exec_env_t e, uint32_t d, int64_t a, uint32_t t) { LWC; LWTRACE("transferTyped", liteWasmHex(A2N(d), 8) + ".. " + std::to_string(a) + " t=" + std::to_string(t)); return g_liteHostServices.transferTyped(cc->ctx, A2N(d), a, (unsigned char)t); }
static void     w_abort(wasm_exec_env_t e, uint32_t code) { LWC; LWTRACE("abort", std::to_string(code)); g_liteHostServices.abort(cc->ctx, code); }
static int64_t  w_burn(wasm_exec_env_t e, int64_t a, uint32_t idx) { LWC; LWTRACE("burn", std::to_string(a) + " for " + std::to_string(idx)); return g_liteHostServices.burn(cc->ctx, a, idx); }
static uint32_t w_epoch(wasm_exec_env_t e) { LWC; return g_liteHostServices.epoch(cc->ctx); }
static uint32_t w_tick(wasm_exec_env_t e) { LWC; return g_liteHostServices.tick(cc->ctx); }
static int32_t  w_numTickTx(wasm_exec_env_t e) { LWC; return g_liteHostServices.numberOfTickTransactions(cc->ctx); }
static uint32_t w_getEntity(wasm_exec_env_t e, uint32_t id, uint32_t out) { LWC; return g_liteHostServices.getEntity(cc->ctx, A2N(id), A2N(out)); }
static int64_t  w_queryFeeReserve(wasm_exec_env_t e, uint32_t ci) { LWC; return g_liteHostServices.queryFeeReserve(cc->ctx, ci); }
static void     w_nextId(wasm_exec_env_t e, uint32_t id, uint32_t out) { LWC; g_liteHostServices.nextId(cc->ctx, A2N(id), A2N(out)); }
static void     w_prevId(wasm_exec_env_t e, uint32_t id, uint32_t out) { LWC; g_liteHostServices.prevId(cc->ctx, A2N(id), A2N(out)); }
static uint32_t w_isContractId(wasm_exec_env_t e, uint32_t id) { LWC; return g_liteHostServices.isContractId(cc->ctx, A2N(id)); }
static void     w_arbitrator(wasm_exec_env_t e, uint32_t out) { LWC; g_liteHostServices.arbitrator(cc->ctx, A2N(out)); }
static void     w_computor(wasm_exec_env_t e, uint32_t idx, uint32_t out) { LWC; g_liteHostServices.computor(cc->ctx, (unsigned short)idx, A2N(out)); }
static uint32_t w_day(wasm_exec_env_t e) { LWC; return g_liteHostServices.day(cc->ctx); }
static uint32_t w_year(wasm_exec_env_t e) { LWC; return g_liteHostServices.year(cc->ctx); }
static uint32_t w_hour(wasm_exec_env_t e) { LWC; return g_liteHostServices.hour(cc->ctx); }
static uint32_t w_minute(wasm_exec_env_t e) { LWC; return g_liteHostServices.minute(cc->ctx); }
static uint32_t w_month(wasm_exec_env_t e) { LWC; return g_liteHostServices.month(cc->ctx); }
static uint32_t w_second(wasm_exec_env_t e) { LWC; return g_liteHostServices.second(cc->ctx); }
static uint32_t w_millisecond(wasm_exec_env_t e) { LWC; return g_liteHostServices.millisecond(cc->ctx); }
static void     w_now(wasm_exec_env_t e, uint32_t out) { LWC; g_liteHostServices.now(cc->ctx, A2N(out)); }
static void     w_prevSpectrumDigest(wasm_exec_env_t e, uint32_t out) { LWC; g_liteHostServices.prevSpectrumDigest(cc->ctx, A2N(out)); }
static void     w_prevUniverseDigest(wasm_exec_env_t e, uint32_t out) { LWC; g_liteHostServices.prevUniverseDigest(cc->ctx, A2N(out)); }
static void     w_prevComputerDigest(wasm_exec_env_t e, uint32_t out) { LWC; g_liteHostServices.prevComputerDigest(cc->ctx, A2N(out)); }
static uint32_t w_isAssetIssued(wasm_exec_env_t e, uint32_t iss, uint64_t name) { LWC; return g_liteHostServices.isAssetIssued(cc->ctx, A2N(iss), name); }
static int64_t  w_issueAsset(wasm_exec_env_t e, uint64_t name, uint32_t iss, uint32_t dec, int64_t shares, uint64_t unit) { LWC; LWTRACE("issueAsset", "name=" + std::to_string(name) + " shares=" + std::to_string(shares)); return g_liteHostServices.issueAsset(cc->ctx, name, A2N(iss), (signed char)dec, shares, unit); }
static int64_t  w_numberOfShares(wasm_exec_env_t e, uint32_t a, uint32_t o, uint32_t p) { LWC; return g_liteHostServices.numberOfShares(cc->ctx, A2N(a), A2N(o), A2N(p)); }
static int64_t  w_numberOfPossessedShares(wasm_exec_env_t e, uint64_t name, uint32_t iss, uint32_t own, uint32_t pos, uint32_t om, uint32_t pm) { LWC; return g_liteHostServices.numberOfPossessedShares(cc->ctx, name, A2N(iss), A2N(own), A2N(pos), (unsigned short)om, (unsigned short)pm); }
static int64_t  w_transferShares(wasm_exec_env_t e, uint64_t name, uint32_t iss, uint32_t own, uint32_t pos, int64_t shares, uint32_t no) { LWC; LWTRACE("transferShares", "name=" + std::to_string(name) + " shares=" + std::to_string(shares)); return g_liteHostServices.transferShareOwnershipAndPossession(cc->ctx, name, A2N(iss), A2N(own), A2N(pos), shares, A2N(no)); }
static uint32_t w_distributeDividends(wasm_exec_env_t e, int64_t a) { LWC; LWTRACE("distributeDividends", std::to_string(a)); return g_liteHostServices.distributeDividends(cc->ctx, a); }
static int32_t  w_liteCallFunction(wasm_exec_env_t e, uint32_t idx, uint32_t it, uint32_t in, uint32_t inSize, uint32_t out, uint32_t outSize) { LWC; LWTRACE("callFunction", "-> " + std::to_string(idx) + "/" + std::to_string(it)); return g_liteHostServices.liteCallFunction(cc->ctx, idx, (unsigned short)it, A2N(in), inSize, A2N(out), outSize); }
static int32_t  w_liteInvokeProcedure(wasm_exec_env_t e, uint32_t idx, uint32_t it, uint32_t in, uint32_t inSize, uint32_t out, uint32_t outSize, int64_t reward) { LWC; LWTRACE("invokeProcedure", "-> " + std::to_string(idx) + "/" + std::to_string(it) + " reward " + std::to_string(reward)); return g_liteHostServices.liteInvokeProcedure(cc->ctx, idx, (unsigned short)it, A2N(in), inSize, A2N(out), outSize, reward); }
static int32_t  w_liteSetShareholderProposal(wasm_exec_env_t e, uint32_t idx, uint32_t prop, int64_t reward) { LWC; LWTRACE("setShareholderProposal", "-> " + std::to_string(idx)); return g_liteHostServices.setShareholderProposal(cc->ctx, idx, A2N(prop), reward); }
static int32_t  w_liteSetShareholderVotes(wasm_exec_env_t e, uint32_t idx, uint32_t vote, uint32_t voteSize, int64_t reward) { LWC; LWTRACE("setShareholderVotes", "-> " + std::to_string(idx)); return g_liteHostServices.setShareholderVotes(cc->ctx, idx, A2N(vote), voteSize, reward); }

#undef LWC
#undef A2N

// WAMR signatures: i=i32, I=i64, ()=void; pointers cross as i32 offsets (converted in-fn), so never "*".
static NativeSymbol g_liteWasmNatives[] = {
    { "beginFn",                            (void*)w_beginFn,                            "(i)",     NULL },
    { "endFn",                              (void*)w_endFn,                              "(i)",     NULL },
    { "markDirty",                          (void*)w_markDirty,                          "(i)",     NULL },
    { "pauseLog",                           (void*)w_pauseLog,                           "()",      NULL },
    { "resumeLog",                          (void*)w_resumeLog,                          "()",      NULL },
    { "acquireScratch",                     (void*)w_acquireScratch,                     "(Ii)i",   NULL },
    { "releaseScratch",                     (void*)w_releaseScratch,                     "(i)",     NULL },
    { "logBytes",                           (void*)w_logBytes,                           "(iiii)",  NULL },
    { "k12",                                (void*)w_k12,                                "(iii)",   NULL },
    { "transfer",                           (void*)w_transfer,                           "(iI)I",   NULL },
    { "transferTyped",                      (void*)w_transferTyped,                      "(iIi)I",  NULL },
    { "abort",                              (void*)w_abort,                              "(i)",     NULL },
    { "burn",                               (void*)w_burn,                               "(Ii)I",   NULL },
    { "epoch",                              (void*)w_epoch,                              "()i",     NULL },
    { "tick",                               (void*)w_tick,                               "()i",     NULL },
    { "numberOfTickTransactions",           (void*)w_numTickTx,                          "()i",     NULL },
    { "getEntity",                          (void*)w_getEntity,                          "(ii)i",   NULL },
    { "queryFeeReserve",                    (void*)w_queryFeeReserve,                    "(i)I",    NULL },
    { "nextId",                             (void*)w_nextId,                             "(ii)",    NULL },
    { "prevId",                             (void*)w_prevId,                             "(ii)",    NULL },
    { "isContractId",                       (void*)w_isContractId,                       "(i)i",    NULL },
    { "arbitrator",                         (void*)w_arbitrator,                         "(i)",     NULL },
    { "computor",                           (void*)w_computor,                           "(ii)",    NULL },
    { "day",                                (void*)w_day,                                "()i",     NULL },
    { "year",                               (void*)w_year,                               "()i",     NULL },
    { "hour",                               (void*)w_hour,                               "()i",     NULL },
    { "minute",                             (void*)w_minute,                             "()i",     NULL },
    { "month",                              (void*)w_month,                              "()i",     NULL },
    { "second",                             (void*)w_second,                             "()i",     NULL },
    { "millisecond",                        (void*)w_millisecond,                        "()i",     NULL },
    { "now",                                (void*)w_now,                                "(i)",     NULL },
    { "prevSpectrumDigest",                 (void*)w_prevSpectrumDigest,                 "(i)",     NULL },
    { "prevUniverseDigest",                 (void*)w_prevUniverseDigest,                 "(i)",     NULL },
    { "prevComputerDigest",                 (void*)w_prevComputerDigest,                 "(i)",     NULL },
    { "isAssetIssued",                      (void*)w_isAssetIssued,                      "(iI)i",   NULL },
    { "issueAsset",                         (void*)w_issueAsset,                         "(IiiII)I",NULL },
    { "numberOfShares",                     (void*)w_numberOfShares,                     "(iii)I",  NULL },
    { "numberOfPossessedShares",            (void*)w_numberOfPossessedShares,            "(Iiiiii)I",NULL },
    { "transferShareOwnershipAndPossession",(void*)w_transferShares,                     "(IiiiIi)I",NULL },
    { "distributeDividends",                (void*)w_distributeDividends,                "(I)i",    NULL },
    { "liteCallFunction",                   (void*)w_liteCallFunction,                   "(iiiiii)i",NULL },
    { "liteInvokeProcedure",                (void*)w_liteInvokeProcedure,                "(iiiiiiI)i",NULL },
    { "liteSetShareholderProposal",         (void*)w_liteSetShareholderProposal,         "(iiI)i",   NULL },
    { "liteSetShareholderVotes",            (void*)w_liteSetShareholderVotes,            "(iiiI)i",  NULL },
};
static const uint32_t g_liteWasmNativesCount = (uint32_t)(sizeof(g_liteWasmNatives) / sizeof(g_liteWasmNatives[0]));

// Every host vtable fn must have exactly one wasm import; adding one + forgetting the other fails the BUILD
// here instead of trapping at runtime. +1 = the leading abiVersion field (a pointer-sized slot).
static_assert(sizeof(LiteHostServices) == sizeof(void*) * (g_liteWasmNativesCount + 1),
              "wasm import table (g_liteWasmNatives) out of sync with the host vtable (LiteHostServices)");

#endif // LITE_WASM_CONTRACTS

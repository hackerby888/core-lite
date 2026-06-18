#pragma once
// Node-side wasm import table (WASM_CONTRACTS.md §13.5): one wasm import per LiteHostServices member, each
// forwarding to the host's g_liteHostServices vtable. ctx is bound per-call out-of-band
// (LiteWasmCallCtx via exec_env user_data, Stage-2 ABI #1); pointers cross as i32 linear-mem offsets,
// converted with addr_app_to_native (the general marshalling pattern). acquireScratch returns a linear-mem
// offset, not a native ptr (ABI #2). Module name = "lhost"; the contract side (lite_wasm_tu.h) imports it.
//
// Most imports are pure forwards (A2N pointer args, pass scalars, call the vtable member) and are GENERATED
// from one X-list (LHOST_TABLE) by LiteQpiImport/LiteInfraImport — the WAMR signature string is DERIVED from
// the member's C type (liteWasmSig), so the hand-typed-sig bug class is gone. Imports with bespoke logic
// (acquireScratch's arena, logBytes' ci-stamp, the debug-traced effectful calls) keep a hand-written wrapper
// but still take their DERIVED sig + a static_assert that it reproduces the previously hand-written string.
#ifdef LITE_WASM_CONTRACTS
#include <cstdint>
#include <type_traits>
#include <array>
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

// ---------------------------------------------------------------------------
// Codegen: derive the WAMR signature + the forwarding wrapper from a vtable member's C type.
// ---------------------------------------------------------------------------
// WAMR sig grammar: i=i32, I=i64, f=f32, F=f64; pointers + narrow ints cross as i32. void return => no suffix.
template<class T> constexpr size_t liteSafeSizeof() { if constexpr (std::is_void_v<T>) return 8; else return sizeof(T); }
template<class T> constexpr char liteWasmSigChar() {
    if constexpr (std::is_void_v<T>)                return '\0';
    else if constexpr (std::is_pointer_v<T>)        return 'i';
    else if constexpr (std::is_floating_point_v<T>) return sizeof(T) == 4 ? 'f' : 'F';
    else                                            return liteSafeSizeof<T>() <= 4 ? 'i' : 'I';
}
// wasm-ABI carrier type for a host param/return: ptr & narrow int -> uint32_t (i32); 64-bit kept; void kept.
template<class T> using liteWasmAbi =
    std::conditional_t<std::is_void_v<T>, void,
      std::conditional_t<std::is_pointer_v<T>, uint32_t,
        std::conditional_t<(std::is_integral_v<T> && liteSafeSizeof<T>() <= 4), uint32_t, T>>>;
// per-arg conversion at the boundary: pointer param <- A2N(offset); scalar <- cast from the carrier.
template<class P> static inline P liteWasmConv(wasm_exec_env_t e, liteWasmAbi<P> a) {
    if constexpr (std::is_pointer_v<P>) return (P)liteWasmA2N(e, (uint32_t)a);
    else                                return (P)a;
}
// constexpr sig string (usable in static_assert + as the NativeSymbol const char*).
template<class Ret, class... Args>
constexpr std::array<char, 4 + sizeof...(Args)> liteWasmSig() {
    std::array<char, 4 + sizeof...(Args)> b{}; int n = 0; b[n++] = '(';
    const char cs[] = { liteWasmSigChar<Args>()..., '\0' };
    for (size_t i = 0; i < sizeof...(Args); ++i) b[n++] = cs[i];
    b[n++] = ')'; char r = liteWasmSigChar<Ret>(); if (r) b[n++] = r; b[n] = '\0'; return b;
}
constexpr bool liteCstrEq(const char* a, const char* b) { while (*a && *a == *b) { ++a; ++b; } return *a == *b; }

// Generated wrapper for a ctx-bound QPI backend: pointer-to-member of g_liteHostServices.
// (g_liteHostServices is defined in lite_dynamic_contracts.h, included before this header.)
template<auto Member> struct LiteQpiImport;
template<class R, class... A, R(*LiteHostServices::*Member)(const void*, A...)>
struct LiteQpiImport<Member> {
    static liteWasmAbi<R> call(wasm_exec_env_t e, liteWasmAbi<A>... a) {
        LiteWasmCallCtx* cc = liteWasmCC(e);
        if constexpr (std::is_void_v<R>) (g_liteHostServices.*Member)(cc->ctx, liteWasmConv<A>(e, a)...);
        else return (liteWasmAbi<R>)(g_liteHostServices.*Member)(cc->ctx, liteWasmConv<A>(e, a)...);
    }
    static constexpr auto sig = liteWasmSig<R, A...>();
};
// Generated wrapper for a ctx-less infra service (every param is a real wasm arg).
template<auto Member> struct LiteInfraImport;
template<class R, class... A, R(*LiteHostServices::*Member)(A...)>
struct LiteInfraImport<Member> {
    static liteWasmAbi<R> call(wasm_exec_env_t e, liteWasmAbi<A>... a) {
        if constexpr (std::is_void_v<R>) (g_liteHostServices.*Member)(liteWasmConv<A>(e, a)...);
        else return (liteWasmAbi<R>)(g_liteHostServices.*Member)(liteWasmConv<A>(e, a)...);
    }
    static constexpr auto sig = liteWasmSig<R, A...>();
};

// ---------------------------------------------------------------------------
// Hand-written wrappers: bespoke logic the generator can't express (arena math, ci-stamp, debug-trace detail).
// Their sig is still DERIVED (below) — only the body is hand-written.
// ---------------------------------------------------------------------------
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
    LWC; void* m = A2N(msgOff);
    if (cc && cc->trace) liteWasmTraceLog((LiteWasmTraceEntry*)cc->trace, (unsigned char)type, m, size); // pre-ci-stamp bytes
    g_liteHostServices.logBytes(ci, (unsigned char)type, m, size);
}
static int64_t  w_transfer(wasm_exec_env_t e, uint32_t d, int64_t a) { LWC; LWTRACE("transfer", liteWasmHex(A2N(d), 8) + ".. " + std::to_string(a)); return g_liteHostServices.transfer(cc->ctx, A2N(d), a); }
static int64_t  w_transferTyped(wasm_exec_env_t e, uint32_t d, int64_t a, uint32_t t) { LWC; LWTRACE("transferTyped", liteWasmHex(A2N(d), 8) + ".. " + std::to_string(a) + " t=" + std::to_string(t)); return g_liteHostServices.transferTyped(cc->ctx, A2N(d), a, (unsigned char)t); }
static void     w_abort(wasm_exec_env_t e, uint32_t code) { LWC; LWTRACE("abort", std::to_string(code)); g_liteHostServices.abort(cc->ctx, code); }
static int64_t  w_burn(wasm_exec_env_t e, int64_t a, uint32_t idx) { LWC; LWTRACE("burn", std::to_string(a) + " for " + std::to_string(idx)); return g_liteHostServices.burn(cc->ctx, a, idx); }
static int64_t  w_issueAsset(wasm_exec_env_t e, uint64_t name, uint32_t iss, uint32_t dec, int64_t shares, uint64_t unit) { LWC; LWTRACE("issueAsset", "name=" + std::to_string(name) + " shares=" + std::to_string(shares)); return g_liteHostServices.issueAsset(cc->ctx, name, A2N(iss), (signed char)dec, shares, unit); }
static int64_t  w_transferShares(wasm_exec_env_t e, uint64_t name, uint32_t iss, uint32_t own, uint32_t pos, int64_t shares, uint32_t no) { LWC; LWTRACE("transferShares", "name=" + std::to_string(name) + " shares=" + std::to_string(shares)); return g_liteHostServices.transferShareOwnershipAndPossession(cc->ctx, name, A2N(iss), A2N(own), A2N(pos), shares, A2N(no)); }
static uint32_t w_distributeDividends(wasm_exec_env_t e, int64_t a) { LWC; LWTRACE("distributeDividends", std::to_string(a)); return g_liteHostServices.distributeDividends(cc->ctx, a); }
static int32_t  w_liteCallFunction(wasm_exec_env_t e, uint32_t idx, uint32_t it, uint32_t in, uint32_t inSize, uint32_t out, uint32_t outSize) { LWC; LWTRACE("callFunction", "-> " + std::to_string(idx) + "/" + std::to_string(it)); return g_liteHostServices.liteCallFunction(cc->ctx, idx, (unsigned short)it, A2N(in), inSize, A2N(out), outSize); }
static int32_t  w_liteInvokeProcedure(wasm_exec_env_t e, uint32_t idx, uint32_t it, uint32_t in, uint32_t inSize, uint32_t out, uint32_t outSize, int64_t reward) { LWC; LWTRACE("invokeProcedure", "-> " + std::to_string(idx) + "/" + std::to_string(it) + " reward " + std::to_string(reward)); return g_liteHostServices.liteInvokeProcedure(cc->ctx, idx, (unsigned short)it, A2N(in), inSize, A2N(out), outSize, reward); }
static int32_t  w_liteSetShareholderProposal(wasm_exec_env_t e, uint32_t idx, uint32_t prop, int64_t reward) { LWC; LWTRACE("setShareholderProposal", "-> " + std::to_string(idx)); return g_liteHostServices.setShareholderProposal(cc->ctx, idx, A2N(prop), reward); }
static int32_t  w_liteSetShareholderVotes(wasm_exec_env_t e, uint32_t idx, uint32_t vote, uint32_t voteSize, int64_t reward) { LWC; LWTRACE("setShareholderVotes", "-> " + std::to_string(idx)); return g_liteHostServices.setShareholderVotes(cc->ctx, idx, A2N(vote), voteSize, reward); }

#undef LWC
#undef A2N

// ---------------------------------------------------------------------------
// The single source of truth: one row per import, in the original table order.
//   GQ/GI  = generated   (Q ctx-bound QPI backend | I ctx-less infra)        -> LiteQpiImport/LiteInfraImport
//   HQ/HI  = hand-written (bespoke body), sig still derived from the member  -> w_*
// The 4th column is the previously hand-typed WAMR sig: each row static_asserts the DERIVED sig reproduces it.
// ---------------------------------------------------------------------------
#define LHOST_TABLE(GQ, GI, HQ, HI) \
    GI("beginFn",                             beginFn,                              "(i)")       \
    GI("endFn",                               endFn,                                "(i)")       \
    GI("markDirty",                           markDirty,                            "(i)")       \
    GI("pauseLog",                            pauseLog,                             "()")        \
    GI("resumeLog",                           resumeLog,                            "()")        \
    HI("acquireScratch",                      acquireScratch, w_acquireScratch,     "(Ii)i")     \
    HI("releaseScratch",                      releaseScratch, w_releaseScratch,     "(i)")       \
    HI("logBytes",                            logBytes,       w_logBytes,           "(iiii)")    \
    GI("k12",                                 k12,                                  "(iii)")     \
    HQ("transfer",                            transfer,       w_transfer,           "(iI)I")     \
    HQ("transferTyped",                       transferTyped,  w_transferTyped,      "(iIi)I")    \
    HQ("abort",                               abort,          w_abort,              "(i)")       \
    HQ("burn",                                burn,           w_burn,               "(Ii)I")     \
    GQ("epoch",                               epoch,                                "()i")       \
    GQ("tick",                                tick,                                 "()i")       \
    GQ("numberOfTickTransactions",            numberOfTickTransactions,             "()i")       \
    GQ("getEntity",                           getEntity,                            "(ii)i")     \
    GQ("queryFeeReserve",                     queryFeeReserve,                      "(i)I")      \
    GQ("nextId",                              nextId,                               "(ii)")      \
    GQ("prevId",                              prevId,                               "(ii)")      \
    GQ("isContractId",                        isContractId,                         "(i)i")      \
    GQ("arbitrator",                          arbitrator,                           "(i)")       \
    GQ("computor",                            computor,                             "(ii)")      \
    GQ("day",                                 day,                                  "()i")       \
    GQ("year",                                year,                                 "()i")       \
    GQ("hour",                                hour,                                 "()i")       \
    GQ("minute",                              minute,                               "()i")       \
    GQ("month",                               month,                                "()i")       \
    GQ("second",                              second,                               "()i")       \
    GQ("millisecond",                         millisecond,                          "()i")       \
    GQ("now",                                 now,                                  "(i)")       \
    GQ("prevSpectrumDigest",                  prevSpectrumDigest,                   "(i)")       \
    GQ("prevUniverseDigest",                  prevUniverseDigest,                   "(i)")       \
    GQ("prevComputerDigest",                  prevComputerDigest,                   "(i)")       \
    GQ("isAssetIssued",                       isAssetIssued,                        "(iI)i")     \
    HQ("issueAsset",                          issueAsset,     w_issueAsset,         "(IiiII)I")  \
    GQ("numberOfShares",                      numberOfShares,                       "(iii)I")    \
    GQ("numberOfPossessedShares",             numberOfPossessedShares,              "(Iiiiii)I") \
    HQ("transferShareOwnershipAndPossession", transferShareOwnershipAndPossession, w_transferShares, "(IiiiIi)I") \
    HQ("distributeDividends",                 distributeDividends, w_distributeDividends,        "(I)i")     \
    HQ("liteCallFunction",                    liteCallFunction,    w_liteCallFunction,           "(iiiiii)i")  \
    HQ("liteInvokeProcedure",                 liteInvokeProcedure, w_liteInvokeProcedure,        "(iiiiiiI)i") \
    HQ("liteSetShareholderProposal",          setShareholderProposal, w_liteSetShareholderProposal, "(iiI)i") \
    HQ("liteSetShareholderVotes",             setShareholderVotes,    w_liteSetShareholderVotes,    "(iiiI)i")

// pass 1 — prove the derived sig reproduces every previously hand-typed string (transition safety net).
#define LHOST_AS_GQ(nm, m, lit)     static_assert(liteCstrEq(LiteQpiImport<&LiteHostServices::m>::sig.data(),   lit), "wasm sig drift: " nm);
#define LHOST_AS_GI(nm, m, lit)     static_assert(liteCstrEq(LiteInfraImport<&LiteHostServices::m>::sig.data(), lit), "wasm sig drift: " nm);
#define LHOST_AS_HQ(nm, m, wfn, lit) static_assert(liteCstrEq(LiteQpiImport<&LiteHostServices::m>::sig.data(),   lit), "wasm sig drift: " nm);
#define LHOST_AS_HI(nm, m, wfn, lit) static_assert(liteCstrEq(LiteInfraImport<&LiteHostServices::m>::sig.data(), lit), "wasm sig drift: " nm);
LHOST_TABLE(LHOST_AS_GQ, LHOST_AS_GI, LHOST_AS_HQ, LHOST_AS_HI)

// pass 2 — the NativeSymbol table. Generated rows use the templated wrapper; hand rows use w_*; sig is DERIVED
// for every row (single source). Pointers cross as i32 offsets (converted in-fn), so the sig never names "*".
#define LHOST_ROW_GQ(nm, m, lit)      { nm, (void*)&LiteQpiImport<&LiteHostServices::m>::call,   LiteQpiImport<&LiteHostServices::m>::sig.data(),   NULL },
#define LHOST_ROW_GI(nm, m, lit)      { nm, (void*)&LiteInfraImport<&LiteHostServices::m>::call, LiteInfraImport<&LiteHostServices::m>::sig.data(), NULL },
#define LHOST_ROW_HQ(nm, m, wfn, lit) { nm, (void*)wfn,                                          LiteQpiImport<&LiteHostServices::m>::sig.data(),   NULL },
#define LHOST_ROW_HI(nm, m, wfn, lit) { nm, (void*)wfn,                                          LiteInfraImport<&LiteHostServices::m>::sig.data(), NULL },
static NativeSymbol g_liteWasmNatives[] = {
    LHOST_TABLE(LHOST_ROW_GQ, LHOST_ROW_GI, LHOST_ROW_HQ, LHOST_ROW_HI)
};
static const uint32_t g_liteWasmNativesCount = (uint32_t)(sizeof(g_liteWasmNatives) / sizeof(g_liteWasmNatives[0]));

// Every host vtable fn must have exactly one wasm import; adding one + forgetting the other fails the BUILD
// here instead of trapping at runtime. +1 = the leading abiVersion field (a pointer-sized slot).
static_assert(sizeof(LiteHostServices) == sizeof(void*) * (g_liteWasmNativesCount + 1),
              "wasm import table (g_liteWasmNatives) out of sync with the host vtable (LiteHostServices)");

#endif // LITE_WASM_CONTRACTS

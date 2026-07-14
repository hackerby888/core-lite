#pragma once
// WASM-executed contracts (testnet) — the contract execution engine for the deploy framework
// (lite_dynamic_contracts.h). Embeds WAMR (one persistent instance per slot) + uses libffi closures to drop a per-(idx,it) trampoline
// into the core's contractUser{Functions,Procedures}[idx][it] tables, so core dispatch (contract_exec.h)
// stays byte-identical to upstream. The contract's state lives in the wasm instance's linear memory; the node's
// contractStates[idx] is pointed AT that region at load (the slot's 1GB reserve is freed). See WASM_CONTRACTS.md.
#ifdef LITE_WASM_CONTRACTS

#if !defined(LITE_DYNAMIC_CONTRACTS)
#error "LITE_WASM_CONTRACTS requires LITE_DYNAMIC_CONTRACTS (shares the host vtable + deploy/slot machinery)"
#endif

#include <ffi.h>
#include <string>
#include <chrono>
#include "wasm_export.h"
#include "extensions/wasm/lite_wasm_arena.h"
#include "extensions/wasm/lite_wasm_imports.h"   // g_liteWasmNatives[] + LiteWasmCallCtx -> g_liteHostServices (+ debug trace)

void logColorToScreen(std::string type, std::string msg);   // defined later in qubic.cpp (same TU)

// Per-call scratch layout inside the contract's exported io_base region: [in | out | locals | arena].
// Sized to the core protocol caps: in/out = uint16 max (registered inputSize/outputSize); locals =
// MAX_SIZE_OF_CONTRACT_LOCALS; arena = the native scratchpad (defaultCommonBuffersSize ~1GB) so any QPI
// HashMap reorg fits. MUST match g_wasmIo in lite_wasm_tu.h.
#define LITE_WASM_IN_SZ     (64u * 1024u)            // input  region (>= uint16 max 65535)
#define LITE_WASM_OUT_SZ    (64u * 1024u)            // output region (>= uint16 max 65535)
#define LITE_WASM_LOCALS_SZ (32u * 1024u)            // = MAX_SIZE_OF_CONTRACT_LOCALS
#ifndef LITE_WASM_ARENA_SZ
#define LITE_WASM_ARENA_SZ  (1024u * 1024u * 1024u)  // acquireScratch bump arena (matches native scratchpad)
#endif
#define LITE_WASM_IO_TOTAL  ((unsigned long long)LITE_WASM_IN_SZ + LITE_WASM_OUT_SZ + LITE_WASM_LOCALS_SZ + LITE_WASM_ARENA_SZ)

static bool    g_liteWasmReady = false;
static ffi_cif g_liteWasmDispatchCif;                 // shared 5-pointer->void CIF for every dispatch closure
static ffi_type* g_liteWasmCifArgs[5];
static ffi_cif g_liteWasmMigrateCif;                  // 4-pointer->void CIF for the per-slot migrate closure
static ffi_type* g_liteWasmMigrateCifArgs[4];

// closure user_data: identifies which contract entry a trampoline stands for.
struct LiteWasmEntryBind { uint32_t idx; uint16_t it; uint8_t kind; };

// One loaded wasm contract = one persistent instance (Stage-3: reused, no per-call churn).
struct LiteWasmSlot {
    bool                 loaded = false;
    wasm_module_t        mod = nullptr;
    wasm_module_inst_t   inst = nullptr;
    wasm_exec_env_t      env = nullptr;
    wasm_function_inst_t dispatchFn = nullptr;
    unsigned char*       wasmBuf = nullptr;       // owned mutable copy; WAMR writes into + references it for life
    uint32_t             stateOff = 0, stateSize = 0;
    uint32_t             ioBase = 0;
    uint32_t             ctxOff = 0;       // contract-side QpiContext buffer; host copies the ctx base in per call
    uint32_t*            arenaTop = nullptr; // optional mutable wasm32 global; reset at each outer dispatch
    uint32_t             entryCount = 0;
    LiteWasmEntryBind    binds[LITE_MAX_USER_ENTRIES] = {};
    ffi_closure*         closures[LITE_MAX_USER_ENTRIES] = {};
    LiteWasmEntryBind    sysBinds[LITE_SP_COUNT] = {};       // system procedures, kind=2, it=LITE_SP_*
    ffi_closure*         sysClosures[LITE_SP_COUNT] = {};
    uint16_t             sysInSize[LITE_SP_COUNT] = {};      // QPI-defined in/out sizes per sysproc (share-mgmt)
    uint16_t             sysOutSize[LITE_SP_COUNT] = {};
    bool                 stubFreed = false;       // set once the slot's 1GB reserve is freed + contractStates[idx] aliased to the resident state
    std::string          lastTrap;                // reason of the most recent dispatch trap (cleared on success); surfaced via dyn-registry
    bool                 hasMigrate = false;      // contract exports __migrate; a redeploy with matching old-state size runs it
    uint32_t             migrateOldStateSize = 0, migrateLocalsSize = 0;
    LiteWasmEntryBind    migBind = {};            // kind=3, it=0
    ffi_closure*         migClosure = nullptr;
    unsigned char*       pendingOldState = nullptr;   // old-state snapshot stashed at redeploy; migrated at construct, then freed
    uint32_t             pendingOldStateSize = 0;
};
#define LITE_WASM_KIND_SYSPROC 2
static LiteWasmSlot g_liteWasmSlots[LITE_DYN_SLOT_COUNT];

static inline int liteWasmSlotLocal(unsigned int idx) {
    int l = (int)idx - (int)liteDynSlotBase();
    return (l >= 0 && l < LITE_DYN_SLOT_COUNT) ? l : -1;
}
static inline bool liteWasmIsWasm(unsigned int idx) {
    int l = liteWasmSlotLocal(idx);
    return l >= 0 && g_liteWasmSlots[l].loaded;
}
// Reason of the most recent dispatch trap on a wasm slot (empty if none / last call ok). For the dyn-registry RPC.
static inline std::string liteWasmLastTrap(unsigned int idx) {
    int l = liteWasmSlotLocal(idx);
    return (l >= 0 && g_liteWasmSlots[l].loaded) ? g_liteWasmSlots[l].lastTrap : std::string();
}
static inline uint32_t liteWasmCallU32(wasm_exec_env_t env, wasm_function_inst_t fn) {
    uint32_t a[1] = { 0 }; wasm_runtime_call_wasm(env, fn, 0, a); return a[0];
}

// Real state span for a wasm slot (its resident size); deflt for non-wasm. The node hashes/saves this, not the
// slot's 1GB reserve, so it never reads past the resident region.
static inline unsigned long long liteWasmEffectiveStateSize(unsigned int idx, unsigned long long deflt) {
    int local = liteWasmSlotLocal(idx);
    if (local < 0 || !g_liteWasmSlots[local].loaded) return deflt;
    return g_liteWasmSlots[local].stateSize;
}

// WAMR requires each thread that runs wasm to init its thread env. load/dispatch run on tick-processor
// threads (and the RPC thread for read-only functions), none of which WAMR created -> init on demand.
static inline void liteWasmEnsureThreadEnv() {
    if (!wasm_runtime_thread_env_inited()) wasm_runtime_init_thread_env();
}

// The current thread's active wasm exec_env, if any. The outermost dispatch (called from native core) uses
// the slot's own env; a NESTED wasm->wasm call (via liteCallFunction) must REUSE the current env and swap its
// module_inst (Stage-3b: a fresh/foreign env traps "invalid exec env" against WAMR's per-thread TLS env).
static thread_local wasm_exec_env_t t_liteWasmCurEnv = nullptr;
// Per-slot depth is distinct from t_liteWasmCurEnv: A -> B is nested on the thread but is an outer call for
// B's arena, while A -> B -> A must preserve A's live arena frames and restore its bump after the inner A.
static thread_local uint32_t t_liteWasmSlotDepth[LITE_DYN_SLOT_COUNT] = {};

// ---- liteWasmDispatch helpers (extracted; behavior identical to the previously inlined code) ----

// Resolve a call's registered input/output sizes by kind; false (+logs) if they exceed the io regions.
static inline bool liteWasmResolveIO(uint32_t idx, uint16_t it, uint8_t kind, const LiteWasmSlot& s,
                                     uint16_t& inSize, uint16_t& outSize) {
    if (kind == LITE_KIND_FUNCTION)          { inSize = contractUserFunctionInputSizes[idx][it];  outSize = contractUserFunctionOutputSizes[idx][it]; }
    else if (kind == LITE_WASM_KIND_SYSPROC) { inSize = s.sysInSize[it]; outSize = s.sysOutSize[it]; }   // QPI sysproc in/out
    else                                     { inSize = contractUserProcedureInputSizes[idx][it]; outSize = contractUserProcedureOutputSizes[idx][it]; }
    // Registered sizes are uint16 (<=65535 < 64K) so this never fires today — defense against a tighter region.
    if (inSize > LITE_WASM_IN_SZ || outSize > LITE_WASM_OUT_SZ) {
        logColorToScreen("ERROR", "LITEWASM dispatch in/out exceeds io region idx=" + std::to_string(idx)
                         + " in=" + std::to_string(inSize) + " out=" + std::to_string(outSize));
        return false;
    }
    return true;
}

// RAII exec_env selection. Outermost dispatch on a thread creates a fresh exec_env on the slot's instance; a
// nested wasm->wasm call (liteCallFunction) reuses the thread's current env + swaps its module_inst. The dtor
// destroys the fresh env (outer) or restores the borrowed env's module_inst + user_data (nested). ok=false if
// the outer exec_env couldn't be created.
struct LiteWasmEnvScope {
    wasm_exec_env_t    env = nullptr;
    bool               outer = false;
    bool               ok = false;
    wasm_module_inst_t savedInst = nullptr;
    void*              savedUD = nullptr;

    explicit LiteWasmEnvScope(const LiteWasmSlot& s) {
        if (t_liteWasmCurEnv) {
            env = t_liteWasmCurEnv;
            savedInst = wasm_runtime_get_module_inst(env);
            savedUD   = wasm_runtime_get_user_data(env);
            wasm_runtime_set_module_inst(env, s.inst);
            outer = false; ok = true;
        } else {
            // WAMR exec_envs are thread-bound + the slot's load-time env belongs to the deploy thread.
            liteWasmEnsureThreadEnv();
            env = wasm_runtime_create_exec_env(s.inst, 64 * 1024);
            if (!env) return;
            t_liteWasmCurEnv = env;
            outer = true; ok = true;
        }
    }
    ~LiteWasmEnvScope() {
        if (!ok) return;
        if (outer) { wasm_runtime_set_user_data(env, nullptr); wasm_runtime_destroy_exec_env(env); t_liteWasmCurEnv = nullptr; }
        else       { wasm_runtime_set_user_data(env, savedUD); wasm_runtime_set_module_inst(env, savedInst); }
    }
    LiteWasmEnvScope(const LiteWasmEnvScope&) = delete;
    LiteWasmEnvScope& operator=(const LiteWasmEnvScope&) = delete;
};

// Copy ctx base + input into linear memory; zero the output region. (State is resident — not passed in.)
static inline void liteWasmMarshalIn(const LiteWasmSlot& s, const void* ctx, const void* input,
                                     uint16_t inSize, uint16_t outSize, uint32_t wIn, uint32_t wOut) {
    if (ctx && s.ctxOff) copyMem(wasm_runtime_addr_app_to_native(s.inst, s.ctxOff), ctx, sizeof(QPI::QpiContext));
    if (inSize) copyMem(wasm_runtime_addr_app_to_native(s.inst, wIn), input, inSize);
    setMem(wasm_runtime_addr_app_to_native(s.inst, wOut), outSize ? outSize : 1, 0);
}

// Copy the output region out; refresh contractStates[idx] (linear-mem base may move on memory.grow); flag the
// slot dirty for write calls so the digest re-hashes.
static inline void liteWasmMarshalOut(const LiteWasmSlot& s, uint32_t idx, uint8_t kind,
                                      void* output, uint16_t outSize, uint32_t wOut) {
    if (outSize) copyMem(output, wasm_runtime_addr_app_to_native(s.inst, wOut), outSize);
    contractStates[idx] = (unsigned char*)wasm_runtime_addr_app_to_native(s.inst, s.stateOff);
    if (kind != LITE_KIND_FUNCTION) g_liteHostServices.markDirty(idx);   // procedures + system procedures write state
}

// The real engine entry: marshal one contract call through the wasm instance. Receives the SAME native ptrs
// the core hands a native contract fn (ctx, state, input, output, locals); copies them in/out of linear memory.
static void liteWasmDispatch(uint32_t idx, uint16_t it, uint8_t kind, const void* ctx,
                             void* statePtr, void* input, void* output, void* locals)
{
    (void)locals; (void)statePtr;   // state is resident in linear mem, not handed in via statePtr
    int local = liteWasmSlotLocal(idx);
    if (local < 0) return;
    LiteWasmSlot& s = g_liteWasmSlots[local];
    if (!s.loaded) return;

    if (kind == 3) {   // MIGRATE (deploy-time, one-shot): old state arrives via `input`. Copy it into the arena,
        // zero the new state, run __migrate. Self-contained — its sizes come from the migrate metadata, not the
        // user/sysproc size tables, so it sits ahead of the per-call marshalling below.
        const uint32_t oldSz = s.migrateOldStateSize;
        if (oldSz > LITE_WASM_ARENA_SZ) {
            logColorToScreen("ERROR", "LITEWASM migrate old-state exceeds arena idx=" + std::to_string(idx));
            return;
        }
        LiteWasmEnvScope envScope(s);
        if (!envScope.ok) return;
        const wasm_exec_env_t env = envScope.env;
        const uint32_t wLocals = s.ioBase + LITE_WASM_IN_SZ + LITE_WASM_OUT_SZ;
        const uint32_t wArena  = wLocals + LITE_WASM_LOCALS_SZ;
        LiteWasmCallCtx cc; cc.ctx = ctx;
        // shift the bump-scratch base past the old blob so an acquireScratch inside __migrate can't clobber it.
        cc.arenaBase = cc.arenaBump = wArena + ((oldSz + 15u) & ~15u); cc.arenaEnd = wArena + LITE_WASM_ARENA_SZ;
        LiteWasmArenaScope arenaScope(t_liteWasmSlotDepth[local], s.arenaTop, cc.arenaBase);
        wasm_runtime_set_user_data(env, &cc);
        if (ctx && s.ctxOff) copyMem(wasm_runtime_addr_app_to_native(s.inst, s.ctxOff), ctx, sizeof(QPI::QpiContext));
        if (oldSz) copyMem(wasm_runtime_addr_app_to_native(s.inst, wArena), input /*oldState*/, oldSz);
        setMem(wasm_runtime_addr_app_to_native(s.inst, s.stateOff), s.stateSize, 0);   // zero new state (match native)
        setMem(wasm_runtime_addr_app_to_native(s.inst, wLocals), LITE_WASM_LOCALS_SZ, 0);
        uint32_t margv[5] = { 3, 0, wArena, 0, wLocals };
        if (!wasm_runtime_call_wasm(env, s.dispatchFn, 5, margv)) {
            const char* ex = wasm_runtime_get_exception(s.inst);
            s.lastTrap = std::string("MIGRATE") + (ex ? std::string(" — ") + ex : std::string(" — trap"));
            logColorToScreen("ERROR", "LITEWASM migrate trap idx=" + std::to_string(idx) + " " + s.lastTrap);
            wasm_runtime_clear_exception(s.inst);
        } else { s.lastTrap.clear(); }
        contractStates[idx] = (unsigned char*)wasm_runtime_addr_app_to_native(s.inst, s.stateOff);   // memory.grow safety
        g_liteHostServices.markDirty(idx);
        return;   // envScope dtor restores/destroys the exec_env
    }

    uint16_t inSize, outSize;
    if (!liteWasmResolveIO(idx, it, kind, s, inSize, outSize)) return;

    LiteWasmEnvScope envScope(s);
    if (!envScope.ok) return;
    const wasm_exec_env_t env = envScope.env;

    const uint32_t wIn     = s.ioBase;
    const uint32_t wOut    = s.ioBase + LITE_WASM_IN_SZ;
    const uint32_t wLocals = s.ioBase + LITE_WASM_IN_SZ + LITE_WASM_OUT_SZ;
    const uint32_t wArena  = s.ioBase + LITE_WASM_IN_SZ + LITE_WASM_OUT_SZ + LITE_WASM_LOCALS_SZ;

    LiteWasmCallCtx cc;
    cc.ctx = ctx;
    cc.arenaBase = wArena; cc.arenaBump = wArena; cc.arenaEnd = wArena + LITE_WASM_ARENA_SZ;
    LiteWasmArenaScope arenaScope(t_liteWasmSlotDepth[local], s.arenaTop, wArena);
    wasm_runtime_set_user_data(env, &cc);

    // debug trace (off by default; one atomic-bool check): capture input, bind the entry so effectful imports
    // record host-calls, and time the call. State diff is via dirty-page tracking (write calls only); committed
    // to the ring after the call.
    LiteWasmTraceEntry te;
    const bool dbg = liteWasmDebugEnabled();
    const bool dbgWrite = dbg && kind != LITE_KIND_FUNCTION;   // only state-mutating calls get a state diff
    unsigned char* dbgState = nullptr;
    std::chrono::steady_clock::time_point t0;
    if (dbg) {
        te.tick = g_liteHostServices.tick(ctx); te.idx = idx; te.it = it; te.kind = kind;
        te.inSize = inSize; te.outSize = outSize; te.stateSize = s.stateSize;
        if (inSize && input) copyMem(te.inHead, input, inSize < LITE_WASM_TRACE_HEAD ? inSize : LITE_WASM_TRACE_HEAD);
        if (kind == LITE_KIND_PROCEDURE) { auto* pc = (const QPI::QpiContextProcedureCall*)ctx; te.invocator = pc->invocator(); te.invocationReward = pc->invocationReward(); }
        cc.trace = &te;
        dbgState = (unsigned char*)wasm_runtime_addr_app_to_native(s.inst, s.stateOff);
        t0 = std::chrono::steady_clock::now();
    }

    // ctx base + input copied into linear memory, output zeroed. The contract's inline qpi.h accessors read the
    // ctx fields directly (QpiContext has no pointers/vtable + m256i is align-8 -> identical wasm32/x64 layout).
    // State is NOT passed in — it's resident in linear memory (contractStates[idx] aliases it), mutated in place.
    liteWasmMarshalIn(s, ctx, input, inSize, outSize, wIn, wOut);

    // protect the state region RO so the contract's writes fault -> dirty-page capture (write calls only).
    if (dbgWrite && dbgState) liteWasmDirtyBegin(dbgState, s.stateSize);

    uint32_t argv[5] = { kind, it, wIn, wOut, wLocals };
    if (!wasm_runtime_call_wasm(env, s.dispatchFn, 5, argv)) {
        const char* ex = wasm_runtime_get_exception(s.inst);   // which contract/entry trapped + why
        s.lastTrap = std::string("it=") + std::to_string(it) + " kind=" + std::to_string(kind)
                   + (ex ? std::string(" — ") + ex : std::string(" — trap"));   // surfaced via dyn-registry (RPC) to tooling
        // Built with LITE_WASM_TRAP_BACKTRACE (classic interp + DUMP_CALL_STACK), WAMR auto-prints the
        // backtrace (#NN: 0xOFF - name) to stdout during the trap; it lands in node.log next to this line.
        // qinit maps those offsets to source via the DWARF sidecar (the frames unwind before we regain control,
        // and copy_callstack carries no offset, so the node can't capture them structurally).
        logColorToScreen("ERROR", "LITEWASM dispatch trap idx=" + std::to_string(idx) + " " + s.lastTrap);
        wasm_runtime_clear_exception(s.inst);                  // clear so a later valid call on this slot still runs
    } else {
        s.lastTrap.clear();                                    // most-recent call succeeded
    }

    // restore state RW + build the changed-byte diff from the dirtied pages.
    if (dbgWrite && dbgState) liteWasmDirtyEnd(te, dbgState, s.stateSize);

    // output copied out; state is resident (nothing to copy). marshalOut refreshes contractStates[idx] (the
    // linear-mem base can move on memory.grow) + flags the slot dirty for write calls so the digest re-hashes.
    liteWasmMarshalOut(s, idx, kind, output, outSize, wOut);

    if (dbg) {   // finish + publish the debug trace entry (output + timing + trap; state diff already built)
        te.ok = s.lastTrap.empty(); te.trap = s.lastTrap;
        te.execNs = (unsigned long long)std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
        if (outSize) copyMem(te.outHead, wasm_runtime_addr_app_to_native(s.inst, wOut), outSize < LITE_WASM_TRACE_HEAD ? outSize : LITE_WASM_TRACE_HEAD);
        cc.trace = nullptr;
        liteWasmTraceCommit(te);
    }
    // envScope dtor restores/destroys the exec_env (outer) or restores the borrowed env's inst + user_data.
}

// libffi closure trampoline: core calls it as a native USER_FUNCTION/USER_PROCEDURE; we recover (idx,it,kind)
// from the bound user_data and forward to liteWasmDispatch. args[] = the 5 ptrs (ctx,state,input,output,locals).
static void liteWasmClosureHandler(ffi_cif*, void* /*ret(void)*/, void** args, void* user)
{
    LiteWasmEntryBind* b = (LiteWasmEntryBind*)user;
    liteWasmDispatch(b->idx, b->it, b->kind,
                     *(const void**)args[0], *(void**)args[1], *(void**)args[2], *(void**)args[3], *(void**)args[4]);
}

// libffi trampoline for the native migrate slot: core calls it as MIGRATE_PROCEDURE(ctx, newState, oldState,
// locals) — 4 pointers. Forward oldState (args[2]) as the dispatch `input`; newState is resident, locals zeroed.
static void liteWasmMigrateClosureHandler(ffi_cif*, void* /*ret(void)*/, void** args, void* user)
{
    LiteWasmEntryBind* b = (LiteWasmEntryBind*)user;
    liteWasmDispatch(b->idx, 0, 3, *(const void**)args[0], *(void**)args[1], *(void**)args[2], nullptr, *(void**)args[3]);
}

// RAII for a freshly loaded WAMR module set (mod + instance + exec_env). The dtor frees them in the correct
// order (env -> inst -> mod); release() drops ownership once the handles have been handed to a slot. The loader
// uses this so every early-return error path cleans up without a hand-written unwind, and the slot only ever
// receives a fully-validated module (no freed-handle stranding).
struct LiteWasmModuleSet {
    wasm_module_t      mod  = nullptr;
    wasm_module_inst_t inst = nullptr;
    wasm_exec_env_t    env  = nullptr;
    LiteWasmModuleSet() = default;
    ~LiteWasmModuleSet() {
        if (env)  wasm_runtime_destroy_exec_env(env);
        if (inst) wasm_runtime_deinstantiate(inst);
        if (mod)  wasm_runtime_unload(mod);
    }
    void release() { mod = nullptr; inst = nullptr; env = nullptr; }
    LiteWasmModuleSet(const LiteWasmModuleSet&) = delete;
    LiteWasmModuleSet& operator=(const LiteWasmModuleSet&) = delete;
};

// Minimal RAII malloc buffer (the state snapshot taken across a redeploy). Frees on scope exit.
struct LiteMallocBuf {
    unsigned char* p = nullptr;
    LiteMallocBuf() = default;
    ~LiteMallocBuf() { if (p) free(p); }
    void alloc(size_t n) { if (p) free(p); p = n ? (unsigned char*)malloc(n) : nullptr; }
    LiteMallocBuf(const LiteMallocBuf&) = delete;
    LiteMallocBuf& operator=(const LiteMallocBuf&) = delete;
};

// Release a slot's loaded instance + closures (before reloading the slot, so a redeploy doesn't leak the
// instance's linear memory or the libffi trampolines).
static void liteWasmSlotUnload(LiteWasmSlot& s)
{
    if (s.env)  { wasm_runtime_destroy_exec_env(s.env); s.env = nullptr; }
    if (s.inst) { wasm_runtime_deinstantiate(s.inst);   s.inst = nullptr; }
    s.arenaTop = nullptr;
    if (s.mod)  { wasm_runtime_unload(s.mod);            s.mod = nullptr; }
    for (uint32_t k = 0; k < s.entryCount; k++)        if (s.closures[k])    { ffi_closure_free(s.closures[k]);    s.closures[k] = nullptr; }
    for (uint32_t sp = 0; sp < LITE_SP_COUNT; sp++)    if (s.sysClosures[sp]) { ffi_closure_free(s.sysClosures[sp]); s.sysClosures[sp] = nullptr; }
    if (s.migClosure) { ffi_closure_free(s.migClosure); s.migClosure = nullptr; }
    s.hasMigrate = false;
    s.entryCount = 0;
}

// Load a contract.wasm into a slot: instantiate, read its registration (reg_count/reg_info) + state/io
// exports, and patch the core dispatch tables with one libffi closure per entry. NOT consensus — local load.
[[maybe_unused]] static bool liteWasmLoadFromBytes(unsigned int idx, const unsigned char* bytes, unsigned int len)
{
    int local = liteWasmSlotLocal(idx);
    if (local < 0) return false;
    LiteWasmSlot& s = g_liteWasmSlots[local];

    liteWasmEnsureThreadEnv();   // load runs on a tick-processor thread (not main)
    // upgrade: the contract state lives in the instance's linear memory, which deinstantiate frees below.
    // Snapshot it now and restore into the fresh instance, so a redeploy preserves state (INITIALIZE runs once).
    // Snapshot held by RAII so a failed reload (any early return below) frees it instead of leaking.
    LiteMallocBuf prevState; uint32_t prevStateSize = 0;
    if (s.inst && s.stubFreed && s.stateSize && contractStates[idx]) {
        prevStateSize = s.stateSize;
        prevState.alloc(prevStateSize);
        if (prevState.p) copyMem(prevState.p, contractStates[idx], prevStateSize);
    }
    if (s.inst) liteWasmSlotUnload(s);   // redeploy into a live slot: free the prior instance first

    // WAMR modifies the load buffer in place and references it for the module's life -> own a mutable copy.
    if (s.wasmBuf) { free(s.wasmBuf); s.wasmBuf = nullptr; }
    s.wasmBuf = (unsigned char*)malloc(len);
    if (!s.wasmBuf) { logToConsole(L"LITEWASM: oom"); return false; }
    copyMem(s.wasmBuf, bytes, len);

    // mod/inst/env held by RAII: any early return below frees them in the right order (no hand-unwind).
    // Ownership is handed to the slot (ms.release()) only after every validation passes, so a failed load
    // never strands the slot on freed handles.
    LiteWasmModuleSet ms;
    char err[192];
    ms.mod = wasm_runtime_load(s.wasmBuf, len, err, sizeof(err));
    if (!ms.mod) { logToConsole(L"LITEWASM: load failed"); free(s.wasmBuf); s.wasmBuf = nullptr; return false; }
    ms.inst = wasm_runtime_instantiate(ms.mod, 64 * 1024, 1024 * 1024, err, sizeof(err));
    if (!ms.inst) { logToConsole(L"LITEWASM: instantiate failed"); return false; }
    ms.env = wasm_runtime_create_exec_env(ms.inst, 64 * 1024);
    if (!ms.env) { logToConsole(L"LITEWASM: exec env alloc failed"); return false; }
    wasm_module_inst_t inst = ms.inst;
    wasm_exec_env_t env = ms.env;
    uint32_t* arenaTop = nullptr;

    wasm_function_inst_t f_state_addr = wasm_runtime_lookup_function(inst, "state_addr");
    wasm_function_inst_t f_state_size = wasm_runtime_lookup_function(inst, "state_size");
    wasm_function_inst_t f_io_base    = wasm_runtime_lookup_function(inst, "io_base");
    wasm_function_inst_t f_reg_count  = wasm_runtime_lookup_function(inst, "reg_count");
    wasm_function_inst_t f_reg_info   = wasm_runtime_lookup_function(inst, "reg_info");
    wasm_function_inst_t f_dispatch   = wasm_runtime_lookup_function(inst, "dispatch");
    if (!f_state_addr || !f_state_size || !f_io_base || !f_reg_count || !f_reg_info || !f_dispatch) {
        logToConsole(L"LITEWASM: missing required export");
        return false;
    }

    s.stateOff = liteWasmCallU32(env, f_state_addr);
    s.stateSize = liteWasmCallU32(env, f_state_size);
    s.ioBase = liteWasmCallU32(env, f_io_base);

    // Qinit compiler modules own their temporary-locals bump in this exported global. Cache its WAMR backing
    // storage so dispatch can reset it without adding an ABI function call. It remains optional for older
    // contract wasm, but a present export must have the compiler ABI's mutable-i32 shape.
    { wasm_global_inst_t arenaGlobal = {};
      if (wasm_runtime_get_export_global_inst(inst, "arena_top", &arenaGlobal)) {
          if (arenaGlobal.kind != WASM_I32 || !arenaGlobal.is_mutable || !arenaGlobal.global_data) {
              logToConsole(L"LITEWASM: arena_top must be a mutable i32 global");
              return false;
          }
          arenaTop = static_cast<uint32_t*>(arenaGlobal.global_data);
      } }

    // The contract's io_base region [in|out|locals|arena] must hold the engine's carve (LITE_WASM_IO_TOTAL).
    // io_size is exported so an engine/contract size mismatch fails loudly here, not as silent over-carve.
    // (optional export: pre-io_size contracts skip the check and keep their matching layout.) Checked BEFORE
    // ms.release() so a mismatch frees the module cleanly instead of stranding the slot on freed handles.
    { wasm_function_inst_t f_io_size = wasm_runtime_lookup_function(inst, "io_size");
      if (f_io_size && liteWasmCallU32(env, f_io_size) < LITE_WASM_IO_TOTAL) {
          logToConsole(L"LITEWASM: contract io region too small for the engine carve (rebuild the contract)");
          return false;
      } }

    // All validation passed: hand the module set to the slot (ms no longer owns it).
    s.mod = ms.mod; s.inst = ms.inst; s.env = ms.env; s.dispatchFn = f_dispatch; s.arenaTop = arenaTop;
    ms.release();

    // Release the slot's reserve (once) and point contractStates[idx] AT the instance's resident state region.
    // Route through the adapter so the free matches the alloc: engine = flush+abandon (memfd, never freed);
    // demand-zero mac/win = abandon the mmap stub (NEVER freePool — free() on a non-malloc pointer aborts on
    // macOS); plain = freePool the committed pool. (The old #else freePool'd the mmap stub -> darwin abort.)
    if (!s.stubFreed) { liteSCOnWasmTakeover(idx); s.stubFreed = true; }
    contractStates[idx] = (unsigned char*)wasm_runtime_addr_app_to_native(inst, s.stateOff);

    // Migration metadata (optional exports — pre-migration contracts have none). Patch the native migrate table
    // with a 4-pointer closure so a redeploy runs __migrate via the same QpiContextMigrateProcedureCall path
    // native contracts use. Cleared first so a non-migrate upgrade resets it.
    s.hasMigrate = false; s.migrateOldStateSize = 0; s.migrateLocalsSize = 0;
    contractMigrateProcedures[idx] = nullptr; contractMigrateOldStateSizes[idx] = 0; contractMigrateLocalsSizes[idx] = 0;
    { wasm_function_inst_t f_hasMig = wasm_runtime_lookup_function(inst, "has_migrate");
      if (f_hasMig && liteWasmCallU32(env, f_hasMig)) {
          wasm_function_inst_t f_migOld = wasm_runtime_lookup_function(inst, "migrate_old_state_size");
          wasm_function_inst_t f_migLoc = wasm_runtime_lookup_function(inst, "migrate_locals_size");
          s.migrateOldStateSize = f_migOld ? liteWasmCallU32(env, f_migOld) : 0;
          s.migrateLocalsSize   = f_migLoc ? liteWasmCallU32(env, f_migLoc) : 0;
          s.migBind = { idx, 0, (uint8_t)3 };
          void* code = nullptr;
          ffi_closure* cl = (ffi_closure*)ffi_closure_alloc(sizeof(ffi_closure), &code);
          if (cl && ffi_prep_closure_loc(cl, &g_liteWasmMigrateCif, liteWasmMigrateClosureHandler, &s.migBind, code) == FFI_OK) {
              s.migClosure = cl; s.hasMigrate = true;
              contractMigrateProcedures[idx]    = (MIGRATE_PROCEDURE)code;
              contractMigrateOldStateSizes[idx] = s.migrateOldStateSize;
              contractMigrateLocalsSizes[idx]   = s.migrateLocalsSize;
          } else if (cl) { ffi_closure_free(cl); logToConsole(L"LITEWASM: migrate closure alloc failed"); }
      } }

    if (prevState.p) {   // upgrade path
        if (s.hasMigrate && s.migrateOldStateSize == prevStateSize) {
            // defer to the framed construct step: stash the old bytes (steal from RAII), run __migrate there.
            s.pendingOldState = prevState.p; s.pendingOldStateSize = prevStateSize; prevState.p = nullptr;
            logColorToScreen("INFO", "LITEWASM: migrate pending — old state " + std::to_string(prevStateSize) + " bytes");
        } else {   // no migrate (or declared OldStateData size mismatch): preserve the overlap, as before
            uint32_t n = prevStateSize < s.stateSize ? prevStateSize : s.stateSize;
            copyMem(contractStates[idx], prevState.p, n);
            if (s.migrateOldStateSize && s.migrateOldStateSize != prevStateSize)
                logColorToScreen("WARNING", "LITEWASM: migrate OldStateData size " + std::to_string(s.migrateOldStateSize)
                                 + " != live state " + std::to_string(prevStateSize) + " — raw-preserved instead");
            else
                logColorToScreen("INFO", "LITEWASM: state preserved across upgrade — " + std::to_string(n) + " bytes");
        }
    }
    { wasm_function_inst_t f_ctx = wasm_runtime_lookup_function(inst, "ctx_addr"); if (f_ctx) s.ctxOff = liteWasmCallU32(env, f_ctx); }
    s.entryCount = liteWasmCallU32(env, f_reg_count);
    if (s.entryCount > LITE_MAX_USER_ENTRIES) s.entryCount = LITE_MAX_USER_ENTRIES;
    logColorToScreen("INFO", "LITEWASM: loaded contract — " + std::to_string(s.entryCount) + " entries, stateSize=" + std::to_string(s.stateSize));

    // reg_info(k, outOff) writes EntryInfo{inputType,kind,inSize,outSize} into linear mem; reuse ioBase as scratch.
    struct EntryInfo { uint32_t inputType, kind, inSize, outSize; };
    for (uint32_t k = 0; k < s.entryCount; k++) {
        uint32_t a[2] = { k, s.ioBase };
        wasm_runtime_call_wasm(env, f_reg_info, 2, a);
        EntryInfo* ei = (EntryInfo*)wasm_runtime_addr_app_to_native(inst, s.ioBase);
        uint16_t it = (uint16_t)ei->inputType;
        s.binds[k] = { idx, it, (uint8_t)ei->kind };

        void* code = nullptr;
        ffi_closure* cl = (ffi_closure*)ffi_closure_alloc(sizeof(ffi_closure), &code);
        if (!cl || ffi_prep_closure_loc(cl, &g_liteWasmDispatchCif, liteWasmClosureHandler, &s.binds[k], code) != FFI_OK) {
            logToConsole(L"LITEWASM: closure alloc failed");
            continue;
        }
        s.closures[k] = cl;

        if (ei->kind == LITE_KIND_FUNCTION) {
            contractUserFunctions[idx][it] = (USER_FUNCTION)code;
            contractUserFunctionInputSizes[idx][it]  = (uint16_t)ei->inSize;
            contractUserFunctionOutputSizes[idx][it] = (uint16_t)ei->outSize;
            contractUserFunctionLocalsSizes[idx][it] = 0;
        } else {
            contractUserProcedures[idx][it] = (USER_PROCEDURE)code;
            contractUserProcedureInputSizes[idx][it]  = (uint16_t)ei->inSize;
            contractUserProcedureOutputSizes[idx][it] = (uint16_t)ei->outSize;
            contractUserProcedureLocalsSizes[idx][it] = 0;

            // Async oracle replies are delivered by full synthetic procId (CONTRACT_INDEX<<22 | __LINE__) through
            // userProcedureRegistry (qubic.cpp), where native contracts register via
            // REGISTER_USER_PROCEDURE_NOTIFICATION. Wasm procs otherwise live only in contractUserProcedures[][],
            // so a wasm contract's notification procedure would be unreachable. Register every wasm procedure here
            // (procedure = the same dispatch closure): notification procs become reachable; non-notification procs
            // are harmless extras (never queried by the oracle). `it` is the low-16 of the procId (== __LINE__ for
            // notification procs) and is unique per contract, so the reconstructed id is unique. idx == the arm
            // slot == the contract's baked CONTRACT_INDEX, so the id matches what QUERY_ORACLE computed.
            if (userProcedureRegistry) {
                const unsigned int fullProcId = (idx << 22) | it;
                userProcedureRegistry->add(fullProcId, { (USER_PROCEDURE)code, idx, 0u, (uint16_t)ei->inSize, (uint16_t)ei->outSize });
            }
        }
    }

    // system procedures (lifecycle): patch contractSystemProcedures[idx][sp] with a closure (kind=2) for each
    // bit the contract reports. INITIALIZE then runs via the normal construct path; begin/end tick+epoch via processTick.
    wasm_function_inst_t f_sysmask = wasm_runtime_lookup_function(inst, "reg_sysproc_mask");
    wasm_function_inst_t f_syslocals = wasm_runtime_lookup_function(inst, "sysproc_locals_size");
    wasm_function_inst_t f_sysin = wasm_runtime_lookup_function(inst, "sysproc_in_size");
    wasm_function_inst_t f_sysout = wasm_runtime_lookup_function(inst, "sysproc_out_size");
    auto callSp = [&](wasm_function_inst_t fn, uint32_t sp) -> uint32_t { if (!fn) return 0; uint32_t a[1] = { sp }; wasm_runtime_call_wasm(env, fn, 1, a); return a[0]; };
    if (f_sysmask) {
        uint32_t mask = liteWasmCallU32(env, f_sysmask);
        for (uint32_t sp = 0; sp < LITE_SP_COUNT; sp++) {
            if (!(mask & (1u << sp))) continue;
            s.sysBinds[sp] = { idx, (uint16_t)sp, (uint8_t)LITE_WASM_KIND_SYSPROC };
            void* code = nullptr;
            ffi_closure* cl = (ffi_closure*)ffi_closure_alloc(sizeof(ffi_closure), &code);
            if (!cl || ffi_prep_closure_loc(cl, &g_liteWasmDispatchCif, liteWasmClosureHandler, &s.sysBinds[sp], code) != FFI_OK) continue;
            s.sysClosures[sp] = cl;
            contractSystemProcedures[idx][sp] = (SYSTEM_PROCEDURE)code;
            contractSystemProcedureLocalsSizes[idx][sp] = (uint16_t)callSp(f_syslocals, sp);
            s.sysInSize[sp]  = (uint16_t)callSp(f_sysin, sp);
            s.sysOutSize[sp] = (uint16_t)callSp(f_sysout, sp);
        }
    }

    s.loaded = true;
    logColorToScreen("INFO", "LITEWASM: slot loaded (" + std::to_string(s.entryCount) + " user entries)");
    return true;
}

// True if a redeploy stashed old state for this slot to migrate (set in load, consumed at construct).
static inline bool liteWasmHasPendingMigrate(unsigned int idx) {
    int l = liteWasmSlotLocal(idx);
    return l >= 0 && g_liteWasmSlots[l].pendingOldState != nullptr;
}
// Run the deferred migration under the native migrate context (locals stack + state lock), then free the stash.
// The context calls contractMigrateProcedures[idx] -> the migrate closure -> liteWasmDispatch(kind=3).
[[maybe_unused]] static void liteWasmRunPendingMigrate(unsigned int idx) {
    int l = liteWasmSlotLocal(idx);
    if (l < 0 || !g_liteWasmSlots[l].pendingOldState) return;
    LiteWasmSlot& s = g_liteWasmSlots[l];
    QpiContextMigrateProcedureCall mctx(idx);
    mctx.call(s.pendingOldState);
    free(s.pendingOldState); s.pendingOldState = nullptr; s.pendingOldStateSize = 0;
    logColorToScreen("INFO", "LITEWASM: migrate complete idx=" + std::to_string(idx));
}

// One-time WAMR bring-up at node boot (WASM_CONTRACTS.md §13.2). Registers the QPI import table (module
// "lhost") + prepares the 5-pointer libffi dispatch CIF used by the per-(idx,it) closures (§13.3).
[[maybe_unused]] static bool liteWasmRuntimeInit()
{
    if (g_liteWasmReady) return true;

    for (int i = 0; i < 5; i++) g_liteWasmCifArgs[i] = &ffi_type_pointer;
    if (ffi_prep_cif(&g_liteWasmDispatchCif, FFI_DEFAULT_ABI, 5, &ffi_type_void, g_liteWasmCifArgs) != FFI_OK) {
        logToConsole(L"LITEWASM: libffi cif prep failed");
        return false;
    }
    for (int i = 0; i < 4; i++) g_liteWasmMigrateCifArgs[i] = &ffi_type_pointer;
    if (ffi_prep_cif(&g_liteWasmMigrateCif, FFI_DEFAULT_ABI, 4, &ffi_type_void, g_liteWasmMigrateCifArgs) != FFI_OK) {
        logToConsole(L"LITEWASM: migrate cif prep failed");
        return false;
    }

    // System allocator: each instance mallocs its own linear memory, so big-state contracts (QX ~593MB,
    // QEARN ~204MB) aren't capped by a fixed pool. (The earlier load crash was the const load buffer, since
    // fixed; a static pool can't hold hundreds of MB.)
    RuntimeInitArgs args;
    setMem(&args, sizeof(args), 0);
    args.mem_alloc_type = Alloc_With_System_Allocator;
    args.native_module_name = "lhost";
    args.native_symbols = g_liteWasmNatives;
    args.n_native_symbols = (int)g_liteWasmNativesCount;
    if (!wasm_runtime_full_init(&args)) {
        logToConsole(L"LITEWASM: WAMR init failed");
        return false;
    }

    g_liteWasmReady = true;
    logToConsole(L"LITEWASM: runtime ready (WAMR + libffi)");
    return true;
}

#endif // LITE_WASM_CONTRACTS

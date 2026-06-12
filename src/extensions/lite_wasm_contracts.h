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
#include "extensions/lite_wasm_imports.h"   // g_liteWasmNatives[] + LiteWasmCallCtx -> g_liteHostServices (+ debug trace)

void logColorToScreen(std::string type, std::string msg);   // defined later in qubic.cpp (same TU)

// Per-call scratch layout inside the contract's exported io_base region: [in | out | locals | arena].
// Sized to the core protocol caps: in/out = uint16 max (registered inputSize/outputSize); locals =
// MAX_SIZE_OF_CONTRACT_LOCALS; arena = the native scratchpad (defaultCommonBuffersSize ~1GB) so any QPI
// HashMap reorg fits. MUST match g_wasmIo in lite_wasm_tu.h.
#define LITE_WASM_IN_SZ     (64u * 1024u)            // input  region (>= uint16 max 65535)
#define LITE_WASM_OUT_SZ    (64u * 1024u)            // output region (>= uint16 max 65535)
#define LITE_WASM_LOCALS_SZ (32u * 1024u)            // = MAX_SIZE_OF_CONTRACT_LOCALS
#define LITE_WASM_ARENA_SZ  (1024u * 1024u * 1024u)  // acquireScratch bump arena (matches native scratchpad)
#define LITE_WASM_IO_TOTAL  ((unsigned long long)LITE_WASM_IN_SZ + LITE_WASM_OUT_SZ + LITE_WASM_LOCALS_SZ + LITE_WASM_ARENA_SZ)

static bool    g_liteWasmReady = false;
static ffi_cif g_liteWasmDispatchCif;                 // shared 5-pointer->void CIF for every dispatch closure
static ffi_type* g_liteWasmCifArgs[5];

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
    uint32_t             entryCount = 0;
    LiteWasmEntryBind    binds[LITE_MAX_USER_ENTRIES] = {};
    ffi_closure*         closures[LITE_MAX_USER_ENTRIES] = {};
    LiteWasmEntryBind    sysBinds[LITE_SP_COUNT] = {};       // system procedures, kind=2, it=LITE_SP_*
    ffi_closure*         sysClosures[LITE_SP_COUNT] = {};
    uint16_t             sysInSize[LITE_SP_COUNT] = {};      // QPI-defined in/out sizes per sysproc (share-mgmt)
    uint16_t             sysOutSize[LITE_SP_COUNT] = {};
    bool                 stubFreed = false;       // set once the slot's 1GB reserve is freed + contractStates[idx] aliased to the resident state
    std::string          lastTrap;                // reason of the most recent dispatch trap (cleared on success); surfaced via dyn-registry
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

    uint16_t inSize, outSize;
    if (kind == LITE_KIND_FUNCTION)            { inSize = contractUserFunctionInputSizes[idx][it];  outSize = contractUserFunctionOutputSizes[idx][it]; }
    else if (kind == LITE_WASM_KIND_SYSPROC)   { inSize = s.sysInSize[it]; outSize = s.sysOutSize[it]; }   // QPI sysproc in/out
    else                                       { inSize = contractUserProcedureInputSizes[idx][it]; outSize = contractUserProcedureOutputSizes[idx][it]; }

    // guard: input/output must fit their io regions before we copy into linear memory. Registered sizes are
    // uint16 (<=65535 < 64K) so this never fires today — defense against a tighter region / wider size type.
    if (inSize > LITE_WASM_IN_SZ || outSize > LITE_WASM_OUT_SZ) {
        logColorToScreen("ERROR", "LITEWASM dispatch in/out exceeds io region idx=" + std::to_string(idx)
                         + " in=" + std::to_string(inSize) + " out=" + std::to_string(outSize));
        return;
    }

    // env selection: outermost uses the slot env; nested reuses the thread's current env + set_module_inst.
    wasm_exec_env_t env;
    bool outer;
    wasm_module_inst_t savedInst = nullptr;
    void* savedUD = nullptr;
    if (t_liteWasmCurEnv) {
        env = t_liteWasmCurEnv;
        savedInst = wasm_runtime_get_module_inst(env);
        savedUD   = wasm_runtime_get_user_data(env);
        wasm_runtime_set_module_inst(env, s.inst);
        outer = false;
    } else {
        // outermost call on this thread: WAMR exec_envs are thread-bound, and the slot's load-time env
        // belongs to the deploy thread. Init this thread's wasm env + use a fresh exec_env on it.
        liteWasmEnsureThreadEnv();
        env = wasm_runtime_create_exec_env(s.inst, 64 * 1024);
        if (!env) return;
        t_liteWasmCurEnv = env;
        outer = true;
    }

    const uint32_t wIn     = s.ioBase;
    const uint32_t wOut    = s.ioBase + LITE_WASM_IN_SZ;
    const uint32_t wLocals = s.ioBase + LITE_WASM_IN_SZ + LITE_WASM_OUT_SZ;
    const uint32_t wArena  = s.ioBase + LITE_WASM_IN_SZ + LITE_WASM_OUT_SZ + LITE_WASM_LOCALS_SZ;

    LiteWasmCallCtx cc;
    cc.ctx = ctx;
    cc.arenaBase = wArena; cc.arenaBump = wArena; cc.arenaEnd = wArena + LITE_WASM_ARENA_SZ;
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

    // ctx base in: the contract's inline qpi.h accessors (invocationReward/invocator/originator/contractIndex)
    // read these fields directly. QpiContext has no pointers/vtable + m256i is align-8 -> layout is identical
    // wasm32/x64, so a raw copy of the base populates them.
    if (ctx && s.ctxOff) copyMem(wasm_runtime_addr_app_to_native(s.inst, s.ctxOff), ctx, sizeof(QPI::QpiContext));

    // input in + zero output. State is not passed in: it's resident in the wasm linear memory (contractStates[idx]
    // aliases it) and the contract mutates it in place.
    if (inSize) copyMem(wasm_runtime_addr_app_to_native(s.inst, wIn), input, inSize);
    setMem(wasm_runtime_addr_app_to_native(s.inst, wOut), outSize ? outSize : 1, 0);

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

    // output out; state is resident (nothing to copy out). Refresh contractStates[idx] in case the linear-mem
    // base moved (memory.grow), then flag dirty so the digest re-hashes.
    if (outSize) copyMem(output, wasm_runtime_addr_app_to_native(s.inst, wOut), outSize);
    contractStates[idx] = (unsigned char*)wasm_runtime_addr_app_to_native(s.inst, s.stateOff);
    if (kind != LITE_KIND_FUNCTION) g_liteHostServices.markDirty(idx);   // procedures + system procedures write state

    if (dbg) {   // finish + publish the debug trace entry (output + timing + trap; state diff already built)
        te.ok = s.lastTrap.empty(); te.trap = s.lastTrap;
        te.execNs = (unsigned long long)std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - t0).count();
        if (outSize) copyMem(te.outHead, wasm_runtime_addr_app_to_native(s.inst, wOut), outSize < LITE_WASM_TRACE_HEAD ? outSize : LITE_WASM_TRACE_HEAD);
        cc.trace = nullptr;
        liteWasmTraceCommit(te);
    }

    if (outer) { wasm_runtime_set_user_data(env, nullptr); wasm_runtime_destroy_exec_env(env); t_liteWasmCurEnv = nullptr; }
    else       { wasm_runtime_set_user_data(env, savedUD); wasm_runtime_set_module_inst(env, savedInst); }
}

// libffi closure trampoline: core calls it as a native USER_FUNCTION/USER_PROCEDURE; we recover (idx,it,kind)
// from the bound user_data and forward to liteWasmDispatch. args[] = the 5 ptrs (ctx,state,input,output,locals).
static void liteWasmClosureHandler(ffi_cif*, void* /*ret(void)*/, void** args, void* user)
{
    LiteWasmEntryBind* b = (LiteWasmEntryBind*)user;
    liteWasmDispatch(b->idx, b->it, b->kind,
                     *(const void**)args[0], *(void**)args[1], *(void**)args[2], *(void**)args[3], *(void**)args[4]);
}

// Release a slot's loaded instance + closures (before reloading the slot, so a redeploy doesn't leak the
// instance's linear memory or the libffi trampolines).
static void liteWasmSlotUnload(LiteWasmSlot& s)
{
    if (s.env)  { wasm_runtime_destroy_exec_env(s.env); s.env = nullptr; }
    if (s.inst) { wasm_runtime_deinstantiate(s.inst);   s.inst = nullptr; }
    if (s.mod)  { wasm_runtime_unload(s.mod);            s.mod = nullptr; }
    for (uint32_t k = 0; k < s.entryCount; k++)        if (s.closures[k])    { ffi_closure_free(s.closures[k]);    s.closures[k] = nullptr; }
    for (uint32_t sp = 0; sp < LITE_SP_COUNT; sp++)    if (s.sysClosures[sp]) { ffi_closure_free(s.sysClosures[sp]); s.sysClosures[sp] = nullptr; }
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
    unsigned char* prevState = nullptr; uint32_t prevStateSize = 0;
    if (s.inst && s.stubFreed && s.stateSize && contractStates[idx]) {
        prevStateSize = s.stateSize;
        prevState = (unsigned char*)malloc(prevStateSize);
        if (prevState) copyMem(prevState, contractStates[idx], prevStateSize);
    }
    if (s.inst) liteWasmSlotUnload(s);   // redeploy into a live slot: free the prior instance first

    // WAMR modifies the load buffer in place and references it for the module's life -> own a mutable copy.
    if (s.wasmBuf) { free(s.wasmBuf); s.wasmBuf = nullptr; }
    s.wasmBuf = (unsigned char*)malloc(len);
    if (!s.wasmBuf) { logToConsole(L"LITEWASM: oom"); return false; }
    copyMem(s.wasmBuf, bytes, len);

    char err[192];
    wasm_module_t mod = wasm_runtime_load(s.wasmBuf, len, err, sizeof(err));
    if (!mod) { logToConsole(L"LITEWASM: load failed"); free(s.wasmBuf); s.wasmBuf = nullptr; return false; }
    wasm_module_inst_t inst = wasm_runtime_instantiate(mod, 64 * 1024, 1024 * 1024, err, sizeof(err));
    if (!inst) { logToConsole(L"LITEWASM: instantiate failed"); wasm_runtime_unload(mod); return false; }
    wasm_exec_env_t env = wasm_runtime_create_exec_env(inst, 64 * 1024);

    wasm_function_inst_t f_state_addr = wasm_runtime_lookup_function(inst, "state_addr");
    wasm_function_inst_t f_state_size = wasm_runtime_lookup_function(inst, "state_size");
    wasm_function_inst_t f_io_base    = wasm_runtime_lookup_function(inst, "io_base");
    wasm_function_inst_t f_reg_count  = wasm_runtime_lookup_function(inst, "reg_count");
    wasm_function_inst_t f_reg_info   = wasm_runtime_lookup_function(inst, "reg_info");
    wasm_function_inst_t f_dispatch   = wasm_runtime_lookup_function(inst, "dispatch");
    if (!f_state_addr || !f_state_size || !f_io_base || !f_reg_count || !f_reg_info || !f_dispatch) {
        logToConsole(L"LITEWASM: missing required export");
        wasm_runtime_destroy_exec_env(env); wasm_runtime_deinstantiate(inst); wasm_runtime_unload(mod);
        return false;
    }

    s.mod = mod; s.inst = inst; s.env = env; s.dispatchFn = f_dispatch;
    s.stateOff = liteWasmCallU32(env, f_state_addr);
    s.stateSize = liteWasmCallU32(env, f_state_size);
    s.ioBase = liteWasmCallU32(env, f_io_base);

    // The contract's io_base region [in|out|locals|arena] must hold the engine's carve (LITE_WASM_IO_TOTAL).
    // io_size is exported so an engine/contract size mismatch fails loudly here, not as silent over-carve.
    // (optional export: pre-io_size contracts skip the check and keep their matching layout.)
    { wasm_function_inst_t f_io_size = wasm_runtime_lookup_function(inst, "io_size");
      if (f_io_size && liteWasmCallU32(env, f_io_size) < LITE_WASM_IO_TOTAL) {
          logToConsole(L"LITEWASM: contract io region too small for the engine carve (rebuild the contract)");
          wasm_runtime_destroy_exec_env(env); wasm_runtime_deinstantiate(inst); wasm_runtime_unload(mod);
          return false;
      } }

    // Release the slot's reserve (once) and point contractStates[idx] AT the instance's resident state region.
    // Route through the adapter so the free matches the alloc: engine = flush+abandon (memfd, never freed);
    // demand-zero mac/win = abandon the mmap stub (NEVER freePool — free() on a non-malloc pointer aborts on
    // macOS); plain = freePool the committed pool. (The old #else freePool'd the mmap stub -> darwin abort.)
    if (!s.stubFreed) { liteSCOnWasmTakeover(idx); s.stubFreed = true; }
    contractStates[idx] = (unsigned char*)wasm_runtime_addr_app_to_native(inst, s.stateOff);
    if (prevState) {   // upgrade: restore the snapshot (copy the overlap — a new layout may differ in size)
        uint32_t n = prevStateSize < s.stateSize ? prevStateSize : s.stateSize;
        copyMem(contractStates[idx], prevState, n);
        free(prevState);
        logColorToScreen("INFO", "LITEWASM: state preserved across upgrade — " + std::to_string(n) + " bytes");
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

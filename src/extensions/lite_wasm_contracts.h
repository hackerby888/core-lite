#pragma once
// WASM-executed contracts (testnet, experimental). Second contract backend beside the native .so engine
// (lite_dynamic_contracts.h); wasm is the default deploy target, native .so the opt-in escape hatch.
// Embeds WAMR (one persistent instance per slot) + uses libffi closures to drop a per-(idx,it) trampoline
// into the core's contractUser{Functions,Procedures}[idx][it] tables, so core dispatch (contract_exec.h)
// stays byte-identical to upstream. State is canonical in contractStates[idx] (v1: copy in/out per call).
// See extensions/WASM_CONTRACTS.md §13.
#ifdef LITE_WASM_CONTRACTS

#if !defined(LITE_DYNAMIC_CONTRACTS)
#error "LITE_WASM_CONTRACTS requires LITE_DYNAMIC_CONTRACTS (shares the host vtable + deploy/slot machinery)"
#endif

#include <ffi.h>
#include <string>
#include "wasm_export.h"
#include "extensions/lite_wasm_imports.h"   // g_liteWasmNatives[] + LiteWasmCallCtx -> g_liteHostServices

void logColorToScreen(std::string type, std::string msg);   // defined later in qubic.cpp (same TU)

// Per-call scratch layout inside the contract's exported io_base region: [in | out | locals | arena].
#define LITE_WASM_IO_SLOT   (32u * 1024u)   // in / out / locals each — >= MAX_SIZE_OF_CONTRACT_LOCALS (32K)
#define LITE_WASM_ARENA_SZ  (16u * 1024u)   // acquireScratch bump arena
#define LITE_WASM_IO_TOTAL  (3u * LITE_WASM_IO_SLOT + LITE_WASM_ARENA_SZ)

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
static inline uint32_t liteWasmCallU32(wasm_exec_env_t env, wasm_function_inst_t fn) {
    uint32_t a[1] = { 0 }; wasm_runtime_call_wasm(env, fn, 0, a); return a[0];
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
    (void)locals;
    int local = liteWasmSlotLocal(idx);
    if (local < 0) return;
    LiteWasmSlot& s = g_liteWasmSlots[local];
    if (!s.loaded) return;

    uint16_t inSize, outSize;
    if (kind == LITE_KIND_FUNCTION)            { inSize = contractUserFunctionInputSizes[idx][it];  outSize = contractUserFunctionOutputSizes[idx][it]; }
    else if (kind == LITE_WASM_KIND_SYSPROC)   { inSize = s.sysInSize[it]; outSize = s.sysOutSize[it]; }   // QPI sysproc in/out
    else                                       { inSize = contractUserProcedureInputSizes[idx][it]; outSize = contractUserProcedureOutputSizes[idx][it]; }

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
    const uint32_t wOut    = s.ioBase + LITE_WASM_IO_SLOT;
    const uint32_t wLocals = s.ioBase + 2 * LITE_WASM_IO_SLOT;
    const uint32_t wArena  = s.ioBase + 3 * LITE_WASM_IO_SLOT;

    LiteWasmCallCtx cc;
    cc.ctx = ctx;
    cc.arenaBase = wArena; cc.arenaBump = wArena; cc.arenaEnd = wArena + LITE_WASM_ARENA_SZ;
    wasm_runtime_set_user_data(env, &cc);

    // ctx base in: the contract's inline qpi.h accessors (invocationReward/invocator/originator/contractIndex)
    // read these fields directly. QpiContext has no pointers/vtable + m256i is align-8 -> layout is identical
    // wasm32/x64, so a raw copy of the base populates them.
    if (ctx && s.ctxOff) copyMem(wasm_runtime_addr_app_to_native(s.inst, s.ctxOff), ctx, sizeof(QPI::QpiContext));

    // state in (v1: contractStates[idx] is canonical) + input in + zero output.
    void* st = wasm_runtime_addr_app_to_native(s.inst, s.stateOff);
    if (s.stateSize) copyMem(st, statePtr, s.stateSize);
    if (inSize)      copyMem(wasm_runtime_addr_app_to_native(s.inst, wIn), input, inSize);
    setMem(wasm_runtime_addr_app_to_native(s.inst, wOut), outSize ? outSize : 1, 0);

    uint32_t argv[5] = { kind, it, wIn, wOut, wLocals };
    if (!wasm_runtime_call_wasm(env, s.dispatchFn, 5, argv))
        logToConsole(L"LITEWASM: dispatch trap");

    // output out + state out (procedures write; functions are read-only).
    if (outSize) copyMem(output, wasm_runtime_addr_app_to_native(s.inst, wOut), outSize);
    if (kind != LITE_KIND_FUNCTION && s.stateSize) {   // procedures + system procedures write state
        copyMem(statePtr, st, s.stateSize);
        g_liteHostServices.markDirty(idx);
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

// Load a contract.wasm into a slot: instantiate, read its registration (reg_count/reg_info) + state/io
// exports, and patch the core dispatch tables with one libffi closure per entry. NOT consensus — local load.
[[maybe_unused]] static bool liteWasmLoadFromBytes(unsigned int idx, const unsigned char* bytes, unsigned int len)
{
    int local = liteWasmSlotLocal(idx);
    if (local < 0) return false;
    LiteWasmSlot& s = g_liteWasmSlots[local];

    liteWasmEnsureThreadEnv();   // load runs on a tick-processor thread (not main)

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

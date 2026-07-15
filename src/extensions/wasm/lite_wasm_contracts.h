#pragma once
// WAMR execution and libffi registration for runtime Wasm contracts.
#ifdef LITE_WASM_SC

#if !defined(TESTNET) || !defined(TESTNET_LITE_RAM)
#error "LITE_WASM_SC requires TESTNET and TESTNET_LITE_RAM"
#endif

#include <ffi.h>
#include <string>
#include <chrono>
#include "wasm_export.h"
#include "extensions/wasm/lite_wasm_arena.h"
#include "extensions/wasm/lite_wasm_imports.h"

void logColorToScreen(std::string type, std::string msg);

#ifndef LITE_WASM_ARENA_SZ
#define LITE_WASM_ARENA_SZ (1024u * 1024u * 1024u)
#endif

static constexpr uint32_t LITE_WASM_INPUT_CAPACITY = 64u * 1024u;
static constexpr uint32_t LITE_WASM_OUTPUT_CAPACITY = 64u * 1024u;
static constexpr uint32_t LITE_WASM_LOCALS_CAPACITY = 32u * 1024u;
static constexpr unsigned long long LITE_WASM_IO_CAPACITY =
    (unsigned long long)LITE_WASM_INPUT_CAPACITY
    + LITE_WASM_OUTPUT_CAPACITY
    + LITE_WASM_LOCALS_CAPACITY
    + LITE_WASM_ARENA_SZ;

static bool g_liteWasmReady = false;
static ffi_cif g_liteWasmDispatchCif;
static ffi_type* g_liteWasmCifArgs[5];
static ffi_cif g_liteWasmMigrateCif;
static ffi_type* g_liteWasmMigrateCifArgs[4];

struct LiteWasmEntryBind
{
    uint32_t contractIndex;
    uint16_t inputType;
    LiteWasmDispatchKind kind;
};

struct LiteWasmSlot
{
    bool loaded = false;
    wasm_module_t module = nullptr;
    wasm_module_inst_t instance = nullptr;
    wasm_exec_env_t loadExecEnv = nullptr;
    wasm_function_inst_t dispatchFunction = nullptr;
    unsigned char* moduleBuffer = nullptr;
    uint32_t stateOffset = 0;
    uint32_t stateSize = 0;
    uint32_t ioBaseOffset = 0;
    uint32_t contextOffset = 0;
    uint32_t* arenaTop = nullptr;
    uint32_t entryCount = 0;
    LiteWasmEntryBind entryBindings[LITE_MAX_USER_ENTRIES] = {};
    ffi_closure* entryClosures[LITE_MAX_USER_ENTRIES] = {};
    LiteWasmEntryBind systemBindings[LITE_SP_COUNT] = {};
    ffi_closure* systemClosures[LITE_SP_COUNT] = {};
    uint16_t systemInputSizes[LITE_SP_COUNT] = {};
    uint16_t systemOutputSizes[LITE_SP_COUNT] = {};
    bool stateStubReleased = false;
    std::string lastTrap;
    bool hasMigration = false;
    uint32_t migrationOldStateSize = 0;
    uint32_t migrationLocalsSize = 0;
    LiteWasmEntryBind migrationBinding = {};
    ffi_closure* migrationClosure = nullptr;
    unsigned char* pendingOldState = nullptr;
    uint32_t pendingOldStateSize = 0;
};

static LiteWasmSlot g_liteWasmSlots[LITE_DYN_SLOT_COUNT];

static inline int liteWasmSlotLocal(unsigned int contractIndex)
{
    const int slotOffset = (int)contractIndex - (int)liteDynSlotBase();
    if (slotOffset < 0 || slotOffset >= LITE_DYN_SLOT_COUNT)
    {
        return -1;
    }

    return slotOffset;
}

static inline bool liteWasmIsWasm(unsigned int contractIndex)
{
    const int slotOffset = liteWasmSlotLocal(contractIndex);
    return slotOffset >= 0 && g_liteWasmSlots[slotOffset].loaded;
}

static inline std::string liteWasmLastTrap(unsigned int contractIndex)
{
    const int slotOffset = liteWasmSlotLocal(contractIndex);
    if (slotOffset < 0 || !g_liteWasmSlots[slotOffset].loaded)
    {
        return std::string();
    }

    return g_liteWasmSlots[slotOffset].lastTrap;
}

static inline uint32_t liteWasmCallU32(
    wasm_exec_env_t execEnv,
    wasm_function_inst_t function)
{
    uint32_t arguments[1] = { 0 };

    wasm_runtime_call_wasm(execEnv, function, 0, arguments);
    return arguments[0];
}

static inline unsigned long long liteWasmEffectiveStateSize(
    unsigned int contractIndex,
    unsigned long long defaultSize)
{
    const int slotOffset = liteWasmSlotLocal(contractIndex);
    if (slotOffset < 0 || !g_liteWasmSlots[slotOffset].loaded)
    {
        return defaultSize;
    }

    return g_liteWasmSlots[slotOffset].stateSize;
}

static inline void liteWasmEnsureThreadEnv()
{
    if (!wasm_runtime_thread_env_inited())
    {
        wasm_runtime_init_thread_env();
    }
}

static thread_local wasm_exec_env_t t_liteWasmCurEnv = nullptr;
static thread_local uint32_t t_liteWasmSlotDepth[LITE_DYN_SLOT_COUNT] = {};

struct LiteWasmIoSizes
{
    uint16_t input = 0;
    uint16_t output = 0;
};

struct LiteWasmMemoryLayout
{
    uint32_t inputOffset;
    uint32_t outputOffset;
    uint32_t localsOffset;
    uint32_t arenaOffset;
};

static LiteWasmMemoryLayout liteWasmResolveMemoryLayout(const LiteWasmSlot& slot)
{
    LiteWasmMemoryLayout layout;

    layout.inputOffset = slot.ioBaseOffset;
    layout.outputOffset = layout.inputOffset + LITE_WASM_INPUT_CAPACITY;
    layout.localsOffset = layout.outputOffset + LITE_WASM_OUTPUT_CAPACITY;
    layout.arenaOffset = layout.localsOffset + LITE_WASM_LOCALS_CAPACITY;
    return layout;
}

static bool liteWasmResolveIO(
    uint32_t contractIndex,
    uint16_t inputType,
    LiteWasmDispatchKind kind,
    const LiteWasmSlot& slot,
    LiteWasmIoSizes& sizes)
{
    switch (kind)
    {
        case LiteWasmDispatchKind::UserFunction:
            sizes.input = contractUserFunctionInputSizes[contractIndex][inputType];
            sizes.output = contractUserFunctionOutputSizes[contractIndex][inputType];
            break;
        case LiteWasmDispatchKind::SystemProcedure:
            sizes.input = slot.systemInputSizes[inputType];
            sizes.output = slot.systemOutputSizes[inputType];
            break;
        case LiteWasmDispatchKind::UserProcedure:
            sizes.input = contractUserProcedureInputSizes[contractIndex][inputType];
            sizes.output = contractUserProcedureOutputSizes[contractIndex][inputType];
            break;
        case LiteWasmDispatchKind::Migration:
            return false;
    }

    if (sizes.input > LITE_WASM_INPUT_CAPACITY
        || sizes.output > LITE_WASM_OUTPUT_CAPACITY)
    {
        logColorToScreen(
            "ERROR",
            "LITEWASM dispatch in/out exceeds io region idx=" + std::to_string(contractIndex)
                + " in=" + std::to_string(sizes.input)
                + " out=" + std::to_string(sizes.output));
        return false;
    }

    return true;
}

struct LiteWasmEnvScope
{
    wasm_exec_env_t execEnv = nullptr;
    bool ownsExecEnv = false;
    bool ready = false;
    wasm_module_inst_t savedInstance = nullptr;
    void* savedUserData = nullptr;

    explicit LiteWasmEnvScope(const LiteWasmSlot& slot)
    {
        if (t_liteWasmCurEnv)
        {
            execEnv = t_liteWasmCurEnv;
            savedInstance = wasm_runtime_get_module_inst(execEnv);
            savedUserData = wasm_runtime_get_user_data(execEnv);
            wasm_runtime_set_module_inst(execEnv, slot.instance);
            ready = true;
            return;
        }

        liteWasmEnsureThreadEnv();
        execEnv = wasm_runtime_create_exec_env(slot.instance, 64 * 1024);
        if (!execEnv)
        {
            return;
        }

        t_liteWasmCurEnv = execEnv;
        ownsExecEnv = true;
        ready = true;
    }

    ~LiteWasmEnvScope()
    {
        if (!ready)
        {
            return;
        }

        if (ownsExecEnv)
        {
            wasm_runtime_set_user_data(execEnv, nullptr);
            wasm_runtime_destroy_exec_env(execEnv);
            t_liteWasmCurEnv = nullptr;
            return;
        }

        wasm_runtime_set_user_data(execEnv, savedUserData);
        wasm_runtime_set_module_inst(execEnv, savedInstance);
    }

    LiteWasmEnvScope(const LiteWasmEnvScope&) = delete;
    LiteWasmEnvScope& operator=(const LiteWasmEnvScope&) = delete;
};

static LiteWasmCallCtx liteWasmCreateCallContext(
    const void* context,
    uint32_t arenaBase,
    uint32_t arenaEnd)
{
    LiteWasmCallCtx callContext;

    callContext.ctx = context;
    callContext.arenaBase = arenaBase;
    callContext.arenaBump = arenaBase;
    callContext.arenaEnd = arenaEnd;
    return callContext;
}

static void liteWasmBindEnvironment(
    wasm_exec_env_t execEnv,
    LiteWasmCallCtx& callContext)
{
    wasm_runtime_set_user_data(execEnv, &callContext);
}

static void liteWasmPrepareMemory(
    const LiteWasmSlot& slot,
    const LiteWasmMemoryLayout& layout,
    const void* context,
    const void* input,
    const LiteWasmIoSizes& sizes)
{
    if (context && slot.contextOffset)
    {
        copyMem(
            wasm_runtime_addr_app_to_native(slot.instance, slot.contextOffset),
            context,
            sizeof(QPI::QpiContext));
    }

    if (sizes.input)
    {
        copyMem(
            wasm_runtime_addr_app_to_native(slot.instance, layout.inputOffset),
            input,
            sizes.input);
    }

    setMem(
        wasm_runtime_addr_app_to_native(slot.instance, layout.outputOffset),
        sizes.output ? sizes.output : 1,
        0);
}

static void liteWasmFinalizeMemory(
    const LiteWasmSlot& slot,
    const LiteWasmMemoryLayout& layout,
    uint32_t contractIndex,
    LiteWasmDispatchKind kind,
    void* output,
    const LiteWasmIoSizes& sizes)
{
    if (sizes.output)
    {
        copyMem(
            output,
            wasm_runtime_addr_app_to_native(slot.instance, layout.outputOffset),
            sizes.output);
    }

    contractStates[contractIndex] = (unsigned char*)wasm_runtime_addr_app_to_native(
        slot.instance,
        slot.stateOffset);

    if (kind != LiteWasmDispatchKind::UserFunction)
    {
        g_liteHostServices.markDirty(contractIndex);
    }
}

struct LiteWasmTraceState
{
    bool enabled = false;
    bool tracksWrites = false;
    LiteWasmTraceEntry entry;
    unsigned char* state = nullptr;
    std::chrono::steady_clock::time_point startedAt;
};

static void liteWasmBeginTrace(
    const LiteWasmSlot& slot,
    uint32_t contractIndex,
    uint16_t inputType,
    LiteWasmDispatchKind kind,
    const void* context,
    const void* input,
    const LiteWasmIoSizes& sizes,
    LiteWasmCallCtx& callContext,
    LiteWasmTraceState& trace)
{
    trace.enabled = liteWasmDebugEnabled();
    if (!trace.enabled)
    {
        return;
    }

    trace.tracksWrites = kind != LiteWasmDispatchKind::UserFunction;
    trace.entry.tick = g_liteHostServices.tick(context);
    trace.entry.contractIndex = contractIndex;
    trace.entry.inputType = inputType;
    trace.entry.kind = (unsigned char)kind;
    trace.entry.inputSize = sizes.input;
    trace.entry.outputSize = sizes.output;
    trace.entry.stateSize = slot.stateSize;

    if (sizes.input && input)
    {
        const unsigned int capturedSize =
            sizes.input < LITE_WASM_TRACE_HEAD ? sizes.input : LITE_WASM_TRACE_HEAD;
        copyMem(trace.entry.inputHead, input, capturedSize);
    }

    if (kind == LiteWasmDispatchKind::UserProcedure)
    {
        auto* procedureContext = (const QPI::QpiContextProcedureCall*)context;
        trace.entry.invocator = procedureContext->invocator();
        trace.entry.invocationReward = procedureContext->invocationReward();
    }

    callContext.trace = &trace.entry;
    trace.state = (unsigned char*)wasm_runtime_addr_app_to_native(
        slot.instance,
        slot.stateOffset);
    trace.startedAt = std::chrono::steady_clock::now();
}

static void liteWasmFinishTrace(
    const LiteWasmSlot& slot,
    const LiteWasmMemoryLayout& layout,
    const LiteWasmIoSizes& sizes,
    LiteWasmCallCtx& callContext,
    LiteWasmTraceState& trace)
{
    if (!trace.enabled)
    {
        return;
    }

    trace.entry.ok = slot.lastTrap.empty();
    trace.entry.trap = slot.lastTrap;
    trace.entry.executionNanoseconds =
        (unsigned long long)std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - trace.startedAt).count();

    if (sizes.output)
    {
        const unsigned int capturedSize =
            sizes.output < LITE_WASM_TRACE_HEAD ? sizes.output : LITE_WASM_TRACE_HEAD;
        copyMem(
            trace.entry.outputHead,
            wasm_runtime_addr_app_to_native(slot.instance, layout.outputOffset),
            capturedSize);
    }

    callContext.trace = nullptr;
    liteWasmTraceCommit(trace.entry);
}

static bool liteWasmInvoke(
    LiteWasmSlot& slot,
    wasm_exec_env_t execEnv,
    LiteWasmDispatchKind kind,
    uint16_t inputType,
    uint32_t inputOffset,
    uint32_t outputOffset,
    uint32_t localsOffset)
{
    uint32_t arguments[5] = {
        (uint32_t)kind,
        inputType,
        inputOffset,
        outputOffset,
        localsOffset,
    };

    return wasm_runtime_call_wasm(execEnv, slot.dispatchFunction, 5, arguments);
}

static void liteWasmHandleDispatchResult(
    LiteWasmSlot& slot,
    uint32_t contractIndex,
    uint16_t inputType,
    LiteWasmDispatchKind kind,
    bool succeeded)
{
    if (succeeded)
    {
        slot.lastTrap.clear();
        return;
    }

    const char* exception = wasm_runtime_get_exception(slot.instance);
    slot.lastTrap = std::string("it=") + std::to_string(inputType)
        + " kind=" + std::to_string((unsigned int)kind)
        + (exception ? std::string(" — ") + exception : std::string(" — trap"));

    // WAMR prints the original Wasm offsets before unwinding so Qinit can map them through DWARF.
    logColorToScreen(
        "ERROR",
        "LITEWASM dispatch trap idx=" + std::to_string(contractIndex) + " " + slot.lastTrap);
    wasm_runtime_clear_exception(slot.instance);
}

static void liteWasmHandleMigrationResult(
    LiteWasmSlot& slot,
    uint32_t contractIndex,
    bool succeeded)
{
    if (succeeded)
    {
        slot.lastTrap.clear();
        return;
    }

    const char* exception = wasm_runtime_get_exception(slot.instance);
    slot.lastTrap = std::string("MIGRATE")
        + (exception ? std::string(" — ") + exception : std::string(" — trap"));
    logColorToScreen(
        "ERROR",
        "LITEWASM migrate trap idx=" + std::to_string(contractIndex) + " " + slot.lastTrap);
    wasm_runtime_clear_exception(slot.instance);
}

static void liteWasmDispatchMigration(
    uint32_t contractIndex,
    int slotOffset,
    LiteWasmSlot& slot,
    const void* context,
    const void* oldState)
{
    const uint32_t oldStateSize = slot.migrationOldStateSize;
    if (oldStateSize > LITE_WASM_ARENA_SZ)
    {
        logColorToScreen(
            "ERROR",
            "LITEWASM migrate old-state exceeds arena idx=" + std::to_string(contractIndex));
        return;
    }

    LiteWasmEnvScope environment(slot);
    if (!environment.ready)
    {
        return;
    }

    const LiteWasmMemoryLayout layout = liteWasmResolveMemoryLayout(slot);
    const uint32_t migrationArenaBase =
        layout.arenaOffset + ((oldStateSize + 15u) & ~15u);
    LiteWasmCallCtx callContext = liteWasmCreateCallContext(
        context,
        migrationArenaBase,
        layout.arenaOffset + LITE_WASM_ARENA_SZ);
    LiteWasmArenaScope arenaScope(
        t_liteWasmSlotDepth[slotOffset],
        slot.arenaTop,
        migrationArenaBase);

    liteWasmBindEnvironment(environment.execEnv, callContext);

    if (context && slot.contextOffset)
    {
        copyMem(
            wasm_runtime_addr_app_to_native(slot.instance, slot.contextOffset),
            context,
            sizeof(QPI::QpiContext));
    }

    if (oldStateSize)
    {
        copyMem(
            wasm_runtime_addr_app_to_native(slot.instance, layout.arenaOffset),
            oldState,
            oldStateSize);
    }

    setMem(
        wasm_runtime_addr_app_to_native(slot.instance, slot.stateOffset),
        slot.stateSize,
        0);
    setMem(
        wasm_runtime_addr_app_to_native(slot.instance, layout.localsOffset),
        LITE_WASM_LOCALS_CAPACITY,
        0);

    const bool succeeded = liteWasmInvoke(
        slot,
        environment.execEnv,
        LiteWasmDispatchKind::Migration,
        0,
        layout.arenaOffset,
        0,
        layout.localsOffset);
    liteWasmHandleMigrationResult(slot, contractIndex, succeeded);

    contractStates[contractIndex] = (unsigned char*)wasm_runtime_addr_app_to_native(
        slot.instance,
        slot.stateOffset);
    g_liteHostServices.markDirty(contractIndex);
}

static void liteWasmDispatch(
    uint32_t contractIndex,
    uint16_t inputType,
    LiteWasmDispatchKind kind,
    const void* context,
    void* statePointer,
    void* input,
    void* output,
    void* locals)
{
    (void)statePointer;
    (void)locals;

    const int slotOffset = liteWasmSlotLocal(contractIndex);
    if (slotOffset < 0)
    {
        return;
    }

    LiteWasmSlot& slot = g_liteWasmSlots[slotOffset];
    if (!slot.loaded)
    {
        return;
    }

    if (kind == LiteWasmDispatchKind::Migration)
    {
        liteWasmDispatchMigration(contractIndex, slotOffset, slot, context, input);
        return;
    }

    LiteWasmIoSizes sizes;
    if (!liteWasmResolveIO(contractIndex, inputType, kind, slot, sizes))
    {
        return;
    }

    LiteWasmEnvScope environment(slot);
    if (!environment.ready)
    {
        return;
    }

    const LiteWasmMemoryLayout layout = liteWasmResolveMemoryLayout(slot);
    LiteWasmCallCtx callContext = liteWasmCreateCallContext(
        context,
        layout.arenaOffset,
        layout.arenaOffset + LITE_WASM_ARENA_SZ);
    LiteWasmArenaScope arenaScope(
        t_liteWasmSlotDepth[slotOffset],
        slot.arenaTop,
        layout.arenaOffset);

    liteWasmBindEnvironment(environment.execEnv, callContext);
    LiteWasmTraceState trace;
    liteWasmBeginTrace(
        slot,
        contractIndex,
        inputType,
        kind,
        context,
        input,
        sizes,
        callContext,
        trace);
    liteWasmPrepareMemory(slot, layout, context, input, sizes);

    LiteWasmPageProtectionScope pageProtection(
        trace.tracksWrites,
        trace.state,
        slot.stateSize);
    const bool succeeded = liteWasmInvoke(
        slot,
        environment.execEnv,
        kind,
        inputType,
        layout.inputOffset,
        layout.outputOffset,
        layout.localsOffset);
    liteWasmHandleDispatchResult(slot, contractIndex, inputType, kind, succeeded);
    pageProtection.finish(trace.entry);

    liteWasmFinalizeMemory(slot, layout, contractIndex, kind, output, sizes);
    liteWasmFinishTrace(slot, layout, sizes, callContext, trace);
}

static void liteWasmClosureHandler(ffi_cif*, void*, void** arguments, void* userData)
{
    LiteWasmEntryBind* binding = (LiteWasmEntryBind*)userData;
    liteWasmDispatch(
        binding->contractIndex,
        binding->inputType,
        binding->kind,
        *(const void**)arguments[0],
        *(void**)arguments[1],
        *(void**)arguments[2],
        *(void**)arguments[3],
        *(void**)arguments[4]);
}

static void liteWasmMigrateClosureHandler(ffi_cif*, void*, void** arguments, void* userData)
{
    LiteWasmEntryBind* binding = (LiteWasmEntryBind*)userData;
    liteWasmDispatch(
        binding->contractIndex,
        0,
        LiteWasmDispatchKind::Migration,
        *(const void**)arguments[0],
        *(void**)arguments[1],
        *(void**)arguments[2],
        nullptr,
        *(void**)arguments[3]);
}

struct LiteWasmModuleSet
{
    wasm_module_t module = nullptr;
    wasm_module_inst_t instance = nullptr;
    wasm_exec_env_t execEnv = nullptr;

    ~LiteWasmModuleSet()
    {
        if (execEnv)
        {
            wasm_runtime_destroy_exec_env(execEnv);
        }

        if (instance)
        {
            wasm_runtime_deinstantiate(instance);
        }

        if (module)
        {
            wasm_runtime_unload(module);
        }
    }

    void release()
    {
        module = nullptr;
        instance = nullptr;
        execEnv = nullptr;
    }

    LiteWasmModuleSet() = default;
    LiteWasmModuleSet(const LiteWasmModuleSet&) = delete;
    LiteWasmModuleSet& operator=(const LiteWasmModuleSet&) = delete;
};

struct LiteWasmOwnedBuffer
{
    unsigned char* data = nullptr;

    ~LiteWasmOwnedBuffer()
    {
        if (data)
        {
            free(data);
        }
    }

    void allocate(size_t size)
    {
        if (data)
        {
            free(data);
        }

        data = size ? (unsigned char*)malloc(size) : nullptr;
    }

    unsigned char* release()
    {
        unsigned char* releasedData = data;

        data = nullptr;
        return releasedData;
    }

    LiteWasmOwnedBuffer() = default;
    LiteWasmOwnedBuffer(const LiteWasmOwnedBuffer&) = delete;
    LiteWasmOwnedBuffer& operator=(const LiteWasmOwnedBuffer&) = delete;
};

struct LiteWasmStateSnapshot
{
    LiteWasmOwnedBuffer buffer;
    uint32_t size = 0;
};

struct LiteWasmRequiredExports
{
    wasm_function_inst_t stateAddress = nullptr;
    wasm_function_inst_t stateSize = nullptr;
    wasm_function_inst_t ioBase = nullptr;
    wasm_function_inst_t registrationCount = nullptr;
    wasm_function_inst_t registrationInfo = nullptr;
    wasm_function_inst_t dispatch = nullptr;
};

struct LiteWasmEntryInfo
{
    uint32_t inputType;
    uint32_t kind;
    uint32_t inputSize;
    uint32_t outputSize;
};

static void liteWasmCaptureState(
    const LiteWasmSlot& slot,
    unsigned int contractIndex,
    LiteWasmStateSnapshot& snapshot)
{
    if (!slot.instance
        || !slot.stateStubReleased
        || !slot.stateSize
        || !contractStates[contractIndex])
    {
        return;
    }

    snapshot.size = slot.stateSize;
    snapshot.buffer.allocate(snapshot.size);
    if (snapshot.buffer.data)
    {
        copyMem(
            snapshot.buffer.data,
            contractStates[contractIndex],
            snapshot.size);
    }
}

static void liteWasmSlotUnload(LiteWasmSlot& slot)
{
    if (slot.loadExecEnv)
    {
        wasm_runtime_destroy_exec_env(slot.loadExecEnv);
        slot.loadExecEnv = nullptr;
    }

    if (slot.instance)
    {
        wasm_runtime_deinstantiate(slot.instance);
        slot.instance = nullptr;
    }

    slot.arenaTop = nullptr;

    if (slot.module)
    {
        wasm_runtime_unload(slot.module);
        slot.module = nullptr;
    }

    for (uint32_t entryIndex = 0; entryIndex < slot.entryCount; ++entryIndex)
    {
        if (slot.entryClosures[entryIndex])
        {
            ffi_closure_free(slot.entryClosures[entryIndex]);
            slot.entryClosures[entryIndex] = nullptr;
        }
    }

    for (uint32_t systemProcedure = 0;
         systemProcedure < LITE_SP_COUNT;
         ++systemProcedure)
    {
        if (slot.systemClosures[systemProcedure])
        {
            ffi_closure_free(slot.systemClosures[systemProcedure]);
            slot.systemClosures[systemProcedure] = nullptr;
        }
    }

    if (slot.migrationClosure)
    {
        ffi_closure_free(slot.migrationClosure);
        slot.migrationClosure = nullptr;
    }

    slot.hasMigration = false;
    slot.entryCount = 0;
}

static bool liteWasmPrepareModuleBuffer(
    LiteWasmSlot& slot,
    const unsigned char* bytes,
    unsigned int length)
{
    // WAMR mutates and retains this buffer for the module lifetime.
    if (slot.moduleBuffer)
    {
        free(slot.moduleBuffer);
        slot.moduleBuffer = nullptr;
    }

    slot.moduleBuffer = (unsigned char*)malloc(length);
    if (!slot.moduleBuffer)
    {
        logToConsole(L"LITEWASM: oom");
        return false;
    }

    copyMem(slot.moduleBuffer, bytes, length);
    return true;
}

static bool liteWasmLoadModule(
    LiteWasmSlot& slot,
    unsigned int length,
    LiteWasmModuleSet& moduleSet)
{
    char error[192];

    moduleSet.module = wasm_runtime_load(
        slot.moduleBuffer,
        length,
        error,
        sizeof(error));
    if (!moduleSet.module)
    {
        logToConsole(L"LITEWASM: load failed");
        free(slot.moduleBuffer);
        slot.moduleBuffer = nullptr;
        return false;
    }

    moduleSet.instance = wasm_runtime_instantiate(
        moduleSet.module,
        64 * 1024,
        1024 * 1024,
        error,
        sizeof(error));
    if (!moduleSet.instance)
    {
        logToConsole(L"LITEWASM: instantiate failed");
        return false;
    }

    moduleSet.execEnv = wasm_runtime_create_exec_env(
        moduleSet.instance,
        64 * 1024);
    if (!moduleSet.execEnv)
    {
        logToConsole(L"LITEWASM: exec env alloc failed");
        return false;
    }

    return true;
}

static bool liteWasmFindRequiredExports(
    wasm_module_inst_t instance,
    LiteWasmRequiredExports& exports)
{
    exports.stateAddress = wasm_runtime_lookup_function(instance, "state_addr");
    exports.stateSize = wasm_runtime_lookup_function(instance, "state_size");
    exports.ioBase = wasm_runtime_lookup_function(instance, "io_base");
    exports.registrationCount = wasm_runtime_lookup_function(instance, "reg_count");
    exports.registrationInfo = wasm_runtime_lookup_function(instance, "reg_info");
    exports.dispatch = wasm_runtime_lookup_function(instance, "dispatch");

    if (!exports.stateAddress
        || !exports.stateSize
        || !exports.ioBase
        || !exports.registrationCount
        || !exports.registrationInfo
        || !exports.dispatch)
    {
        logToConsole(L"LITEWASM: missing required export");
        return false;
    }

    return true;
}

static bool liteWasmDiscoverLayout(
    LiteWasmSlot& slot,
    const LiteWasmModuleSet& moduleSet,
    const LiteWasmRequiredExports& exports,
    uint32_t*& arenaTop)
{
    slot.stateOffset = liteWasmCallU32(moduleSet.execEnv, exports.stateAddress);
    slot.stateSize = liteWasmCallU32(moduleSet.execEnv, exports.stateSize);
    slot.ioBaseOffset = liteWasmCallU32(moduleSet.execEnv, exports.ioBase);

    wasm_global_inst_t arenaGlobal = {};
    if (wasm_runtime_get_export_global_inst(
            moduleSet.instance,
            "arena_top",
            &arenaGlobal))
    {
        if (arenaGlobal.kind != WASM_I32
            || !arenaGlobal.is_mutable
            || !arenaGlobal.global_data)
        {
            logToConsole(L"LITEWASM: arena_top must be a mutable i32 global");
            return false;
        }

        arenaTop = static_cast<uint32_t*>(arenaGlobal.global_data);
    }

    wasm_function_inst_t ioSize = wasm_runtime_lookup_function(
        moduleSet.instance,
        "io_size");
    if (ioSize
        && liteWasmCallU32(moduleSet.execEnv, ioSize) < LITE_WASM_IO_CAPACITY)
    {
        logToConsole(
            L"LITEWASM: contract io region too small for the engine carve (rebuild the contract)");
        return false;
    }

    return true;
}

static void liteWasmAdoptModule(
    LiteWasmSlot& slot,
    LiteWasmModuleSet& moduleSet,
    const LiteWasmRequiredExports& exports,
    uint32_t* arenaTop)
{
    slot.module = moduleSet.module;
    slot.instance = moduleSet.instance;
    slot.loadExecEnv = moduleSet.execEnv;
    slot.dispatchFunction = exports.dispatch;
    slot.arenaTop = arenaTop;
    moduleSet.release();
}

static void liteWasmTakeOverState(
    LiteWasmSlot& slot,
    unsigned int contractIndex)
{
    if (!slot.stateStubReleased)
    {
        // The adapter releases the reserve according to the active state backend.
        liteSCOnWasmTakeover(contractIndex);
        slot.stateStubReleased = true;
    }

    contractStates[contractIndex] = (unsigned char*)wasm_runtime_addr_app_to_native(
        slot.instance,
        slot.stateOffset);
}

static void liteWasmConfigureMigration(
    LiteWasmSlot& slot,
    unsigned int contractIndex)
{
    slot.hasMigration = false;
    slot.migrationOldStateSize = 0;
    slot.migrationLocalsSize = 0;
    contractMigrateProcedures[contractIndex] = nullptr;
    contractMigrateOldStateSizes[contractIndex] = 0;
    contractMigrateLocalsSizes[contractIndex] = 0;

    wasm_function_inst_t hasMigration = wasm_runtime_lookup_function(
        slot.instance,
        "has_migrate");
    if (!hasMigration || !liteWasmCallU32(slot.loadExecEnv, hasMigration))
    {
        return;
    }

    wasm_function_inst_t oldStateSize = wasm_runtime_lookup_function(
        slot.instance,
        "migrate_old_state_size");
    wasm_function_inst_t localsSize = wasm_runtime_lookup_function(
        slot.instance,
        "migrate_locals_size");

    slot.migrationOldStateSize = oldStateSize
        ? liteWasmCallU32(slot.loadExecEnv, oldStateSize)
        : 0;
    slot.migrationLocalsSize = localsSize
        ? liteWasmCallU32(slot.loadExecEnv, localsSize)
        : 0;
    slot.migrationBinding = {
        contractIndex,
        0,
        LiteWasmDispatchKind::Migration,
    };

    void* code = nullptr;
    ffi_closure* closure = (ffi_closure*)ffi_closure_alloc(
        sizeof(ffi_closure),
        &code);
    if (closure
        && ffi_prep_closure_loc(
               closure,
               &g_liteWasmMigrateCif,
               liteWasmMigrateClosureHandler,
               &slot.migrationBinding,
               code) == FFI_OK)
    {
        slot.migrationClosure = closure;
        slot.hasMigration = true;
        contractMigrateProcedures[contractIndex] = (MIGRATE_PROCEDURE)code;
        contractMigrateOldStateSizes[contractIndex] = slot.migrationOldStateSize;
        contractMigrateLocalsSizes[contractIndex] = slot.migrationLocalsSize;
        return;
    }

    if (closure)
    {
        ffi_closure_free(closure);
        logToConsole(L"LITEWASM: migrate closure alloc failed");
    }
}

static void liteWasmRestoreState(
    LiteWasmSlot& slot,
    unsigned int contractIndex,
    LiteWasmStateSnapshot& snapshot)
{
    if (!snapshot.buffer.data)
    {
        return;
    }

    if (slot.hasMigration && slot.migrationOldStateSize == snapshot.size)
    {
        slot.pendingOldState = snapshot.buffer.release();
        slot.pendingOldStateSize = snapshot.size;
        logColorToScreen(
            "INFO",
            "LITEWASM: migrate pending — old state "
                + std::to_string(snapshot.size)
                + " bytes");
        return;
    }

    const uint32_t preservedSize = snapshot.size < slot.stateSize
        ? snapshot.size
        : slot.stateSize;
    copyMem(
        contractStates[contractIndex],
        snapshot.buffer.data,
        preservedSize);

    if (slot.migrationOldStateSize
        && slot.migrationOldStateSize != snapshot.size)
    {
        logColorToScreen(
            "WARNING",
            "LITEWASM: migrate OldStateData size "
                + std::to_string(slot.migrationOldStateSize)
                + " != live state "
                + std::to_string(snapshot.size)
                + " — raw-preserved instead");
        return;
    }

    logColorToScreen(
        "INFO",
        "LITEWASM: state preserved across upgrade — "
            + std::to_string(preservedSize)
            + " bytes");
}

static void liteWasmDiscoverRegistration(
    LiteWasmSlot& slot,
    const LiteWasmRequiredExports& exports)
{
    wasm_function_inst_t contextAddress = wasm_runtime_lookup_function(
        slot.instance,
        "ctx_addr");
    if (contextAddress)
    {
        slot.contextOffset = liteWasmCallU32(slot.loadExecEnv, contextAddress);
    }

    slot.entryCount = liteWasmCallU32(
        slot.loadExecEnv,
        exports.registrationCount);
    if (slot.entryCount > LITE_MAX_USER_ENTRIES)
    {
        slot.entryCount = LITE_MAX_USER_ENTRIES;
    }

    logColorToScreen(
        "INFO",
        "LITEWASM: loaded contract — "
            + std::to_string(slot.entryCount)
            + " entries, stateSize="
            + std::to_string(slot.stateSize));
}

static void liteWasmRegisterUserFunction(
    unsigned int contractIndex,
    uint16_t inputType,
    const LiteWasmEntryInfo& entry,
    void* code)
{
    contractUserFunctions[contractIndex][inputType] = (USER_FUNCTION)code;
    contractUserFunctionInputSizes[contractIndex][inputType] =
        (uint16_t)entry.inputSize;
    contractUserFunctionOutputSizes[contractIndex][inputType] =
        (uint16_t)entry.outputSize;
    contractUserFunctionLocalsSizes[contractIndex][inputType] = 0;
}

static void liteWasmRegisterUserProcedure(
    unsigned int contractIndex,
    uint16_t inputType,
    const LiteWasmEntryInfo& entry,
    void* code)
{
    contractUserProcedures[contractIndex][inputType] = (USER_PROCEDURE)code;
    contractUserProcedureInputSizes[contractIndex][inputType] =
        (uint16_t)entry.inputSize;
    contractUserProcedureOutputSizes[contractIndex][inputType] =
        (uint16_t)entry.outputSize;
    contractUserProcedureLocalsSizes[contractIndex][inputType] = 0;

    // Oracle notifications use the synthetic procedure ID registered by native contracts.
    if (userProcedureRegistry)
    {
        const unsigned int fullProcedureId = (contractIndex << 22) | inputType;
        userProcedureRegistry->add(
            fullProcedureId,
            {
                (USER_PROCEDURE)code,
                contractIndex,
                0u,
                (uint16_t)entry.inputSize,
                (uint16_t)entry.outputSize,
            });
    }
}

static void liteWasmRegisterUserEntries(
    LiteWasmSlot& slot,
    unsigned int contractIndex,
    const LiteWasmRequiredExports& exports)
{
    for (uint32_t entryIndex = 0; entryIndex < slot.entryCount; ++entryIndex)
    {
        uint32_t arguments[2] = {
            entryIndex,
            slot.ioBaseOffset,
        };
        wasm_runtime_call_wasm(
            slot.loadExecEnv,
            exports.registrationInfo,
            2,
            arguments);

        auto* entry = (LiteWasmEntryInfo*)wasm_runtime_addr_app_to_native(
            slot.instance,
            slot.ioBaseOffset);
        const uint16_t inputType = (uint16_t)entry->inputType;
        slot.entryBindings[entryIndex] = {
            contractIndex,
            inputType,
            (LiteWasmDispatchKind)entry->kind,
        };

        void* code = nullptr;
        ffi_closure* closure = (ffi_closure*)ffi_closure_alloc(
            sizeof(ffi_closure),
            &code);
        if (!closure
            || ffi_prep_closure_loc(
                   closure,
                   &g_liteWasmDispatchCif,
                   liteWasmClosureHandler,
                   &slot.entryBindings[entryIndex],
                   code) != FFI_OK)
        {
            logToConsole(L"LITEWASM: closure alloc failed");
            continue;
        }

        slot.entryClosures[entryIndex] = closure;
        if (entry->kind == LITE_KIND_FUNCTION)
        {
            liteWasmRegisterUserFunction(
                contractIndex,
                inputType,
                *entry,
                code);
        }
        else
        {
            liteWasmRegisterUserProcedure(
                contractIndex,
                inputType,
                *entry,
                code);
        }
    }
}

static uint32_t liteWasmCallWithU32Argument(
    wasm_exec_env_t execEnv,
    wasm_function_inst_t function,
    uint32_t argument)
{
    if (!function)
    {
        return 0;
    }

    uint32_t arguments[1] = { argument };

    wasm_runtime_call_wasm(execEnv, function, 1, arguments);
    return arguments[0];
}

static void liteWasmRegisterSystemProcedures(
    LiteWasmSlot& slot,
    unsigned int contractIndex)
{
    wasm_function_inst_t maskFunction = wasm_runtime_lookup_function(
        slot.instance,
        "reg_sysproc_mask");
    wasm_function_inst_t localsSizeFunction = wasm_runtime_lookup_function(
        slot.instance,
        "sysproc_locals_size");
    wasm_function_inst_t inputSizeFunction = wasm_runtime_lookup_function(
        slot.instance,
        "sysproc_in_size");
    wasm_function_inst_t outputSizeFunction = wasm_runtime_lookup_function(
        slot.instance,
        "sysproc_out_size");
    if (!maskFunction)
    {
        return;
    }

    const uint32_t mask = liteWasmCallU32(slot.loadExecEnv, maskFunction);
    for (uint32_t systemProcedure = 0;
         systemProcedure < LITE_SP_COUNT;
         ++systemProcedure)
    {
        if (!(mask & (1u << systemProcedure)))
        {
            continue;
        }

        slot.systemBindings[systemProcedure] = {
            contractIndex,
            (uint16_t)systemProcedure,
            LiteWasmDispatchKind::SystemProcedure,
        };

        void* code = nullptr;
        ffi_closure* closure = (ffi_closure*)ffi_closure_alloc(
            sizeof(ffi_closure),
            &code);
        if (!closure
            || ffi_prep_closure_loc(
                   closure,
                   &g_liteWasmDispatchCif,
                   liteWasmClosureHandler,
                   &slot.systemBindings[systemProcedure],
                   code) != FFI_OK)
        {
            continue;
        }

        slot.systemClosures[systemProcedure] = closure;
        contractSystemProcedures[contractIndex][systemProcedure] =
            (SYSTEM_PROCEDURE)code;
        contractSystemProcedureLocalsSizes[contractIndex][systemProcedure] =
            (uint16_t)liteWasmCallWithU32Argument(
                slot.loadExecEnv,
                localsSizeFunction,
                systemProcedure);
        slot.systemInputSizes[systemProcedure] =
            (uint16_t)liteWasmCallWithU32Argument(
                slot.loadExecEnv,
                inputSizeFunction,
                systemProcedure);
        slot.systemOutputSizes[systemProcedure] =
            (uint16_t)liteWasmCallWithU32Argument(
                slot.loadExecEnv,
                outputSizeFunction,
                systemProcedure);
    }
}

[[maybe_unused]] static bool liteWasmLoadFromBytes(
    unsigned int contractIndex,
    const unsigned char* bytes,
    unsigned int length)
{
    const int slotOffset = liteWasmSlotLocal(contractIndex);
    if (slotOffset < 0)
    {
        return false;
    }

    LiteWasmSlot& slot = g_liteWasmSlots[slotOffset];
    LiteWasmStateSnapshot previousState;

    liteWasmEnsureThreadEnv();
    liteWasmCaptureState(slot, contractIndex, previousState);
    if (slot.instance)
    {
        liteWasmSlotUnload(slot);
    }

    if (!liteWasmPrepareModuleBuffer(slot, bytes, length))
    {
        return false;
    }

    LiteWasmModuleSet moduleSet;
    if (!liteWasmLoadModule(slot, length, moduleSet))
    {
        return false;
    }

    LiteWasmRequiredExports exports;
    if (!liteWasmFindRequiredExports(moduleSet.instance, exports))
    {
        return false;
    }

    uint32_t* arenaTop = nullptr;
    if (!liteWasmDiscoverLayout(slot, moduleSet, exports, arenaTop))
    {
        return false;
    }

    liteWasmAdoptModule(slot, moduleSet, exports, arenaTop);
    liteWasmTakeOverState(slot, contractIndex);
    liteWasmConfigureMigration(slot, contractIndex);
    liteWasmRestoreState(slot, contractIndex, previousState);
    liteWasmDiscoverRegistration(slot, exports);
    liteWasmRegisterUserEntries(slot, contractIndex, exports);
    liteWasmRegisterSystemProcedures(slot, contractIndex);

    slot.loaded = true;
    logColorToScreen(
        "INFO",
        "LITEWASM: slot loaded ("
            + std::to_string(slot.entryCount)
            + " user entries)");
    return true;
}

static inline bool liteWasmHasPendingMigrate(unsigned int contractIndex)
{
    const int slotOffset = liteWasmSlotLocal(contractIndex);
    return slotOffset >= 0
        && g_liteWasmSlots[slotOffset].pendingOldState != nullptr;
}

[[maybe_unused]] static void liteWasmRunPendingMigrate(unsigned int contractIndex)
{
    const int slotOffset = liteWasmSlotLocal(contractIndex);
    if (slotOffset < 0 || !g_liteWasmSlots[slotOffset].pendingOldState)
    {
        return;
    }

    LiteWasmSlot& slot = g_liteWasmSlots[slotOffset];
    QpiContextMigrateProcedureCall migrationContext(contractIndex);

    migrationContext.call(slot.pendingOldState);
    free(slot.pendingOldState);
    slot.pendingOldState = nullptr;
    slot.pendingOldStateSize = 0;
    logColorToScreen(
        "INFO",
        "LITEWASM: migrate complete idx=" + std::to_string(contractIndex));
}

[[maybe_unused]] static bool liteWasmRuntimeInit()
{
    if (g_liteWasmReady)
    {
        return true;
    }

    for (int argumentIndex = 0; argumentIndex < 5; ++argumentIndex)
    {
        g_liteWasmCifArgs[argumentIndex] = &ffi_type_pointer;
    }

    if (ffi_prep_cif(
            &g_liteWasmDispatchCif,
            FFI_DEFAULT_ABI,
            5,
            &ffi_type_void,
            g_liteWasmCifArgs) != FFI_OK)
    {
        logToConsole(L"LITEWASM: libffi cif prep failed");
        return false;
    }

    for (int argumentIndex = 0; argumentIndex < 4; ++argumentIndex)
    {
        g_liteWasmMigrateCifArgs[argumentIndex] = &ffi_type_pointer;
    }

    if (ffi_prep_cif(
            &g_liteWasmMigrateCif,
            FFI_DEFAULT_ABI,
            4,
            &ffi_type_void,
            g_liteWasmMigrateCifArgs) != FFI_OK)
    {
        logToConsole(L"LITEWASM: migrate cif prep failed");
        return false;
    }

    // Per-instance allocation allows contracts with large resident state.
    RuntimeInitArgs arguments;
    setMem(&arguments, sizeof(arguments), 0);
    arguments.mem_alloc_type = Alloc_With_System_Allocator;
    arguments.native_module_name = "lhost";
    arguments.native_symbols = g_liteWasmNatives;
    arguments.n_native_symbols = (int)g_liteWasmNativesCount;
    if (!wasm_runtime_full_init(&arguments))
    {
        logToConsole(L"LITEWASM: WAMR init failed");
        return false;
    }

    g_liteWasmReady = true;
    logToConsole(L"LITEWASM: runtime ready (WAMR + libffi)");
    return true;
}

#endif // LITE_WASM_SC

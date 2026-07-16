#pragma once

// WAMR module loading, export discovery, state takeover, and migration setup.
#ifdef LITE_WASM_SC

#include "extensions/wasm/runtime/dispatch.h"
#include "extensions/wasm/runtime/state_backend.h"

namespace Wasm::Runtime
{

static void captureState(
    const EngineSlot& slot,
    unsigned int contractIndex,
    StateSnapshot& snapshot)
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

static void unloadSlot(EngineSlot& slot)
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
         systemProcedure < WASM_SYSTEM_PROCEDURE_COUNT;
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

static bool prepareModuleBuffer(
    EngineSlot& slot,
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

static bool loadModule(
    EngineSlot& slot,
    unsigned int length,
    ModuleResources& moduleSet)
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

static bool findRequiredExports(
    wasm_module_inst_t instance,
    RequiredExports& exports)
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

static bool discoverMemoryLayout(
    EngineSlot& slot,
    const ModuleResources& moduleSet,
    const RequiredExports& exports,
    uint32_t*& arenaTop)
{
    slot.stateOffset = callU32(moduleSet.execEnv, exports.stateAddress);
    slot.stateSize = callU32(moduleSet.execEnv, exports.stateSize);
    slot.ioBaseOffset = callU32(moduleSet.execEnv, exports.ioBase);

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
        && callU32(moduleSet.execEnv, ioSize) < WASM_IO_CAPACITY)
    {
        logToConsole(
            L"LITEWASM: contract io region too small for the engine carve (rebuild the contract)");
        return false;
    }

    return true;
}

static void adoptModule(
    EngineSlot& slot,
    ModuleResources& moduleSet,
    const RequiredExports& exports,
    uint32_t* arenaTop)
{
    slot.module = moduleSet.module;
    slot.instance = moduleSet.instance;
    slot.loadExecEnv = moduleSet.execEnv;
    slot.dispatchFunction = exports.dispatch;
    slot.arenaTop = arenaTop;
    moduleSet.release();
}

static void takeOverState(
    EngineSlot& slot,
    unsigned int contractIndex)
{
    if (!slot.stateStubReleased)
    {
        // The adapter releases the reserve according to the active state backend.
        transferContractStateToWasm(contractIndex);
        slot.stateStubReleased = true;
    }

    contractStates[contractIndex] = (unsigned char*)wasm_runtime_addr_app_to_native(
        slot.instance,
        slot.stateOffset);
}

static void configureMigration(
    EngineSlot& slot,
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
    if (!hasMigration || !callU32(slot.loadExecEnv, hasMigration))
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
        ? callU32(slot.loadExecEnv, oldStateSize)
        : 0;
    slot.migrationLocalsSize = localsSize
        ? callU32(slot.loadExecEnv, localsSize)
        : 0;
    slot.migrationBinding = {
        contractIndex,
        0,
        DispatchKind::Migration,
    };

    void* code = nullptr;
    ffi_closure* closure = (ffi_closure*)ffi_closure_alloc(
        sizeof(ffi_closure),
        &code);
    if (closure
        && ffi_prep_closure_loc(
               closure,
               &migrationCallInterface,
               migrationClosure,
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

static void restoreState(
    EngineSlot& slot,
    unsigned int contractIndex,
    StateSnapshot& snapshot)
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

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

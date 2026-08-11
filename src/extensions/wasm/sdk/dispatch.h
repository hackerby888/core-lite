#pragma once

// Contract-side dispatch for user, system, and migration calls.
#ifdef LITE_WASM_TU_BUILD

#include "extensions/wasm/sdk/module_storage.h"

#ifdef CONTRACT_STATE_TYPE

namespace Wasm::Sdk
{

static const ModuleEntry* findUserEntry(
    unsigned int inputType,
    DispatchKind kind)
{
    for (unsigned int entryIndex = 0; entryIndex < moduleEntryCount; entryIndex++)
    {
        const ModuleEntry& entry = moduleEntries[entryIndex];
        if (entry.inputType == (unsigned short)inputType && entry.kind == kind)
        {
            return &entry;
        }
    }

    return nullptr;
}

static void callSystemProcedure(
    SystemProcedure procedure,
    void* input,
    void* output,
    void* locals)
{
    auto& context = *reinterpret_cast<QPI::QpiContextProcedureCall*>(&moduleContextStorage[0]);
    procedure(context, &moduleState, input, output, locals);
}

static void dispatchSystemProcedure(
    unsigned int systemProcedureId,
    void* input,
    void* output,
    void* locals)
{
    switch (systemProcedureId)
    {
        case WASM_SYSTEM_PROCEDURE_INITIALIZE:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__initialize, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_BEGIN_EPOCH:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__beginEpoch, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_END_EPOCH:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__endEpoch, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_BEGIN_TICK:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__beginTick, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_END_TICK:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__endTick, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_PRE_RELEASE_SHARES:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__preReleaseShares, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_PRE_ACQUIRE_SHARES:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__preAcquireShares, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_POST_RELEASE_SHARES:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__postReleaseShares, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_POST_ACQUIRE_SHARES:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__postAcquireShares, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_POST_INCOMING_TRANSFER:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__postIncomingTransfer, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_SET_SHAREHOLDER_PROPOSAL:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__setShareholderProposal, input, output, locals);
            break;
        case WASM_SYSTEM_PROCEDURE_SET_SHAREHOLDER_VOTES:
            callSystemProcedure((SystemProcedure)(void*)CONTRACT_STATE_TYPE::__setShareholderVotes, input, output, locals);
            break;
        default:
            break;
    }
}

static void dispatchMigration(void* oldState, void* locals)
{
    auto& context = *reinterpret_cast<QPI::QpiContextFunctionCall*>(&moduleContextStorage[0]);
    auto migrate = (MigrateProcedure)(void*)CONTRACT_STATE_TYPE::__migrate;

    migrate(context, &moduleState, oldState, locals);
}

static void dispatchUserFunction(
    unsigned int inputType,
    void* input,
    void* output,
    void* locals)
{
    const ModuleEntry* entry = findUserEntry(inputType, DispatchKind::UserFunction);
    if (!entry)
    {
        return;
    }

    auto& context = *reinterpret_cast<QPI::QpiContextFunctionCall*>(&moduleContextStorage[0]);
    auto function = (UserFunction)entry->function;

    function(context, &moduleState, input, output, locals);
}

static void dispatchUserProcedure(
    unsigned int inputType,
    void* input,
    void* output,
    void* locals)
{
    const ModuleEntry* entry = findUserEntry(inputType, DispatchKind::UserProcedure);
    if (!entry)
    {
        return;
    }

    auto& context = *reinterpret_cast<QPI::QpiContextProcedureCall*>(&moduleContextStorage[0]);
    auto procedure = (UserProcedure)entry->function;

    procedure(context, &moduleState, input, output, locals);
}

extern "C"
{
LH_EXPORT(dispatch)
void dispatch(
    unsigned int kindValue,
    unsigned int inputType,
    unsigned int inputOffset,
    unsigned int outputOffset,
    unsigned int localsOffset)
{
    ensureModuleRegistered();

    const DispatchKind kind = (DispatchKind)kindValue;
    void* input = (void*)(unsigned long)inputOffset;
    void* output = (void*)(unsigned long)outputOffset;
    void* locals = (void*)(unsigned long)localsOffset;

    switch (kind)
    {
        case DispatchKind::UserFunction:
            dispatchUserFunction(inputType, input, output, locals);
            break;
        case DispatchKind::UserProcedure:
            dispatchUserProcedure(inputType, input, output, locals);
            break;
        case DispatchKind::SystemProcedure:
            dispatchSystemProcedure(inputType, input, output, locals);
            break;
        case DispatchKind::Migration:
            dispatchMigration(input, locals);
            break;
    }
}
} // extern "C"

} // namespace Wasm::Sdk

#endif // CONTRACT_STATE_TYPE

#endif // LITE_WASM_TU_BUILD

#pragma once

// Contract registration capture exposed through the module metadata exports.
#ifdef LITE_WASM_TU_BUILD

#include "extensions/wasm/shared/abi_types.h"

// ---- registration capture (read by reg_info) ----
#ifndef WASM_MAX_USER_ENTRIES
#define WASM_MAX_USER_ENTRIES 1024
#endif

namespace Wasm::Sdk
{

struct ModuleEntry
{
    unsigned short inputType;
    DispatchKind kind;
    unsigned short inputSize;
    unsigned short outputSize;
    unsigned int localsSize;
    void* function;
};
static ModuleEntry moduleEntries[WASM_MAX_USER_ENTRIES];
static unsigned int moduleEntryCount = 0;

} // namespace Wasm::Sdk

QPI::QpiContextForInit::QpiContextForInit(unsigned int contractIndex)
    : QpiContext(contractIndex, QPI::NULL_ID, QPI::NULL_ID, 0, 0)
{
}

void QPI::QpiContextForInit::__registerUserFunction(
    USER_FUNCTION function,
    unsigned short inputType,
    unsigned short inputSize,
    unsigned short outputSize,
    unsigned int localsSize) const
{
    if (Wasm::Sdk::moduleEntryCount >= WASM_MAX_USER_ENTRIES)
    {
        return;
    }

    Wasm::Sdk::moduleEntries[Wasm::Sdk::moduleEntryCount++] = {
        inputType,
        Wasm::DispatchKind::UserFunction,
        inputSize,
        outputSize,
        localsSize,
        (void*)function,
    };
}

void QPI::QpiContextForInit::__registerUserProcedure(
    USER_PROCEDURE procedure,
    unsigned short inputType,
    unsigned short inputSize,
    unsigned short outputSize,
    unsigned int localsSize) const
{
    if (Wasm::Sdk::moduleEntryCount >= WASM_MAX_USER_ENTRIES)
    {
        return;
    }

    Wasm::Sdk::moduleEntries[Wasm::Sdk::moduleEntryCount++] = {
        inputType,
        Wasm::DispatchKind::UserProcedure,
        inputSize,
        outputSize,
        localsSize,
        (void*)procedure,
    };
}

// Oracle notification dispatch uses the low 16 bits of its synthetic procedure ID.
void QPI::QpiContextForInit::__registerUserProcedureNotification(
    USER_PROCEDURE procedure,
    unsigned int procedureId,
    unsigned short inputSize,
    unsigned short outputSize,
    unsigned int localsSize) const
{
    if (Wasm::Sdk::moduleEntryCount >= WASM_MAX_USER_ENTRIES)
    {
        return;
    }

    Wasm::Sdk::moduleEntries[Wasm::Sdk::moduleEntryCount++] = {
        (unsigned short)procedureId,
        Wasm::DispatchKind::UserProcedure,
        inputSize,
        outputSize,
        localsSize,
        (void*)procedure,
    };
}


#endif // LITE_WASM_TU_BUILD

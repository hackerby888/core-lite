#pragma once

// Contract module state, context, IO storage, and metadata exports.
#ifdef LITE_WASM_TU_BUILD

#include "extensions/wasm/sdk/registration.h"

#ifdef CONTRACT_STATE_TYPE

namespace Wasm::Sdk
{

typedef void (*UserFunction)(const QPI::QpiContextFunctionCall&, void*, void*, void*, void*);
typedef void (*UserProcedure)(const QPI::QpiContextProcedureCall&, void*, void*, void*, void*);
typedef void (*SystemProcedure)(const QPI::QpiContextProcedureCall&, void*, void*, void*, void*);
typedef void (*MigrateProcedure)(const QPI::QpiContextFunctionCall&, void*, void*, void*);

// Alignment keeps debug page protection for state separate from context and IO.
// Raw storage avoids running constructors that native contract state never runs.
#ifdef QINIT_CORPUS_RUNNER
// Corpus runs use engine-deployed state, so the local region only reserves one page.
alignas(65536) static unsigned char moduleStateStorage[65536];
#else
alignas(65536) static unsigned char moduleStateStorage[sizeof(CONTRACT_STATE_TYPE::StateData)];
#endif
static CONTRACT_STATE_TYPE::StateData& moduleState = *reinterpret_cast<CONTRACT_STATE_TYPE::StateData*>(moduleStateStorage);
alignas(65536) static unsigned char moduleContextStorage[256];
#ifndef WASM_ARENA_SIZE
#define WASM_ARENA_SIZE (1024 * 1024 * 1024)
#endif

// Room past the carve for the state-write journal. Reserved whether or not the artifact carries one,
// so the layout never depends on instrumentation; shared-memory builds pack modules and pass 0.
#ifndef WASM_JOURNAL_SIZE
#define WASM_JOURNAL_SIZE (72 * 1024 * 1024)
#endif

#define WASM_IO_CARVE_SIZE ((64 * 1024) + (64 * 1024) + (32 * 1024) + WASM_ARENA_SIZE)

// Layout is input, output, locals, then scratch arena; it must match the node carve.
alignas(65536) static unsigned char moduleIoStorage[WASM_IO_CARVE_SIZE + WASM_JOURNAL_SIZE];

static bool moduleRegistered = false;
static void ensureModuleRegistered()
{
    if (moduleRegistered)
    {
        return;
    }

    moduleRegistered = true;
    QPI::QpiContextForInit qpi(CONTRACT_INDEX);

    CONTRACT_STATE_TYPE::__registerUserFunctionsAndProcedures(qpi);
}

extern "C"
{
LH_EXPORT(contract_index)
unsigned int contract_index()
{
    return CONTRACT_INDEX;
}

LH_EXPORT(state_addr)
unsigned int state_addr()
{
    return (unsigned int)(unsigned long)&moduleState;
}

LH_EXPORT(state_size)
unsigned int state_size()
{
    return (unsigned int)sizeof(moduleState);
}

LH_EXPORT(io_base)
unsigned int io_base()
{
    return (unsigned int)(unsigned long)&moduleIoStorage[0];
}

LH_EXPORT(io_size)
unsigned int io_size()
{
    // The carve only: the journal past it is not the host's to hand out.
    return (unsigned int)WASM_IO_CARVE_SIZE;
}

LH_EXPORT(ctx_addr)
unsigned int ctx_addr()
{
    return (unsigned int)(unsigned long)&moduleContextStorage[0];
}

LH_EXPORT(reg_count)
unsigned int reg_count()
{
    ensureModuleRegistered();
    return moduleEntryCount;
}

struct ModuleEntryInfo
{
    unsigned int inputType;
    unsigned int kind;
    unsigned int inputSize;
    unsigned int outputSize;
};

LH_EXPORT(reg_info)
void reg_info(unsigned int entryIndex, ModuleEntryInfo* output)
{
    ensureModuleRegistered();
    if (entryIndex >= moduleEntryCount)
    {
        setMem(output, sizeof(*output), 0);
        return;
    }

    const ModuleEntry& entry = moduleEntries[entryIndex];

    output->inputType = entry.inputType;
    output->kind = (unsigned int)entry.kind;
    output->inputSize = entry.inputSize;
    output->outputSize = entry.outputSize;
}

// System procedure bits use the IDs declared in shared/abi_types.h.
LH_EXPORT(reg_sysproc_mask)
unsigned int reg_sysproc_mask()
{
    unsigned int mask = 0;
#define WASM_SYSTEM_PROCEDURE_MASK(symbol, id, method, emptyMember) \
    if (!CONTRACT_STATE_TYPE::emptyMember)                 \
    {                                                      \
        mask |= (1u << id);                                \
    }
    WASM_SYSTEM_PROCEDURE_ROWS(WASM_SYSTEM_PROCEDURE_MASK)
#undef WASM_SYSTEM_PROCEDURE_MASK
    return mask;
}

LH_EXPORT(sysproc_locals_size)
unsigned int sysproc_locals_size(unsigned int systemProcedure)
{
    switch (systemProcedure)
    {
        case 0:
            return (unsigned int)CONTRACT_STATE_TYPE::__initializeLocalsSize;
        case 1:
            return (unsigned int)CONTRACT_STATE_TYPE::__beginEpochLocalsSize;
        case 2:
            return (unsigned int)CONTRACT_STATE_TYPE::__endEpochLocalsSize;
        case 3:
            return (unsigned int)CONTRACT_STATE_TYPE::__beginTickLocalsSize;
        case 4:
            return (unsigned int)CONTRACT_STATE_TYPE::__endTickLocalsSize;
        case 5:
            return (unsigned int)CONTRACT_STATE_TYPE::__preReleaseSharesLocalsSize;
        case 6:
            return (unsigned int)CONTRACT_STATE_TYPE::__preAcquireSharesLocalsSize;
        case 7:
            return (unsigned int)CONTRACT_STATE_TYPE::__postReleaseSharesLocalsSize;
        case 8:
            return (unsigned int)CONTRACT_STATE_TYPE::__postAcquireSharesLocalsSize;
        case 9:
            return (unsigned int)CONTRACT_STATE_TYPE::__postIncomingTransferLocalsSize;
        case 10:
            return (unsigned int)CONTRACT_STATE_TYPE::__setShareholderProposalLocalsSize;
        case 11:
            return (unsigned int)CONTRACT_STATE_TYPE::__setShareholderVotesLocalsSize;
    }

    return 0;
}

LH_EXPORT(sysproc_in_size)
unsigned int sysproc_in_size(unsigned int systemProcedure)
{
    switch (systemProcedure)
    {
        case 5:
        case 6:
            return (unsigned int)sizeof(QPI::PreManagementRightsTransfer_input);
        case 7:
        case 8:
            return (unsigned int)sizeof(QPI::PostManagementRightsTransfer_input);
        case 9:
            return (unsigned int)sizeof(QPI::PostIncomingTransfer_input);
        case 10:
            return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_PROPOSAL_input);
        case 11:
            return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_VOTES_input);
    }

    return 0;
}

LH_EXPORT(sysproc_out_size)
unsigned int sysproc_out_size(unsigned int systemProcedure)
{
    switch (systemProcedure)
    {
        case 5:
        case 6:
            return (unsigned int)sizeof(QPI::PreManagementRightsTransfer_output);
        case 10:
            return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_PROPOSAL_output);
        case 11:
            return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_VOTES_output);
    }

    return 0;
}

LH_EXPORT(has_migrate)
unsigned int has_migrate()
{
    return CONTRACT_STATE_TYPE::__migrateEmpty ? 0u : 1u;
}

LH_EXPORT(migrate_old_state_size)
unsigned int migrate_old_state_size()
{
    return (unsigned int)CONTRACT_STATE_TYPE::__migrateOldStateSize;
}

LH_EXPORT(migrate_locals_size)
unsigned int migrate_locals_size()
{
    return (unsigned int)CONTRACT_STATE_TYPE::__migrateLocalsSize;
}
} // extern "C"

} // namespace Wasm::Sdk

#endif // CONTRACT_STATE_TYPE

#endif // LITE_WASM_TU_BUILD

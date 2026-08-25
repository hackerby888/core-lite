#pragma once

// Module upload, deployment, activation scheduling, and boot setup.
#ifdef LITE_WASM_SC

#include "extensions/wasm/runtime/contract_slots.h"
#include "extensions/wasm/runtime/deployment_protocol.h"
#include "extensions/wasm/runtime/engine.h"

namespace Wasm::Runtime
{

[[maybe_unused]] static void beginModuleUpload(unsigned long long sessionId, unsigned int totalSize, unsigned int chunkCount, const unsigned char* finalHash)
{
    const bool retry = moduleUpload.active;
    if (!tryBeginModuleUpload(sessionId, totalSize, chunkCount, finalHash))
    {
        if (moduleUpload.active)
        {
            logColorToScreen("WARN", "LITEDYN: UploadBegin rejected; session " + std::to_string(moduleUpload.sessionId) + " is active");
        }
        else
        {
            logColorToScreen("WARN", "LITEDYN: UploadBegin rejected; invalid size or chunk count");
        }
        return;
    }

    logToConsole(retry ? L"LITEDYN: UploadBegin retry accepted" : L"LITEDYN: UploadBegin received");
}

[[maybe_unused]] static void receiveModuleChunk(unsigned long long sessionId, unsigned int sequence, const unsigned char* data, unsigned int dataLength)
{
    tryReceiveModuleChunk(sessionId, sequence, data, dataLength);
}

static bool moduleUploadComplete()
{
    if (!moduleUpload.active || moduleUpload.receivedCount != moduleUpload.chunkCount)
    {
        return false;
    }

    unsigned char calculatedHash[32];

    KangarooTwelve(moduleUploadBuffer, moduleUpload.totalSize, calculatedHash, 32);
    for (int index = 0; index < 32; index++)
    {
        if (calculatedHash[index] != moduleUpload.finalHash[index])
        {
            return false;
        }
    }

    return true;
}

// Defined by runtime/engine.h earlier in the extension translation unit.
static bool loadFromBytes(unsigned int contractIndex, const unsigned char* bytes, unsigned int length);
static bool isContractLoaded(unsigned int contractIndex);
static bool hasPendingMigration(unsigned int contractIndex);
static void runPendingMigration(unsigned int contractIndex);

[[maybe_unused]] static void deployModule(unsigned long long sessionId, unsigned int targetSlot, const unsigned char* finalHash, unsigned int abiVersion,
    unsigned int /*stateLayoutVersion*/,
    const char* name)
{
    const int slotOffset = reservedSlotOffset(targetSlot);
    if (slotOffset < 0)
    {
        return;
    }

    if (abiVersion != WASM_ABI_VERSION)
    {
        logColorToScreen("ERROR", "LITEDYN: unsupported Wasm ABI version " + std::to_string(abiVersion) + "; expected " + std::to_string(WASM_ABI_VERSION));
        return;
    }

    if (sessionId != moduleUpload.sessionId || !moduleUploadComplete())
    {
        return;
    }

    for (int index = 0; index < 32; index++)
    {
        if (finalHash[index] != moduleUpload.finalHash[index])
        {
            return;
        }
    }

    bool loadOk = false;
    const unsigned char* artifact = moduleUploadBuffer;
    const bool hasWasmMagic = moduleUpload.totalSize >= 4 && artifact[0] == 0x00 && artifact[1] == 0x61 && artifact[2] == 0x73 && artifact[3] == 0x6d;

    if (hasWasmMagic)
    {
        loadOk = loadFromBytes(targetSlot, moduleUploadBuffer, moduleUpload.totalSize);
        logToConsole(loadOk ? L"LITEDYN: wasm contract loaded" : L"LITEDYN: ERROR wasm load failed");
    }
    else
    {
        logToConsole(L"LITEDYN: ERROR upload is not a wasm module ('\\0asm' expected)");
    }

    if (!loadOk)
    {
        logToConsole(L"LITEDYN: ERROR load failed - resident slot unchanged");
        moduleUpload.active = false;
        return;
    }

    ContractSlot& slot = contractSlots[slotOffset];
    copyMem(slot.codeHash, finalHash, 32);
    if (name)
    {
        copyMem(slot.name, name, 32);
        slot.name[31] = 0;
    }

    slot.armed = true;
    slot.version++;
    logToConsole(L"LITEDYN: Deploy accepted, slot armed");

    slot.needsMigrate = hasPendingMigration(targetSlot);
    slot.constructed = slot.everInitialized && !slot.needsMigrate;
    if (slot.needsMigrate)
    {
        logToConsole(L"LITEDYN: migrate scheduled for next tick");
    }

    moduleUpload.active = false;
}


[[maybe_unused]] static void dispatchDeploymentTransaction(unsigned short inputType, const unsigned char* input, unsigned int size)
{
    if (inputType == WASM_DEPLOYMENT_UPLOAD_BEGIN_INPUT_TYPE)
    {
        DeploymentProtocol::UploadBeginMessage message;
        if (size < sizeof(message))
        {
            return;
        }

        copyMem(&message, input, sizeof(message));
        beginModuleUpload(message.sessionId, message.totalSize, message.chunkCount, message.finalHash);
    }
    else if (inputType == WASM_DEPLOYMENT_UPLOAD_CHUNK_INPUT_TYPE)
    {
        DeploymentProtocol::UploadChunkHeader message;
        if (size < sizeof(message))
        {
            return;
        }

        copyMem(&message, input, sizeof(message));
        if (sizeof(message) + message.dataLength > size)
        {
            return;
        }

        receiveModuleChunk(message.sessionId, message.sequence, input + sizeof(message), message.dataLength);
    }
    else if (inputType == WASM_DEPLOYMENT_DEPLOY_INPUT_TYPE)
    {
        DeploymentProtocol::DeployHeader message;
        if (size < sizeof(message))
        {
            return;
        }

        copyMem(&message, input, sizeof(message));
        const char* name = nullptr;
        if (size >= sizeof(DeploymentProtocol::DeployMessage))
        {
            name = reinterpret_cast<const char*>(input + sizeof(message));
        }

        deployModule(message.sessionId, message.targetSlot, message.finalHash, message.abiVersion, message.stateLayoutVersion, name);
    }
}

static bool hasPendingActivation()
{
    for (unsigned int slotOffset = 0; slotOffset < WASM_RESERVED_SLOT_COUNT; slotOffset++)
    {
        const ContractSlot& slot = contractSlots[slotOffset];
        if (slot.armed && (!slot.constructed || slot.needsMigrate))
        {
            return true;
        }
    }

    return false;
}

[[maybe_unused]] static void activatePendingContracts()
{
    for (unsigned int slotOffset = 0; slotOffset < WASM_RESERVED_SLOT_COUNT; slotOffset++)
    {
        ContractSlot& slot = contractSlots[slotOffset];
        if (!slot.armed)
        {
            continue;
        }

        const unsigned int contractIndex = WASM_RESERVED_SLOT_BASE + slotOffset;
        if (slot.needsMigrate)
        {
            runPendingMigration(contractIndex);
            slot.needsMigrate = false;
            slot.constructed = true;
            continue;
        }

        if (slot.constructed)
        {
            continue;
        }

        if (contractSystemProcedures[contractIndex][INITIALIZE])
        {
            QpiContextSystemProcedureCall qpiContext(contractIndex, INITIALIZE);

            qpiContext.call();
            slot.everInitialized = true;
            logToConsole(L"LITEDYN: slot constructed (INITIALIZE ran)");
        }
        else
        {
            logToConsole(L"LITEDYN: ERROR construct skipped - tables unpatched (load failed)");
        }

        slot.constructed = true;
    }
}

[[maybe_unused]] static void initializeDeployment()
{
    logToConsole(L"LITEWASM: runtime deployment enabled for testnet lite RAM");

    for (unsigned int slotOffset = 0; slotOffset < WASM_RESERVED_SLOT_COUNT; slotOffset++)
    {
        const unsigned int contractIndex = WASM_RESERVED_SLOT_BASE + slotOffset;

        contractError[contractIndex] = NoContractError;
        if (getContractFeeReserve(contractIndex) <= 0)
        {
            setContractFeeReserve(contractIndex, 1000000000000ll);
        }
    }
}

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

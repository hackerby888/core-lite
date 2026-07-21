#pragma once

// Reserved contract slots and in-progress module upload state.
#ifdef LITE_WASM_SC

#include <cstring>
#include <string>

#ifndef WASM_MAX_MODULE_SIZE
#define WASM_MAX_MODULE_SIZE (4u * 1024u * 1024u)
#endif

namespace Wasm::Runtime
{

struct ContractSlot
{
    bool armed = false;
    bool constructed = false;
    bool everInitialized = false;
    bool needsMigrate = false;
    unsigned char codeHash[32] = {};
    unsigned int activationTick = 0;
    unsigned int version = 0;
    char name[32] = {};
    std::string sourceH;
};

static ContractSlot contractSlots[WASM_RESERVED_SLOT_COUNT];

struct ModuleUpload
{
    bool active = false;
    unsigned long long sessionId = 0;
    unsigned int totalSize = 0;
    unsigned int chunkCount = 0;
    unsigned int receivedCount = 0;
    unsigned char finalHash[32] = {};
};

static ModuleUpload moduleUpload;
static unsigned char moduleUploadBuffer[WASM_MAX_MODULE_SIZE];
static unsigned char receivedChunkBits[(WASM_MAX_MODULE_SIZE / 1008u) / 8u + 1u];

static inline bool tryBeginModuleUpload(
    unsigned long long sessionId,
    unsigned int totalSize,
    unsigned int chunkCount,
    const unsigned char* finalHash)
{
    if (moduleUpload.active)
    {
        return moduleUpload.sessionId == sessionId;
    }

    moduleUpload.active = true;
    moduleUpload.sessionId = sessionId;
    moduleUpload.totalSize = totalSize;
    moduleUpload.chunkCount = chunkCount;
    moduleUpload.receivedCount = 0;
    std::memcpy(moduleUpload.finalHash, finalHash, sizeof(moduleUpload.finalHash));
    std::memset(receivedChunkBits, 0, sizeof(receivedChunkBits));
    return true;
}

static inline unsigned int reservedSlotBase()
{
    return WASM_RESERVED_SLOT_BASE;
}

static inline int reservedSlotOffset(unsigned int contractIndex)
{
    const int slotOffset = (int)contractIndex - (int)WASM_RESERVED_SLOT_BASE;
    if (slotOffset < 0 || slotOffset >= (int)WASM_RESERVED_SLOT_COUNT)
    {
        return -1;
    }

    return slotOffset;
}

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

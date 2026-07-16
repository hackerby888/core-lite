#pragma once

// Reserved contract slots and in-progress module upload state.
#ifdef LITE_WASM_SC

#include <string>

#ifndef WASM_MAX_MODULE_SIZE
#define WASM_MAX_MODULE_SIZE (4u * 1024u * 1024u)
#endif

#define WASM_RESERVED_SLOT_COUNT 4

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

static inline unsigned int reservedSlotBase()
{
    return LITEDYN0_CONTRACT_INDEX;
}

static inline int reservedSlotOffset(unsigned int contractIndex)
{
    const int slotOffset = (int)contractIndex - (int)LITEDYN0_CONTRACT_INDEX;
    if (slotOffset < 0 || slotOffset >= (int)WASM_RESERVED_SLOT_COUNT)
    {
        return -1;
    }

    return slotOffset;
}

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

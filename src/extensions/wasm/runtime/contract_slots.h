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

static constexpr unsigned int WASM_UPLOAD_CHUNK_SIZE = 1008u;
static constexpr unsigned int WASM_MAX_UPLOAD_CHUNKS =
    (WASM_MAX_MODULE_SIZE - 1u) / WASM_UPLOAD_CHUNK_SIZE + 1u;

struct ContractSlot
{
    bool armed = false;
    bool constructed = false;
    bool everInitialized = false;
    bool needsMigrate = false;
    unsigned char codeHash[32] = {};
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
static unsigned char receivedChunkBits[(WASM_MAX_UPLOAD_CHUNKS + 7u) / 8u];

static inline unsigned int expectedModuleUploadChunkCount(unsigned int totalSize)
{
    if (!totalSize)
    {
        return 0;
    }

    return (totalSize - 1u) / WASM_UPLOAD_CHUNK_SIZE + 1u;
}

static inline bool validModuleUploadShape(unsigned int totalSize, unsigned int chunkCount)
{
    return totalSize > 0
        && totalSize <= WASM_MAX_MODULE_SIZE
        && chunkCount == expectedModuleUploadChunkCount(totalSize);
}

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

    if (!finalHash || !validModuleUploadShape(totalSize, chunkCount))
    {
        return false;
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

static inline bool tryReceiveModuleChunk(
    unsigned long long sessionId,
    unsigned int sequence,
    const unsigned char* data,
    unsigned int dataLength)
{
    if (!moduleUpload.active || sessionId != moduleUpload.sessionId)
    {
        return false;
    }

    const unsigned long long destinationOffset =
        (unsigned long long)sequence * WASM_UPLOAD_CHUNK_SIZE;
    if (!data || sequence != moduleUpload.receivedCount || sequence >= moduleUpload.chunkCount)
    {
        return false;
    }

    const unsigned int remainingSize = moduleUpload.totalSize - (unsigned int)destinationOffset;
    const unsigned int expectedDataLength = remainingSize < WASM_UPLOAD_CHUNK_SIZE
        ? remainingSize
        : WASM_UPLOAD_CHUNK_SIZE;
    if (dataLength != expectedDataLength)
    {
        return false;
    }

    const unsigned int sequenceByte = sequence >> 3;
    const unsigned int sequenceBit = 1u << (sequence & 7);
    if (receivedChunkBits[sequenceByte] & sequenceBit)
    {
        return false;
    }

    std::memcpy(moduleUploadBuffer + destinationOffset, data, dataLength);
    receivedChunkBits[sequenceByte] |= sequenceBit;
    moduleUpload.receivedCount++;
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

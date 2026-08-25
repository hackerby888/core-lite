#pragma once

// Deployment transaction values and wire layouts.
#ifdef LITE_WASM_SC

#include "platform/m256.h"

// These input types are part of the @qinit/proto deployment wire format.
#define WASM_DEPLOYMENT_UPLOAD_BEGIN_INPUT_TYPE 240
#define WASM_DEPLOYMENT_UPLOAD_CHUNK_INPUT_TYPE 241
#define WASM_DEPLOYMENT_DEPLOY_INPUT_TYPE 242

namespace Wasm::Runtime
{

namespace DeploymentProtocol
{

static const m256i DeploymentAddress(99999ULL, 0, 0, 0);

#pragma pack(push, 1)

struct UploadBeginMessage
{
    uint64_t sessionId;
    uint32_t totalSize;
    uint32_t chunkCount;
    unsigned char finalHash[32];
};

struct UploadChunkHeader
{
    uint64_t sessionId;
    uint32_t sequence;
    uint16_t dataLength;
};

struct DeployHeader
{
    uint64_t sessionId;
    uint32_t targetSlot;
    unsigned char finalHash[32];
    uint32_t abiVersion;
    uint32_t stateLayoutVersion;
};

struct DeployMessage
{
    DeployHeader header;
    char name[32];
};

#pragma pack(pop)

static_assert(sizeof(UploadBeginMessage) == 48, "UploadBeginMessage layout drifted");
static_assert(sizeof(UploadChunkHeader) == 14, "UploadChunkHeader layout drifted");
static_assert(sizeof(DeployHeader) == 52, "DeployHeader layout drifted");
static_assert(sizeof(DeployMessage) == 84, "DeployMessage layout drifted");

} // namespace DeploymentProtocol

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

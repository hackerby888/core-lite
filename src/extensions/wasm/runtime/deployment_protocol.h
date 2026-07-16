#pragma once

// Deployment transaction values, wire offsets, and little-endian readers.
#ifdef LITE_WASM_SC

// These input types and offsets are part of the @qinit/proto deployment wire format.
#define WASM_DEPLOYMENT_UPLOAD_BEGIN_INPUT_TYPE 240
#define WASM_DEPLOYMENT_UPLOAD_CHUNK_INPUT_TYPE 241
#define WASM_DEPLOYMENT_DEPLOY_INPUT_TYPE 242

namespace Wasm::Runtime
{

namespace DeploymentProtocol
{
constexpr unsigned int SessionIdOffset = 0;
constexpr unsigned int UploadTotalSizeOffset = 8;
constexpr unsigned int UploadChunkCountOffset = 12;
constexpr unsigned int UploadHashOffset = 16;
constexpr unsigned int UploadBeginSize = 48;
constexpr unsigned int ChunkSequenceOffset = 8;
constexpr unsigned int ChunkLengthOffset = 12;
constexpr unsigned int ChunkDataOffset = 14;
constexpr unsigned int ChunkHeaderSize = 14;
constexpr unsigned int DeploySlotOffset = 8;
constexpr unsigned int DeployHashOffset = 12;
constexpr unsigned int DeployAbiVersionOffset = 44;
constexpr unsigned int DeployStateLayoutVersionOffset = 48;
constexpr unsigned int DeployNameOffset = 52;
constexpr unsigned int DeployBaseSize = 52;
constexpr unsigned int DeployNamedSize = 84;
}

static unsigned long long readU64(const unsigned char* input, unsigned int offset)
{
    unsigned long long value = 0;

    for (int byteIndex = 0; byteIndex < 8; byteIndex++)
    {
        value |= (unsigned long long)input[offset + byteIndex] << (8 * byteIndex);
    }

    return value;
}

static unsigned int readU32(const unsigned char* input, unsigned int offset)
{
    unsigned int value = 0;

    for (int byteIndex = 0; byteIndex < 4; byteIndex++)
    {
        value |= (unsigned int)input[offset + byteIndex] << (8 * byteIndex);
    }

    return value;
}

static unsigned int readU16(const unsigned char* input, unsigned int offset)
{
    return (unsigned int)input[offset] | ((unsigned int)input[offset + 1] << 8);
}

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

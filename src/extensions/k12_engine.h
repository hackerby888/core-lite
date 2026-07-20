#pragma once

#ifdef K12_ENGINE_CONTRACT_COUNT
constexpr unsigned int contractCount = K12_ENGINE_CONTRACT_COUNT;
#else
#include "contract_core/contract_def.h"
#endif
#include <K12/kangaroo_twelve_xkcp.h>
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

// qintrin maps AVX2 names to SIMDe on ARM; Blosc2 must see the real target ISA.
#if (defined(__aarch64__) || defined(_M_ARM64)) && defined(__AVX2__)
#undef __AVX2__
#include <blosc2.h>
#define __AVX2__ 1
#else
#include <blosc2.h>
#endif

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

class K12Engine
{
protected:
    struct Intermediate
    {
        unsigned char intermediate[maxCapacityInBytes];
    };

    // Per-chunk intermediate hashes and dirty flags.
    std::vector<Intermediate> intermediateMap;
    std::vector<unsigned char> isChunkChangedMap;
    unsigned int maxChunks;
    unsigned char *_state;
    size_t _stateSize;

    unsigned char *_lastOutput;
    size_t _lastOutputSize;

    int _KangarooTwelve_Update(
        XKCP::KangarooTwelve_Instance *ktInstance,
        const unsigned char *input,
        size_t inputByteLen,
        bool useCache)
    {
        if (ktInstance->phase != XKCP::ABSORBING)
            return 1;

        if (ktInstance->blockNumber == 0)
        {
            /* First block, absorb in final node */
            unsigned int len = inputByteLen < (K12_chunkSize - ktInstance->queueAbsorbedLen)
                ? (unsigned int)inputByteLen
                : (K12_chunkSize - ktInstance->queueAbsorbedLen);
            XKCP::TurboSHAKE_Absorb(&ktInstance->finalNode, input, len);
            input += len;
            inputByteLen -= len;
            ktInstance->queueAbsorbedLen += len;
            if ((ktInstance->queueAbsorbedLen == K12_chunkSize) && (inputByteLen != 0))
            {
                /* First block complete and more input data available, finalize it */
                const unsigned char padding = 0x03; /* '110^6': message hop, simple padding */
                ktInstance->queueAbsorbedLen = 0;
                ktInstance->blockNumber = 1;
                XKCP::TurboSHAKE_Absorb(&ktInstance->finalNode, &padding, 1);
                // Zero-pad to the next 64-bit boundary.
                ktInstance->finalNode.byteIOIndex =
                    (ktInstance->finalNode.byteIOIndex + 7) & ~7;
            }
        }
        else if (ktInstance->queueAbsorbedLen != 0)
        {
            /* There is data in the queue, absorb further in queue until block complete */
            unsigned int len = inputByteLen < (K12_chunkSize - ktInstance->queueAbsorbedLen)
                ? (unsigned int)inputByteLen
                : (K12_chunkSize - ktInstance->queueAbsorbedLen);
            XKCP::TurboSHAKE_Absorb(&ktInstance->queueNode, input, len);
            input += len;
            inputByteLen -= len;
            ktInstance->queueAbsorbedLen += len;
            if (ktInstance->queueAbsorbedLen == K12_chunkSize)
            {
                int capacityInBytes = 2 * (ktInstance->securityLevel) / 8;
                unsigned char intermediate[maxCapacityInBytes];
                // assert(capacityInBytes <= maxCapacityInBytes);
                ktInstance->queueAbsorbedLen = 0;
                ++ktInstance->blockNumber;
                XKCP::TurboSHAKE_AbsorbDomainSeparationByte(&ktInstance->queueNode, K12_suffixLeaf);
                XKCP::TurboSHAKE_Squeeze(&ktInstance->queueNode, intermediate, capacityInBytes);
                XKCP::TurboSHAKE_Absorb(&ktInstance->finalNode, intermediate, capacityInBytes);
            }
        }

        while (inputByteLen > 0)
        {
            int capacityInBytes = 2 * (ktInstance->securityLevel) / 8;
            unsigned int len = inputByteLen < K12_chunkSize
                ? (unsigned int)inputByteLen
                : K12_chunkSize;
            unsigned int chunkIndex = ktInstance->blockNumber;

            if (!isChunkChangedMap[chunkIndex] && useCache)
            {
                if (len == K12_chunkSize)
                {
                    unsigned char *intermediate = intermediateMap[chunkIndex].intermediate;
                    XKCP::TurboSHAKE_Absorb(&ktInstance->finalNode, intermediate, capacityInBytes);
                    input += len;
                    inputByteLen -= len;
                    ++ktInstance->blockNumber;
                    continue;
                }
            }

            XKCP::TurboSHAKE_Initialize(&ktInstance->queueNode, ktInstance->securityLevel);
            XKCP::TurboSHAKE_Absorb(&ktInstance->queueNode, input, len);
            input += len;
            inputByteLen -= len;
            if (len == K12_chunkSize)
            {
                unsigned char intermediate[maxCapacityInBytes];
                // assert(capacityInBytes <= maxCapacityInBytes);
                ++ktInstance->blockNumber;
                XKCP::TurboSHAKE_AbsorbDomainSeparationByte(&ktInstance->queueNode, K12_suffixLeaf);
                XKCP::TurboSHAKE_Squeeze(&ktInstance->queueNode, intermediate, capacityInBytes);
                XKCP::TurboSHAKE_Absorb(&ktInstance->finalNode, intermediate, capacityInBytes);

                // Cache the intermediate state.
                Intermediate &inter = intermediateMap[chunkIndex];
                std::memcpy(inter.intermediate, intermediate, capacityInBytes);
                isChunkChangedMap[chunkIndex] = false;
            }
            else
            {
                ktInstance->queueAbsorbedLen = len;
            }
        }

        return 0;
    }

public:
    K12Engine(unsigned char *state, size_t stateSize)
    {
        _state = state;
        _stateSize = stateSize;
        maxChunks = (stateSize + K12_chunkSize - 1) / K12_chunkSize;

        isChunkChangedMap.resize(maxChunks);
        intermediateMap.resize(maxChunks);
        for (unsigned int i = 0; i < maxChunks; i++)
        {
            isChunkChangedMap[i] = true;
        }
        for (unsigned int i = 0; i < maxChunks; i++)
        {
            std::memset(intermediateMap[i].intermediate, 0, maxCapacityInBytes);
        }

        _lastOutput = new unsigned char[1024];
        _lastOutputSize = 0;
    }

    ~K12Engine()
    {
        delete[] _lastOutput;
    }

    int getHash(unsigned char* output, size_t outputByteLen, bool useCache = true)
    {
        if (_lastOutput && outputByteLen == _lastOutputSize && isAllChunksUnchanged())
        {
            // Reuse the complete digest when no chunk changed.
            std::memcpy(output, _lastOutput, outputByteLen);
            return 0;
        }

        XKCP::KangarooTwelve_Instance ktInstance;

        if (outputByteLen == 0)
            return 1;
        XKCP::KangarooTwelve_Initialize(&ktInstance, 128, outputByteLen);
        if (_KangarooTwelve_Update(&ktInstance, _state, _stateSize, useCache) != 0)
            return 1;
        int ok = XKCP::KangarooTwelve_Final(&ktInstance, output, nullptr, 0);

        // Cache the complete digest.
        std::memcpy(_lastOutput, output, outputByteLen);
        _lastOutputSize = outputByteLen;

        return ok;
    }

    void markChunkChanged(unsigned int chunkIndex)
    {
        if (chunkIndex < maxChunks)
        {
            isChunkChangedMap[chunkIndex] = true;
        }
    }

    bool isAllChunksUnchanged() const
    {
        for (unsigned int i = 0; i < maxChunks; i++)
        {
            if (isChunkChangedMap[i])
            {
                return false;
            }
        }
        return true;
    }

    unsigned int getMaxChunks() const
    {
        return maxChunks;
    }
};

// In-RAM contract-state paging shared by Linux, macOS, and Windows.
class ContractStateEngine : public K12Engine
{
public:
#if defined(TESTNET) && defined(LITE_WASM_SC)
    static inline size_t MAX_RAM_USAGE = 1ULL * 1024 * 1024 * 1024;
#else
    static inline size_t MAX_RAM_USAGE = 10ULL * 1024 * 1024 * 1024;
#endif

    enum class BlockState : unsigned char
    {
        Zero,
        Compressed,
        Clean,
        Dirty,
        Evicting,
    };

    struct Block
    {
        std::atomic<BlockState> state{BlockState::Zero};
        std::atomic<bool> recent{false};
        std::vector<unsigned char> compressed;
    };

    static bool create(unsigned char **state, size_t stateSize, unsigned int contractIndex)
    {
        if (!state || contractIndex >= contractCount)
        {
            return false;
        }

        initializePager();

        const size_t paddedSize = roundUp(stateSize, sharedBlockSize);
        void* reservation = reserveState(stateSize);
        ContractStateEngine* engine;
        try
        {
            engine = new ContractStateEngine(state, reservation, stateSize);
        }
        catch (...)
        {
            releaseReservation(reservation, paddedSize);
            throw;
        }
        ContractStateEngine* previous = allEngines[contractIndex].exchange(
            engine,
            std::memory_order_acq_rel);
        delete previous;

        return true;
    }

    static void release(unsigned int contractIndex)
    {
        if (contractIndex >= contractCount)
        {
            return;
        }

        delete allEngines[contractIndex].exchange(nullptr, std::memory_order_acq_rel);
    }

    static ContractStateEngine* getEngine(unsigned int contractIndex)
    {
        if (contractIndex < contractCount)
        {
            return allEngines[contractIndex].load(std::memory_order_acquire);
        }
        return nullptr;
    }

    static bool handleFault(void* address)
    {
        ContractStateEngine* engine = findEngine(address);
        if (!engine)
        {
            return false;
        }

        const size_t blockIndex = engine->blockIndex(address);
        Block& block = engine->blocks[blockIndex];
        BlockState state = block.state.load(std::memory_order_acquire);

        while (state == BlockState::Evicting)
        {
            state = block.state.load(std::memory_order_acquire);
        }

        if (state == BlockState::Clean)
        {
            BlockState expected = BlockState::Clean;
            if (block.state.compare_exchange_strong(
                    expected,
                    BlockState::Dirty,
                    std::memory_order_acq_rel))
            {
                engine->markBlockChanged(blockIndex);
                block.recent.store(true, std::memory_order_relaxed);
                if (!engine->protectBlock(blockIndex, true))
                {
                    block.state.store(BlockState::Clean, std::memory_order_release);
                    return false;
                }
            }
            return true;
        }

        if (state == BlockState::Dirty)
        {
            return true;
        }

        // ponytail: cold faults are serialized; add a request queue if profiling needs it.
        while (coldFaultLock.test_and_set(std::memory_order_acquire))
        {
        }

        state = block.state.load(std::memory_order_acquire);
        bool success = true;
        if (state == BlockState::Zero || state == BlockState::Compressed)
        {
            success = requestRestore(engine, blockIndex);
        }
        coldFaultLock.clear(std::memory_order_release);
        return success;
    }

    static size_t tryEvictChunks(size_t requiredSize = 0)
    {
        size_t freed = 0;
        size_t attempts = totalBlockCount() * 2;
        while (getRamUsageByAllEngines() + requiredSize > MAX_RAM_USAGE && attempts--)
        {
            ContractStateEngine* engine;
            size_t blockIndex;
            if (!nextClockBlock(engine, blockIndex))
            {
                break;
            }

            Block& block = engine->blocks[blockIndex];
            if (block.state.load(std::memory_order_acquire) != BlockState::Clean)
            {
                continue;
            }
            if (block.recent.exchange(false, std::memory_order_relaxed))
            {
                continue;
            }
            if (engine->evictBlock(blockIndex))
            {
                freed += engine->blockSize;
            }
        }

        if (getRamUsageByAllEngines() + requiredSize > MAX_RAM_USAGE)
        {
            warnOverLimit();
        }
        return freed;
    }

    static size_t getRamUsageByAllEngines()
    {
        return residentBytes.load(std::memory_order_relaxed)
            + compressedBytes.load(std::memory_order_relaxed);
    }

    static size_t getResidentBytes()
    {
        return residentBytes.load(std::memory_order_relaxed);
    }

    static size_t getCompressedBytes()
    {
        return compressedBytes.load(std::memory_order_relaxed);
    }

    size_t getBlockSize() const
    {
        return blockSize;
    }

    int getHashAndProtect(unsigned char* output, size_t outputByteLen)
    {
        const int result = getHash(output, outputByteLen);
        for (size_t i = 0; i < blockCount; i++)
        {
            BlockState expected = BlockState::Dirty;
            if (blocks[i].state.load(std::memory_order_acquire) == BlockState::Dirty
                && protectBlock(i, false))
            {
                blocks[i].state.compare_exchange_strong(
                    expected,
                    BlockState::Clean,
                    std::memory_order_acq_rel);
            }
        }
        return result;
    }

private:
    static inline std::atomic<ContractStateEngine*> allEngines[contractCount]{};
    static inline std::atomic<size_t> residentBytes{0};
    static inline std::atomic<size_t> compressedBytes{0};
    static inline std::atomic_flag coldFaultLock = ATOMIC_FLAG_INIT;
    static inline std::once_flag pagerOnce;
    static inline size_t systemPageSize = 4096;
    static inline size_t sharedBlockSize = K12_chunkSize;
    static inline size_t clockEngineIndex = 0;
    static inline size_t clockBlockIndex = 0;

    struct PagerRequest
    {
        PagerRequest() : engine(nullptr), blockIndex(0), success(false) {}

        std::atomic<ContractStateEngine*> engine;
        std::atomic<size_t> blockIndex;
        std::atomic<bool> success;
    };
    static inline PagerRequest pagerRequest;

#ifdef _WIN32
    static inline HANDLE requestEvent = nullptr;
    static inline HANDLE responseEvent = nullptr;
#else
    static inline int requestPipe[2] = {-1, -1};
    static inline int responsePipe[2] = {-1, -1};
#endif

    const size_t blockSize;
    const size_t paddedSize;
    const size_t blockCount;
    std::unique_ptr<Block[]> blocks;

    static size_t roundUp(size_t value, size_t alignment)
    {
        return (value + alignment - 1) / alignment * alignment;
    }

    static void* reserveState(size_t size)
    {
        const size_t padded = roundUp(size, sharedBlockSize);
#ifdef _WIN32
        void* state = VirtualAlloc(nullptr, padded, MEM_RESERVE, PAGE_NOACCESS);
        if (!state)
        {
            throw std::runtime_error("VirtualAlloc(MEM_RESERVE) failed for contract state");
        }
#else
        void* state = mmap(
            nullptr,
            padded,
            PROT_NONE,
            MAP_PRIVATE | MAP_ANON,
            -1,
            0);
        if (state == MAP_FAILED)
        {
            throw std::runtime_error("mmap failed for contract state");
        }
#endif
        return state;
    }

    static void releaseReservation(void* state, size_t size)
    {
#ifdef _WIN32
        VirtualFree(state, 0, MEM_RELEASE);
#else
        munmap(state, size);
#endif
    }

    ContractStateEngine(unsigned char **state, void* reservation, size_t stateSize)
        : K12Engine((unsigned char*)reservation, stateSize),
          blockSize(sharedBlockSize),
          paddedSize(roundUp(stateSize, blockSize)),
          blockCount(paddedSize / blockSize),
          blocks(new Block[blockCount])
    {
        *state = _state;
        seedZeroStateCache();
    }

    ~ContractStateEngine()
    {
        for (size_t i = 0; i < blockCount; i++)
        {
            const BlockState state = blocks[i].state.load(std::memory_order_relaxed);
            if (state == BlockState::Clean
                || state == BlockState::Dirty
                || state == BlockState::Evicting)
            {
                residentBytes.fetch_sub(blockSize, std::memory_order_relaxed);
            }
            compressedBytes.fetch_sub(blocks[i].compressed.size(), std::memory_order_relaxed);
        }

        releaseReservation(_state, paddedSize);
    }

    void seedZeroStateCache()
    {
        const int securityLevel = 128;
        const int capacityInBytes = 2 * securityLevel / 8;
        static const unsigned char zeroChunk[K12_chunkSize] = {};
        Intermediate zeroIntermediate;
        std::memset(zeroIntermediate.intermediate, 0, maxCapacityInBytes);

        XKCP::TurboSHAKE_Instance queue;
        XKCP::TurboSHAKE_Initialize(&queue, securityLevel);
        XKCP::TurboSHAKE_Absorb(&queue, zeroChunk, K12_chunkSize);
        XKCP::TurboSHAKE_AbsorbDomainSeparationByte(&queue, K12_suffixLeaf);
        XKCP::TurboSHAKE_Squeeze(
            &queue,
            zeroIntermediate.intermediate,
            capacityInBytes);

        for (unsigned int i = 0; i < maxChunks; i++)
        {
            intermediateMap[i] = zeroIntermediate;
            isChunkChangedMap[i] = false;
        }
        _lastOutputSize = 0;
    }

    static ContractStateEngine* findEngine(void* address)
    {
        const uintptr_t fault = (uintptr_t)address;
        for (size_t i = 0; i < contractCount; i++)
        {
            ContractStateEngine* engine = allEngines[i].load(std::memory_order_acquire);
            if (engine
                && fault >= (uintptr_t)engine->_state
                && fault < (uintptr_t)engine->_state + engine->paddedSize)
            {
                return engine;
            }
        }
        return nullptr;
    }

    size_t blockIndex(void* address) const
    {
        return ((uintptr_t)address - (uintptr_t)_state) / blockSize;
    }

    unsigned char* blockAddress(size_t blockIndex) const
    {
        return _state + blockIndex * blockSize;
    }

    void markBlockChanged(size_t blockIndex)
    {
        const size_t firstChunk = blockIndex * blockSize / K12_chunkSize;
        const size_t chunkCount = blockSize / K12_chunkSize;
        for (size_t i = 0; i < chunkCount && firstChunk + i < maxChunks; i++)
        {
            markChunkChanged((unsigned int)(firstChunk + i));
        }
    }

    bool protectBlock(size_t blockIndex, bool writable)
    {
        unsigned char* address = blockAddress(blockIndex);
#ifdef _WIN32
        DWORD oldProtection;
        return VirtualProtect(
            address,
            blockSize,
            writable ? PAGE_READWRITE : PAGE_READONLY,
            &oldProtection) != 0;
#else
        return mprotect(
            address,
            blockSize,
            writable ? (PROT_READ | PROT_WRITE) : PROT_READ) == 0;
#endif
    }

    bool evictBlock(size_t blockIndex)
    {
        Block& block = blocks[blockIndex];
        BlockState expected = BlockState::Clean;
        if (!block.state.compare_exchange_strong(
                expected,
                BlockState::Evicting,
                std::memory_order_acq_rel))
        {
            return false;
        }

        unsigned char* address = blockAddress(blockIndex);
        const bool zero = std::all_of(
            address,
            address + blockSize,
            [](unsigned char byte) { return byte == 0; });

        std::vector<unsigned char> compressed;
        if (!zero)
        {
            compressed.resize(blockSize + BLOSC2_MAX_OVERHEAD);
            const int compressedSize = blosc2_compress(
                1,
                BLOSC_NOSHUFFLE,
                1,
                address,
                (int32_t)blockSize,
                compressed.data(),
                (int32_t)compressed.size());
            if (compressedSize <= 0)
            {
                block.state.store(BlockState::Clean, std::memory_order_release);
                return false;
            }
            compressed.resize((size_t)compressedSize);
        }

        if (!releasePhysicalBlock(address))
        {
            block.state.store(BlockState::Clean, std::memory_order_release);
            return false;
        }

        block.compressed = std::move(compressed);
        residentBytes.fetch_sub(blockSize, std::memory_order_relaxed);
        compressedBytes.fetch_add(block.compressed.size(), std::memory_order_relaxed);
        block.state.store(
            zero ? BlockState::Zero : BlockState::Compressed,
            std::memory_order_release);
        return true;
    }

    static bool releasePhysicalBlock(unsigned char* address)
    {
#ifdef _WIN32
        return VirtualFree(address, sharedBlockSize, MEM_DECOMMIT) != 0;
#else
        void* replacement = mmap(
            address,
            sharedBlockSize,
            PROT_NONE,
            MAP_PRIVATE | MAP_ANON | MAP_FIXED,
            -1,
            0);
        return replacement == address;
#endif
    }

    bool restoreBlock(size_t blockIndex)
    {
        Block& block = blocks[blockIndex];
        const BlockState previous = block.state.load(std::memory_order_acquire);
        if (previous != BlockState::Zero && previous != BlockState::Compressed)
        {
            return true;
        }

        unsigned char* address = blockAddress(blockIndex);
        if (!commitBlock(address))
        {
            return false;
        }

        if (previous == BlockState::Compressed)
        {
            const int decompressedSize = blosc2_decompress(
                block.compressed.data(),
                (int32_t)block.compressed.size(),
                address,
                (int32_t)blockSize);
            if (decompressedSize != (int)blockSize)
            {
                releasePhysicalBlock(address);
                return false;
            }
        }

        const bool clean = protectBlock(blockIndex, false);
        const size_t oldCompressedSize = block.compressed.size();
        std::vector<unsigned char>().swap(block.compressed);
        compressedBytes.fetch_sub(oldCompressedSize, std::memory_order_relaxed);
        residentBytes.fetch_add(blockSize, std::memory_order_relaxed);
        block.recent.store(true, std::memory_order_relaxed);
        if (clean)
        {
            block.state.store(BlockState::Clean, std::memory_order_release);
        }
        else
        {
            markBlockChanged(blockIndex);
            block.state.store(BlockState::Dirty, std::memory_order_release);
        }
        return true;
    }

    static bool commitBlock(unsigned char* address)
    {
#ifdef _WIN32
        return VirtualAlloc(
            address,
            sharedBlockSize,
            MEM_COMMIT,
            PAGE_READWRITE) == address;
#else
        return mmap(
            address,
            sharedBlockSize,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANON | MAP_FIXED,
            -1,
            0) == address;
#endif
    }

    static void initializePager()
    {
        std::call_once(pagerOnce, []()
        {
#ifdef _WIN32
            SYSTEM_INFO systemInfo;
            GetSystemInfo(&systemInfo);
            systemPageSize = systemInfo.dwPageSize ? systemInfo.dwPageSize : 4096;
            requestEvent = ::CreateEventW(nullptr, FALSE, FALSE, nullptr);
            responseEvent = ::CreateEventW(nullptr, FALSE, FALSE, nullptr);
            if (!requestEvent || !responseEvent)
            {
                throw std::runtime_error("CreateEvent failed for contract-state pager");
            }
            if (!AddVectoredExceptionHandler(1, handleWindowsException))
            {
                throw std::runtime_error("AddVectoredExceptionHandler failed for contract-state pager");
            }
#else
            const long pageSize = sysconf(_SC_PAGESIZE);
            systemPageSize = pageSize > 0 ? (size_t)pageSize : 4096;
            if (pipe(requestPipe) != 0 || pipe(responsePipe) != 0)
            {
                throw std::runtime_error("pipe failed for contract-state pager");
            }
#endif
            sharedBlockSize = K12_chunkSize;
            while (sharedBlockSize % systemPageSize != 0)
            {
                sharedBlockSize += K12_chunkSize;
            }
            std::call_once(bloscInitOnce, []() { blosc2_init(); });
            std::thread(pagerLoop).detach();
        });
    }

    static inline std::once_flag bloscInitOnce;

    static void pagerLoop()
    {
        while (true)
        {
#ifdef _WIN32
            if (WaitForSingleObject(requestEvent, INFINITE) != WAIT_OBJECT_0)
            {
                continue;
            }
#else
            if (!readByte(requestPipe[0]))
            {
                continue;
            }
#endif
            ContractStateEngine* engine = pagerRequest.engine.load(std::memory_order_acquire);
            const size_t blockIndex = pagerRequest.blockIndex.load(std::memory_order_acquire);
            const bool success = engine && engine->restoreBlock(blockIndex);
            pagerRequest.success.store(success, std::memory_order_release);
#ifdef _WIN32
            SetEvent(responseEvent);
#else
            writeByte(responsePipe[1]);
#endif
        }
    }

    static bool requestRestore(ContractStateEngine* engine, size_t blockIndex)
    {
        pagerRequest.engine.store(engine, std::memory_order_release);
        pagerRequest.blockIndex.store(blockIndex, std::memory_order_release);
        pagerRequest.success.store(false, std::memory_order_release);
#ifdef _WIN32
        if (!SetEvent(requestEvent)
            || WaitForSingleObject(responseEvent, INFINITE) != WAIT_OBJECT_0)
        {
            return false;
        }
#else
        if (!writeByte(requestPipe[1]) || !readByte(responsePipe[0]))
        {
            return false;
        }
#endif
        return pagerRequest.success.load(std::memory_order_acquire);
    }

#ifndef _WIN32
    static bool readByte(int fd)
    {
        char byte;
        ssize_t result;
        do
        {
            result = read(fd, &byte, 1);
        } while (result < 0 && errno == EINTR);
        return result == 1;
    }

    static bool writeByte(int fd)
    {
        const char byte = 1;
        ssize_t result;
        do
        {
            result = write(fd, &byte, 1);
        } while (result < 0 && errno == EINTR);
        return result == 1;
    }
#else
    static LONG WINAPI handleWindowsException(EXCEPTION_POINTERS* exception)
    {
        if (exception->ExceptionRecord->ExceptionCode != EXCEPTION_ACCESS_VIOLATION)
        {
            return EXCEPTION_CONTINUE_SEARCH;
        }
        void* address = (void*)exception->ExceptionRecord->ExceptionInformation[1];
        return handleFault(address)
            ? EXCEPTION_CONTINUE_EXECUTION
            : EXCEPTION_CONTINUE_SEARCH;
    }
#endif

    static size_t totalBlockCount()
    {
        size_t count = 0;
        for (size_t i = 0; i < contractCount; i++)
        {
            ContractStateEngine* engine = allEngines[i].load(std::memory_order_acquire);
            if (engine)
            {
                count += engine->blockCount;
            }
        }
        return count;
    }

    static bool nextClockBlock(ContractStateEngine*& engine, size_t& blockIndex)
    {
        for (size_t attempts = 0; attempts <= contractCount; attempts++)
        {
            engine = allEngines[clockEngineIndex].load(std::memory_order_acquire);
            if (engine && clockBlockIndex < engine->blockCount)
            {
                blockIndex = clockBlockIndex++;
                return true;
            }
            clockEngineIndex = (clockEngineIndex + 1) % contractCount;
            clockBlockIndex = 0;
        }
        return false;
    }

    static void warnOverLimit()
    {
        static auto lastWarning = std::chrono::steady_clock::time_point::min();
        const auto now = std::chrono::steady_clock::now();
        if (lastWarning == std::chrono::steady_clock::time_point::min()
            || now - lastWarning >= std::chrono::minutes(1))
        {
            std::cerr << "Contract-state memory remains above --max-sc-mem; "
                      << "compressed state stays in RAM\n";
            lastWarning = now;
        }
    }
};

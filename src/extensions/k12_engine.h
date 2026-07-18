#pragma once

#define _GNU_SOURCE
#include "contract_core/contract_def.h"
#include "extensions/utils.h"
#include "userfaultfd.h"
#include <K12/kangaroo_twelve_xkcp.h>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <linux/userfaultfd.h>
#include <list>
#include <mutex>
#include <stdio.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/poll.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <functional>
#include <zstd.h>

#ifdef LITE_ENGINE_DEBUG
extern std::function<void()> engineCustomeActionCallback;
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
    std::vector<bool> isChunkChangedMap;
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

// Add Linux userfaultfd paging to K12Engine.
class ContractStateEngine : public K12Engine
{
public:
    // Global access tracker for LRU eviction.
#if defined(TESTNET) && defined(LITE_WASM_SC)
    static inline size_t MAX_RAM_USEAGE = 1ULL * 1024 * 1024 * 1024;
#else
    static inline size_t MAX_RAM_USEAGE = 10ULL * 1024 * 1024 * 1024;
#endif
    static inline std::list<unsigned long long> accessList;
    static inline std::unordered_map<
        unsigned long long,
        std::list<unsigned long long>::iterator> accessMap;

    // Cold chunks use either compressed memory or per-contract files.
    enum class EvictMode
    {
        Compress,
        Disk,
    };
    static inline EvictMode evictMode = EvictMode::Compress;
    static inline size_t g_compressedBytes = 0;

    // IO related
    static constexpr size_t MAX_IO_NAME_LEN = 128;
    static constexpr CHAR16 BASE_DIR[] = L"contract_states/";
    static inline std::unordered_map<unsigned int, std::mutex> ioLocks;

    // Lazy loading related
    static inline std::vector<ContractStateEngine*> allEngines;
    UserFaultFD uffd;
    size_t nonPaddedSize;
    size_t paddedSize;
    unsigned int contractIndex;
    bool isUffdRegistered = false;
    std::mutex faultLock;
    int diskFd = -1;
    int memfdFd = -1;

    std::vector<bool> isChunkLoadedInMemoryMap;
    // Compressed chunks replace the disk file in memory mode.
    std::unordered_map<unsigned int, std::vector<unsigned char>> compressedChunks;
    std::vector<unsigned char> compressScratch;
    unsigned char tmpBuffer[K12_chunkSize];
    // Keep fault loading separate from concurrent eviction writes.
    unsigned char loadBuffer[K12_chunkSize];

    // Pass allocateState's memfd through the base-class constructor call.
    static inline thread_local int lastMemfdFd = -1;

    static void* allocateState(size_t size)
    {
        size = alignToPageSize(size);
        int fd = memfd_create("qlite", MFD_CLOEXEC);
        if (fd == -1)
        {
            throw std::runtime_error(
                "Error: memfd_create failed | Line: " + std::to_string(__LINE__));
        }
        if (ftruncate(fd, size) == -1)
        {
            close(fd);
            throw std::runtime_error("Error: ftruncate failed | Line: " + std::to_string(__LINE__));
        }

        void* buffer = mmap(
            nullptr,
            size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            fd,
            0);
        if (buffer == MAP_FAILED)
        {
            close(fd);
            throw std::runtime_error("Error: mmap failed | Line: " + std::to_string(__LINE__));
        }

        // Compressed mode keeps the zero-filled memfd demand-zero.
        if (evictMode != EvictMode::Compress)
        {
            memset(buffer, 0, size);
        }
        lastMemfdFd = fd;
        return buffer;
    }

    static bool create(unsigned char **state, size_t stateSize, unsigned int contractIndex)
    {
        static std::once_flag flag;
        std::call_once(flag, []()
        {
            allEngines.resize(contractCount);
        });

        auto engine = new ContractStateEngine(state, stateSize, contractIndex);
        engine->registerUserFaultFD();

        allEngines[contractIndex] = engine;

        return true;
    }

    static void registerAllUserFaultFDs()
    {
        for (auto engine : allEngines)
        {
            if (engine)
            {
                engine->registerUserFaultFD();
            }
        }
    }

    static ContractStateEngine* getEngine(unsigned int contractIndex)
    {
        if (contractIndex < allEngines.size())
        {
            return allEngines[contractIndex];
        }
        return nullptr;
    }

    static void updateAccessTracker(unsigned int contractIndex, unsigned int chunkIndex)
    {
        unsigned long long key = ((unsigned long long)contractIndex << 32) | chunkIndex;
        auto it = accessMap.find(key);
        if (it != accessMap.end())
        {
            accessList.splice(accessList.begin(), accessList, it->second);
        }
        else
        {
            accessList.push_front(key);
            accessMap[key] = accessList.begin();
        }
    }

    static size_t getRamUsageByAllEngines()
    {
        size_t usage = g_compressedBytes;
        for (auto engine : allEngines)
        {
            if (engine)
            {
                usage += engine->getTotalMemoryInRam();
            }
        }
        return usage;
    }

    static size_t tryEvictChunks(size_t requiredSize = 0)
    {
        size_t freedSize = 0;
        while (getRamUsageByAllEngines() + requiredSize > MAX_RAM_USEAGE && !accessList.empty())
        {
            unsigned long long key = accessList.back();
            accessList.pop_back();
            accessMap.erase(key);

            unsigned int contractIndex = (unsigned int)(key >> 32);
            unsigned int chunkIndex = (unsigned int)(key & 0xFFFFFFFF);

            ContractStateEngine* engine = getEngine(contractIndex);
            if (engine && engine->isChunkLoadedInMemoryMap[chunkIndex])
            {
                if (engine->saveChunkToDisk(chunkIndex))
                {
                    freedSize += K12_chunkSize;
                }
            }
        }
        return freedSize;
    }

    // Per-engine cursor for bounded resident-chunk scans.
    static inline std::unordered_map<unsigned int, unsigned int> evictCursor;

    // Evict a bounded batch of cold resident chunks when over the RAM cap.
    static size_t tryEvictResidentBatch(size_t maxToEvict)
    {
        if (getRamUsageByAllEngines() <= MAX_RAM_USEAGE)
        {
            return 0;
        }

        size_t freedSize = 0;
        size_t evictedCount = 0;
        for (auto* engine : allEngines)
        {
            if (!engine)
            {
                continue;
            }

            unsigned int chunkIndex = evictCursor[engine->contractIndex];
            for (;
                 chunkIndex < engine->maxChunks && evictedCount < maxToEvict;
                 chunkIndex++)
            {
                if (!engine->isChunkLoadedInMemoryMap[chunkIndex])
                {
                    continue;
                }
                unsigned long long key =
                    ((unsigned long long)engine->contractIndex << 32) | chunkIndex;
                if (accessMap.find(key) != accessMap.end())
                {
                    continue;
                }
                if (engine->saveChunkToDisk(chunkIndex))
                {
                    freedSize += K12_chunkSize;
                    evictedCount++;
                }
            }
            evictCursor[engine->contractIndex] =
                chunkIndex >= engine->maxChunks ? 0 : chunkIndex;
            if (evictedCount >= maxToEvict)
            {
                break;
            }
        }
        return freedSize;
    }

    // Seed every chunk with the canonical zero-state intermediate hash.
    void seedZeroStateCache()
    {
        const int securityLevel = 128;
        const int capacityInBytes = 2 * securityLevel / 8;
        static const unsigned char zeroChunk[K12_chunkSize] = {};
        Intermediate zeroInter;
        std::memset(zeroInter.intermediate, 0, maxCapacityInBytes);
        XKCP::TurboSHAKE_Instance q;
        XKCP::TurboSHAKE_Initialize(&q, securityLevel);
        XKCP::TurboSHAKE_Absorb(&q, zeroChunk, K12_chunkSize);
        XKCP::TurboSHAKE_AbsorbDomainSeparationByte(&q, K12_suffixLeaf);
        XKCP::TurboSHAKE_Squeeze(&q, zeroInter.intermediate, capacityInBytes);
        for (unsigned int i = 0; i < maxChunks; i++)
        {
            intermediateMap[i] = zeroInter;
            isChunkChangedMap[i] = false;
        }
        // Recompute the first digest from cached zero-state leaves.
        _lastOutputSize = 0;
    }

    ContractStateEngine(unsigned char **state, size_t stateSize, unsigned int contractIndex)
        : K12Engine((unsigned char*)allocateState(stateSize), stateSize)
    {
        memfdFd = lastMemfdFd;
        lastMemfdFd = -1;
        *state = _state;
        this->contractIndex = contractIndex;
        this->nonPaddedSize = stateSize;
        this->paddedSize = alignToPageSize(stateSize);
        this->isChunkLoadedInMemoryMap.resize(maxChunks);
        // Compressed mode leaves untouched memfd pages non-resident.
        const bool bootResident = (evictMode != EvictMode::Compress);
        for (unsigned int i = 0; i < maxChunks; i++)
        {
            isChunkLoadedInMemoryMap[i] = bootResident;
        }

        // Avoid faulting in zero-state pages for the initial digest.
        seedZeroStateCache();

        // Disk mode uses one sparse backing file per contract.
        if (evictMode == EvictMode::Disk)
        {
            createDir(BASE_DIR);
            char path[64];
            std::snprintf(path, sizeof(path), "contract_states/contract_%04u.bin", contractIndex);
            diskFd = open(path, O_RDWR | O_CREAT, 0600);
            if (diskFd == -1)
            {
                throw std::runtime_error(
                    "Error: open contract state file failed: " + std::string(path));
            }
            if (ftruncate(diskFd, paddedSize) == -1)
            {
                close(diskFd);
                throw std::runtime_error("Error: ftruncate failed: " + std::string(path));
            }
        }
    }

    bool loadChunkFromDisk(unsigned int chunkIndex, unsigned char *destBuffer, size_t chunkSize)
    {
        std::lock_guard<std::mutex> lock(ioLocks[contractIndex]);

        size_t expectedSize = K12_chunkSize;
        if (paddedSize % K12_chunkSize != 0 && chunkIndex == maxChunks - 1)
        {
            expectedSize = paddedSize % K12_chunkSize;
        }
        if (chunkSize != expectedSize)
        {
            std::cout << "Contract " << contractIndex << ": Chunk " << chunkIndex
                      << " requested size mismatch. Requested: " << chunkSize
                      << ", expected: " << expectedSize << "\n";
            return false;
        }

        if (evictMode == EvictMode::Compress)
        {
            auto it = compressedChunks.find(chunkIndex);
            if (it == compressedChunks.end())
            {
                // Missing compressed chunks are still zero-filled and sparse.
                setMem(destBuffer, chunkSize, 0);
            }
            else
            {
                const size_t decompressedSize = ZSTD_decompress(
                    destBuffer,
                    chunkSize,
                    it->second.data(),
                    it->second.size());
                if (ZSTD_isError(decompressedSize) || decompressedSize != chunkSize)
                {
                    std::cout << "Contract " << contractIndex
                              << ": zstd decompress chunk " << chunkIndex
                              << " failed\n";
                    return false;
                }
                g_compressedBytes -= it->second.size();
                compressedChunks.erase(it);
            }
            isChunkLoadedInMemoryMap[chunkIndex] = true;
            return true;
        }

        off_t offset = (off_t)chunkIndex * (off_t)K12_chunkSize;
        ssize_t n = pread(diskFd, destBuffer, chunkSize, offset);
        if (n != (ssize_t)chunkSize)
        {
            std::cout << "Contract " << contractIndex << ": pread chunk " << chunkIndex
                      << " failed, got " << n << " expected " << chunkSize << "\n";
            return false;
        }
        isChunkLoadedInMemoryMap[chunkIndex] = true;
        return true;
    }

    bool saveChunkToDisk(unsigned int chunkIndex)
    {
        std::lock_guard<std::mutex> lock(ioLocks[contractIndex]);

        size_t offset = chunkIndex * (size_t)K12_chunkSize;
        size_t chunkSize = K12_chunkSize;
        if (paddedSize % K12_chunkSize != 0 && chunkIndex == maxChunks - 1)
        {
            chunkSize = paddedSize % K12_chunkSize;
        }

        // Read through memfd so sparse pages return zeros without UFFD faults.
        const ssize_t bytesRead = pread(memfdFd, tmpBuffer, chunkSize, (off_t)offset);
        bool success = bytesRead == (ssize_t)chunkSize;
        if (!success)
        {
            std::cout << "Contract " << contractIndex << ": pread memfd chunk " << chunkIndex
                      << " failed, got " << bytesRead << " expected " << chunkSize
                      << " errno=" << errno << " (" << strerror(errno) << ")\n";
        }
        else if (evictMode == EvictMode::Compress)
        {
            // Store an exact-size level-one zstd blob in the reused scratch buffer.
            const size_t compressionBound = ZSTD_compressBound(chunkSize);
            if (compressScratch.size() < compressionBound)
            {
                compressScratch.resize(compressionBound);
            }
            const size_t compressedSize = ZSTD_compress(
                compressScratch.data(),
                compressScratch.size(),
                tmpBuffer,
                chunkSize,
                1);
            if (ZSTD_isError(compressedSize))
            {
                std::cout << "Contract " << contractIndex
                          << ": zstd compress chunk " << chunkIndex
                          << " failed\n";
                success = false;
            }
            else
            {
                compressedChunks[chunkIndex].assign(
                    compressScratch.data(),
                    compressScratch.data() + compressedSize);
                g_compressedBytes += compressedSize;
            }
        }
        else
        {
            const ssize_t bytesWritten =
                pwrite(diskFd, tmpBuffer, chunkSize, (off_t)offset);
            success = bytesWritten == (ssize_t)chunkSize;
            if (!success)
            {
                std::cout << "Contract " << contractIndex << ": pwrite disk chunk " << chunkIndex
                          << " failed, got " << bytesWritten << " expected " << chunkSize
                          << " errno=" << errno << " (" << strerror(errno) << ")\n";
            }
        }
        if (success)
        {
            isChunkLoadedInMemoryMap[chunkIndex] = false;
        }

        // Release the evicted memfd pages.
        if (madvise(_state + offset, chunkSize, MADV_REMOVE) == -1)
        {
            std::cout << "Contract " << contractIndex << ": madvise failed for chunk " << chunkIndex << "\n";
            success = false;
        }

        return success;
    }

    bool flushAllChunksToDisk(bool needToBeChanged = false)
    {
        bool allOk = true;
        for (unsigned int i = 0; i < maxChunks; i++)
        {
            if ((!needToBeChanged || isChunkChangedMap[i]) && isChunkLoadedInMemoryMap[i])
            {
                bool ok = saveChunkToDisk(i);
                if (!ok)
                {
                    std::cout << "Contract " << contractIndex << ": Failed to save chunk " << i << " to disk\n";
                    allOk = false;
                }
            }
        }
        return allOk;
    }

    void registerUserFaultFD()
    {
        if (isUffdRegistered)
        {
            return;
        }

        // Register missing, minor, and write-protect page faults.
        uffdio_register reg{};
        reg.range.start = (uint64_t)_state;
        reg.range.len = paddedSize;
        reg.mode = UFFDIO_REGISTER_MODE_WP
            | UFFDIO_REGISTER_MODE_MISSING
            | UFFDIO_REGISTER_MODE_MINOR;

        if (ioctl(uffd.get(), UFFDIO_REGISTER, &reg) == -1)
        {
            throw std::runtime_error(
                "Error: UFFDIO_REGISTER ioctl failed for contract "
                + std::to_string(contractIndex));
        }

        isUffdRegistered = true;

        std::thread handler([=]()
            {
                size_t pageSize = SYSTEM_PAGE_SIZE;
                pollfd pfd{ uffd.get(), POLLIN, 0 };
                while (true)
                {
                    poll(&pfd, 1, -1);
                    uffd_msg msg;
                    if (read(uffd.get(), &msg, sizeof(msg)) != sizeof(msg))
                    {
                        continue;
                    }

                    if (msg.event != UFFD_EVENT_PAGEFAULT)
                    {
                        continue;
                    }

                    const auto flags = msg.arg.pagefault.flags;
                    const bool isWriteProtect =
                        flags & UFFD_PAGEFAULT_FLAG_WP;
                    const bool isMinor = flags & UFFD_PAGEFAULT_FLAG_MINOR;
                    const bool isMissing = !isWriteProtect && !isMinor;

                    auto accessAddress = msg.arg.pagefault.address;

                    size_t offset = accessAddress - (size_t)_state;
                    unsigned int chunkIndex = offset / K12_chunkSize;

                    size_t startRange =
                        (size_t)_state + (chunkIndex * (size_t)K12_chunkSize);
                    size_t lenRange = std::min(
                        paddedSize - (chunkIndex * (size_t)K12_chunkSize),
                        (size_t)K12_chunkSize);

#ifdef LITE_ENGINE_DEBUG
                    if (engineCustomeActionCallback)
                    {
                        engineCustomeActionCallback();
                    }
#endif

                    {
                        // Resume a write after recording the dirty chunk.
                        if (isWriteProtect)
                        {
                            updateAccessTracker(contractIndex, chunkIndex);
                            markChunkChanged(chunkIndex);
#ifdef LITE_ENGINE_DEBUG
                            printf(
                                "Contract %u: page fault at address 0x%llx, chunk %u marked changed\n",
                                contractIndex,
                                (unsigned long long)accessAddress,
                                chunkIndex);
#endif

                            uffdio_writeprotect uwp{};
                            uwp.range.start = startRange;
                            uwp.range.len = lenRange;
                            uwp.mode = 0;

                            if (ioctl(uffd.get(), UFFDIO_WRITEPROTECT, &uwp) == -1)
                            {
                                std::cout << "Contract " << contractIndex
                                          << ": UFFDIO_WRITEPROTECT remove failed\n";
                            }
                        }

                        // Restore a missing chunk before resuming the faulting thread.
                        if (isMissing)
                        {
                            bool loadOk = false;
                            do
                            {
                                loadOk = loadChunkFromDisk(chunkIndex, loadBuffer, lenRange);
                                if (!loadOk)
                                {
                                    std::cout << "Critical error: Contract " << contractIndex
                                              << ": Failed to load chunk " << chunkIndex
                                              << " from disk. Retrying in 1 second...\n";
                                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                                }
                            } while (!loadOk);
#ifdef LITE_ENGINE_DEBUG
                            printf(
                                "Loaded chunk %u for contract %u from disk\n",
                                chunkIndex,
                                contractIndex);
#endif

                            uffdio_copy copyRequest{};
                            copyRequest.src = (uint64_t)loadBuffer;
                            copyRequest.dst = startRange;
                            copyRequest.len = lenRange;
                            copyRequest.mode = UFFDIO_CONTINUE_MODE_WP;
                            if (ioctl(
                                    uffd.get(),
                                    UFFDIO_COPY,
                                    &copyRequest) == -1)
                            {
                                std::cout << "Contract " << contractIndex
                                          << ": UFFDIO_COPY failed\n";
                            }
                            if (copyRequest.copy != copyRequest.len)
                            {
                                std::cout << "Contract " << contractIndex
                                          << ": UFFDIO_COPY incomplete copy\n";
                            }
                            // Protect the restored chunk from immediate eviction.
                            updateAccessTracker(contractIndex, chunkIndex);
                        }

                        // Minor faults restore mappings for resident memfd pages.
                        if (isMinor)
                        {
                            updateAccessTracker(contractIndex, chunkIndex);
#ifdef LITE_ENGINE_DEBUG
                            printf(
                                "Found minor page fault at contract %llu address 0x%llx, chunk %u\n",
                                contractIndex,
                                (unsigned long long)accessAddress,
                                chunkIndex);
#endif
                            uffdio_continue continueRequest{};
                            continueRequest.range.start = startRange;
                            continueRequest.range.len = lenRange;
                            continueRequest.mode = UFFDIO_CONTINUE_MODE_WP;
                            if (ioctl(
                                    uffd.get(),
                                    UFFDIO_CONTINUE,
                                    &continueRequest) == -1)
                            {
                                std::cout << "Contract " << contractIndex
                                          << ": UFFDIO_CONTINUE failed\n";
                                while (true)
                                {
                                    // Remove the PTE before retrying the continuation.
                                    if (madvise((void*)startRange, lenRange, MADV_DONTNEED) == -1)
                                    {
                                        std::cout << "Contract " << contractIndex
                                                  << ": madvise failed during UFFDIO_CONTINUE retry\n";
                                        std::cout << "Error " << errno << ": "
                                                  << strerror(errno) << "\n";
                                    }
                                    if (ioctl(
                                            uffd.get(),
                                            UFFDIO_CONTINUE,
                                            &continueRequest) != -1)
                                    {
                                        break;
                                    }
                                    std::cout << "Contract " << contractIndex
                                              << ": UFFDIO_CONTINUE retry failed\n";
                                    std::cout << "Error " << errno << ": "
                                              << strerror(errno) << "\n";
                                    std::cout << "Details: address 0x" << std::hex
                                              << continueRequest.range.start << std::dec
                                              << ", length "
                                              << continueRequest.range.len << "\n";
                                    std::cout << "Chunk index: " << chunkIndex
                                              << " | Max chunks: " << maxChunks << "\n";
                                    if (continueRequest.range.start % pageSize != 0)
                                    {
                                        std::cout << "Contract " << contractIndex
                                                  << ": UFFDIO_CONTINUE failed due to unaligned address 0x"
                                                  << std::hex
                                                  << continueRequest.range.start
                                                  << std::dec << "\n";
                                    }
                                    if (continueRequest.range.len % pageSize != 0)
                                    {
                                        std::cout << "Contract " << contractIndex
                                                  << ": UFFDIO_CONTINUE failed due to unaligned length "
                                                  << continueRequest.range.len << "\n";
                                    }
                                    if (errno != EEXIST)
                                    {
                                        std::cout << "Contract " << contractIndex
                                                  << ": UFFDIO_CONTINUE failed due to unexpected error (cannot ignored)\n";
                                    }
                                    else
                                    {
                                        std::cout << "Contract " << contractIndex
                                                  << ": UFFDIO_CONTINUE failed due to EEXIST (page already present), skip continue operation\n";
                                        markChunkChanged(chunkIndex);
                                        // EEXIST requires an explicit wake for the faulting thread.
                                        uffdio_range range;
                                        range.start = startRange;
                                        range.len = lenRange;
                                        if (ioctl(uffd.get(), UFFDIO_WAKE, &range) == -1)
                                        {
                                            std::cout << "Contract " << contractIndex << ": UFFDIO_WAKE failed\n";
                                        }
                                        break;
                                    }
                                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                                }
                            }
                        }
                    }
                }
            });
        handler.detach();
    }

    size_t getTotalMemoryInRam()
    {
        size_t totalRam = 0;
        for (unsigned int i = 0; i < maxChunks; i++)
        {
            if (isChunkLoadedInMemoryMap[i])
            {
                if (paddedSize % K12_chunkSize != 0 && i == maxChunks - 1)
                {
                    totalRam += paddedSize % K12_chunkSize;
                }
                else
                {
                    totalRam += K12_chunkSize;
                }
            }
        }
        return totalRam;
    }

    void reprotectWriteRegion(size_t startOffset = 0, size_t len = 0)
    {
        if (!isUffdRegistered)
        {
            return;
        }

        std::lock_guard<std::mutex> lock(faultLock);

        if (len == 0 && startOffset == 0)
        {
            len = paddedSize;
        }

        uffdio_writeprotect wp {};

        wp.range.start = (uint64_t)_state + startOffset;
        wp.range.len   = len;

        wp.mode = UFFDIO_WRITEPROTECT_MODE_WP;

        if (ioctl(uffd.get(), UFFDIO_WRITEPROTECT, &wp) == -1)
        {
            std::cout << "Contract " << contractIndex << ": UFFDIO_WRITEPROTECT failesd\n";

            // Fall back to recomputing every intermediate hash.
            for (unsigned int i = 0; i < maxChunks; i++)
            {
                isChunkChangedMap[i] = true;
            }
        }
    }

    void reprotectReadRegion(size_t startOffset = 0, size_t len = 0)
    {
        if (!isUffdRegistered)
        {
            return;
        }

        std::lock_guard<std::mutex> lock(faultLock);

        if (len == 0 && startOffset == 0)
        {
            len = paddedSize;
        }

        // Drop PTE mappings while retaining the memfd data.
        madvise(_state + startOffset, len, MADV_DONTNEED);
    }

    unsigned long long touchAllPages()
    {
        unsigned long long sum = 0;
        for (size_t offset = 0; offset < paddedSize; offset += SYSTEM_PAGE_SIZE)
        {
            sum += _state[offset];
        }
        return sum;
    }

    int getHashAndReprotect(unsigned char* output, size_t outputByteLen)
    {
        int res = getHash(output, outputByteLen);
        reprotectWriteRegion();
        reprotectReadRegion();
        return res;
    }

};

// Focused parity and lifecycle checks for the in-RAM contract-state engine.

#define NO_UEFI

#include "platform/msvc_polyfill.h"
#include "platform/file_io.h"
#include "platform/m256.h"
#include <extensions/k12_engine.h>
#ifdef _WIN32
#include "contract_core/contract_exec.h"
#endif
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cstring>
#include <mutex>
#include <thread>
#include <vector>

#ifndef _WIN32
#include <csignal>
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace
{

#ifndef _WIN32
void managedFaultHandler(int signalNumber, siginfo_t* info, void*)
{
    if (info && ContractStateEngine::handleFault(info->si_addr))
    {
        return;
    }

    signal(signalNumber, SIG_DFL);
    raise(signalNumber);
}

void installManagedFaultHandler()
{
    static std::once_flag once;
    std::call_once(once, []()
    {
        struct sigaction action{};
        action.sa_sigaction = managedFaultHandler;
        action.sa_flags = SA_SIGINFO;
        sigemptyset(&action.sa_mask);
        sigaddset(&action.sa_mask, SIGSEGV);
        sigaddset(&action.sa_mask, SIGBUS);
        ASSERT_EQ(sigaction(SIGSEGV, &action, nullptr), 0);
        ASSERT_EQ(sigaction(SIGBUS, &action, nullptr), 0);
    });
}
#else
void installManagedFaultHandler()
{
}
#endif

struct ManagedState
{
    unsigned char* data = nullptr;
    ContractStateEngine* engine = nullptr;
    size_t previousLimit = ContractStateEngine::MAX_RAM_USAGE;

    explicit ManagedState(size_t size, unsigned int contractIndex = 0)
        : contractIndex(contractIndex)
    {
        installManagedFaultHandler();
        EXPECT_TRUE(ContractStateEngine::create(&data, size, contractIndex));
        engine = ContractStateEngine::getEngine(contractIndex);
        EXPECT_NE(engine, nullptr);
    }

    ~ManagedState()
    {
        ContractStateEngine::MAX_RAM_USAGE = previousLimit;
        ContractStateEngine::release(contractIndex);
    }

private:
    unsigned int contractIndex;
};

std::array<unsigned char, 32> canonicalHash(const unsigned char* state, size_t size)
{
    std::array<unsigned char, 32> hash{};
    XKCP::KangarooTwelve(state, size, hash.data(), hash.size());
    return hash;
}

std::array<unsigned char, 32> engineHash(ManagedState& state)
{
    std::array<unsigned char, 32> hash{};
    EXPECT_EQ(state.engine->getHashAndProtect(hash.data(), hash.size()), 0);
    return hash;
}

void trimAllResidentState()
{
    ContractStateEngine::MAX_RAM_USAGE = 0;
    ContractStateEngine::tryEvictChunks();
    ContractStateEngine::tryEvictChunks();
}

} // namespace

TEST(K12EngineTest, IncrementalDigestEqualsCanonicalK12)
{
    constexpr size_t size = 2 * 1024 * 1024 + 37;
    std::vector<unsigned char> state(size);
    K12Engine engine(state.data(), state.size());

    for (size_t i = 0; i < state.size(); i++)
    {
        state[i] = (unsigned char)((i * 1315423911ULL) >> 17);
    }

    std::array<unsigned char, 32> hash{};
    EXPECT_EQ(engine.getHash(hash.data(), hash.size()), 0);
    EXPECT_EQ(hash, canonicalHash(state.data(), state.size()));

    for (unsigned int chunk = 1; chunk < engine.getMaxChunks(); chunk += 17)
    {
        state[(size_t)chunk * K12_chunkSize] ^= 0x5A;
        engine.markChunkChanged(chunk);
    }

    EXPECT_EQ(engine.getHash(hash.data(), hash.size()), 0);
    EXPECT_EQ(hash, canonicalHash(state.data(), state.size()));
}

TEST(ContractStateEngineTest, ZeroSparseRandomPartialEvictRestoreAndModify)
{
    const size_t size = 11 * K12_chunkSize + 37;
    ManagedState state(size);

    EXPECT_EQ(engineHash(state), canonicalHash(state.data, size));

    state.data[3] = 0x7A;
    state.data[5 * K12_chunkSize + 19] = 0xC3;
    state.data[size - 1] = 0x11;
    unsigned int random = 0x12345678;
    for (size_t i = K12_chunkSize; i < 3 * K12_chunkSize; i++)
    {
        random = random * 1664525u + 1013904223u;
        state.data[i] = (unsigned char)(random >> 24);
    }

    const auto beforeEviction = engineHash(state);
    EXPECT_EQ(beforeEviction, canonicalHash(state.data, size));
    const size_t residentBefore = ContractStateEngine::getResidentBytes();

    trimAllResidentState();
    EXPECT_LT(ContractStateEngine::getResidentBytes(), residentBefore);
    EXPECT_GT(ContractStateEngine::getCompressedBytes(), 0u);
    EXPECT_EQ(engineHash(state), beforeEviction);

    state.data[6 * K12_chunkSize + 5] ^= 0xFF;
    const auto modified = engineHash(state);
    EXPECT_NE(modified, beforeEviction);
    EXPECT_EQ(modified, canonicalHash(state.data, size));

    trimAllResidentState();
    EXPECT_EQ(engineHash(state), modified);
}

TEST(ContractStateEngineTest, ZeroBlocksNeedNoCompressedStorage)
{
    ManagedState state(6 * K12_chunkSize);
    EXPECT_EQ(engineHash(state), canonicalHash(state.data, 6 * K12_chunkSize));

    trimAllResidentState();
    EXPECT_EQ(ContractStateEngine::getCompressedBytes(), 0u);
}

TEST(ContractStateEngineTest, ConcurrentColdFaultsAreSerializedWithoutCorruption)
{
    ManagedState state(16 * K12_chunkSize);
    for (size_t block = 0; block < 16; block++)
    {
        state.data[block * K12_chunkSize] = (unsigned char)(block + 1);
    }
    engineHash(state);
    trimAllResidentState();

    std::atomic<bool> valuesOk{true};
    std::vector<std::thread> threads;
    for (size_t block = 0; block < 8; block++)
    {
        threads.emplace_back([&, block]()
        {
            unsigned char* value = state.data + block * K12_chunkSize;
            if (*value != (unsigned char)(block + 1))
            {
                valuesOk.store(false, std::memory_order_relaxed);
            }
            *value = (unsigned char)(*value + 40);
        });
    }
    for (auto& thread : threads)
    {
        thread.join();
    }

    EXPECT_TRUE(valuesOk.load(std::memory_order_relaxed));
    for (size_t block = 0; block < 8; block++)
    {
        EXPECT_EQ(state.data[block * K12_chunkSize], (unsigned char)(block + 41));
    }
    EXPECT_EQ(engineHash(state), canonicalHash(state.data, 16 * K12_chunkSize));
}

TEST(ContractStateEngineTest, EvictionReleasesNativePages)
{
    ManagedState state(4 * K12_chunkSize);
    std::memset(state.data, 0xA5, 4 * K12_chunkSize);
    engineHash(state);
    trimAllResidentState();

#ifdef _WIN32
    MEMORY_BASIC_INFORMATION info{};
    ASSERT_EQ(VirtualQuery(state.data, &info, sizeof(info)), sizeof(info));
    EXPECT_EQ(info.State, (DWORD)MEM_RESERVE);
#elif defined(__APPLE__)
    const size_t pageSize = (size_t)sysconf(_SC_PAGESIZE);
    std::vector<char> residency(state.engine->getBlockSize() / pageSize);
    ASSERT_EQ(mincore(state.data, state.engine->getBlockSize(), residency.data()), 0);
    for (char page : residency)
    {
        EXPECT_EQ(page & 1, 0);
    }
#else
    EXPECT_EQ(ContractStateEngine::getResidentBytes(), 0u);
#endif
}

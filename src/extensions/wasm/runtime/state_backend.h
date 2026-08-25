#pragma once
// Routes state allocation, digesting, and eviction through the active backend.
// The native state pager is shared by Linux, macOS, and Windows.

// LITE_SC_NO_PAGER selects the resident fallback for testing.
#if (defined(__linux__) || defined(__APPLE__) || defined(_WIN32)) \
    && defined(LITE_WASM_SC) && !defined(LITE_SC_NO_PAGER)
#define LITE_SC_PAGER 1
#endif

// Non-pager test builds use the ordinary resident allocator.
#if !defined(LITE_SC_PAGER) && defined(TESTNET) && defined(LITE_WASM_SC)
#define LITE_SC_CONTRACT_LEVEL 1
#endif

namespace Wasm::Runtime
{

inline bool g_wasmOwnedSlot[contractCount] = {};

inline bool statePagerActive(unsigned int contractIndex)
{
#ifdef LITE_SC_PAGER
    return ContractStatePager::getPager(contractIndex) != nullptr && !g_wasmOwnedSlot[contractIndex];
#else
    (void)contractIndex;
    return false;
#endif
}

inline bool allocateContractState(unsigned int contractIndex, unsigned long long size)
{
#if defined(LITE_SC_PAGER)
    return ContractStatePager::create(&contractStates[contractIndex], size, contractIndex);
#elif defined(LITE_SC_CONTRACT_LEVEL)
    contractStates[contractIndex] = (unsigned char*)qVirtualAlloc(size, /*commitMem=*/true);
    return contractStates[contractIndex] != nullptr;
#else
    return allocPoolWithErrorLog(L"contractStates", size, (void**)&contractStates[contractIndex], __LINE__);
#endif
}

inline void hashContractState(unsigned int contractIndex, unsigned char* output, unsigned long long effectiveSize)
{
    if (statePagerActive(contractIndex))
    {
#ifdef LITE_SC_PAGER
        ContractStatePager::getPager(contractIndex)->getHashAndProtect(output, 32);
#endif
    }
    else
    {
        KangarooTwelve(contractStates[contractIndex], (unsigned int)effectiveSize, output, 32);
    }
}

inline void evictContractState()
{
#ifdef LITE_SC_PAGER
    ContractStatePager::tryEvictBlocks();
#endif
}

inline bool handleManagedStateFault(void* address)
{
#ifdef LITE_SC_PAGER
    return ContractStatePager::handleFault(address);
#else
    (void)address;
    return false;
#endif
}

inline void setContractStateMemoryLimit(unsigned long long bytes)
{
#ifdef LITE_SC_PAGER
    ContractStatePager::MAX_RAM_USAGE = (size_t)bytes;
#else
    (void)bytes;
#endif
}

inline void transferContractStateToWasm(unsigned int contractIndex)
{
#if defined(LITE_SC_PAGER)
    ContractStatePager::release(contractIndex);
    g_wasmOwnedSlot[contractIndex] = true;
#elif defined(LITE_SC_CONTRACT_LEVEL)
    g_wasmOwnedSlot[contractIndex] = true;
#else
    freePool(contractStates[contractIndex]);
#endif
}

// Only the plain pool backend returns state through freePool.
inline void freeContractState(unsigned int contractIndex)
{
#if defined(LITE_SC_PAGER)
    ContractStatePager::release(contractIndex);
#elif defined(LITE_SC_CONTRACT_LEVEL)
    (void)contractIndex;
#else
    if (contractStates[contractIndex])
    {
        freePool(contractStates[contractIndex]);
    }
#endif
}

inline unsigned long long contractStateRamUsage()
{
#ifdef LITE_SC_PAGER
    return ContractStatePager::getTotalRamUsage();
#else
    return 0;
#endif
}

} // namespace Wasm::Runtime

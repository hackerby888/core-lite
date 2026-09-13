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

#include <atomic>

namespace Wasm::Runtime
{

// Without the pager each reserved slot commits its whole state up front, so keep that window small.
#if !defined(LITE_SC_PAGER) && defined(LITE_WASM_SC)
static_assert(WASM_RESERVED_SLOT_COUNT <= 8, "more than 8 reserved Wasm slots need the state pager");
#endif

inline bool g_wasmOwnedSlot[contractCount] = {};

// Per-slot write sequence: odd while a dispatch may be writing the slot, even when its bytes are quiescent.
// Node-local, outside the state bytes, so digests and consensus are unchanged by it.
inline std::atomic<unsigned long long> g_stateSeq[contractCount] = {};

// Raised around a dispatch that may write a slot's state; a scope so entry and exit always pair.
// A read-only function cannot write, and a nested frame is covered by its outermost caller, so neither raises it.
struct StateWriteSeqScope
{
    StateWriteSeqScope(bool enabled, unsigned int contractIndex) : index(contractIndex), engaged(enabled)
    {
        if (engaged)
        {
            g_stateSeq[index].fetch_add(1, std::memory_order_release); // odd: a write may be in flight
        }
    }

    ~StateWriteSeqScope()
    {
        if (engaged)
        {
            g_stateSeq[index].fetch_add(1, std::memory_order_release); // even: the bytes are quiescent again
        }
    }

    StateWriteSeqScope(const StateWriteSeqScope&) = delete;
    StateWriteSeqScope& operator=(const StateWriteSeqScope&) = delete;

private:
    const unsigned int index;
    const bool engaged;
};

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

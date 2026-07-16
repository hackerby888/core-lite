#pragma once
// Routes state allocation, digesting, and eviction through the active backend.
// The userfaultfd engine is Linux-only.

// LITE_SC_NO_ENGINE selects the demand-zero fallback on Linux for testing.
#if defined(__linux__) && defined(LITE_WASM_SC) && !defined(LITE_SC_NO_ENGINE)
#define LITE_SC_ENGINE 1
#endif

// Non-engine builds reserve demand-zero state so unused contracts consume little RSS.
#if !defined(LITE_SC_ENGINE) && defined(TESTNET) && defined(LITE_WASM_SC)
#define LITE_SC_CONTRACT_LEVEL 1
#endif

namespace Wasm::Runtime
{

inline bool g_wasmOwnedSlot[contractCount] = {};

inline bool stateEngineActive(unsigned int contractIndex)
{
#ifdef LITE_SC_ENGINE
    return ContractStateEngine::getEngine(contractIndex) != nullptr
        && !g_wasmOwnedSlot[contractIndex];
#else
    (void)contractIndex;
    return false;
#endif
}

inline bool allocateContractState(unsigned int contractIndex, unsigned long long size)
{
#if defined(LITE_SC_ENGINE)
    return ContractStateEngine::create(
        &contractStates[contractIndex],
        size,
        contractIndex);
#elif defined(LITE_SC_CONTRACT_LEVEL)
    // Demand-zero reservation keeps commit charge proportional to written state.
#ifdef _MSC_VER
    contractStates[contractIndex] = (unsigned char*)qVirtualAllocLazy(size);
#else
    contractStates[contractIndex] = (unsigned char*)qVirtualAlloc(
        size,
        /*commitMem=*/true);
#endif
    return contractStates[contractIndex] != nullptr;
#else
    return allocPoolWithErrorLog(
        L"contractStates",
        size,
        (void**)&contractStates[contractIndex],
        __LINE__);
#endif
}

inline void hashContractState(
    unsigned int contractIndex,
    unsigned char* output,
    unsigned long long effectiveSize)
{
    if (stateEngineActive(contractIndex))
    {
#ifdef LITE_SC_ENGINE
        ContractStateEngine::getEngine(contractIndex)->getHashAndReprotect(
            output,
            32);
#endif
    }
    else
    {
#ifdef _MSC_VER
        // Hash reserved Windows pages as zero without committing them.
        KangarooTwelvePaged(
            contractStates[contractIndex],
            (unsigned int)effectiveSize,
            output,
            32);
#else
        KangarooTwelve(
            contractStates[contractIndex],
            (unsigned int)effectiveSize,
            output,
            32);
#endif
    }
}

inline void evictContractState()
{
#ifdef LITE_SC_ENGINE
    ContractStateEngine::tryEvictChunks();
    ContractStateEngine::tryEvictResidentBatch(50000);
#endif
}

inline void flushContractState(unsigned int contractIndex)
{
#ifdef LITE_SC_ENGINE
    if (auto* engine = ContractStateEngine::getEngine(contractIndex))
    {
        engine->flushAllChunksToDisk();
    }
#else
    (void)contractIndex;
#endif
}

inline void touchContractState(unsigned int contractIndex)
{
#ifdef LITE_SC_ENGINE
    if (auto* engine = ContractStateEngine::getEngine(contractIndex))
    {
        engine->touchAllPages();
    }
#else
    (void)contractIndex;
#endif
}

inline void reprotectContractState(unsigned int contractIndex)
{
#ifdef LITE_SC_ENGINE
    if (auto* engine = ContractStateEngine::getEngine(contractIndex))
    {
        engine->reprotectWriteRegion();
        engine->reprotectReadRegion();
    }
#else
    (void)contractIndex;
#endif
}

inline void transferContractStateToWasm(unsigned int contractIndex)
{
#if defined(LITE_SC_ENGINE)
    // The engine retains ownership of its memfd after Wasm takes over the slot.
    flushContractState(contractIndex);
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
#if defined(LITE_SC_ENGINE) || defined(LITE_SC_CONTRACT_LEVEL)
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
#ifdef LITE_SC_ENGINE
    return ContractStateEngine::getRamUsageByAllEngines();
#else
    return 0;
#endif
}

} // namespace Wasm::Runtime

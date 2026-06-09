#pragma once
// Adapter over the contract-state RAM engine (k12_engine.h). Routes contract-state
// alloc/digest/evict through the engine when active, else the plain committed-pool path.
// Engine is Linux-only today (userfaultfd); cross-platform backends land later. Included from
// qubic.cpp after contract_exec.h + overload.h (+ k12_engine.h on Linux), before lite_wasm_contracts.h.

// The contract-state engine is testnet dynamic-contract only (Linux). Mainnet and normal testnet keep
// the plain committed-pool path (original behavior) — engine off, no memfd/uffd, no trims.
#if defined(__linux__) && defined(LITE_DYNAMIC_CONTRACTS)
#define LITE_SC_ENGINE 1
#endif

// Slot taken over by a wasm contract: its state lives in WAMR linear memory, engine abandoned for it.
inline bool g_wasmOwnedSlot[contractCount] = {};

// True when the engine owns contract idx's state (Linux, not wasm-owned). False -> plain path.
inline bool liteSCEngineActive(unsigned int idx)
{
#ifdef LITE_SC_ENGINE
    return ContractStateEngine::getEngine(idx) != nullptr && !g_wasmOwnedSlot[idx];
#else
    (void)idx;
    return false;
#endif
}

// Allocate contract idx's state. Engine-backed on Linux; plain committed pool otherwise.
inline bool liteSCAlloc(unsigned int idx, unsigned long long size)
{
#ifdef LITE_SC_ENGINE
    return ContractStateEngine::create(&contractStates[idx], size, idx);
#else
    return allocPoolWithErrorLog(L"contractStates", size, (void**)&contractStates[idx], __LINE__);
#endif
}

// Digest of contract idx's state into out (32 bytes). effectiveSize = bytes to hash (wasm hashes a prefix).
inline void liteSCDigest(unsigned int idx, unsigned char* out, unsigned long long effectiveSize)
{
    if (liteSCEngineActive(idx))
    {
#ifdef LITE_SC_ENGINE
        ContractStateEngine::getEngine(idx)->getHashAndReprotect(out, 32);
#endif
    }
    else
    {
        KangarooTwelve(contractStates[idx], (unsigned int)effectiveSize, out, 32);
    }
}

// Per-tick: LRU-evict cold chunks down to the RAM cap. No-op when the engine is off.
inline void liteSCEvictTick()
{
#ifdef LITE_SC_ENGINE
    ContractStateEngine::tryEvictChunks();              // LRU evict of faulted chunks
    ContractStateEngine::tryEvictResidentBatch(50000);  // ~400 MB/tick of boot-resident chunks down to cap
#endif
}

// Flush contract idx's resident chunks (before save / on wasm takeover). No-op when off.
inline void liteSCFlush(unsigned int idx)
{
#ifdef LITE_SC_ENGINE
    if (auto* e = ContractStateEngine::getEngine(idx)) e->flushAllChunksToDisk();
#else
    (void)idx;
#endif
}

// Make idx's whole state resident + uncompressed (before save / digest fallback needs the bytes).
inline void liteSCTouchAll(unsigned int idx)
{
#ifdef LITE_SC_ENGINE
    if (auto* e = ContractStateEngine::getEngine(idx)) e->touchAllPages();
#else
    (void)idx;
#endif
}

// Re-arm read/write protection for idx after a bulk operation (digest/setup).
inline void liteSCReprotect(unsigned int idx)
{
#ifdef LITE_SC_ENGINE
    if (auto* e = ContractStateEngine::getEngine(idx)) { e->reprotectWriteRegion(); e->reprotectReadRegion(); }
#else
    (void)idx;
#endif
}

// A wasm contract is taking over the slot. Engine path: flush + abandon the region (engine owns the
// memfd, never free it), mark wasm-owned. Plain path: free the committed pool buffer as before.
inline void liteSCOnWasmTakeover(unsigned int idx)
{
#ifdef LITE_SC_ENGINE
    liteSCFlush(idx);
    g_wasmOwnedSlot[idx] = true;
#else
    freePool(contractStates[idx]);
#endif
}

// Resident contract-state RAM across all engines (for logging / cap checks).
inline unsigned long long liteSCRamUsage()
{
#ifdef LITE_SC_ENGINE
    return ContractStateEngine::getRamUsageByAllEngines();
#else
    return 0;
#endif
}

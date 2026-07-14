#pragma once

#include <cstdint>

// Reset a compiler-owned locals arena for an outer call, but preserve it across same-slot reentrancy. The
// destructor restores the nested bump even when wasm traps, because wasm_runtime_call_wasm returns through
// the normal C++ stack on failure. A null top keeps modules predating the arena_top export on the host-owned
// scratch path while still balancing the per-slot depth.
struct LiteWasmArenaScope {
    uint32_t& depth;
    uint32_t* top;
    uint32_t  savedTop = 0;
    bool      nested;

    LiteWasmArenaScope(uint32_t& slotDepth, uint32_t* arenaTop, uint32_t arenaBase)
        : depth(slotDepth), top(arenaTop), nested(depth++ != 0) {
        if (!top) return;
        savedTop = *top;
        if (!nested) *top = arenaBase;
    }
    ~LiteWasmArenaScope() {
        if (top && nested) *top = savedTop;
        --depth;
    }
    LiteWasmArenaScope(const LiteWasmArenaScope&) = delete;
    LiteWasmArenaScope& operator=(const LiteWasmArenaScope&) = delete;
};

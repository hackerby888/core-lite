#pragma once

#include <cstdint>
#include <cstring>

namespace Wasm::Runtime
{

static constexpr uint32_t WASM_INPUT_CAPACITY = 64u * 1024u;
static constexpr uint32_t WASM_OUTPUT_CAPACITY = 64u * 1024u;
static constexpr uint32_t WASM_LOCALS_CAPACITY = 32u * 1024u;
static constexpr uint32_t WASM_DISPATCH_FRAME_CAPACITY =
    WASM_INPUT_CAPACITY + WASM_OUTPUT_CAPACITY + WASM_LOCALS_CAPACITY;

struct MemoryLayout
{
    uint32_t inputOffset;
    uint32_t outputOffset;
    uint32_t localsOffset;
    uint32_t arenaOffset;
};

static inline MemoryLayout fixedMemoryLayout(uint32_t ioBaseOffset)
{
    MemoryLayout layout;

    layout.inputOffset = ioBaseOffset;
    layout.outputOffset = layout.inputOffset + WASM_INPUT_CAPACITY;
    layout.localsOffset = layout.outputOffset + WASM_OUTPUT_CAPACITY;
    layout.arenaOffset = layout.localsOffset + WASM_LOCALS_CAPACITY;
    return layout;
}

static inline bool nestedMemoryLayout(
    const MemoryLayout& fixedLayout,
    uint32_t arenaEnd,
    uint32_t arenaTop,
    uint32_t hostArenaBump,
    MemoryLayout& layout)
{
    unsigned long long frameBase = fixedLayout.arenaOffset;
    if (arenaTop > frameBase)
    {
        frameBase = arenaTop;
    }
    if (hostArenaBump > frameBase)
    {
        frameBase = hostArenaBump;
    }

    frameBase = (frameBase + 7ull) & ~7ull;
    const unsigned long long frameEnd = frameBase + WASM_DISPATCH_FRAME_CAPACITY;
    if (frameEnd > arenaEnd)
    {
        return false;
    }

    layout.inputOffset = (uint32_t)frameBase;
    layout.outputOffset = layout.inputOffset + WASM_INPUT_CAPACITY;
    layout.localsOffset = layout.outputOffset + WASM_OUTPUT_CAPACITY;
    layout.arenaOffset = layout.localsOffset + WASM_LOCALS_CAPACITY;
    return true;
}

static inline void zeroEntryLocals(void* locals)
{
    std::memset(locals, 0, WASM_LOCALS_CAPACITY);
}

// Reset compiler temporaries for independent calls. Nested calls advance past their private frame and
// restore the parent arena when they return.
struct ArenaScope
{
    uint32_t& depth;
    uint32_t* top;
    uint32_t  savedTop = 0;
    bool      nested;

    ArenaScope(
        uint32_t& slotDepth,
        uint32_t* arenaTop,
        uint32_t outerArenaBase,
        uint32_t callArenaBase)
        : depth(slotDepth), top(arenaTop), nested(depth++ != 0)
    {
        if (top)
        {
            savedTop = *top;
            *top = nested ? callArenaBase : outerArenaBase;
        }
    }

    ~ArenaScope()
    {
        if (top && nested)
        {
            *top = savedTop;
        }

        --depth;
    }

    ArenaScope(const ArenaScope&) = delete;
    ArenaScope& operator=(const ArenaScope&) = delete;
};

} // namespace Wasm::Runtime

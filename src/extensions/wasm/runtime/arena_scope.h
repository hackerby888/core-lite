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
    uint32_t arenaLimit,
    uint32_t guestArenaCursor,
    uint32_t hostArenaCursor,
    MemoryLayout& layout)
{
    unsigned long long frameBase = fixedLayout.arenaOffset;
    // Guest code and lhost.acquireScratch advance separate cursors in the same arena.
    if (guestArenaCursor > frameBase)
    {
        frameBase = guestArenaCursor;
    }
    if (hostArenaCursor > frameBase)
    {
        frameBase = hostArenaCursor;
    }

    frameBase = (frameBase + 7ull) & ~7ull;
    const unsigned long long frameEnd = frameBase + WASM_DISPATCH_FRAME_CAPACITY;
    if (frameEnd > arenaLimit)
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

// Reset compiler temporaries for independent calls.
// Nested calls restore their parent arena.
struct GuestArenaCursorScope
{
    uint32_t& depth;
    uint32_t* guestCursor;
    uint32_t  savedGuestCursor = 0;
    bool      nested;

    GuestArenaCursorScope(
        uint32_t& slotDepth,
        uint32_t* guestArenaCursor,
        uint32_t outerArenaStart,
        uint32_t callArenaStart)
        : depth(slotDepth),
          guestCursor(guestArenaCursor),
          nested(depth++ != 0)
    {
        if (guestCursor)
        {
            savedGuestCursor = *guestCursor;
            *guestCursor = nested ? callArenaStart : outerArenaStart;
        }
    }

    ~GuestArenaCursorScope()
    {
        if (guestCursor && nested)
        {
            *guestCursor = savedGuestCursor;
        }

        --depth;
    }

    GuestArenaCursorScope(const GuestArenaCursorScope&) = delete;
    GuestArenaCursorScope& operator=(const GuestArenaCursorScope&) = delete;
};

} // namespace Wasm::Runtime

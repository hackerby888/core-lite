#pragma once

#include <cstdint>

// Reset compiler temporaries for independent calls and preserve them across nested re-entry.
struct LiteWasmArenaScope
{
    uint32_t& depth;
    uint32_t* top;
    uint32_t  savedTop = 0;
    bool      nested;

    LiteWasmArenaScope(uint32_t& slotDepth, uint32_t* arenaTop, uint32_t arenaBase)
        : depth(slotDepth), top(arenaTop), nested(depth++ != 0)
    {
        if (!top)
        {
            return;
        }

        savedTop = *top;
        if (!nested)
        {
            *top = arenaBase;
        }
    }

    ~LiteWasmArenaScope()
    {
        if (top && nested)
        {
            *top = savedTop;
        }

        --depth;
    }

    LiteWasmArenaScope(const LiteWasmArenaScope&) = delete;
    LiteWasmArenaScope& operator=(const LiteWasmArenaScope&) = delete;
};

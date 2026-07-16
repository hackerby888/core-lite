#pragma once

#include <cstdint>

namespace Wasm::Runtime
{

// Reset compiler temporaries for independent calls and preserve them across nested re-entry.
struct ArenaScope
{
    uint32_t& depth;
    uint32_t* top;
    uint32_t  savedTop = 0;
    bool      nested;

    ArenaScope(uint32_t& slotDepth, uint32_t* arenaTop, uint32_t arenaBase)
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

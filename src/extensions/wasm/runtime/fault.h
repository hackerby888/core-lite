#pragma once

// A contract abort from a procedure entry point stops the tick loop for good. Nothing else records that:
// the dispatch never returns, so its trace entry is never committed. This is the one record of the halt,
// written before the abort and served over HTTP so a reader can tell "halted" from "not ticking yet".
#ifdef LITE_WASM_SC

#include <string>
#include "extensions/wasm/runtime/trace.h"

namespace Wasm::Runtime
{

struct FaultRecord
{
    bool set = false;
    std::string message;
    std::string phase;
    unsigned int failedTick = 0;
    unsigned short failedEpoch = 0;
    unsigned int slot = 0;
    unsigned char kind = 0;
    unsigned short entry = 0;
};

static FaultRecord nodeFault;

// The first fault describes the halt; anything after it is a consequence.
static inline void recordFault(unsigned int contractIndex, unsigned char kind, unsigned short inputType, unsigned int errorCode, unsigned int tick,
    unsigned short epoch)
{
    TraceLockScope lock;
    if (nodeFault.set)
    {
        return;
    }

    nodeFault.set = true;
    nodeFault.message = "abort(" + std::to_string(errorCode) + ")";
    nodeFault.phase = "transaction";
    nodeFault.failedTick = tick;
    nodeFault.failedEpoch = epoch;
    nodeFault.slot = contractIndex;
    nodeFault.kind = kind;
    nodeFault.entry = inputType;
}

static inline FaultRecord faultSnapshot()
{
    TraceLockScope lock;
    return nodeFault;
}

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

#pragma once

// A contract abort or trap from a procedure entry point stops the tick loop for good once the frame has
// committed its trace. This record is served over HTTP so a reader can tell "halted" from "not ticking yet".
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
static inline void recordFault(unsigned int contractIndex, unsigned char kind, unsigned short inputType, const std::string& message, unsigned int tick,
    unsigned short epoch)
{
    TraceLockScope lock;
    if (nodeFault.set)
    {
        return;
    }

    nodeFault.set = true;
    nodeFault.message = message;
    nodeFault.phase = "transaction";
    nodeFault.failedTick = tick;
    nodeFault.failedEpoch = epoch;
    nodeFault.slot = contractIndex;
    nodeFault.kind = kind;
    nodeFault.entry = inputType;
}

static inline std::string abortMessage(unsigned int errorCode)
{
    return "abort(" + std::to_string(errorCode) + ")";
}

static inline FaultRecord faultSnapshot()
{
    TraceLockScope lock;
    return nodeFault;
}

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

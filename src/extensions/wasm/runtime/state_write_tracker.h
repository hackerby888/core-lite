#pragma once

// Dirty-page tracking and state-write restoration for traced calls.
#ifdef LITE_WASM_SC

#include "extensions/wasm/runtime/trace.h"
#include "platform/pointer_align.h"
#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#include <unistd.h>
#include <sys/mman.h>
#endif
#include <cstdlib>
#include <mutex>

namespace Wasm::Runtime
{

// Dirty-page tracking is thread-local because state-changing calls are serialized.
static long systemPageSize = 4096;
#ifndef _WIN32
static struct sigaction previousSegvAction;
static struct sigaction previousBusAction;
#endif
static std::once_flag writeFaultHandlerOnce;
static bool writeFaultHandlerInstalled = false;
// One top-level dispatch plus Core's maximum of ten nested contract calls.
static constexpr unsigned int STATE_WRITE_FRAME_CAPACITY = 11u;

struct StateWriteFrame
{
    unsigned char* stateStart = nullptr;
    unsigned char* stateEnd = nullptr;
    unsigned char* protectionLow = nullptr;
    unsigned char* protectionHigh = nullptr;
    unsigned char* pageSnapshots = nullptr;
    unsigned char* dirtyPageBits = nullptr;
    unsigned int pageCapacity = 0;
    unsigned int pageCount = 0;
    bool captureFailed = false;
};

static thread_local StateWriteFrame stateWriteFrames[STATE_WRITE_FRAME_CAPACITY];
static thread_local unsigned int stateWriteFrameCount = 0;

// The fault handler cannot allocate. Each active frame reserves one snapshot slot per protected page
// before the contract starts, then overlapping frames independently capture the same write.
static inline bool captureStateWrite(unsigned char* faultAddress, unsigned char* page)
{
    bool matched = false;

    for (unsigned int frameIndex = 0; frameIndex < stateWriteFrameCount; frameIndex++)
    {
        StateWriteFrame& frame = stateWriteFrames[frameIndex];
        if (faultAddress < frame.protectionLow || faultAddress >= frame.protectionHigh)
        {
            continue;
        }

        matched = true;
        const unsigned int pageIndex =
            (unsigned int)((page - frame.protectionLow) / systemPageSize);
        if (pageIndex >= frame.pageCount || !frame.pageSnapshots || !frame.dirtyPageBits)
        {
            frame.captureFailed = true;
            continue;
        }

        if (!frame.dirtyPageBits[pageIndex])
        {
            memcpy(
                frame.pageSnapshots + (size_t)pageIndex * systemPageSize,
                page,
                systemPageSize);
            frame.dirtyPageBits[pageIndex] = 1;
        }
    }

    return matched;
}

#ifdef _WIN32
static LONG WINAPI handleStateWriteException(EXCEPTION_POINTERS* exceptionPointers)
{
    EXCEPTION_RECORD* record = exceptionPointers ? exceptionPointers->ExceptionRecord : nullptr;
    if (!record
        || record->ExceptionCode != EXCEPTION_ACCESS_VIOLATION
        || record->NumberParameters < 2
        || record->ExceptionInformation[0] != 1)
    {
        return EXCEPTION_CONTINUE_SEARCH;
    }

    unsigned char* faultAddress = (unsigned char*)record->ExceptionInformation[1];
    unsigned char* page = alignPointerDown(faultAddress, systemPageSize);
    if (captureStateWrite(faultAddress, page))
    {
        DWORD oldProtection;
        if (VirtualProtect(
                page,
                (SIZE_T)systemPageSize,
                PAGE_READWRITE,
                &oldProtection))
        {
            return EXCEPTION_CONTINUE_EXECUTION;
        }
    }

    return EXCEPTION_CONTINUE_SEARCH;
}
#else
static void handleStateWriteFault(int signalNumber, siginfo_t* info, void* context)
{
    unsigned char* faultAddress = (unsigned char*)info->si_addr;
    unsigned char* page = alignPointerDown(faultAddress, systemPageSize);
    if (captureStateWrite(faultAddress, page)
        && mprotect(page, systemPageSize, PROT_READ | PROT_WRITE) == 0)
    {
        return;
    }

    const struct sigaction& previous = signalNumber == SIGBUS ? previousBusAction : previousSegvAction;

    if ((previous.sa_flags & SA_SIGINFO) && previous.sa_sigaction)
    {
        previous.sa_sigaction(signalNumber, info, context);
        return;
    }

    if (previous.sa_handler && previous.sa_handler != SIG_DFL && previous.sa_handler != SIG_IGN)
    {
        previous.sa_handler(signalNumber);
        return;
    }

    signal(signalNumber, SIG_DFL);
    raise(signalNumber);
}
#endif

// Capture is on from boot, so the handler cannot wait for setTraceEnabled(): without it the read-only
// state protection faults into the crash path instead of being repaired.
static void installWriteFaultHandler()
{
#ifdef _WIN32
    SYSTEM_INFO systemInfo;

    GetSystemInfo(&systemInfo);
    systemPageSize = systemInfo.dwPageSize ? (long)systemInfo.dwPageSize : 4096;
    writeFaultHandlerInstalled =
        AddVectoredExceptionHandler(1, handleStateWriteException) != nullptr;
#else
    systemPageSize = sysconf(_SC_PAGESIZE);
    if (systemPageSize <= 0)
    {
        systemPageSize = 4096;
    }

    struct sigaction action;

    memset(&action, 0, sizeof(action));
    action.sa_sigaction = handleStateWriteFault;
    action.sa_flags = SA_SIGINFO | SA_RESTART;
    sigemptyset(&action.sa_mask);
    // A write to a read-only mapping raises SIGSEGV on Linux and SIGBUS on Darwin. Both are registered so
    // an owned page is repaired and every other signal still reaches the previous handler.
    if (sigaction(SIGSEGV, &action, &previousSegvAction) != 0)
    {
        return;
    }
    if (sigaction(SIGBUS, &action, &previousBusAction) != 0)
    {
        sigaction(SIGSEGV, &previousSegvAction, nullptr);
        return;
    }
    writeFaultHandlerInstalled = true;
#endif
}

static inline bool ensureWriteFaultHandler()
{
    std::call_once(writeFaultHandlerOnce, installWriteFaultHandler);
    return writeFaultHandlerInstalled;
}

static inline void setTraceEnabled(bool enabled)
{
    if (enabled)
    {
        ensureWriteFaultHandler();
    }

    traceActive.store(enabled, std::memory_order_relaxed);
}

// Grow both buffers so every page in this frame has a slot. Storage is reused by later calls at the
// same nesting depth.
static inline bool reserveDirtyPages(StateWriteFrame& frame, unsigned int pageCount)
{
    if (pageCount <= frame.pageCapacity)
    {
        return true;
    }

    unsigned char* snapshots = (unsigned char*)realloc(
        frame.pageSnapshots,
        (size_t)pageCount * systemPageSize);
    if (!snapshots)
    {
        return false;
    }
    frame.pageSnapshots = snapshots;

    unsigned char* dirtyBits = (unsigned char*)realloc(
        frame.dirtyPageBits,
        pageCount);
    if (!dirtyBits)
    {
        return false;
    }
    frame.dirtyPageBits = dirtyBits;
    frame.pageCapacity = pageCount;
    return true;
}

static inline bool setStatePageProtection(
    unsigned char* protectionLow,
    unsigned char* protectionHigh,
    bool readOnly)
{
    if (!protectionLow || protectionHigh <= protectionLow)
    {
        return false;
    }

#ifdef _WIN32
    DWORD oldProtection;
    return VirtualProtect(
        protectionLow,
        (SIZE_T)(protectionHigh - protectionLow),
        readOnly ? PAGE_READONLY : PAGE_READWRITE,
        &oldProtection) != 0;
#else
    return mprotect(
        protectionLow,
        protectionHigh - protectionLow,
        readOnly ? PROT_READ : PROT_READ | PROT_WRITE) == 0;
#endif
}

static inline bool beginStateWriteTracking(unsigned char* stateStart, unsigned int stateSize)
{
    if (!stateStart || !stateSize)
    {
        return false;
    }

    if (!ensureWriteFaultHandler() || stateWriteFrameCount >= STATE_WRITE_FRAME_CAPACITY)
    {
        return false;
    }

    unsigned char* protectionLow = alignPointerDown(stateStart, systemPageSize);
    unsigned char* protectionHigh = alignPointerUp(stateStart + stateSize, systemPageSize);
    const unsigned int pageCount =
        (unsigned int)((protectionHigh - protectionLow) / systemPageSize);
    StateWriteFrame& frame = stateWriteFrames[stateWriteFrameCount];

    if (!reserveDirtyPages(frame, pageCount))
    {
        return false;
    }

    frame.stateStart = stateStart;
    frame.stateEnd = stateStart + stateSize;
    frame.protectionLow = protectionLow;
    frame.protectionHigh = protectionHigh;
    frame.pageCount = pageCount;
    frame.captureFailed = false;
    memset(frame.dirtyPageBits, 0, pageCount);

    if (!setStatePageProtection(protectionLow, protectionHigh, true))
    {
        return false;
    }

    stateWriteFrameCount++;
    return true;
}

static inline void rearmParentStateWriteFrames()
{
    for (unsigned int frameIndex = 0; frameIndex < stateWriteFrameCount; frameIndex++)
    {
        StateWriteFrame& frame = stateWriteFrames[frameIndex];
        if (!setStatePageProtection(frame.protectionLow, frame.protectionHigh, true))
        {
            frame.captureFailed = true;
        }
    }
}

static inline StateWriteFrame* popStateWriteFrame(
    unsigned char* stateStart,
    unsigned int stateSize)
{
    if (!stateWriteFrameCount)
    {
        return nullptr;
    }

    StateWriteFrame& frame = stateWriteFrames[stateWriteFrameCount - 1];
    if (frame.stateStart != stateStart || frame.stateEnd != stateStart + stateSize)
    {
        return nullptr;
    }

    if (!setStatePageProtection(frame.protectionLow, frame.protectionHigh, false))
    {
        // Continuing would leave read-only pages without an owning tracker frame.
        for (unsigned int frameIndex = 0; frameIndex < stateWriteFrameCount; frameIndex++)
        {
            StateWriteFrame& activeFrame = stateWriteFrames[frameIndex];
            for (unsigned char* page = activeFrame.protectionLow;
                 page < activeFrame.protectionHigh;
                 page += systemPageSize)
            {
                setStatePageProtection(page, page + systemPageSize, false);
            }
        }
        std::abort();
    }
    stateWriteFrameCount--;
    rearmParentStateWriteFrames();
    return &frame;
}

static inline void discardStateWriteTracking(
    unsigned char* stateStart,
    unsigned int stateSize)
{
    popStateWriteFrame(stateStart, stateSize);
}

static inline void finishStateWriteTracking(
    TraceEntry& traceEntry,
    unsigned char* stateStart,
    unsigned int stateSize)
{
    StateWriteFrame* frame = popStateWriteFrame(stateStart, stateSize);
    if (!frame)
    {
        traceEntry.stateTruncated = true;
        return;
    }

    traceEntry.stateTruncated = traceEntry.stateTruncated || frame->captureFailed;

    unsigned char* stateEnd = stateStart + stateSize;
    for (unsigned int pageIndex = 0; pageIndex < frame->pageCount; pageIndex++)
    {
        if (!frame->dirtyPageBits[pageIndex])
        {
            continue;
        }

        unsigned char* page = frame->protectionLow + (size_t)pageIndex * systemPageSize;
        const unsigned char* before =
            frame->pageSnapshots + (size_t)pageIndex * systemPageSize;
        unsigned char* rangeStart = page > stateStart ? page : stateStart;
        unsigned char* rangeEnd = (page + systemPageSize) < stateEnd ? page + systemPageSize : stateEnd;

        // Adjacent windows merge, so a value straddling a window boundary still arrives whole.
        unsigned char* pendingStart = nullptr;
        unsigned char* pendingEnd = nullptr;
        auto flushPending = [&]()
        {
            if (!pendingStart)
            {
                return;
            }

            const unsigned int size = (unsigned int)(pendingEnd - pendingStart);
            traceEntry.stateDiff.push_back({
                (unsigned int)(pendingStart - stateStart),
                hex(before + (unsigned int)(pendingStart - page), size),
                hex(pendingStart, size),
            });
            pendingStart = nullptr;
        };

        for (unsigned char* current = rangeStart; current < rangeEnd;)
        {
            if (before[(unsigned int)(current - page)] == *current)
            {
                current++;
                continue;
            }

            // Report the aligned window holding the change, not the changed bytes alone: writing a small
            // number into a zeroed field dirties too few bytes to decode the value it landed in.
            const size_t stateOffset = (size_t)(current - stateStart);
            unsigned char* windowStart = current - (stateOffset % WASM_TRACE_DIFF_WINDOW);
            unsigned char* windowEnd = windowStart + WASM_TRACE_DIFF_WINDOW;

            if (windowStart < rangeStart)
            {
                windowStart = rangeStart;
            }
            if (windowEnd > rangeEnd)
            {
                windowEnd = rangeEnd;
            }

            if (pendingStart && pendingEnd == windowStart)
            {
                pendingEnd = windowEnd;
            }
            else
            {
                flushPending();
                pendingStart = windowStart;
                pendingEnd = windowEnd;
            }

            current = windowEnd;
        }

        flushPending();
    }
}

struct StateWriteScope
{
    unsigned char* stateStart = nullptr;
    unsigned int stateSize = 0;
    bool engaged = false;
    bool captureFailed = false;
    bool finished = false;

    StateWriteScope(
        bool enabled,
        unsigned char* protectedStateStart,
        unsigned int protectedStateSize)
    {
        if (!enabled)
        {
            return;
        }

        stateStart = protectedStateStart;
        stateSize = protectedStateSize;
        engaged = beginStateWriteTracking(stateStart, stateSize);
        captureFailed = stateStart && stateSize && !engaged;
    }

    ~StateWriteScope()
    {
        if (engaged && !finished)
        {
            discardStateWriteTracking(stateStart, stateSize);
        }
    }

    void finish(TraceEntry& traceEntry)
    {
        if (finished)
        {
            return;
        }

        if (engaged)
        {
            finishStateWriteTracking(traceEntry, stateStart, stateSize);
        }
        else if (captureFailed)
        {
            traceEntry.stateTruncated = true;
        }

        finished = true;
    }

    StateWriteScope(const StateWriteScope&) = delete;
    StateWriteScope& operator=(const StateWriteScope&) = delete;
};

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

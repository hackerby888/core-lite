#pragma once

// Dirty-page tracking and state-write restoration for traced calls.
#ifdef LITE_WASM_SC

#include "extensions/wasm/runtime/trace.h"
#include "platform/memory_util.h"
#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#include <unistd.h>
#include <sys/mman.h>
#endif

namespace Wasm::Runtime
{

// Dirty-page tracking is thread-local because state-changing calls are serialized.
static long systemPageSize = 4096;
#ifndef _WIN32
static struct sigaction previousSegvAction;
#endif
static bool writeFaultHandlerInstalled = false;
static thread_local bool stateWriteTrackingActive = false;
static thread_local unsigned char* t_liteWasmProtLo = nullptr;
static thread_local unsigned char* t_liteWasmProtHi = nullptr;
// Both buffers are sized to the contract's own state in beginStateWriteTracking: the fault handler runs
// under SIGSEGV and must not allocate, so every page it can dirty needs a slot before the call starts.
static thread_local unsigned char* dirtyPageSnapshots = nullptr;
static thread_local unsigned char** dirtyPages = nullptr;
static thread_local unsigned int dirtyPageCapacity = 0;
static thread_local unsigned int dirtyPageCount = 0;
static thread_local bool t_liteWasmDirtyTrunc = false;

#ifdef _WIN32
static LONG WINAPI handleStateWriteException(EXCEPTION_POINTERS* exceptionPointers)
{
    if (exceptionPointers->ExceptionRecord->ExceptionCode != EXCEPTION_ACCESS_VIOLATION)
    {
        return EXCEPTION_CONTINUE_SEARCH;
    }

    unsigned char* faultAddress = (unsigned char*)exceptionPointers->ExceptionRecord->ExceptionInformation[1];
    if (stateWriteTrackingActive && faultAddress >= t_liteWasmProtLo && faultAddress < t_liteWasmProtHi)
    {
        unsigned char* page = alignPointerDown(faultAddress, systemPageSize);

        if (dirtyPageCount < dirtyPageCapacity && dirtyPageSnapshots)
        {
            memcpy(dirtyPageSnapshots + (size_t)dirtyPageCount * systemPageSize, page, systemPageSize);
            dirtyPages[dirtyPageCount++] = page;
        }
        else
        {
            t_liteWasmDirtyTrunc = true;
        }

        DWORD oldProtection;
        VirtualProtect(page, (SIZE_T)systemPageSize, PAGE_READWRITE, &oldProtection);
        return EXCEPTION_CONTINUE_EXECUTION;
    }

    return EXCEPTION_CONTINUE_SEARCH;
}
#else
static void handleStateWriteFault(int signalNumber, siginfo_t* info, void* context)
{
    unsigned char* faultAddress = (unsigned char*)info->si_addr;
    if (stateWriteTrackingActive && faultAddress >= t_liteWasmProtLo && faultAddress < t_liteWasmProtHi)
    {
        unsigned char* page = alignPointerDown(faultAddress, systemPageSize);

        if (dirtyPageCount < dirtyPageCapacity && dirtyPageSnapshots)
        {
            memcpy(dirtyPageSnapshots + (size_t)dirtyPageCount * systemPageSize, page, systemPageSize);
            dirtyPages[dirtyPageCount++] = page;
        }
        else
        {
            t_liteWasmDirtyTrunc = true;
        }

        mprotect(page, systemPageSize, PROT_READ | PROT_WRITE);
        return;
    }

    if ((previousSegvAction.sa_flags & SA_SIGINFO) && previousSegvAction.sa_sigaction)
    {
        previousSegvAction.sa_sigaction(signalNumber, info, context);
        return;
    }

    if (previousSegvAction.sa_handler && previousSegvAction.sa_handler != SIG_DFL && previousSegvAction.sa_handler != SIG_IGN)
    {
        previousSegvAction.sa_handler(signalNumber);
        return;
    }

    signal(signalNumber, SIG_DFL);
    raise(signalNumber);
}
#endif

static inline void setTraceEnabled(bool enabled)
{
    if (enabled && !writeFaultHandlerInstalled)
    {
#ifdef _WIN32
        SYSTEM_INFO systemInfo;

        GetSystemInfo(&systemInfo);
        systemPageSize = systemInfo.dwPageSize ? (long)systemInfo.dwPageSize : 4096;
        AddVectoredExceptionHandler(1, handleStateWriteException);
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
        sigaction(SIGSEGV, &action, &previousSegvAction);
#endif
        writeFaultHandlerInstalled = true;
    }

    traceActive.store(enabled, std::memory_order_relaxed);
}

// Grow both buffers so every page of this contract's state has a slot. Grow-only, so a big contract
// pays once and later calls reuse the allocation.
static inline void reserveDirtyPages(unsigned int pageCount)
{
    if (pageCount <= dirtyPageCapacity)
    {
        return;
    }

    unsigned char* snapshots = (unsigned char*)realloc(
        dirtyPageSnapshots,
        (size_t)pageCount * systemPageSize);
    if (!snapshots)
    {
        return;
    }
    dirtyPageSnapshots = snapshots;

    unsigned char** pages = (unsigned char**)realloc(
        dirtyPages,
        (size_t)pageCount * sizeof(unsigned char*));
    if (!pages)
    {
        return;
    }
    dirtyPages = pages;

    dirtyPageCapacity = pageCount;
}

static inline void beginStateWriteTracking(unsigned char* stateStart, unsigned int stateSize)
{
    if (!stateStart || !stateSize)
    {
        return;
    }

    unsigned char* protectionLow = alignPointerDown(stateStart, systemPageSize);
    unsigned char* protectionHigh = alignPointerUp(stateStart + stateSize, systemPageSize);
    reserveDirtyPages((unsigned int)((protectionHigh - protectionLow) / systemPageSize));

    if (!dirtyPageSnapshots || !dirtyPages)
    {
        return;
    }

    dirtyPageCount = 0;
    t_liteWasmDirtyTrunc = false;
    t_liteWasmProtLo = stateStart;
    t_liteWasmProtHi = alignPointerUp(stateStart + stateSize, systemPageSize);

#ifdef _WIN32
    DWORD oldProtection;
    if (VirtualProtect(t_liteWasmProtLo, (SIZE_T)(t_liteWasmProtHi - t_liteWasmProtLo), PAGE_READONLY, &oldProtection))
    {
        stateWriteTrackingActive = true;
    }
#else
    if (mprotect(t_liteWasmProtLo, t_liteWasmProtHi - t_liteWasmProtLo, PROT_READ) == 0)
    {
        stateWriteTrackingActive = true;
    }
#endif
}

static inline void restoreStatePageProtection()
{
    if (!stateWriteTrackingActive)
    {
        return;
    }

    stateWriteTrackingActive = false;

#ifdef _WIN32
    DWORD oldProtection;
    VirtualProtect(t_liteWasmProtLo, (SIZE_T)(t_liteWasmProtHi - t_liteWasmProtLo), PAGE_READWRITE, &oldProtection);
#else
    mprotect(t_liteWasmProtLo, t_liteWasmProtHi - t_liteWasmProtLo, PROT_READ | PROT_WRITE);
#endif
}

static inline void finishStateWriteTracking(
    TraceEntry& traceEntry,
    unsigned char* stateStart,
    unsigned int stateSize)
{
    if (!stateWriteTrackingActive)
    {
        return;
    }

    restoreStatePageProtection();
    traceEntry.stateTruncated = t_liteWasmDirtyTrunc;

    unsigned char* stateEnd = stateStart + stateSize;
    for (unsigned int pageIndex = 0; pageIndex < dirtyPageCount; pageIndex++)
    {
        unsigned char* page = dirtyPages[pageIndex];
        const unsigned char* before = dirtyPageSnapshots + (size_t)pageIndex * systemPageSize;
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
        engaged = true;
        beginStateWriteTracking(stateStart, stateSize);
    }

    ~StateWriteScope()
    {
        if (engaged && !finished)
        {
            restoreStatePageProtection();
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

        finished = true;
    }

    StateWriteScope(const StateWriteScope&) = delete;
    StateWriteScope& operator=(const StateWriteScope&) = delete;
};

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

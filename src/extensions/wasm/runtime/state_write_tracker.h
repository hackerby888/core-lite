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
static thread_local unsigned char* dirtyPageSnapshots = nullptr;
static thread_local unsigned char* dirtyPages[WASM_MAX_DIRTY_PAGES];
static thread_local unsigned int dirtyPageCount = 0;
static thread_local bool t_liteWasmDirtyTrunc = false;

#ifdef _WIN32
static LONG WINAPI handleStateWriteException(EXCEPTION_POINTERS* exceptionPointers)
{
    if (exceptionPointers->ExceptionRecord->ExceptionCode != EXCEPTION_ACCESS_VIOLATION)
    {
        return EXCEPTION_CONTINUE_SEARCH;
    }

    unsigned char* faultAddress =
        (unsigned char*)exceptionPointers->ExceptionRecord->ExceptionInformation[1];
    if (stateWriteTrackingActive
        && faultAddress >= t_liteWasmProtLo
        && faultAddress < t_liteWasmProtHi)
    {
        unsigned char* page = alignPointerDown(faultAddress, systemPageSize);

        if (dirtyPageCount < WASM_MAX_DIRTY_PAGES && dirtyPageSnapshots)
        {
            memcpy(
                dirtyPageSnapshots + (size_t)dirtyPageCount * systemPageSize,
                page,
                systemPageSize);
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
    if (stateWriteTrackingActive
        && faultAddress >= t_liteWasmProtLo
        && faultAddress < t_liteWasmProtHi)
    {
        unsigned char* page = alignPointerDown(faultAddress, systemPageSize);

        if (dirtyPageCount < WASM_MAX_DIRTY_PAGES && dirtyPageSnapshots)
        {
            memcpy(
                dirtyPageSnapshots + (size_t)dirtyPageCount * systemPageSize,
                page,
                systemPageSize);
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

    if (previousSegvAction.sa_handler
        && previousSegvAction.sa_handler != SIG_DFL
        && previousSegvAction.sa_handler != SIG_IGN)
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

static inline void beginStateWriteTracking(unsigned char* stateStart, unsigned int stateSize)
{
    if (!dirtyPageSnapshots)
    {
        dirtyPageSnapshots = (unsigned char*)malloc(
            (size_t)WASM_MAX_DIRTY_PAGES * systemPageSize);
    }

    if (!dirtyPageSnapshots || !stateStart || !stateSize)
    {
        return;
    }

    dirtyPageCount = 0;
    t_liteWasmDirtyTrunc = false;
    t_liteWasmProtLo = stateStart;
    t_liteWasmProtHi = alignPointerUp(stateStart + stateSize, systemPageSize);

#ifdef _WIN32
    DWORD oldProtection;
    if (VirtualProtect(
        t_liteWasmProtLo,
        (SIZE_T)(t_liteWasmProtHi - t_liteWasmProtLo),
        PAGE_READONLY,
        &oldProtection))
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
    VirtualProtect(
        t_liteWasmProtLo,
        (SIZE_T)(t_liteWasmProtHi - t_liteWasmProtLo),
        PAGE_READWRITE,
        &oldProtection);
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
    for (unsigned int pageIndex = 0;
         pageIndex < dirtyPageCount && traceEntry.stateDiff.size() < 256;
         pageIndex++)
    {
        unsigned char* page = dirtyPages[pageIndex];
        const unsigned char* before =
            dirtyPageSnapshots + (size_t)pageIndex * systemPageSize;
        unsigned char* rangeStart = page > stateStart
            ? page
            : stateStart;
        unsigned char* rangeEnd = (page + systemPageSize) < stateEnd
            ? page + systemPageSize
            : stateEnd;

        for (unsigned char* current = rangeStart; current < rangeEnd;)
        {
            const unsigned int beforeIndex = (unsigned int)(current - page);
            if (before[beforeIndex] == *current)
            {
                current++;
                continue;
            }

            unsigned char* changedEnd = current;
            while (changedEnd < rangeEnd
                && before[(unsigned int)(changedEnd - page)] != *changedEnd)
            {
                changedEnd++;
            }

            const unsigned int changedSize = (unsigned int)(changedEnd - current);
            traceEntry.stateDiff.push_back({
                (unsigned int)(current - stateStart),
                hex(before + beforeIndex, changedSize),
                hex(current, changedSize),
            });
            current = changedEnd;
        }
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

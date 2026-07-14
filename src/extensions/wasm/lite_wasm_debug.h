#pragma once
// Optional per-call Wasm tracing for testnet development. State writes are captured by protecting the state
// pages and saving each page on its first fault. The signal handler stays inert outside a tracked write call.
#ifdef LITE_WASM_SC
#include <atomic>
#include <string>
#include <vector>
#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#ifdef _WIN32
#include <windows.h>
#else
#include <signal.h>
#include <unistd.h>
#include <sys/mman.h>
#endif

#ifndef LITE_WASM_TRACE_RING
#define LITE_WASM_TRACE_RING 256u
#endif
#ifndef LITE_WASM_TRACE_HEAD
#define LITE_WASM_TRACE_HEAD 256u
#endif
#ifndef LITE_WASM_DIRTY_MAX
#define LITE_WASM_DIRTY_MAX 4096u
#endif

struct LiteWasmHostCall
{
    const char* name;
    std::string detail;
};

struct LiteWasmStateRegion
{
    unsigned int offset;
    std::string before;
    std::string after;
};

struct LiteWasmLogRec
{
    unsigned char type = 0;
    unsigned int size = 0;
    std::string hex;
};

struct LiteWasmTraceEntry
{
    unsigned long long sequence = 0;
    unsigned int tick = 0;
    unsigned int contractIndex = 0;
    unsigned short inputType = 0;
    unsigned char kind = 0;
    bool ok = true;
    bool used = false;
    m256i invocator = m256i::zero();
    long long invocationReward = 0;
    unsigned int inputSize = 0;
    unsigned int outputSize = 0;
    unsigned int stateSize = 0;
    bool stateTruncated = false;
    unsigned char inputHead[LITE_WASM_TRACE_HEAD] = {};
    unsigned char outputHead[LITE_WASM_TRACE_HEAD] = {};
    unsigned long long executionNanoseconds = 0;
    std::string trap;
    std::vector<LiteWasmStateRegion> stateDiff;
    std::vector<LiteWasmHostCall> hostCalls;
    std::vector<LiteWasmLogRec> logs;
};

static std::atomic<bool> g_liteWasmDebug{ false };

static inline bool liteWasmDebugEnabled()
{
    return g_liteWasmDebug.load(std::memory_order_relaxed);
}

static LiteWasmTraceEntry g_liteWasmTrace[LITE_WASM_TRACE_RING];
static volatile long g_liteWasmTraceLock = 0;
static unsigned int g_liteWasmTraceHead = 0;
static unsigned long long g_liteWasmTraceSeq = 0;

#ifdef _MSC_VER
static inline void liteWasmTraceAcquire()
{
    while (_InterlockedExchange(&g_liteWasmTraceLock, 1))
    {
    }
}

static inline void liteWasmTraceRelease()
{
    _InterlockedExchange(&g_liteWasmTraceLock, 0);
}
#else
static inline void liteWasmTraceAcquire()
{
    while (__sync_lock_test_and_set(&g_liteWasmTraceLock, 1))
    {
    }
}

static inline void liteWasmTraceRelease()
{
    __sync_lock_release(&g_liteWasmTraceLock);
}
#endif

struct LiteWasmTraceLockScope
{
    LiteWasmTraceLockScope()
    {
        liteWasmTraceAcquire();
    }

    ~LiteWasmTraceLockScope()
    {
        liteWasmTraceRelease();
    }

    LiteWasmTraceLockScope(const LiteWasmTraceLockScope&) = delete;
    LiteWasmTraceLockScope& operator=(const LiteWasmTraceLockScope&) = delete;
};

static inline void liteWasmTraceHostCall(
    LiteWasmTraceEntry* entry,
    const char* name,
    const std::string& detail)
{
    if (entry && entry->hostCalls.size() < 64)
    {
        entry->hostCalls.push_back({
            name,
            detail,
        });
    }
}

static inline void liteWasmTraceCommit(LiteWasmTraceEntry& entry)
{
    LiteWasmTraceLockScope lock;

    entry.sequence = ++g_liteWasmTraceSeq;
    entry.used = true;
    g_liteWasmTrace[g_liteWasmTraceHead % LITE_WASM_TRACE_RING] = entry;
    g_liteWasmTraceHead++;
}

static inline std::vector<LiteWasmTraceEntry> liteWasmTraceSnapshot(
    unsigned long long since,
    unsigned int limit)
{
    std::vector<LiteWasmTraceEntry> entries;

    {
        LiteWasmTraceLockScope lock;

        for (unsigned int index = 0; index < LITE_WASM_TRACE_RING; index++)
        {
            if (g_liteWasmTrace[index].used
                && g_liteWasmTrace[index].sequence > since)
            {
                entries.push_back(g_liteWasmTrace[index]);
            }
        }
    }

    std::sort(
        entries.begin(),
        entries.end(),
        [](const LiteWasmTraceEntry& left, const LiteWasmTraceEntry& right)
        {
            return left.sequence < right.sequence;
        });

    if (entries.size() > limit)
    {
        entries.erase(entries.begin(), entries.end() - limit);
    }

    return entries;
}

static inline void liteWasmTraceClear()
{
    LiteWasmTraceLockScope lock;

    for (unsigned int index = 0; index < LITE_WASM_TRACE_RING; index++)
    {
        g_liteWasmTrace[index].used = false;
    }

    g_liteWasmTraceHead = 0;
}

static inline std::string liteWasmHex(const void* bytes, unsigned int size)
{
    if (!bytes)
    {
        return "null";
    }

    static const char* digits = "0123456789abcdef";
    const unsigned char* input = (const unsigned char*)bytes;
    std::string result;

    result.reserve(size * 2);
    for (unsigned int index = 0; index < size; index++)
    {
        result += digits[input[index] >> 4];
        result += digits[input[index] & 15];
    }

    return result;
}

static inline void liteWasmTraceLog(
    LiteWasmTraceEntry* entry,
    unsigned char type,
    const void* bytes,
    unsigned int size)
{
    if (!entry)
    {
        return;
    }

    const unsigned int capturedSize = size > LITE_WASM_TRACE_HEAD
        ? LITE_WASM_TRACE_HEAD
        : size;
    entry->logs.push_back(LiteWasmLogRec{
        type,
        size,
        liteWasmHex(bytes, capturedSize),
    });
}

// Dirty-page tracking is thread-local because state-changing calls are serialized.
static long g_liteWasmPageSize = 4096;
#ifndef _WIN32
static struct sigaction g_liteWasmOldSegv;
#endif
static bool g_liteWasmSegvInstalled = false;
static thread_local bool t_liteWasmDirtyActive = false;
static thread_local unsigned char* t_liteWasmProtLo = nullptr;
static thread_local unsigned char* t_liteWasmProtHi = nullptr;
static thread_local unsigned char* t_liteWasmDirtyBefore = nullptr;
static thread_local unsigned char* t_liteWasmDirtyPages[LITE_WASM_DIRTY_MAX];
static thread_local unsigned int t_liteWasmDirtyCount = 0;
static thread_local bool t_liteWasmDirtyTrunc = false;

#ifdef _WIN32
static LONG WINAPI liteWasmVeh(EXCEPTION_POINTERS* exceptionPointers)
{
    if (exceptionPointers->ExceptionRecord->ExceptionCode != EXCEPTION_ACCESS_VIOLATION)
    {
        return EXCEPTION_CONTINUE_SEARCH;
    }

    unsigned char* faultAddress =
        (unsigned char*)exceptionPointers->ExceptionRecord->ExceptionInformation[1];
    if (t_liteWasmDirtyActive
        && faultAddress >= t_liteWasmProtLo
        && faultAddress < t_liteWasmProtHi)
    {
        unsigned char* page = (unsigned char*)((uintptr_t)faultAddress
            & ~(uintptr_t)(g_liteWasmPageSize - 1));

        if (t_liteWasmDirtyCount < LITE_WASM_DIRTY_MAX && t_liteWasmDirtyBefore)
        {
            memcpy(
                t_liteWasmDirtyBefore + (size_t)t_liteWasmDirtyCount * g_liteWasmPageSize,
                page,
                g_liteWasmPageSize);
            t_liteWasmDirtyPages[t_liteWasmDirtyCount++] = page;
        }
        else
        {
            t_liteWasmDirtyTrunc = true;
        }

        DWORD oldProtection;
        VirtualProtect(page, (SIZE_T)g_liteWasmPageSize, PAGE_READWRITE, &oldProtection);
        return EXCEPTION_CONTINUE_EXECUTION;
    }

    return EXCEPTION_CONTINUE_SEARCH;
}
#else
static void liteWasmSegv(int signalNumber, siginfo_t* info, void* context)
{
    unsigned char* faultAddress = (unsigned char*)info->si_addr;
    if (t_liteWasmDirtyActive
        && faultAddress >= t_liteWasmProtLo
        && faultAddress < t_liteWasmProtHi)
    {
        unsigned char* page = (unsigned char*)((uintptr_t)faultAddress
            & ~(uintptr_t)(g_liteWasmPageSize - 1));

        if (t_liteWasmDirtyCount < LITE_WASM_DIRTY_MAX && t_liteWasmDirtyBefore)
        {
            memcpy(
                t_liteWasmDirtyBefore + (size_t)t_liteWasmDirtyCount * g_liteWasmPageSize,
                page,
                g_liteWasmPageSize);
            t_liteWasmDirtyPages[t_liteWasmDirtyCount++] = page;
        }
        else
        {
            t_liteWasmDirtyTrunc = true;
        }

        mprotect(page, g_liteWasmPageSize, PROT_READ | PROT_WRITE);
        return;
    }

    if ((g_liteWasmOldSegv.sa_flags & SA_SIGINFO) && g_liteWasmOldSegv.sa_sigaction)
    {
        g_liteWasmOldSegv.sa_sigaction(signalNumber, info, context);
        return;
    }

    if (g_liteWasmOldSegv.sa_handler
        && g_liteWasmOldSegv.sa_handler != SIG_DFL
        && g_liteWasmOldSegv.sa_handler != SIG_IGN)
    {
        g_liteWasmOldSegv.sa_handler(signalNumber);
        return;
    }

    signal(signalNumber, SIG_DFL);
    raise(signalNumber);
}
#endif

static inline void liteWasmDebugSetEnabled(bool enabled)
{
    if (enabled && !g_liteWasmSegvInstalled)
    {
#ifdef _WIN32
        SYSTEM_INFO systemInfo;

        GetSystemInfo(&systemInfo);
        g_liteWasmPageSize = systemInfo.dwPageSize ? (long)systemInfo.dwPageSize : 4096;
        AddVectoredExceptionHandler(1, liteWasmVeh);
#else
        g_liteWasmPageSize = sysconf(_SC_PAGESIZE);
        if (g_liteWasmPageSize <= 0)
        {
            g_liteWasmPageSize = 4096;
        }

        struct sigaction action;

        memset(&action, 0, sizeof(action));
        action.sa_sigaction = liteWasmSegv;
        action.sa_flags = SA_SIGINFO | SA_RESTART;
        sigemptyset(&action.sa_mask);
        sigaction(SIGSEGV, &action, &g_liteWasmOldSegv);
#endif
        g_liteWasmSegvInstalled = true;
    }

    g_liteWasmDebug.store(enabled, std::memory_order_relaxed);
}

static inline void liteWasmDirtyBegin(unsigned char* stateStart, unsigned int stateSize)
{
    if (!t_liteWasmDirtyBefore)
    {
        t_liteWasmDirtyBefore = (unsigned char*)malloc(
            (size_t)LITE_WASM_DIRTY_MAX * g_liteWasmPageSize);
    }

    if (!t_liteWasmDirtyBefore || !stateStart || !stateSize)
    {
        return;
    }

    t_liteWasmDirtyCount = 0;
    t_liteWasmDirtyTrunc = false;
    t_liteWasmProtLo = stateStart;
    t_liteWasmProtHi = (unsigned char*)(((uintptr_t)(stateStart + stateSize)
        + g_liteWasmPageSize - 1) & ~(uintptr_t)(g_liteWasmPageSize - 1));

#ifdef _WIN32
    DWORD oldProtection;
    if (VirtualProtect(
        t_liteWasmProtLo,
        (SIZE_T)(t_liteWasmProtHi - t_liteWasmProtLo),
        PAGE_READONLY,
        &oldProtection))
    {
        t_liteWasmDirtyActive = true;
    }
#else
    if (mprotect(t_liteWasmProtLo, t_liteWasmProtHi - t_liteWasmProtLo, PROT_READ) == 0)
    {
        t_liteWasmDirtyActive = true;
    }
#endif
}

static inline void liteWasmRestorePageProtection()
{
    if (!t_liteWasmDirtyActive)
    {
        return;
    }

    t_liteWasmDirtyActive = false;

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

static inline void liteWasmDirtyEnd(
    LiteWasmTraceEntry& traceEntry,
    unsigned char* stateStart,
    unsigned int stateSize)
{
    if (!t_liteWasmDirtyActive)
    {
        return;
    }

    liteWasmRestorePageProtection();
    traceEntry.stateTruncated = t_liteWasmDirtyTrunc;

    unsigned char* stateEnd = stateStart + stateSize;
    for (unsigned int pageIndex = 0;
         pageIndex < t_liteWasmDirtyCount && traceEntry.stateDiff.size() < 256;
         pageIndex++)
    {
        unsigned char* page = t_liteWasmDirtyPages[pageIndex];
        const unsigned char* before =
            t_liteWasmDirtyBefore + (size_t)pageIndex * g_liteWasmPageSize;
        unsigned char* rangeStart = page > stateStart
            ? page
            : stateStart;
        unsigned char* rangeEnd = (page + g_liteWasmPageSize) < stateEnd
            ? page + g_liteWasmPageSize
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
                liteWasmHex(before + beforeIndex, changedSize),
                liteWasmHex(current, changedSize),
            });
            current = changedEnd;
        }
    }
}

struct LiteWasmPageProtectionScope
{
    unsigned char* stateStart = nullptr;
    unsigned int stateSize = 0;
    bool engaged = false;
    bool finished = false;

    LiteWasmPageProtectionScope(
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
        liteWasmDirtyBegin(stateStart, stateSize);
    }

    ~LiteWasmPageProtectionScope()
    {
        if (engaged && !finished)
        {
            liteWasmRestorePageProtection();
        }
    }

    void finish(LiteWasmTraceEntry& traceEntry)
    {
        if (finished)
        {
            return;
        }

        if (engaged)
        {
            liteWasmDirtyEnd(traceEntry, stateStart, stateSize);
        }

        finished = true;
    }

    LiteWasmPageProtectionScope(const LiteWasmPageProtectionScope&) = delete;
    LiteWasmPageProtectionScope& operator=(const LiteWasmPageProtectionScope&) = delete;
};
#endif // LITE_WASM_SC

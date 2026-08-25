#pragma once

// Fork-rollback degrade-to-strict observability: process-shared counters on Linux
// (GET /v1/fork-stats) + durable unforkable-tick log (GET /v1/unforkable-ticks).

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <ctime>
#include <mutex>
#include <new>
#include <string>
#if defined(__linux__)
#include <sys/mman.h>
#endif

namespace ForkStats
{
enum Reason
{
    CENSUS = 0,
    PARK_TIMEOUT,
    PIPE_FAIL,
    FORK_FAIL,
    QUIESCE_TIMEOUT,
    REASON_COUNT,
};

inline const char* reasonName(int reason)
{
    switch (reason)
    {
    case CENSUS:       return "census";
    case PARK_TIMEOUT: return "park_timeout";
    case PIPE_FAIL:    return "pipe_fail";
    case FORK_FAIL:    return "fork_fail";
    case QUIESCE_TIMEOUT: return "quiesce_timeout";
    default:           return "unknown";
    }
}

struct State
{
    std::atomic<unsigned long long> forksRequested{ 0 };
    std::atomic<unsigned long long> forksOk{ 0 };
    std::atomic<unsigned long long> forksSkippedTotal{ 0 };
    std::atomic<unsigned long long> skipByReason[REASON_COUNT]{};
    std::atomic<unsigned long long> matches{ 0 };
    std::atomic<unsigned long long> mismatches{ 0 };
    std::atomic<unsigned int> lastSkipTick{ 0 };
    std::atomic<int> lastSkipReason{ -1 };
    std::atomic<const char*> lastOffender{ nullptr };   // stable __FILE__/literal name from the census
};

inline State& state()
{
#if defined(__linux__)
    static_assert(std::atomic<unsigned long long>::is_always_lock_free);
    static_assert(std::atomic<unsigned int>::is_always_lock_free);
    static_assert(std::atomic<int>::is_always_lock_free);
    static_assert(std::atomic<const char*>::is_always_lock_free);

    static State* state = [] {
        void* mapping = mmap(nullptr, sizeof(State), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        if (mapping == MAP_FAILED)
        {
            perror("[FORK] shared stats mmap failed");
            abort();
        }
        return new (mapping) State{};
    }();
    return *state;
#else
    static State state;
    return state;
#endif
}

inline auto& forksRequested = state().forksRequested;
inline auto& forksOk = state().forksOk;
inline auto& forksSkippedTotal = state().forksSkippedTotal;
inline auto& skipByReason = state().skipByReason;
inline auto& matches = state().matches;
inline auto& mismatches = state().mismatches;
inline auto& lastSkipTick = state().lastSkipTick;
inline auto& lastSkipReason = state().lastSkipReason;
inline auto& lastOffender = state().lastOffender;

inline const char* kLogPath = "unforkable_ticks.log";   // node CWD, like logging_health_bad_ticks.log
inline std::mutex gLogMtx;   // SMARTMUTEX-EXEMPT: stats-file append only; never held over node state nor across fork()

inline void onForkRequested() { forksRequested.fetch_add(1, std::memory_order_relaxed); }
inline void onForkOk()        { forksOk.fetch_add(1, std::memory_order_relaxed); }
inline void onVerdict(bool mismatch) { (mismatch ? mismatches : matches).fetch_add(1, std::memory_order_relaxed); }

// One unforkable tick: bump counters + append the complete record. Per-event fopen/fclose so no
// inherited buffered FILE* can double-flush across fork() (this only ever runs on the parent paths).
inline void onForkSkipped(int reason, unsigned int tick, const char* offender)
{
    forksSkippedTotal.fetch_add(1, std::memory_order_relaxed);
    if (reason >= 0 && reason < REASON_COUNT)
        skipByReason[reason].fetch_add(1, std::memory_order_relaxed);
    lastSkipTick.store(tick, std::memory_order_relaxed);
    lastSkipReason.store(reason, std::memory_order_relaxed);
    lastOffender.store(offender ? offender : "", std::memory_order_relaxed);

    std::lock_guard<std::mutex> guard(gLogMtx);
    FILE* logFile = fopen(kLogPath, "a");
    if (!logFile)
        return;
    char timestamp[32] = { 0 };
    time_t now = time(nullptr);
    struct tm utcTime;
#if defined(_WIN32)
    gmtime_s(&utcTime, &now);
#else
    gmtime_r(&now, &utcTime);
#endif
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%SZ", &utcTime);
    fprintf(logFile, "%s tick=%u reason=%s offender=%s\n", timestamp, tick, reasonName(reason), offender ? offender : "");
    fclose(logFile);
}

// JSON summary for GET /v1/fork-stats.
inline std::string summaryJson()
{
    const char* offender = lastOffender.load(std::memory_order_relaxed);
    int lastReason = lastSkipReason.load(std::memory_order_relaxed);
    char json[768];
    snprintf(json, sizeof(json),
        "{\"forksRequested\":%llu,\"forksOk\":%llu,\"forksSkippedTotal\":%llu,"
        "\"skip\":{\"census\":%llu,\"parkTimeout\":%llu,\"pipeFail\":%llu,\"forkFail\":%llu,"
        "\"quiesceTimeout\":%llu},"
        "\"matches\":%llu,\"mismatches\":%llu,"
        "\"lastUnforkable\":{\"tick\":%u,\"reason\":\"%s\",\"offender\":\"%s\"}}",
        forksRequested.load(std::memory_order_relaxed),
        forksOk.load(std::memory_order_relaxed),
        forksSkippedTotal.load(std::memory_order_relaxed),
        skipByReason[CENSUS].load(std::memory_order_relaxed),
        skipByReason[PARK_TIMEOUT].load(std::memory_order_relaxed),
        skipByReason[PIPE_FAIL].load(std::memory_order_relaxed),
        skipByReason[FORK_FAIL].load(std::memory_order_relaxed),
        skipByReason[QUIESCE_TIMEOUT].load(std::memory_order_relaxed),
        matches.load(std::memory_order_relaxed),
        mismatches.load(std::memory_order_relaxed),
        lastSkipTick.load(std::memory_order_relaxed),
        lastReason >= 0 ? reasonName(lastReason) : "",
        offender ? offender : "");
    return std::string(json);
}

// Whole durable log for GET /v1/unforkable-ticks (the complete record).
inline std::string readLogAll()
{
    std::string contents;
    std::lock_guard<std::mutex> guard(gLogMtx);
    FILE* logFile = fopen(kLogPath, "r");
    if (!logFile)
        return contents;
    char buffer[4096];
    size_t bytesRead;
    while ((bytesRead = fread(buffer, 1, sizeof(buffer), logFile)) > 0)
        contents.append(buffer, bytesRead);
    fclose(logFile);
    return contents;
}
}

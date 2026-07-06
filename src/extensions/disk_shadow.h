#pragma once

// Fork rollback disk shadow: parent VM writes divert to /s/ so the child can
// read pristine files; commit on match, discard on mismatch.

#include <atomic>

inline volatile bool gForkWindowActive = false;
inline std::atomic<bool> gReRunStrict{ false };
inline std::atomic<unsigned int> gReRunStrictUntilTick{ 0 };
// Disk shadow failures poison the optimistic state; verdict then forces strict child replay.
inline std::atomic<bool> gShadowPoisoned{ false };
inline volatile bool gVerifyForkRollback = false;

inline volatile bool gForkForceFork = false;
inline volatile bool gForkForceMatch = false;
inline volatile bool gForkForceMismatch = false;
inline volatile bool gForkBench = false;
inline unsigned int gForkForceRollbackEvery = 0;

inline std::atomic<bool> gForkQuiesceRequest{ false };
inline std::atomic<int> gForkParked{ 0 };
inline std::atomic<unsigned> gForkParkGen{ 0 };   // bumped per fork window; see liteForkRequestPark

#ifdef __linux__

#include <map>
#include <set>
#include <string>
#include <vector>
#include <mutex>
#include <new>
#include <thread>
#include <chrono>
#include <filesystem>
#include <utility>
#include <cstdlib>
#include <unistd.h>

static inline void liteForkRequestPark()
{
    if (!gForkQuiesceRequest.load(std::memory_order_acquire))
        return;
    // Count once per fork window; there is no decrement-on-release to race the next reset.
    static thread_local unsigned myGen = (unsigned)-1;
    unsigned g = gForkParkGen.load(std::memory_order_acquire);
    if (myGen != g) { myGen = g; gForkParked.fetch_add(1, std::memory_order_acq_rel); }
    while (gForkQuiesceRequest.load(std::memory_order_acquire))
        std::this_thread::yield();
}

class DiskShadow
{
    std::mutex mtx;   // SMARTMUTEX-EXEMPT: shadow-dir lock, owner-reinit in reinitForChildPromote; provably not held across fork()
    std::map<std::string, std::vector<CHAR16>> shadowDir;     // real dir (utf8) -> shadow dir buffer
    std::set<std::pair<std::string, std::string>> written;   // (real dir utf8, page name utf8)

    // 2-byte safe length; keep clang from rewriting this into libc wcslen.
    static size_t len16(const CHAR16* s)
    {
        const volatile CHAR16* p = s;
        size_t n = 0;
        while (p[n])
            ++n;
        return n;
    }

    CHAR16* ensure(const std::string& realUtf8, const CHAR16* realDir)
    {
        auto it = shadowDir.find(realUtf8);
        if (it == shadowDir.end())
        {
            size_t n = len16(realDir);
            std::vector<CHAR16> buf(n + 3);
            for (size_t i = 0; i < n; i++) buf[i] = realDir[i];
            buf[n] = (CHAR16)'/'; buf[n + 1] = (CHAR16)'s'; buf[n + 2] = 0;
            if (!createDir(buf.data()))
            {
                gShadowPoisoned.store(true, std::memory_order_release);
                fprintf(stderr, "[SHADOW] createDir failed for %s/s -> poison (force strict replay)\n", realUtf8.c_str());
                fflush(stderr);
            }
            it = shadowDir.emplace(realUtf8, std::move(buf)).first;
        }
        return it->second.data();
    }

    void clearWindow()
    {
        gForkWindowActive = false;
        active.store(false, std::memory_order_release);
        written.clear();
    }

public:
    std::atomic<bool> active{ false };

    void arm()
    {
        std::lock_guard<std::mutex> g(mtx);
        // Start each fork window from a clean shadow dir.
        for (const auto& kv : shadowDir)
        {
            std::error_code ec;
            std::filesystem::remove_all(kv.first + "/s", ec);
        }
        shadowDir.clear();
        written.clear();
        gShadowPoisoned.store(false, std::memory_order_release);
        active.store(true, std::memory_order_release);
        gForkWindowActive = true;
    }

    CHAR16* writeDir(CHAR16* realDir, const CHAR16* pageName)
    {
        if (!active.load(std::memory_order_acquire)) return realDir;
        std::lock_guard<std::mutex> g(mtx);
        if (!active.load(std::memory_order_acquire)) return realDir;
        std::string realUtf8 = wchar_to_string(realDir);
        CHAR16* sd = ensure(realUtf8, realDir);
        std::string pageUtf8 = wchar_to_string(pageName);
        if (gForkBench)
        {
            fprintf(stderr, "[SHADOW] divert %s/%s\n", realUtf8.c_str(), pageUtf8.c_str());
            fflush(stderr);
        }
        written.insert({ std::move(realUtf8), std::move(pageUtf8) });
        return sd;
    }

    CHAR16* readDir(CHAR16* realDir, const CHAR16* pageName)
    {
        if (!active.load(std::memory_order_acquire)) return realDir;
        std::lock_guard<std::mutex> g(mtx);
        if (!active.load(std::memory_order_acquire)) return realDir;
        std::string realUtf8 = wchar_to_string(realDir);
        auto it = shadowDir.find(realUtf8);
        if (it == shadowDir.end()) return realDir;
        // Only this window's writes may read from /s/; stale orphan files must be ignored.
        if (!written.count({ realUtf8, wchar_to_string(pageName) })) return realDir;
        if (getFileSize((CHAR16*)pageName, it->second.data()) < 0) return realDir;
        return it->second.data();
    }

    // On commit, failed rename can lose the only copy of an evicted page; exit for restart.
    void commit()
    {
        std::lock_guard<std::mutex> g(mtx);
        if (gForkBench && !written.empty())
        {
            fprintf(stderr, "[SHADOW] commit %zu diverted page(s) -> real\n", written.size());
            fflush(stderr);
        }
        for (const auto& [real, name] : written)
        {
            const std::string from = real + "/s/" + name;
            const std::string to = real + "/" + name;
            unsigned int delayMs = 100;   // mirrors SWAPVM_IO_INITIAL_DELAY_MS
            bool ok = false;
            for (int attempt = 0; attempt < 5; attempt++)   // mirrors SWAPVM_IO_MAX_ATTEMPTS
            {
                std::error_code ec;
                std::filesystem::rename(from, to, ec);
                if (!ec) { ok = true; break; }
                fprintf(stderr, "[SHADOW] commit rename failed (attempt %d/5) %s -> %s: %s\n",
                        attempt + 1, from.c_str(), to.c_str(), ec.message().c_str());
                fflush(stderr);
                if (attempt + 1 < 5) { std::this_thread::sleep_for(std::chrono::milliseconds(delayMs)); delayMs *= 2; }
            }
            if (!ok)
            {
                fprintf(stderr, "[SHADOW] FATAL: commit could not persist %s (disk failure) -> exit for restart from snapshot\n", to.c_str());
                fflush(stderr);
                _exit(1);   // skip atexit/global dtors under held locks
            }
        }
        clearWindow();
    }

    void discard()
    {
        std::lock_guard<std::mutex> g(mtx);
        if (gForkBench && !written.empty())
        {
            fprintf(stderr, "[SHADOW] discard %zu diverted page(s)\n", written.size());
            fflush(stderr);
        }
        for (const auto& [real, name] : written)
        {
            std::error_code ec;
            std::filesystem::remove(real + "/s/" + name, ec);
        }
        clearWindow();
    }

    // Promoted child inherits mutex state from threads that did not survive fork().
    void reinitForChildPromote()
    {
        new (&mtx) std::mutex();
    }

    void purgeOrphans()
    {
        std::lock_guard<std::mutex> g(mtx);
        if (gForkBench && !written.empty())
        {
            fprintf(stderr, "[SHADOW] child purgeOrphans: drop %zu diverted page(s); real pristine\n", written.size());
            fflush(stderr);
        }
        for (const auto& kv : shadowDir)
        {
            std::error_code ec;
            std::filesystem::remove_all(kv.first + "/s", ec);
        }
        written.clear();
        active.store(false, std::memory_order_release);
    }
};

inline DiskShadow gShadow;

static inline CHAR16* liteShadowWriteDir(CHAR16* pageDir, const CHAR16* pageName)
{
    return gShadow.writeDir(pageDir, pageName);
}
static inline CHAR16* liteShadowReadDir(CHAR16* pageDir, const CHAR16* pageName)
{
    return gShadow.readDir(pageDir, pageName);
}

#else

static inline void liteForkRequestPark() {}
static inline CHAR16* liteShadowWriteDir(CHAR16* pageDir, const CHAR16* pageName)
{
    (void)pageName;
    return pageDir;
}
static inline CHAR16* liteShadowReadDir(CHAR16* pageDir, const CHAR16* pageName)
{
    (void)pageName;
    return pageDir;
}

#endif // __linux__

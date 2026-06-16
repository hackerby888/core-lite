#pragma once

// Disk-rollback shadow for the fork tick rollback: during a window, parent VM page writes divert
// to a per-dir /s subdir (real files stay pristine for the child); commit on match, discard on
// mismatch. Include before virtual_memory.h. CHAR16 = 2-byte (-fshort-wchar): no std::wstring/libc-wide.
// The fork machinery is Linux-only; only the flags below + the VM hooks are cross-platform (the heavy
// std deps pull <process.h> on MSVC, which collides with system.h's `system` macro), so they are gated.

#include <atomic>   // the fork-rollback flags below are CLI-settable on every platform

// Armed on the parent for the duration of one fork window.
inline volatile bool gForkWindowActive = false;
// Drives isRevalidation strict scoring during the child re-run (not forceVerifySolutions).
inline std::atomic<bool> gReRunStrict{ false };
// Checkpoint-and-replay: the promoted child re-runs strict through this tick (the window's last
// processed tick at mismatch), then resumes optimistic. 0 = single-tick strict (legacy/fork-fail).
inline std::atomic<unsigned int> gReRunStrictUntilTick{ 0 };
// Set when a shadow dir/commit disk op fails; verdict then forces a strict child replay from the
// pristine real files (the optimistic on-disk state can no longer be trusted). Cleared at arm().
inline std::atomic<bool> gShadowPoisoned{ false };
// Test: assert the fork re-run reproduces the quorum digest.
inline volatile bool gVerifyForkRollback = false;

// Test hooks (fork mode): force a fork every tick (exercise the MATCH path on clean ticks),
// force the verdict to take the match branch, and print per-fork timing + RSS.
inline volatile bool gForkForceFork = false;
inline volatile bool gForkForceMatch = false;
inline volatile bool gForkForceMismatch = false;
inline volatile bool gForkBench = false;

// Request-processor quiesce for a consistent fork snapshot.
inline std::atomic<bool> gForkQuiesceRequest{ false };
inline std::atomic<int> gForkParked{ 0 };
inline std::atomic<unsigned> gForkParkGen{ 0 };   // bumped per fork window; see liteForkRequestPark

#ifdef __linux__   // fork-based disk rollback: Linux-only (fork/COW); these std deps pull <process.h> on MSVC

#include <map>
#include <set>
#include <string>
#include <vector>
#include <mutex>
#include <new>
#include <thread>
#include <filesystem>
#include <utility>

// Called by request processors at loop top; parks while a fork window is set up.
static inline void liteForkRequestPark()
{
    if (!gForkQuiesceRequest.load(std::memory_order_acquire)) return;
    // Count once per fork window (generation): a straggler from a prior window can't double-count or
    // underflow the barrier, and there is no decrement-on-release to race the next window's reset.
    static thread_local unsigned myGen = (unsigned)-1;
    unsigned g = gForkParkGen.load(std::memory_order_acquire);
    if (myGen != g) { myGen = g; gForkParked.fetch_add(1, std::memory_order_acq_rel); }
    while (gForkQuiesceRequest.load(std::memory_order_acquire)) std::this_thread::yield();
}

class DiskShadow
{
    std::mutex mtx;
    std::map<std::string, std::vector<CHAR16>> shadowDir;     // real dir (utf8) -> shadow dir buffer
    std::set<std::pair<std::string, std::string>> written;   // (real dir utf8, page name utf8)

    // 2-byte safe length; volatile defeats clang rewriting the scan into libc wcslen.
    static size_t len16(const CHAR16* s)
    {
        const volatile CHAR16* p = s;
        size_t n = 0;
        while (p[n]) ++n;
        return n;
    }

    // mtx held; cached "<realDir>/s" CHAR16 buffer, created on first use.
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
        written.clear();
        gShadowPoisoned.store(false, std::memory_order_release);
        active.store(true, std::memory_order_release);
        gForkWindowActive = true;
    }

    // Write choke-point: record the page and redirect to the shadow dir.
    CHAR16* writeDir(CHAR16* realDir, const CHAR16* pageName)
    {
        if (!active.load(std::memory_order_acquire)) return realDir;
        std::lock_guard<std::mutex> g(mtx);
        std::string realUtf8 = wchar_to_string(realDir);
        CHAR16* sd = ensure(realUtf8, realDir);
        std::string pageUtf8 = wchar_to_string(pageName);
        if (gForkBench) { fprintf(stderr, "[SHADOW] divert %s/%s\n", realUtf8.c_str(), pageUtf8.c_str()); fflush(stderr); }
        written.insert({ std::move(realUtf8), std::move(pageUtf8) });
        return sd;
    }

    // Read choke-point: serve from the shadow dir if the page was diverted, else real.
    CHAR16* readDir(CHAR16* realDir, const CHAR16* pageName)
    {
        if (!active.load(std::memory_order_acquire)) return realDir;
        std::lock_guard<std::mutex> g(mtx);
        auto it = shadowDir.find(wchar_to_string(realDir));
        if (it == shadowDir.end()) return realDir;
        if (getFileSize((CHAR16*)pageName, it->second.data()) < 0) return realDir;
        return it->second.data();
    }

    // Quorum match: move diverted pages into their real dirs.
    void commit()
    {
        std::lock_guard<std::mutex> g(mtx);
        if (gForkBench && !written.empty()) { fprintf(stderr, "[SHADOW] commit %zu diverted page(s) -> real\n", written.size()); fflush(stderr); }
        size_t failed = 0;
        for (const auto& [real, name] : written)
        {
            std::error_code ec;
            std::filesystem::rename(real + "/s/" + name, real + "/" + name, ec);
            if (ec) failed++;
        }
        if (failed)
        {
            // Match path: RAM state is authoritative and correct; only the on-disk copy is stale, which
            // bites only a later USE_SWAP reload. Loud so a failing disk is visible to the operator.
            fprintf(stderr, "[SHADOW] commit: %zu page(s) failed to persist (disk error); RAM state is authoritative\n", failed);
            fflush(stderr);
        }
        clearWindow();
    }

    // Quorum mismatch: drop diverted pages; real files were never touched.
    void discard()
    {
        std::lock_guard<std::mutex> g(mtx);
        if (gForkBench && !written.empty()) { fprintf(stderr, "[SHADOW] discard %zu diverted page(s)\n", written.size()); fflush(stderr); }
        for (const auto& [real, name] : written)
        {
            std::error_code ec;
            std::filesystem::remove(real + "/s/" + name, ec);
        }
        clearWindow();
    }

    // Promoted fork child: the inherited mtx may be held by a thread that did not survive the fork.
    // Reinit it (mirrors Overload::resetForChildPromote) so the following purgeOrphans cannot deadlock.
    void reinitForChildPromote()
    {
        new (&mtx) std::mutex();
    }

    // Defensive cleanup of leftover shadow subdirs (e.g. after a parent crash).
    void purgeOrphans()
    {
        std::lock_guard<std::mutex> g(mtx);
        if (gForkBench && !written.empty()) { fprintf(stderr, "[SHADOW] child purgeOrphans: drop %zu diverted page(s); real pristine\n", written.size()); fflush(stderr); }
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

// Hooks called from the VM disk choke-points in virtual_memory.h.
static inline CHAR16* liteShadowWriteDir(CHAR16* pageDir, const CHAR16* pageName)
{
    return gShadow.writeDir(pageDir, pageName);
}
static inline CHAR16* liteShadowReadDir(CHAR16* pageDir, const CHAR16* pageName)
{
    return gShadow.readDir(pageDir, pageName);
}

#else  // !__linux__ : no fork rollback; the VM hooks pass through and the request park is a no-op.

static inline void liteForkRequestPark() {}
static inline CHAR16* liteShadowWriteDir(CHAR16* pageDir, const CHAR16*) { return pageDir; }
static inline CHAR16* liteShadowReadDir(CHAR16* pageDir, const CHAR16*) { return pageDir; }

#endif // __linux__

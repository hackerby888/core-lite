#pragma once

// Disk-rollback shadow for the fork-based tick rollback.
// During a fork window the parent's VM page writes are redirected into a per-dir
// shadow subdir, leaving the real page files pristine for the child. On a quorum
// match the shadow is committed (moved into the real dir); on mismatch it is
// discarded. In-RAM VM bookkeeping rides fork-COW, so only on-disk bytes divert.
//
// Included before virtual_memory.h so the VM disk choke-points can call the hooks.
// Depends only on file_io.h (save/load/getFileSize/createDir + wchar_to_string).
// CHAR16 is wchar_t built with -fshort-wchar (2 byte): never use std::wstring or
// libc wide funcs on it; keys are UTF-8, dir buffers are raw CHAR16 vectors.

#include <map>
#include <set>
#include <string>
#include <vector>
#include <mutex>
#include <atomic>
#include <thread>
#include <filesystem>
#include <utility>

// Armed on the parent for the duration of one fork window.
inline volatile bool gForkWindowActive = false;
// Drives isRevalidation strict scoring during the child re-run (not forceVerifySolutions).
inline volatile bool gReRunStrict = false;
// Test: assert the fork re-run reproduces the quorum digest.
inline volatile bool gVerifyForkRollback = false;

// Test hooks (fork mode): force a fork every tick (exercise the MATCH path on clean ticks),
// force the verdict to take the match branch, and print per-fork timing + RSS.
inline volatile bool gForkForceFork = false;
inline volatile bool gForkForceMatch = false;
inline volatile bool gForkBench = false;

// Request-processor quiesce for a consistent fork snapshot.
inline volatile bool gForkQuiesceRequest = false;
inline std::atomic<int> gForkParked{ 0 };

// Called by request processors at loop top; parks while a fork window is set up.
static inline void liteForkRequestPark()
{
    if (!gForkQuiesceRequest) return;
    gForkParked.fetch_add(1, std::memory_order_acq_rel);
    while (gForkQuiesceRequest) std::this_thread::yield();
    gForkParked.fetch_sub(1, std::memory_order_acq_rel);
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
            createDir(buf.data());
            it = shadowDir.emplace(realUtf8, std::move(buf)).first;
        }
        return it->second.data();
    }

    void clearWindow()
    {
        gForkWindowActive = false;
        active = false;
        written.clear();
    }

public:
    volatile bool active = false;

    void arm()
    {
        std::lock_guard<std::mutex> g(mtx);
        written.clear();
        active = true;
        gForkWindowActive = true;
    }

    // Write choke-point: record the page and redirect to the shadow dir.
    CHAR16* writeDir(CHAR16* realDir, const CHAR16* pageName)
    {
        if (!active) return realDir;
        std::lock_guard<std::mutex> g(mtx);
        std::string realUtf8 = wchar_to_string(realDir);
        CHAR16* sd = ensure(realUtf8, realDir);
        written.insert({ std::move(realUtf8), wchar_to_string(pageName) });
        return sd;
    }

    // Read choke-point: serve from the shadow dir if the page was diverted, else real.
    CHAR16* readDir(CHAR16* realDir, const CHAR16* pageName)
    {
        if (!active) return realDir;
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
        for (const auto& [real, name] : written)
        {
            std::error_code ec;
            std::filesystem::rename(real + "/s/" + name, real + "/" + name, ec);
        }
        clearWindow();
    }

    // Quorum mismatch: drop diverted pages; real files were never touched.
    void discard()
    {
        std::lock_guard<std::mutex> g(mtx);
        for (const auto& [real, name] : written)
        {
            std::error_code ec;
            std::filesystem::remove(real + "/s/" + name, ec);
        }
        clearWindow();
    }

    // Defensive cleanup of leftover shadow subdirs (e.g. after a parent crash).
    void purgeOrphans()
    {
        std::lock_guard<std::mutex> g(mtx);
        for (const auto& kv : shadowDir)
        {
            std::error_code ec;
            std::filesystem::remove_all(kv.first + "/s", ec);
        }
        written.clear();
        active = false;
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

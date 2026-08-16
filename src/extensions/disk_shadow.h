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

class DiskShadow
{
    struct PageKey
    {
        std::string realDirPath;
        std::string pageFileName;

        bool operator<(const PageKey& other) const
        {
            if (realDirPath != other.realDirPath)
                return realDirPath < other.realDirPath;
            return pageFileName < other.pageFileName;
        }
    };

    using ShadowDirBuffer = std::vector<CHAR16>;

    static constexpr int ioMaxAttempts = 5;
    static constexpr unsigned int ioInitialDelayMs = 100;

    std::mutex shadowMutex; // SMARTMUTEX-EXEMPT: reinitialized after promotion; never held across fork()
    std::map<std::string, ShadowDirBuffer> shadowDirByRealDirPath;
    std::set<PageKey> writtenPages;
    std::set<PageKey> removedPages;

    // 2-byte safe length; keep clang from rewriting this into libc wcslen.
    static size_t char16Length(const CHAR16* text)
    {
        const volatile CHAR16* volatileText = text;
        size_t length = 0;
        while (volatileText[length]) ++length;
        return length;
    }

    static std::string realPagePath(const PageKey& page)
    {
        return page.realDirPath + "/" + page.pageFileName;
    }

    static std::string shadowPagePath(const PageKey& page)
    {
        return page.realDirPath + "/s/" + page.pageFileName;
    }

    static void sleepBeforeRetry(unsigned int& delayMs)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
        delayMs *= 2;
    }

    static void exitAfterPersistFailure(const std::string& realPath)
    {
        fprintf(stderr, "[SHADOW] FATAL: commit could not persist %s (disk failure) -> exit for restart from snapshot\n", realPath.c_str());
        fflush(stderr);
        _exit(1);   // skip atexit/global dtors under held locks
    }

    static void exitAfterDeleteFailure(const std::string& realPath)
    {
        fprintf(stderr, "[SHADOW] FATAL: commit could not delete %s -> exit for restart from snapshot\n", realPath.c_str());
        fflush(stderr);
        _exit(1);   // skip atexit/global dtors under held locks
    }

    static void renameShadowPageToReal(const PageKey& page)
    {
        const std::string shadowPath = shadowPagePath(page);
        const std::string realPath = realPagePath(page);
        unsigned int delayMs = ioInitialDelayMs;

        for (int attempt = 1; attempt <= ioMaxAttempts; attempt++)
        {
            std::error_code ec;
            std::filesystem::rename(shadowPath, realPath, ec);
            if (!ec)
                return;

            fprintf(stderr, "[SHADOW] commit rename failed (attempt %d/%d) %s -> %s: %s\n",
                    attempt, ioMaxAttempts, shadowPath.c_str(), realPath.c_str(), ec.message().c_str());
            fflush(stderr);

            if (attempt < ioMaxAttempts)
                sleepBeforeRetry(delayMs);
        }

        exitAfterPersistFailure(realPath);
    }

    static void removeRealPage(const PageKey& page)
    {
        const std::string realPath = realPagePath(page);
        unsigned int delayMs = ioInitialDelayMs;

        for (int attempt = 1; attempt <= ioMaxAttempts; attempt++)
        {
            std::error_code ec;
            std::filesystem::remove(realPath, ec);
            if (!ec)
                return;

            fprintf(stderr, "[SHADOW] commit delete failed (attempt %d/%d) %s: %s\n", attempt, ioMaxAttempts, realPath.c_str(), ec.message().c_str());
            fflush(stderr);

            if (attempt < ioMaxAttempts)
            {
                sleepBeforeRetry(delayMs);
                continue;
            }

            exitAfterDeleteFailure(realPath);
        }
    }

    static void removeShadowPageBestEffort(const PageKey& page)
    {
        std::error_code ec;
        std::filesystem::remove(shadowPagePath(page), ec);
    }

    ShadowDirBuffer& registerDirUnlocked(const std::string& realDirPath, const CHAR16* realDir)
    {
        auto it = shadowDirByRealDirPath.find(realDirPath);
        if (it == shadowDirByRealDirPath.end())
        {
            const size_t realDirLength = char16Length(realDir);
            ShadowDirBuffer shadowDirBuffer(realDirLength + 3);

            for (size_t i = 0; i < realDirLength; i++)
                shadowDirBuffer[i] = realDir[i];

            shadowDirBuffer[realDirLength] = (CHAR16)'/';
            shadowDirBuffer[realDirLength + 1] = (CHAR16)'s';
            shadowDirBuffer[realDirLength + 2] = 0;

            it = shadowDirByRealDirPath.emplace(realDirPath, std::move(shadowDirBuffer)).first;
        }

        return it->second;
    }

    const CHAR16* ensureShadowDir(const std::string& realDirPath, ShadowDirBuffer& shadowDir)
    {
        std::error_code error;
        std::filesystem::create_directory(realDirPath + "/s", error);
        if (error)
        {
            gShadowPoisoned.store(true, std::memory_order_release);
            fprintf(stderr, "[SHADOW] createDir failed for %s/s -> poison (force strict replay)\n", realDirPath.c_str());
            fflush(stderr);
        }
        return shadowDir.data();
    }

    bool removeRegisteredShadowDirs()
    {
        bool success = true;
        for (const auto& entry : shadowDirByRealDirPath)
        {
            std::error_code ec;
            std::filesystem::remove_all(entry.first + "/s", ec);
            if (ec)
            {
                success = false;
                fprintf(stderr, "[SHADOW] cleanup failed for %s/s: %s\n", entry.first.c_str(), ec.message().c_str());
                fflush(stderr);
            }
        }
        return success;
    }

    void clearWindow()
    {
        gForkWindowActive = false;
        active.store(false, std::memory_order_release);
        writtenPages.clear();
        removedPages.clear();
    }

public:
    std::atomic<bool> active{ false };

    void registerDir(const CHAR16* realDir)
    {
        if (!realDir || !realDir[0])
            return;

        std::lock_guard<std::mutex> guard(shadowMutex);
        const std::string realDirPath = wchar_to_string(realDir);
        registerDirUnlocked(realDirPath, realDir);
    }

    bool arm()
    {
        std::lock_guard<std::mutex> guard(shadowMutex);
        clearWindow();
        if (!removeRegisteredShadowDirs())
        {
            gShadowPoisoned.store(true, std::memory_order_release);
            return false;
        }

        gShadowPoisoned.store(false, std::memory_order_release);
        active.store(true, std::memory_order_release);
        gForkWindowActive = true;
        return true;
    }

    const CHAR16* dirForWrite(const CHAR16* realDir, const CHAR16* pageName)
    {
        if (!active.load(std::memory_order_acquire) || !realDir || !realDir[0] || !pageName || !pageName[0])
            return realDir;

        std::lock_guard<std::mutex> guard(shadowMutex);
        if (!active.load(std::memory_order_acquire))
            return realDir;

        const std::string realDirPath = wchar_to_string(realDir);
        auto shadowDirEntry = shadowDirByRealDirPath.find(realDirPath);
        if (shadowDirEntry == shadowDirByRealDirPath.end())
            return realDir;

        const std::string pageFileName = wchar_to_string(pageName);
        const PageKey page{ realDirPath, pageFileName };
        const CHAR16* shadowDirForPage = ensureShadowDir(realDirPath, shadowDirEntry->second);

        removedPages.erase(page);
        writtenPages.insert(page);

        if (gForkBench)
        {
            fprintf(stderr, "[SHADOW] divert %s/%s\n", realDirPath.c_str(), pageFileName.c_str());
            fflush(stderr);
        }

        return shadowDirForPage;
    }

    const CHAR16* dirForRemove(const CHAR16* realDir, const CHAR16* pageName)
    {
        if (!active.load(std::memory_order_acquire) || !realDir || !realDir[0] || !pageName || !pageName[0])
            return realDir;

        std::lock_guard<std::mutex> guard(shadowMutex);
        if (!active.load(std::memory_order_acquire))
            return realDir;

        const std::string realDirPath = wchar_to_string(realDir);
        auto shadowDirEntry = shadowDirByRealDirPath.find(realDirPath);
        if (shadowDirEntry == shadowDirByRealDirPath.end())
            return realDir;

        const std::string pageFileName = wchar_to_string(pageName);
        const PageKey page{ realDirPath, pageFileName };
        const CHAR16* shadowDirForPage = ensureShadowDir(realDirPath, shadowDirEntry->second);

        writtenPages.erase(page);
        removedPages.insert(page);

        if (gForkBench)
        {
            fprintf(stderr, "[SHADOW] tombstone %s/%s\n", realDirPath.c_str(), pageFileName.c_str());
            fflush(stderr);
        }

        return shadowDirForPage;
    }

    const CHAR16* dirForRead(const CHAR16* realDir, const CHAR16* pageName)
    {
        if (!active.load(std::memory_order_acquire) || !realDir || !realDir[0] || !pageName || !pageName[0])
            return realDir;

        std::lock_guard<std::mutex> guard(shadowMutex);
        if (!active.load(std::memory_order_acquire))
            return realDir;

        const std::string realDirPath = wchar_to_string(realDir);
        auto shadowDirEntry = shadowDirByRealDirPath.find(realDirPath);
        if (shadowDirEntry == shadowDirByRealDirPath.end())
            return realDir;

        const std::string pageFileName = wchar_to_string(pageName);
        const PageKey page{ realDirPath, pageFileName };

        if (removedPages.count(page))
            return ensureShadowDir(realDirPath, shadowDirEntry->second);

        // Only this window's writes may read from /s/; stale orphan files must be ignored.
        if (!writtenPages.count(page))
            return realDir;

        std::error_code error;
        std::filesystem::file_size(shadowPagePath(page), error);
        if (error)
            return realDir;

        return shadowDirEntry->second.data();
    }

    // On commit, failed rename can lose the only copy of an evicted page; exit for restart.
    void commit()
    {
        std::lock_guard<std::mutex> guard(shadowMutex);

        if (gForkBench && !writtenPages.empty())
        {
            fprintf(stderr, "[SHADOW] commit %zu diverted page(s) -> real\n", writtenPages.size());
            fflush(stderr);
        }

        for (const PageKey& page : writtenPages)
            renameShadowPageToReal(page);

        if (gForkBench && !removedPages.empty())
        {
            fprintf(stderr, "[SHADOW] commit %zu tombstone(s) -> real\n", removedPages.size());
            fflush(stderr);
        }

        for (const PageKey& page : removedPages)
        {
            removeRealPage(page);
            removeShadowPageBestEffort(page);
        }

        clearWindow();
    }

    void discard()
    {
        std::lock_guard<std::mutex> guard(shadowMutex);

        if (gForkBench && !writtenPages.empty())
        {
            fprintf(stderr, "[SHADOW] discard %zu diverted page(s)\n", writtenPages.size());
            fflush(stderr);
        }

        for (const PageKey& page : writtenPages)
            removeShadowPageBestEffort(page);

        for (const PageKey& page : removedPages)
            removeShadowPageBestEffort(page);

        clearWindow();
    }

    // Promoted child inherits mutex state from threads that did not survive fork().
    void reinitForChildPromote()
    {
        new (&shadowMutex) std::mutex();
    }

    bool purgeOrphans()
    {
        std::lock_guard<std::mutex> guard(shadowMutex);

        if (gForkBench && !writtenPages.empty())
        {
            fprintf(stderr, "[SHADOW] child purgeOrphans: drop %zu diverted page(s); real pristine\n", writtenPages.size());
            fflush(stderr);
        }

        clearWindow();
        const bool success = removeRegisteredShadowDirs();
        if (!success)
            gShadowPoisoned.store(true, std::memory_order_release);
        return success;
    }
};

inline DiskShadow gShadow;

static inline const CHAR16* liteShadowWriteDir(const CHAR16* pageDir, const CHAR16* pageName)
{
    return gShadow.dirForWrite(pageDir, pageName);
}
static inline const CHAR16* liteShadowReadDir(const CHAR16* pageDir, const CHAR16* pageName)
{
    return gShadow.dirForRead(pageDir, pageName);
}
static inline const CHAR16* liteShadowRemoveDir(const CHAR16* pageDir, const CHAR16* pageName)
{
    return gShadow.dirForRemove(pageDir, pageName);
}

#else

static inline const CHAR16* liteShadowWriteDir(const CHAR16* pageDir, const CHAR16* pageName)
{
    (void)pageName;
    return pageDir;
}
static inline const CHAR16* liteShadowReadDir(const CHAR16* pageDir, const CHAR16* pageName)
{
    (void)pageName;
    return pageDir;
}
static inline const CHAR16* liteShadowRemoveDir(const CHAR16* pageDir, const CHAR16* pageName)
{
    (void)pageName;
    return pageDir;
}

#endif // __linux__

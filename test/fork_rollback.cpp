#define NO_UEFI

#include "gtest/gtest.h"

#include "extensions/utils.h"        // wchar_to_string, wcharToNumber
#include "platform/file_io.h"        // CHAR16, createDir, getFileSize, setText (via console_logging.h)
#include "extensions/disk_shadow.h"  // gShadow, DiskShadow (must precede virtual_memory.h; not included here)
#include "extensions/swapvm_dirty_track.h"
#include "platform/concurrency.h"    // fork-eligibility census (forkCensusEnter/Leave/SumExcept/Offender)
#include "extensions/fork_census.h"  // SmartMutex / SmartSharedMutex
#include "extensions/fork_stats.h"   // ForkStats (unforkable-tick counters + durable log)

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#if defined(__linux__)
#include <cstdlib>   // posix_memalign, free
#endif

namespace {

const char* kBase = "fork_rollback_test";

void writeFileUtf8(const std::string& path, const std::string& content)
{
    std::ofstream f(path, std::ios::binary | std::ios::trunc);
    f << content;
}
std::string readFileUtf8(const std::string& path)
{
    std::ifstream f(path, std::ios::binary);
    std::stringstream ss; ss << f.rdbuf();
    return ss.str();
}

// gShadow is a process-global; reset it between tests so `written`/`shadowDir`/active don't leak.
class ForkRollback : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::filesystem::remove_all(kBase);
        std::filesystem::create_directories(kBase);
        gShadow.discard();   // clear active + `written` from any prior test
    }
    void TearDown() override
    {
        gShadow.discard();
        std::filesystem::remove_all(kBase);
    }
};

} // namespace

// 1. arm + writeDir diverts the page to <dir>/s and records it; the real file is untouched.
TEST_F(ForkRollback, WriteDirDivertsToShadow)
{
    std::filesystem::create_directories(std::string(kBase) + "/divert");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/divert");
    CHAR16 page[64]; setText(page, L"pg0");

    gShadow.arm();
    CHAR16* sd = gShadow.writeDir(dir, page);

    EXPECT_EQ(wchar_to_string(sd), std::string("fork_rollback_test/divert/s"));
    EXPECT_TRUE(std::filesystem::exists("fork_rollback_test/divert/s"));   // ensure() created it
    EXPECT_FALSE(std::filesystem::exists("fork_rollback_test/divert/pg0")); // real not written
}

// 2. readDir serves /s ONLY for a page in `written` whose /s file exists; otherwise real.
//    Guards the orphan-gate fix: a stale /s file alone must not divert a read.
TEST_F(ForkRollback, ReadDirWrittenGate)
{
    std::filesystem::create_directories(std::string(kBase) + "/gate");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/gate");
    CHAR16 pageA[64]; setText(pageA, L"pgA");
    CHAR16 pageB[64]; setText(pageB, L"pgB");
    CHAR16 pageC[64]; setText(pageC, L"pgC");

    gShadow.arm();
    CHAR16* sd = gShadow.writeDir(dir, pageA);          // shadowDir[dir] populated, written={pgA}
    writeFileUtf8("fork_rollback_test/gate/s/pgA", "NEW");

    // pgA: written + /s file exists -> diverts to /s
    EXPECT_EQ(wchar_to_string(gShadow.readDir(dir, pageA)), wchar_to_string(sd));
    // pgB: dir is in shadowDir but pgB not in `written` -> real (the orphan gate)
    EXPECT_EQ(wchar_to_string(gShadow.readDir(dir, pageB)), wchar_to_string(dir));
    // pgC: written but its /s file is absent -> real (getFileSize gate)
    gShadow.writeDir(dir, pageC);
    EXPECT_EQ(wchar_to_string(gShadow.readDir(dir, pageC)), wchar_to_string(dir));
}

// 3. commit renames /s/<page> over an EXISTING real file (atomic replace).
TEST_F(ForkRollback, CommitOverExistingRealFile)
{
    std::filesystem::create_directories(std::string(kBase) + "/commit");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/commit");
    CHAR16 page[64]; setText(page, L"pg0");

    writeFileUtf8("fork_rollback_test/commit/pg0", "OLD");
    gShadow.arm();
    gShadow.writeDir(dir, page);
    writeFileUtf8("fork_rollback_test/commit/s/pg0", "NEW");

    gShadow.commit();

    EXPECT_EQ(readFileUtf8("fork_rollback_test/commit/pg0"), "NEW");          // replaced
    EXPECT_FALSE(std::filesystem::exists("fork_rollback_test/commit/s/pg0")); // moved out
}

// 4. (#4) commit retries then exit(1) when a written page can't be renamed (here: /s file missing).
TEST_F(ForkRollback, CommitRetryThenFatalOnRenameFailure)
{
    EXPECT_EXIT({
        std::filesystem::create_directories("fork_rollback_test/fatal");
        CHAR16 dir[256]; setText(dir, L"fork_rollback_test/fatal");
        CHAR16 page[64]; setText(page, L"pg0");
        gShadow.arm();
        gShadow.writeDir(dir, page);   // registers pg0 in `written`; /s/pg0 is never created
        gShadow.commit();              // rename(/s/pg0 -> /pg0) ENOENTs every retry -> exit(1)
    }, ::testing::ExitedWithCode(1), "FATAL: commit could not persist");
}

// 5. discard drops the /s page; the real file stays pristine (pre-window).
TEST_F(ForkRollback, DiscardKeepsRealPristine)
{
    std::filesystem::create_directories(std::string(kBase) + "/discard");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/discard");
    CHAR16 page[64]; setText(page, L"pg0");

    writeFileUtf8("fork_rollback_test/discard/pg0", "OLD");
    gShadow.arm();
    gShadow.writeDir(dir, page);
    writeFileUtf8("fork_rollback_test/discard/s/pg0", "NEW");

    gShadow.discard();

    EXPECT_FALSE(std::filesystem::exists("fork_rollback_test/discard/s/pg0"));
    EXPECT_EQ(readFileUtf8("fork_rollback_test/discard/pg0"), "OLD");
}

// 6. arm() purges a prior window's diverted /s pages so the next window starts clean.
TEST_F(ForkRollback, ArmPurgesPriorWindowShadow)
{
    std::filesystem::create_directories(std::string(kBase) + "/armpurge");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/armpurge");
    CHAR16 page[64]; setText(page, L"pg0");

    gShadow.arm();
    gShadow.writeDir(dir, page);
    writeFileUtf8("fork_rollback_test/armpurge/s/pg0", "X");   // window 1 divert, no commit/discard
    EXPECT_TRUE(std::filesystem::exists("fork_rollback_test/armpurge/s/pg0"));

    gShadow.arm();   // window 2: purges shadowDir's /s

    EXPECT_FALSE(std::filesystem::exists("fork_rollback_test/armpurge/s/pg0"));
}

// 7. purgeOrphans (child path): drop /s, real pristine, window inactive.
TEST_F(ForkRollback, PurgeOrphansDropsShadow)
{
    std::filesystem::create_directories(std::string(kBase) + "/purge");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/purge");
    CHAR16 page[64]; setText(page, L"pg0");

    writeFileUtf8("fork_rollback_test/purge/pg0", "OLD");
    gShadow.arm();
    gShadow.writeDir(dir, page);
    writeFileUtf8("fork_rollback_test/purge/s/pg0", "NEW");

    gShadow.purgeOrphans();

    EXPECT_FALSE(std::filesystem::exists("fork_rollback_test/purge/s/pg0"));
    EXPECT_EQ(readFileUtf8("fork_rollback_test/purge/pg0"), "OLD");
    EXPECT_FALSE(gShadow.active.load());
}

#if defined(__linux__)

// 8. registerPool + tryMarkDirty: in-range address marks its slot dirty; out-of-range is ignored.
TEST(ForkRollbackDirtyTrack, MarkDirtyInRange)
{
    const int N = 4;
    const unsigned long long stride = 4096;
    unsigned char* buf = nullptr;
    ASSERT_EQ(posix_memalign((void**)&buf, 4096, stride * N), 0);
    unsigned char* poolBase = buf;
    volatile unsigned char dirty[N] = {0};

    gSwapDirtyTrackEnabled = true;
    SwapDirtyTrack::registerPool(&poolBase, stride, N, dirty);

    EXPECT_TRUE(SwapDirtyTrack::tryMarkDirty(buf + 2 * stride + 100));
    EXPECT_EQ(dirty[2], 1);
    EXPECT_FALSE(SwapDirtyTrack::tryMarkDirty(buf + N * stride + 10));   // past the pool

    SwapDirtyTrack::unregisterPool(&poolBase);
    gSwapDirtyTrackEnabled = false;
    free(buf);
}

// 9. unregisterPool stops the fault path for that pool; a later register reuses the dead slot.
TEST(ForkRollbackDirtyTrack, UnregisterSkipsAndReusesSlot)
{
    const int N = 4;
    const unsigned long long stride = 4096;
    unsigned char* buf = nullptr;
    ASSERT_EQ(posix_memalign((void**)&buf, 4096, stride * N), 0);
    gSwapDirtyTrackEnabled = true;

    unsigned char* poolBase = buf;
    volatile unsigned char dirty1[N] = {0};
    SwapDirtyTrack::registerPool(&poolBase, stride, N, dirty1);
    SwapDirtyTrack::unregisterPool(&poolBase);
    EXPECT_FALSE(SwapDirtyTrack::tryMarkDirty(buf + 100));   // basePtr -> gDeadBase -> skipped

    const int countBeforeReuse = SwapDirtyTrack::gPoolCount.load();
    unsigned char* poolBase2 = buf;
    volatile unsigned char dirty2[N] = {0};
    SwapDirtyTrack::registerPool(&poolBase2, stride, N, dirty2);
    EXPECT_EQ(SwapDirtyTrack::gPoolCount.load(), countBeforeReuse);   // reused the dead slot, no growth
    EXPECT_TRUE(SwapDirtyTrack::tryMarkDirty(buf + 2 * stride + 1));
    EXPECT_EQ(dirty2[2], 1);

    SwapDirtyTrack::unregisterPool(&poolBase2);
    gSwapDirtyTrackEnabled = false;
    free(buf);
}

// 10. the registry never grows past MAX_POOLS no matter how many pools are registered.
TEST(ForkRollbackDirtyTrack, OverflowGuardCapsPoolCount)
{
    const int MAXP = SwapDirtyTrack::MAX_POOLS;
    const int N = 4;
    const unsigned long long stride = 4096;
    unsigned char* buf = nullptr;
    ASSERT_EQ(posix_memalign((void**)&buf, 4096, stride * N), 0);
    gSwapDirtyTrackEnabled = true;

    const int over = MAXP + 4;
    std::vector<unsigned char*> poolBases(over);
    std::vector<std::vector<unsigned char>> dirty(over, std::vector<unsigned char>(N, 0));
    for (int i = 0; i < over; i++)
    {
        poolBases[i] = buf;
        SwapDirtyTrack::registerPool(&poolBases[i], stride, N, (volatile unsigned char*)dirty[i].data());
        EXPECT_LE(SwapDirtyTrack::gPoolCount.load(), MAXP);   // guard holds at every step
    }
    for (int i = 0; i < over; i++) SwapDirtyTrack::unregisterPool(&poolBases[i]);

    gSwapDirtyTrackEnabled = false;
    free(buf);
}

#endif // __linux__

// ---------------------------------------------------------------------------------------------------
// Fork-eligibility census: the choke-point that replaces a hand-maintained lock list.
// ---------------------------------------------------------------------------------------------------

// 11. A lock held by the CALLING thread is excluded (BSP self-exclusion); a lock held by ANOTHER
//     thread is counted and named; once that thread exits, its slot frees and the count returns to 0
//     (proves the global-slot design does not dangle on a short-lived thread).
TEST(ForkCensus, SelfExcludedOtherCounted)
{
    forkCensusEnter("selfHeld");
    EXPECT_EQ(forkCensusSumExcept(), 0);   // only this thread holds -> excluded from its own gate view
    forkCensusLeave();
    EXPECT_EQ(forkCensusSumExcept(), 0);

    std::atomic<bool> held{ false }, release{ false };
    std::thread t([&] {
        forkCensusEnter("otherThreadLock");
        held.store(true, std::memory_order_release);
        while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
        forkCensusLeave();
    });
    while (!held.load(std::memory_order_acquire)) std::this_thread::yield();

    EXPECT_GE(forkCensusSumExcept(), 1);                 // the other thread's held lock is visible
    const char* off = forkCensusOffender();
    ASSERT_NE(off, nullptr);
    EXPECT_NE(std::string(off).find("otherThreadLock"), std::string::npos);

    release.store(true, std::memory_order_release);
    t.join();
    EXPECT_EQ(forkCensusSumExcept(), 0);                 // slot freed at thread exit; no dangling read
    EXPECT_EQ(forkCensusOffender(), nullptr);
}

// 12. SmartMutex / SmartSharedMutex (incl. the shared path RPC handlers take) feed the same census, so
//     a non-AP mutex holder trips the gate exactly like a spin-lock. Verified cross-thread.
TEST(ForkCensus, SmartMutexCounted)
{
    SmartMutex sm{ "smTest" };
    SmartSharedMutex ss{ "ssTest" };

    // exclusive SmartMutex held by another thread is counted
    {
        std::atomic<bool> held{ false }, release{ false };
        std::thread t([&] {
            std::lock_guard<SmartMutex> g(sm);
            held.store(true, std::memory_order_release);
            while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
        });
        while (!held.load(std::memory_order_acquire)) std::this_thread::yield();
        EXPECT_GE(forkCensusSumExcept(), 1);
        release.store(true, std::memory_order_release);
        t.join();
        EXPECT_EQ(forkCensusSumExcept(), 0);
    }
    // shared SmartSharedMutex held by another thread is counted (the gRpcDispatchLock shared path)
    {
        std::atomic<bool> held{ false }, release{ false };
        std::thread t([&] {
            std::shared_lock<SmartSharedMutex> g(ss);
            held.store(true, std::memory_order_release);
            while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
        });
        while (!held.load(std::memory_order_acquire)) std::this_thread::yield();
        EXPECT_GE(forkCensusSumExcept(), 1);
        release.store(true, std::memory_order_release);
        t.join();
        EXPECT_EQ(forkCensusSumExcept(), 0);
    }
}

// 13. ForkStats: recorders move the counters and append the COMPLETE record (one durable line per
//     unforkable tick, not a ring).
TEST(ForkStatsTest, CountersAndDurableLog)
{
    std::filesystem::remove(ForkStats::kLogPath);

    unsigned long long total0 = ForkStats::forksSkippedTotal.load();
    unsigned long long census0 = ForkStats::skipByReason[ForkStats::CENSUS].load();
    unsigned long long ok0 = ForkStats::forksOk.load();
    unsigned long long mm0 = ForkStats::mismatches.load();

    ForkStats::onForkOk();
    ForkStats::onVerdict(true);   // mismatch
    ForkStats::onForkSkipped(ForkStats::CENSUS, 1001, "spectrumLock @ x");
    ForkStats::onForkSkipped(ForkStats::CENSUS, 1002, "tickDataLock @ y");
    ForkStats::onForkSkipped(ForkStats::PARK_TIMEOUT, 1003, "");

    EXPECT_EQ(ForkStats::forksOk.load() - ok0, 1u);
    EXPECT_EQ(ForkStats::mismatches.load() - mm0, 1u);
    EXPECT_EQ(ForkStats::forksSkippedTotal.load() - total0, 3u);
    EXPECT_EQ(ForkStats::skipByReason[ForkStats::CENSUS].load() - census0, 2u);
    EXPECT_EQ(ForkStats::lastSkipTick.load(), 1003u);
    EXPECT_EQ(ForkStats::lastSkipReason.load(), (int)ForkStats::PARK_TIMEOUT);

    // durable log holds every skipped tick (3 lines), with tick + reason
    std::string all = ForkStats::readLogAll();
    int lines = 0;
    for (char c : all) if (c == '\n') lines++;
    EXPECT_EQ(lines, 3);
    EXPECT_NE(all.find("tick=1001"), std::string::npos);
    EXPECT_NE(all.find("tick=1002"), std::string::npos);
    EXPECT_NE(all.find("tick=1003"), std::string::npos);
    EXPECT_NE(all.find("reason=census"), std::string::npos);
    EXPECT_NE(all.find("reason=park_timeout"), std::string::npos);

    // summary JSON reflects the counters
    std::string js = ForkStats::summaryJson();
    EXPECT_NE(js.find("\"forksSkippedTotal\""), std::string::npos);
    EXPECT_NE(js.find("\"lastUnforkable\""), std::string::npos);

    std::filesystem::remove(ForkStats::kLogPath);
}

#define NO_UEFI

#include "gtest/gtest.h"

#include "extensions/utils.h"        // wchar_to_string, wcharToNumber
#include "platform/file_io.h"        // CHAR16, createDir, getFileSize, setText (via console_logging.h)
#include "extensions/disk_shadow.h"  // gShadow, DiskShadow (must precede virtual_memory.h; not included here)
#include "extensions/swapvm_dirty_track.h"
#include "platform/concurrency.h"    // fork-eligibility census (forkCensusEnter/Leave/SumExcept/Offender)
#include "extensions/fork_census.h"  // SmartMutex / SmartSharedMutex
#include "extensions/fork_stats.h"   // ForkStats (unforkable-tick counters + durable log)
#include "extensions/tick_fork_control.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <future>
#include <chrono>
#if defined(__linux__)
#include <cerrno>
#include <cstdlib>   // posix_memalign, free
#include <cstring>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
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

// gShadow is a process-global; reset it between tests so writtenPages/shadow dirs/active don't leak.
class ForkRollback : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::filesystem::remove_all(kBase);
        std::filesystem::create_directories(kBase);
        gShadow.discard();   // clear active + writtenPages from any prior test
    }
    void TearDown() override
    {
        gShadow.discard();
        std::filesystem::remove_all(kBase);
    }
};

} // namespace

// 1. arm + dirForWrite diverts the page to <dir>/s and records it; the real file is untouched.
TEST_F(ForkRollback, WriteDirDivertsToShadow)
{
    std::filesystem::create_directories(std::string(kBase) + "/divert");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/divert");
    CHAR16 page[64]; setText(page, L"pg0");

    gShadow.arm();
    CHAR16* sd = gShadow.dirForWrite(dir, page);

    EXPECT_EQ(wchar_to_string(sd), std::string("fork_rollback_test/divert/s"));
    EXPECT_TRUE(std::filesystem::exists("fork_rollback_test/divert/s"));   // ensure() created it
    EXPECT_FALSE(std::filesystem::exists("fork_rollback_test/divert/pg0")); // real not written
}

// 2. dirForRead serves /s ONLY for a written page whose /s file exists; otherwise real.
//    Guards the orphan-gate fix: a stale /s file alone must not divert a read.
TEST_F(ForkRollback, ReadDirWrittenGate)
{
    std::filesystem::create_directories(std::string(kBase) + "/gate");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/gate");
    CHAR16 pageA[64]; setText(pageA, L"pgA");
    CHAR16 pageB[64]; setText(pageB, L"pgB");
    CHAR16 pageC[64]; setText(pageC, L"pgC");

    gShadow.arm();
    CHAR16* sd = gShadow.dirForWrite(dir, pageA);       // shadow dir registered, writtenPages={pgA}
    writeFileUtf8("fork_rollback_test/gate/s/pgA", "NEW");

    // pgA: written + /s file exists -> diverts to /s
    EXPECT_EQ(wchar_to_string(gShadow.dirForRead(dir, pageA)), wchar_to_string(sd));
    // pgB: dir has a shadow dir but pgB was not written -> real (the orphan gate)
    EXPECT_EQ(wchar_to_string(gShadow.dirForRead(dir, pageB)), wchar_to_string(dir));
    // pgC: written but its /s file is absent -> real (getFileSize gate)
    gShadow.dirForWrite(dir, pageC);
    EXPECT_EQ(wchar_to_string(gShadow.dirForRead(dir, pageC)), wchar_to_string(dir));
}

// 3. commit renames /s/<page> over an EXISTING real file (atomic replace).
TEST_F(ForkRollback, CommitOverExistingRealFile)
{
    std::filesystem::create_directories(std::string(kBase) + "/commit");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/commit");
    CHAR16 page[64]; setText(page, L"pg0");

    writeFileUtf8("fork_rollback_test/commit/pg0", "OLD");
    gShadow.arm();
    gShadow.dirForWrite(dir, page);
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
        gShadow.dirForWrite(dir, page);   // registers pg0 in `writtenPages`; /s/pg0 is never created
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
    gShadow.dirForWrite(dir, page);
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
    gShadow.dirForWrite(dir, page);
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
    gShadow.dirForWrite(dir, page);
    writeFileUtf8("fork_rollback_test/purge/s/pg0", "NEW");

    gShadow.purgeOrphans();

    EXPECT_FALSE(std::filesystem::exists("fork_rollback_test/purge/s/pg0"));
    EXPECT_EQ(readFileUtf8("fork_rollback_test/purge/pg0"), "OLD");
    EXPECT_FALSE(gShadow.active.load());
}

// The child inherits this registration at fork time. Parent-created files are not in the child's
// written-page set, so cleanup must use the retained directory list.
TEST_F(ForkRollback, RegisteredDirSurvivesArmForChildPurge)
{
    std::filesystem::create_directories(std::string(kBase) + "/registered");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/registered");
    gShadow.registerDir(dir);

    ASSERT_TRUE(gShadow.arm());
    std::filesystem::create_directories(std::string(kBase) + "/registered/s");
    writeFileUtf8("fork_rollback_test/registered/s/late.pg", "optimistic");

    EXPECT_TRUE(gShadow.purgeOrphans());
    EXPECT_FALSE(std::filesystem::exists("fork_rollback_test/registered/s"));
    EXPECT_FALSE(gShadow.active.load());
    EXPECT_FALSE(gForkWindowActive);
}

TEST_F(ForkRollback, CleanupFailureStaysInactive)
{
    writeFileUtf8(std::string(kBase) + "/not-a-directory", "x");
    CHAR16 dir[256]; setText(dir, L"fork_rollback_test/not-a-directory");
    DiskShadow shadow;
    shadow.registerDir(dir);

    EXPECT_FALSE(shadow.arm());
    EXPECT_FALSE(shadow.active.load());
    EXPECT_FALSE(gForkWindowActive);
    EXPECT_FALSE(shadow.purgeOrphans());
    EXPECT_FALSE(shadow.active.load());
    EXPECT_FALSE(gForkWindowActive);

    gShadowPoisoned.store(false, std::memory_order_release);
}

#if defined(__linux__)

namespace {

bool waitForRetireState(
    tickForkControl::BspRetireHandoff& handoff,
    tickForkControl::BspRetireHandoff::State expected)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (handoff.state() == expected)
            return true;
        std::this_thread::yield();
    }
    return handoff.state() == expected;
}

} // namespace

TEST(ForkRollbackControl, RetireRequestWaitsForBspCompletion)
{
    tickForkControl::BspRetireHandoff handoff;
    auto result = std::async(std::launch::async, [&] {
        return handoff.requestAndWait(1000);
    });

    ASSERT_TRUE(waitForRetireState(
        handoff,
        tickForkControl::BspRetireHandoff::State::Requested));
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    ASSERT_TRUE(handoff.tryStart());
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);

    ASSERT_TRUE(handoff.finish(true));
    ASSERT_EQ(result.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_TRUE(result.get());
    EXPECT_EQ(handoff.state(), tickForkControl::BspRetireHandoff::State::Idle);
}

TEST(ForkRollbackControl, RetireRequestTimeoutCancels)
{
    tickForkControl::BspRetireHandoff handoff;

    EXPECT_FALSE(handoff.requestAndWait(1));
    EXPECT_EQ(handoff.state(), tickForkControl::BspRetireHandoff::State::Idle);
    EXPECT_FALSE(handoff.tryStart());
}

TEST(ForkRollbackControl, RunningRetireIgnoresRequestTimeout)
{
    tickForkControl::BspRetireHandoff handoff;
    auto result = std::async(std::launch::async, [&] {
        return handoff.requestAndWait(10);
    });

    ASSERT_TRUE(waitForRetireState(
        handoff,
        tickForkControl::BspRetireHandoff::State::Requested));
    ASSERT_TRUE(handoff.tryStart());
    const auto afterTimeout =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(20);
    while (std::chrono::steady_clock::now() < afterTimeout)
        std::this_thread::yield();

    EXPECT_EQ(handoff.state(), tickForkControl::BspRetireHandoff::State::Running);
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    ASSERT_TRUE(handoff.finish(true));
    ASSERT_EQ(result.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_TRUE(result.get());
    EXPECT_EQ(handoff.state(), tickForkControl::BspRetireHandoff::State::Idle);
}

TEST(ForkRollbackControl, FailedRetireReturnsFailure)
{
    tickForkControl::BspRetireHandoff handoff;
    auto result = std::async(std::launch::async, [&] {
        return handoff.requestAndWait(1000);
    });

    ASSERT_TRUE(waitForRetireState(
        handoff,
        tickForkControl::BspRetireHandoff::State::Requested));
    ASSERT_TRUE(handoff.tryStart());
    ASSERT_TRUE(handoff.finish(false));
    ASSERT_EQ(result.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    EXPECT_FALSE(result.get());
    EXPECT_EQ(handoff.state(), tickForkControl::BspRetireHandoff::State::Idle);
}

TEST(ForkRollbackControl, ExplicitRetireDoesNotPromote)
{
    int pipeFds[2];
    ASSERT_EQ(pipe(pipeFds), 0);

    ASSERT_TRUE(tickForkControl::writeRetireCommand(pipeFds[1]));
    close(pipeFds[1]);

    const auto command = tickForkControl::readChildCommand(pipeFds[0], 1234);
    close(pipeFds[0]);

    EXPECT_EQ(command.action, tickForkControl::ChildAction::Retire);
    EXPECT_EQ(command.targetTick, 0U);
}

TEST(ForkRollbackControl, ParentCrashEofStillPromotes)
{
    int pipeFds[2];
    ASSERT_EQ(pipe(pipeFds), 0);
    close(pipeFds[1]);

    const auto command = tickForkControl::readChildCommand(pipeFds[0], 1234);
    close(pipeFds[0]);

    EXPECT_EQ(command.action, tickForkControl::ChildAction::Promote);
    EXPECT_EQ(command.targetTick, 1234U);
}

TEST(ForkRollbackControl, PromoteCommandCarriesTargetTick)
{
    int pipeFds[2];
    ASSERT_EQ(pipe(pipeFds), 0);

    const char tag = tickForkControl::promoteTag;
    const unsigned int targetTick = 5678;
    const char sentinel = 'x';
    ASSERT_EQ(write(pipeFds[1], &tag, 1), 1);
    ASSERT_EQ(write(pipeFds[1], &targetTick, sizeof(targetTick)), (ssize_t)sizeof(targetTick));
    ASSERT_EQ(write(pipeFds[1], &sentinel, 1), 1);
    close(pipeFds[1]);

    const auto command = tickForkControl::readChildCommand(pipeFds[0], 1234);
    close(pipeFds[0]);

    EXPECT_EQ(command.action, tickForkControl::ChildAction::Promote);
    EXPECT_EQ(command.targetTick, targetTick);
}

TEST(ForkRollbackControl, PromoteWaitsForParentEof)
{
    int pipeFds[2];
    ASSERT_EQ(pipe(pipeFds), 0);

    const char tag = tickForkControl::promoteTag;
    const unsigned int targetTick = 5678;
    ASSERT_EQ(write(pipeFds[1], &tag, 1), 1);
    ASSERT_EQ(write(pipeFds[1], &targetTick, sizeof(targetTick)), (ssize_t)sizeof(targetTick));

    std::promise<tickForkControl::ChildCommand> commandPromise;
    std::future<tickForkControl::ChildCommand> commandFuture = commandPromise.get_future();
    std::thread reader([&] {
        commandPromise.set_value(tickForkControl::readChildCommand(pipeFds[0], 1234));
        close(pipeFds[0]);
    });

    bool drained = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (std::chrono::steady_clock::now() < deadline
           && commandFuture.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready)
    {
        int bytesAvailable = -1;
        if (ioctl(pipeFds[0], FIONREAD, &bytesAvailable) == 0 && bytesAvailable == 0)
        {
            drained = true;
            break;
        }
        std::this_thread::yield();
    }
    EXPECT_TRUE(drained);
    EXPECT_EQ(commandFuture.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);

    close(pipeFds[1]);
    const bool ready = commandFuture.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    EXPECT_TRUE(ready);
    reader.join();
    if (!ready)
        return;
    const auto command = commandFuture.get();

    EXPECT_EQ(command.action, tickForkControl::ChildAction::Promote);
    EXPECT_EQ(command.targetTick, targetTick);
}

TEST(ForkRollbackControl, PromoteClosesOnlyInheritedRpcUnixSockets)
{
    EXPECT_EXIT(
    {
        const std::string rpcPath =
            "/tmp/qubic-rpc-promote-" + std::to_string(getpid()) + ".sock";
        unlink(rpcPath.c_str());

        const int listenerFd = socket(AF_UNIX, SOCK_STREAM, 0);
        sockaddr_un rpcAddress{};
        rpcAddress.sun_family = AF_UNIX;
        std::strncpy(rpcAddress.sun_path, rpcPath.c_str(), sizeof(rpcAddress.sun_path) - 1);
        if (listenerFd < 0
            || bind(listenerFd, (sockaddr*)&rpcAddress, sizeof(rpcAddress)) != 0
            || listen(listenerFd, 1) != 0)
        {
            unlink(rpcPath.c_str());
            _exit(1);
        }

        const int clientFd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (clientFd < 0
            || connect(clientFd, (sockaddr*)&rpcAddress, sizeof(rpcAddress)) != 0)
        {
            unlink(rpcPath.c_str());
            _exit(1);
        }

        const int acceptedFd = accept(listenerFd, nullptr, nullptr);
        int unrelatedUnixFds[2];
        int pipeFds[2];
        if (acceptedFd < 0
            || socketpair(AF_UNIX, SOCK_STREAM, 0, unrelatedUnixFds) != 0
            || pipe(pipeFds) != 0)
        {
            unlink(rpcPath.c_str());
            _exit(1);
        }

        const int inetFd = socket(AF_INET, SOCK_STREAM, 0);
        if (inetFd < 0)
        {
            unlink(rpcPath.c_str());
            _exit(1);
        }

        const unsigned int closedCount =
            tickForkControl::closeInheritedRpcUnixSocketsForPromote(
                listenerFd,
                rpcPath.c_str());
        unlink(rpcPath.c_str());

        errno = 0;
        const bool listenerClosed =
            fcntl(listenerFd, F_GETFD) == -1 && errno == EBADF;
        errno = 0;
        const bool acceptedConnectionClosed =
            fcntl(acceptedFd, F_GETFD) == -1 && errno == EBADF;
        const bool clientOpen = fcntl(clientFd, F_GETFD) != -1;
        const bool unrelatedUnixOpen =
            fcntl(unrelatedUnixFds[0], F_GETFD) != -1
            && fcntl(unrelatedUnixFds[1], F_GETFD) != -1;
        const bool inetOpen = fcntl(inetFd, F_GETFD) != -1;
        const bool pipeOpen =
            fcntl(pipeFds[0], F_GETFD) != -1
            && fcntl(pipeFds[1], F_GETFD) != -1;

        _exit(closedCount == 2
                  && listenerClosed
                  && acceptedConnectionClosed
                  && clientOpen
                  && unrelatedUnixOpen
                  && inetOpen
                  && pipeOpen
              ? 0
              : 2);
    },
    ::testing::ExitedWithCode(0),
    "");
}

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
    for (int i = 0; i < over; i++)
        SwapDirtyTrack::unregisterPool(&poolBases[i]);

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
        while (!release.load(std::memory_order_acquire))
            std::this_thread::yield();
        forkCensusLeave();
    });
    while (!held.load(std::memory_order_acquire))
        std::this_thread::yield();

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
            while (!release.load(std::memory_order_acquire))
            std::this_thread::yield();
        });
        while (!held.load(std::memory_order_acquire))
        std::this_thread::yield();
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
            while (!release.load(std::memory_order_acquire))
            std::this_thread::yield();
        });
        while (!held.load(std::memory_order_acquire))
        std::this_thread::yield();
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
    for (char c : all)
    {
        if (c == '\n')
            lines++;
    }
    EXPECT_EQ(lines, 3);
    EXPECT_NE(all.find("tick=1001"), std::string::npos);
    EXPECT_NE(all.find("tick=1002"), std::string::npos);
    EXPECT_NE(all.find("tick=1003"), std::string::npos);
    EXPECT_NE(all.find("reason=census"), std::string::npos);
    EXPECT_NE(all.find("reason=park_timeout"), std::string::npos);

    // summary JSON reflects the counters
    std::string js = ForkStats::summaryJson();
    EXPECT_NE(js.find("\"forksSkippedTotal\""), std::string::npos);
    EXPECT_NE(js.find("\"quiesceTimeout\""), std::string::npos);
    EXPECT_NE(js.find("\"lastUnforkable\""), std::string::npos);

    std::filesystem::remove(ForkStats::kLogPath);
}

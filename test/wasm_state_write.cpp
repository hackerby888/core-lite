// A traced call maps contract state read-only and repairs each written page from the fault handler.
#ifdef LITE_WASM_SC

#include "platform/m256.h"
#include "extensions/wasm/runtime/state_write_tracker.h"
#include "gtest/gtest.h"

#include <array>
#include <cstring>
#include <optional>
#include <vector>
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace
{

size_t testPageSize()
{
#ifdef _WIN32
    SYSTEM_INFO systemInfo;
    GetSystemInfo(&systemInfo);
    return systemInfo.dwPageSize ? (size_t)systemInfo.dwPageSize : 4096;
#else
    const long pageSize = sysconf(_SC_PAGESIZE);
    return pageSize > 0 ? (size_t)pageSize : 4096;
#endif
}

unsigned char* allocateTestPages(size_t size)
{
#ifdef _WIN32
    return (unsigned char*)VirtualAlloc(nullptr, size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
#else
    void* memory = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    return memory == MAP_FAILED ? nullptr : (unsigned char*)memory;
#endif
}

void freeTestPages(unsigned char* memory, size_t size)
{
#ifdef _WIN32
    (void)size;
    VirtualFree(memory, 0, MEM_RELEASE);
#else
    munmap(memory, size);
#endif
}

void expectStateRegion(const Wasm::Runtime::TraceEntry& entry, size_t regionIndex, unsigned int offset, const std::vector<unsigned char>& before,
    const std::vector<unsigned char>& after)
{
    ASSERT_LT(regionIndex, entry.stateDiff.size());
    const Wasm::Runtime::StateRegionTrace& region = entry.stateDiff[regionIndex];

    EXPECT_EQ(region.offset, offset);
    EXPECT_EQ(region.before, Wasm::Runtime::hex(before.data(), (unsigned int)before.size()));
    EXPECT_EQ(region.after, Wasm::Runtime::hex(after.data(), (unsigned int)after.size()));
}

void expectSingleStateRegion(const Wasm::Runtime::TraceEntry& entry, unsigned int offset, const std::vector<unsigned char>& before,
    const std::vector<unsigned char>& after)
{
    ASSERT_EQ(entry.stateDiff.size(), 1u);
    expectStateRegion(entry, 0, offset, before, after);
}

} // namespace

TEST(WasmContracts, StateWriteFaultIsRepaired)
{
    const size_t pageSize = testPageSize();
    unsigned char* state = allocateTestPages(pageSize);
    ASSERT_NE(state, nullptr);
    memset(state, 0, pageSize);

    Wasm::Runtime::TraceEntry entry;
    ASSERT_TRUE(Wasm::Runtime::beginStateWriteTracking(state, (unsigned int)pageSize));
    state[8] = 0x42;
    Wasm::Runtime::finishStateWriteTracking(entry, state, (unsigned int)pageSize);

    EXPECT_EQ(state[8], 0x42);
    EXPECT_FALSE(entry.stateTruncated);
    ASSERT_FALSE(entry.stateDiff.empty());
    EXPECT_NE(entry.stateDiff[0].before, entry.stateDiff[0].after);

#ifndef _WIN32
    // Linux raises SIGSEGV for this mapping and Darwin SIGBUS. Both must stay routed to the tracker.
    for (int signalNumber : { SIGSEGV, SIGBUS })
    {
        struct sigaction installed;
        ASSERT_EQ(sigaction(signalNumber, nullptr, &installed), 0);
        EXPECT_EQ(installed.sa_sigaction, &Wasm::Runtime::handleStateWriteFault) << "signal " << signalNumber << " is not routed to the state-write handler";
    }
#endif

    freeTestPages(state, pageSize);
}

TEST(WasmContracts, StateWriteFinishWithoutWritesRestoresWritability)
{
    const size_t pageSize = testPageSize();
    unsigned char* state = allocateTestPages(pageSize);
    ASSERT_NE(state, nullptr);
    memset(state, 0, pageSize);

    Wasm::Runtime::TraceEntry entry;
    Wasm::Runtime::StateWriteScope scope(true, state, (unsigned int)WASM_TRACE_DIFF_WINDOW);
    ASSERT_TRUE(scope.engaged);

    scope.finish(entry);

    EXPECT_FALSE(entry.stateTruncated);
    EXPECT_TRUE(entry.stateDiff.empty());
    state[8] = 0x42;
    EXPECT_EQ(state[8], 0x42);

    freeTestPages(state, pageSize);
}

TEST(WasmContracts, NestedStateWriteScopesRestoreTheOuterTracker)
{
    const size_t pageSize = testPageSize();
    const size_t allocationSize = pageSize * 3;
    unsigned char* memory = allocateTestPages(allocationSize);
    ASSERT_NE(memory, nullptr);
    memset(memory, 0, allocationSize);

    Wasm::Runtime::TraceEntry outerEntry;
    Wasm::Runtime::TraceEntry innerEntry;
    Wasm::Runtime::StateWriteScope outerScope(true, memory, (unsigned int)(pageSize * 2));
    ASSERT_TRUE(outerScope.engaged);
    memory[8] = 0x11;

    Wasm::Runtime::StateWriteScope innerScope(true, memory + pageSize * 2, (unsigned int)pageSize);
    ASSERT_TRUE(innerScope.engaged);
    memory[pageSize * 2 + 8] = 0x22;
    innerScope.finish(innerEntry);

    // The inner scope must re-arm the parent; this write is the old crash path.
    memory[pageSize + 8] = 0x33;
    outerScope.finish(outerEntry);

    EXPECT_FALSE(innerEntry.stateTruncated);
    EXPECT_FALSE(outerEntry.stateTruncated);

    std::vector<unsigned char> innerBefore(WASM_TRACE_DIFF_WINDOW, 0);
    std::vector<unsigned char> innerAfter = innerBefore;
    innerAfter[8] = 0x22;
    expectSingleStateRegion(innerEntry, 0, innerBefore, innerAfter);

    ASSERT_EQ(outerEntry.stateDiff.size(), 2u);
    std::vector<unsigned char> outerBefore(WASM_TRACE_DIFF_WINDOW, 0);
    std::vector<unsigned char> outerFirstAfter = outerBefore;
    std::vector<unsigned char> outerSecondAfter = outerBefore;
    outerFirstAfter[8] = 0x11;
    outerSecondAfter[8] = 0x33;
    expectStateRegion(outerEntry, 0, 0, outerBefore, outerFirstAfter);
    expectStateRegion(outerEntry, 1, (unsigned int)pageSize, outerBefore, outerSecondAfter);

    memory[16] = 0x44;
    memory[pageSize + 16] = 0x55;
    memory[pageSize * 2 + 16] = 0x66;
    freeTestPages(memory, allocationSize);
}

TEST(WasmContracts, OverlappingUnalignedStateWriteScopesKeepBothDiffs)
{
    const size_t pageSize = testPageSize();
    unsigned char* page = allocateTestPages(pageSize);
    ASSERT_NE(page, nullptr);
    memset(page, 0, pageSize);

    unsigned char* outerState = page + 8;
    unsigned char* innerState = page + 16;
    Wasm::Runtime::TraceEntry outerEntry;
    Wasm::Runtime::TraceEntry innerEntry;
    Wasm::Runtime::StateWriteScope outerScope(true, outerState, (unsigned int)pageSize - 16);
    ASSERT_TRUE(outerScope.engaged);
    Wasm::Runtime::StateWriteScope innerScope(true, innerState, 128);
    ASSERT_TRUE(innerScope.engaged);

    page[24] = 0x77;
    innerScope.finish(innerEntry);
    page[40] = 0x88;
    outerScope.finish(outerEntry);

    EXPECT_FALSE(innerEntry.stateTruncated);
    EXPECT_FALSE(outerEntry.stateTruncated);

    std::vector<unsigned char> innerBefore(128, 0);
    std::vector<unsigned char> innerAfter = innerBefore;
    innerAfter[8] = 0x77;
    expectSingleStateRegion(innerEntry, 0, innerBefore, innerAfter);

    std::vector<unsigned char> outerBefore(WASM_TRACE_DIFF_WINDOW, 0);
    std::vector<unsigned char> outerAfter = outerBefore;
    outerAfter[16] = 0x77;
    outerAfter[32] = 0x88;
    expectSingleStateRegion(outerEntry, 0, outerBefore, outerAfter);

    page[48] = 0x99;
    freeTestPages(page, pageSize);
}

TEST(WasmContracts, StateWriteTracksUnalignedLogicalEdgesAcrossPages)
{
    const size_t pageSize = testPageSize();
    ASSERT_GT(pageSize, (size_t)WASM_TRACE_DIFF_WINDOW);
    const size_t allocationSize = pageSize * 3;
    unsigned char* memory = allocateTestPages(allocationSize);
    ASSERT_NE(memory, nullptr);
    memset(memory, 0, allocationSize);

    const size_t leadingPageBytes = 17;
    unsigned char* state = memory + pageSize - leadingPageBytes;
    const size_t stateSize = pageSize + leadingPageBytes * 2;
    Wasm::Runtime::TraceEntry entry;
    Wasm::Runtime::StateWriteScope scope(true, state, (unsigned int)stateSize);
    ASSERT_TRUE(scope.engaged);

    state[0] = 0x11;
    state[leadingPageBytes - 1] = 0x22;
    state[leadingPageBytes] = 0x33;
    state[stateSize - 1] = 0x44;
    scope.finish(entry);

    EXPECT_FALSE(entry.stateTruncated);
    ASSERT_EQ(entry.stateDiff.size(), 3u);

    std::vector<unsigned char> firstBefore(leadingPageBytes, 0);
    std::vector<unsigned char> firstAfter = firstBefore;
    firstAfter[0] = 0x11;
    firstAfter[leadingPageBytes - 1] = 0x22;
    expectStateRegion(entry, 0, 0, firstBefore, firstAfter);

    const size_t middleWindowSize = WASM_TRACE_DIFF_WINDOW - leadingPageBytes;
    std::vector<unsigned char> middleBefore(middleWindowSize, 0);
    std::vector<unsigned char> middleAfter = middleBefore;
    middleAfter[0] = 0x33;
    expectStateRegion(entry, 1, (unsigned int)leadingPageBytes, middleBefore, middleAfter);

    std::vector<unsigned char> lastBefore(leadingPageBytes, 0);
    std::vector<unsigned char> lastAfter = lastBefore;
    lastAfter[leadingPageBytes - 1] = 0x44;
    expectStateRegion(entry, 2, (unsigned int)(pageSize + leadingPageBytes), lastBefore, lastAfter);

    freeTestPages(memory, allocationSize);
}

TEST(WasmContracts, StateWriteIgnoresWritesOutsideTheLogicalRange)
{
    const size_t pageSize = testPageSize();
    unsigned char* page = allocateTestPages(pageSize);
    ASSERT_NE(page, nullptr);
    memset(page, 0, pageSize);

    unsigned char* state = page + 64;
    const unsigned int stateSize = 128;
    Wasm::Runtime::TraceEntry entry;
    Wasm::Runtime::StateWriteScope scope(true, state, stateSize);
    ASSERT_TRUE(scope.engaged);

    page[32] = 0x55;
    page[64 + stateSize + 32] = 0x66;
    state[8] = 0x77;
    scope.finish(entry);

    EXPECT_FALSE(entry.stateTruncated);
    std::vector<unsigned char> before(stateSize, 0);
    std::vector<unsigned char> after = before;
    after[8] = 0x77;
    expectSingleStateRegion(entry, 0, before, after);
    EXPECT_EQ(page[32], 0x55);
    EXPECT_EQ(page[64 + stateSize + 32], 0x66);

    freeTestPages(page, pageSize);
}

TEST(WasmContracts, StateWriteKeepsTheFirstSnapshotAndDropsRevertedBytes)
{
    const size_t pageSize = testPageSize();
    unsigned char* state = allocateTestPages(pageSize);
    ASSERT_NE(state, nullptr);
    memset(state, 0, pageSize);

    Wasm::Runtime::TraceEntry entry;
    Wasm::Runtime::StateWriteScope scope(true, state, (unsigned int)WASM_TRACE_DIFF_WINDOW);
    ASSERT_TRUE(scope.engaged);

    state[8] = 0x11;
    state[8] = 0x22;
    state[16] = 0x33;
    state[16] = 0;
    scope.finish(entry);

    std::vector<unsigned char> before(WASM_TRACE_DIFF_WINDOW, 0);
    std::vector<unsigned char> after = before;
    after[8] = 0x22;
    expectSingleStateRegion(entry, 0, before, after);

    freeTestPages(state, pageSize);
}

TEST(WasmContracts, FourNestedStateWriteScopesRestoreEveryParent)
{
    const size_t pageSize = testPageSize();
    const size_t allocationSize = pageSize * 4;
    unsigned char* memory = allocateTestPages(allocationSize);
    ASSERT_NE(memory, nullptr);
    memset(memory, 0, allocationSize);

    const unsigned int stateSize = (unsigned int)WASM_TRACE_DIFF_WINDOW;
    Wasm::Runtime::StateWriteScope outerScope(true, memory, stateSize);
    ASSERT_TRUE(outerScope.engaged);
    Wasm::Runtime::StateWriteScope secondScope(true, memory + pageSize, stateSize);
    ASSERT_TRUE(secondScope.engaged);
    Wasm::Runtime::StateWriteScope thirdScope(true, memory + pageSize * 2, stateSize);
    ASSERT_TRUE(thirdScope.engaged);
    Wasm::Runtime::StateWriteScope innerScope(true, memory + pageSize * 3, stateSize);
    ASSERT_TRUE(innerScope.engaged);

    std::array<Wasm::Runtime::TraceEntry, 4> entries;
    memory[pageSize * 3 + 8] = 0x44;
    innerScope.finish(entries[3]);
    memory[pageSize * 2 + 8] = 0x33;
    thirdScope.finish(entries[2]);
    memory[pageSize + 8] = 0x22;
    secondScope.finish(entries[1]);
    memory[8] = 0x11;
    outerScope.finish(entries[0]);

    std::vector<unsigned char> before(WASM_TRACE_DIFF_WINDOW, 0);
    for (size_t index = 0; index < entries.size(); index++)
    {
        std::vector<unsigned char> after = before;
        after[8] = (unsigned char)((index + 1) * 0x11);
        EXPECT_FALSE(entries[index].stateTruncated);
        expectSingleStateRegion(entries[index], 0, before, after);
        memory[pageSize * index + 16] = 0x55;
    }

    freeTestPages(memory, allocationSize);
}

TEST(WasmContracts, StateWriteScopeCapacityUnwindsAndAcceptsAFreshScope)
{
    const size_t pageSize = testPageSize();
    unsigned char* state = allocateTestPages(pageSize);
    ASSERT_NE(state, nullptr);
    memset(state, 0, pageSize);

    constexpr unsigned int frameCapacity = Wasm::Runtime::STATE_WRITE_FRAME_CAPACITY;
    const unsigned int stateSize = (unsigned int)WASM_TRACE_DIFF_WINDOW;
    std::array<
        std::optional<Wasm::Runtime::StateWriteScope>,
        frameCapacity + 1>
        scopes;

    for (unsigned int frameIndex = 0; frameIndex < frameCapacity; frameIndex++)
    {
        scopes[frameIndex].emplace(true, state, stateSize);
        ASSERT_TRUE(scopes[frameIndex]->engaged) << frameIndex;
        EXPECT_FALSE(scopes[frameIndex]->captureFailed) << frameIndex;
    }

    scopes[frameCapacity].emplace(true, state, stateSize);
    EXPECT_FALSE(scopes[frameCapacity]->engaged);
    EXPECT_TRUE(scopes[frameCapacity]->captureFailed);
    Wasm::Runtime::TraceEntry overflowEntry;
    scopes[frameCapacity]->finish(overflowEntry);
    EXPECT_TRUE(overflowEntry.stateTruncated);
    EXPECT_TRUE(overflowEntry.stateDiff.empty());

    state[8] = 0x5a;
    std::array<Wasm::Runtime::TraceEntry, frameCapacity> entries;
    std::vector<unsigned char> before(WASM_TRACE_DIFF_WINDOW, 0);
    std::vector<unsigned char> after = before;
    after[8] = 0x5a;
    for (unsigned int remaining = frameCapacity; remaining > 0; remaining--)
    {
        const unsigned int frameIndex = remaining - 1;
        scopes[frameIndex]->finish(entries[frameIndex]);
        EXPECT_FALSE(entries[frameIndex].stateTruncated) << frameIndex;
        expectSingleStateRegion(entries[frameIndex], 0, before, after);
    }

    state[16] = 0x6b;
    Wasm::Runtime::TraceEntry freshEntry;
    Wasm::Runtime::StateWriteScope freshScope(true, state, stateSize);
    ASSERT_TRUE(freshScope.engaged);
    state[24] = 0x7c;
    freshScope.finish(freshEntry);

    std::vector<unsigned char> freshBefore = after;
    freshBefore[16] = 0x6b;
    std::vector<unsigned char> freshAfter = freshBefore;
    freshAfter[24] = 0x7c;
    EXPECT_FALSE(freshEntry.stateTruncated);
    expectSingleStateRegion(freshEntry, 0, freshBefore, freshAfter);

    state[32] = 0x8d;
    EXPECT_EQ(state[32], 0x8d);
    freeTestPages(state, pageSize);
}

TEST(WasmContracts, StateWriteScopeDiscardRestoresTracking)
{
    const size_t pageSize = testPageSize();
    unsigned char* state = allocateTestPages(pageSize);
    ASSERT_NE(state, nullptr);
    memset(state, 0, pageSize);

    const unsigned int stateSize = (unsigned int)WASM_TRACE_DIFF_WINDOW;
    {
        Wasm::Runtime::StateWriteScope discardedScope(true, state, stateSize);
        ASSERT_TRUE(discardedScope.engaged);
        state[8] = 0x11;
    }

    state[16] = 0x22;
    Wasm::Runtime::TraceEntry freshEntry;
    Wasm::Runtime::StateWriteScope freshScope(true, state, stateSize);
    ASSERT_TRUE(freshScope.engaged);
    state[24] = 0x33;
    freshScope.finish(freshEntry);

    std::vector<unsigned char> before(WASM_TRACE_DIFF_WINDOW, 0);
    before[8] = 0x11;
    before[16] = 0x22;
    std::vector<unsigned char> after = before;
    after[24] = 0x33;
    EXPECT_FALSE(freshEntry.stateTruncated);
    expectSingleStateRegion(freshEntry, 0, before, after);

    state[32] = 0x44;
    EXPECT_EQ(state[32], 0x44);
    freeTestPages(state, pageSize);
}

#endif // LITE_WASM_SC

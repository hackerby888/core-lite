// A traced call maps contract state read-only and repairs each written page from the fault handler.
#ifdef LITE_WASM_SC

#include "platform/m256.h"
#include "extensions/wasm/runtime/state_write_tracker.h"
#include "gtest/gtest.h"

#include <cstring>
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
    return (unsigned char*)VirtualAlloc(
        nullptr,
        size,
        MEM_RESERVE | MEM_COMMIT,
        PAGE_READWRITE);
#else
    void* memory = mmap(
        nullptr,
        size,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANON,
        -1,
        0);
    return memory == MAP_FAILED ? nullptr : (unsigned char*)memory;
#endif
}

void freeTestPages(unsigned char* memory, size_t size)
{
#ifdef _WIN32
    VirtualFree(memory, 0, MEM_RELEASE);
#else
    munmap(memory, size);
#endif
}

bool diffContainsOffset(const Wasm::Runtime::TraceEntry& entry, unsigned int offset)
{
    for (const auto& region : entry.stateDiff)
    {
        if (offset >= region.offset
            && offset < region.offset + region.after.size() / 2)
        {
            return true;
        }
    }
    return false;
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
        EXPECT_EQ(installed.sa_sigaction, &Wasm::Runtime::handleStateWriteFault)
            << "signal " << signalNumber << " is not routed to the state-write handler";
    }
#endif

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
    ASSERT_TRUE(Wasm::Runtime::beginStateWriteTracking(memory, (unsigned int)(pageSize * 2)));
    memory[8] = 0x11;

    ASSERT_TRUE(Wasm::Runtime::beginStateWriteTracking(
        memory + pageSize * 2,
        (unsigned int)pageSize));
    memory[pageSize * 2 + 8] = 0x22;
    Wasm::Runtime::finishStateWriteTracking(
        innerEntry,
        memory + pageSize * 2,
        (unsigned int)pageSize);

    // The inner scope must re-arm the parent; this write is the old crash path.
    memory[pageSize + 8] = 0x33;
    Wasm::Runtime::finishStateWriteTracking(
        outerEntry,
        memory,
        (unsigned int)(pageSize * 2));

    EXPECT_FALSE(innerEntry.stateTruncated);
    EXPECT_FALSE(outerEntry.stateTruncated);
    EXPECT_TRUE(diffContainsOffset(innerEntry, 8));
    EXPECT_TRUE(diffContainsOffset(outerEntry, 8));
    EXPECT_TRUE(diffContainsOffset(outerEntry, (unsigned int)pageSize + 8));

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
    ASSERT_TRUE(Wasm::Runtime::beginStateWriteTracking(
        outerState,
        (unsigned int)pageSize - 16));
    ASSERT_TRUE(Wasm::Runtime::beginStateWriteTracking(innerState, 128));

    page[24] = 0x77;
    Wasm::Runtime::finishStateWriteTracking(innerEntry, innerState, 128);
    page[40] = 0x88;
    Wasm::Runtime::finishStateWriteTracking(
        outerEntry,
        outerState,
        (unsigned int)pageSize - 16);

    EXPECT_FALSE(innerEntry.stateTruncated);
    EXPECT_FALSE(outerEntry.stateTruncated);
    EXPECT_TRUE(diffContainsOffset(innerEntry, 8));
    EXPECT_TRUE(diffContainsOffset(outerEntry, 16));
    EXPECT_TRUE(diffContainsOffset(outerEntry, 32));

    page[48] = 0x99;
    freeTestPages(page, pageSize);
}

#endif // LITE_WASM_SC

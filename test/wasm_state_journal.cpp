// The state-write journal an instrumented contract carries, and its agreement with the page write
// tracker it is meant to replace.
#ifdef LITE_WASM_SC

#include "platform/m256.h"
#include "extensions/wasm/runtime/state_write_journal.h"
#include "extensions/wasm/runtime/state_write_tracker.h"
#include "gtest/gtest.h"
#include "wasm_export.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <utility>
#include <vector>
#ifdef _WIN32
#include <windows.h>
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace
{

using namespace Wasm::Runtime;

size_t journalTestPageSize()
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

unsigned char* allocateJournalTestPages(size_t size)
{
#ifdef _WIN32
    return (unsigned char*)VirtualAlloc(nullptr, size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE);
#else
    void* memory = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    return memory == MAP_FAILED ? nullptr : (unsigned char*)memory;
#endif
}

void freeJournalTestPages(unsigned char* memory, size_t size)
{
#ifdef _WIN32
    (void)size;
    VirtualFree(memory, 0, MEM_RELEASE);
#else
    munmap(memory, size);
#endif
}

unsigned int nextPowerOfTwo(unsigned int value)
{
    unsigned int slots = 1u;
    while (slots < value)
    {
        slots *= 2u;
    }
    return slots;
}

/** Writes the header the module's own `__q_journal_reset` would leave behind on first use. */
JournalHeader initJournal(unsigned char* memory, unsigned int base, unsigned int stateSize, unsigned int capacityBlocks)
{
    const unsigned int tableSlots = nextPowerOfTwo(capacityBlocks * 2u);
    const unsigned int journalBytes = JOURNAL_HEADER_BYTES + tableSlots * JOURNAL_SLOT_BYTES + capacityBlocks * JOURNAL_ENTRY_BYTES;
    memset(memory + base, 0, journalBytes);

    writeJournalU32(memory, base + JournalHeaderOffset::MAGIC, JOURNAL_MAGIC);
    writeJournalU32(memory, base + JournalHeaderOffset::VERSION, JOURNAL_FORMAT_VERSION);
    writeJournalU32(memory, base + JournalHeaderOffset::CAPACITY, capacityBlocks);
    writeJournalU32(memory, base + JournalHeaderOffset::STATE_SIZE, stateSize);
    writeJournalU32(memory, base + JournalHeaderOffset::TABLE_MASK, tableSlots - 1u);
    writeJournalU32(memory, base + JournalHeaderOffset::GENERATION, JOURNAL_FIRST_GENERATION);

    JournalHeader header;
    EXPECT_TRUE(readJournalHeader(memory, (size_t)base + journalBytes, base, header));
    return header;
}

/** One instrumented store: note the block first, then let the write land. */
void noteAndWrite(unsigned char* memory, unsigned int base, const JournalHeader& header, unsigned int stateAddr, unsigned int offset,
    const std::vector<unsigned char>& bytes)
{
    noteHostWrite(memory, base, header, stateAddr, stateAddr + offset, (unsigned int)bytes.size());
    memcpy(memory + stateAddr + offset, bytes.data(), bytes.size());
}

std::vector<unsigned char> filled(size_t size, unsigned char value)
{
    return std::vector<unsigned char>(size, value);
}

/**
 * Regions flattened to one entry per byte. The tracker walks page by page and never merges across a
 * page boundary, while the journal merges any adjacent blocks, so the two partition a contiguous run
 * differently while describing the very same bytes. Comparing per byte is what makes them comparable.
 */
std::map<unsigned int, std::pair<std::string, std::string>> changedBytes(const std::vector<StateRegionTrace>& regions)
{
    std::map<unsigned int, std::pair<std::string, std::string>> bytes;
    for (const StateRegionTrace& region : regions)
    {
        for (size_t index = 0u; index * 2u + 1u < region.before.size(); index++)
        {
            bytes[region.offset + (unsigned int)index] = {region.before.substr(index * 2u, 2u), region.after.substr(index * 2u, 2u)};
        }
    }
    return bytes;
}

} // namespace

TEST(WasmContracts, StateJournalHeaderReportsGeometryFromTheModule)
{
    std::vector<unsigned char> memory(64u * 1024u, 0);
    const JournalHeader header = initJournal(memory.data(), 4096u, 2048u, 8u);

    EXPECT_EQ(header.version, JOURNAL_FORMAT_VERSION);
    EXPECT_EQ(header.stateSize, 2048u);
    EXPECT_EQ(header.capacityBlocks, 8u);
    EXPECT_EQ(header.tableSlots, 16u);
    EXPECT_EQ(header.generation, JOURNAL_FIRST_GENERATION);
    EXPECT_EQ(header.entriesOffset, JOURNAL_HEADER_BYTES + 16u * JOURNAL_SLOT_BYTES);
    EXPECT_FALSE(header.overflowed);
}

TEST(WasmContracts, StateJournalIsIgnoredWithoutMagicOrAMatchingVersion)
{
    std::vector<unsigned char> memory(64u * 1024u, 0);
    JournalHeader header;

    EXPECT_FALSE(readJournalHeader(memory.data(), memory.size(), 4096u, header));

    initJournal(memory.data(), 4096u, 2048u, 8u);
    writeJournalU32(memory.data(), 4096u + JournalHeaderOffset::VERSION, JOURNAL_FORMAT_VERSION + 1u);
    EXPECT_FALSE(readJournalHeader(memory.data(), memory.size(), 4096u, header));
}

TEST(WasmContracts, StateJournalKeepsTheFirstBeforeImageOfARepeatedBlock)
{
    std::vector<unsigned char> memory(64u * 1024u, 0);
    const unsigned int base = 4096u;
    const JournalHeader header = initJournal(memory.data(), base, 2048u, 8u);

    noteAndWrite(memory.data(), base, header, 0u, 0u, filled(4u, 0x11));
    noteAndWrite(memory.data(), base, header, 0u, 8u, filled(4u, 0x22));

    JournalHeader after;
    ASSERT_TRUE(readJournalHeader(memory.data(), memory.size(), base, after));
    EXPECT_EQ(after.entryCount, 1u);

    std::vector<StateRegionTrace> regions;
    journalRegions(memory.data(), base, after, 0u, regions);
    ASSERT_EQ(regions.size(), 1u);
    EXPECT_EQ(regions[0].offset, 0u);
    // The before-image is the block as it stood before the first of the two writes.
    EXPECT_EQ(regions[0].before.substr(0, 24), std::string(24, '0'));
}

TEST(WasmContracts, StateJournalDropsBlocksWrittenBackToTheirOriginalBytes)
{
    std::vector<unsigned char> memory(64u * 1024u, 0);
    const unsigned int base = 4096u;
    const JournalHeader header = initJournal(memory.data(), base, 2048u, 8u);

    noteAndWrite(memory.data(), base, header, 0u, 0u, filled(4u, 0x11));
    noteAndWrite(memory.data(), base, header, 0u, 0u, filled(4u, 0x00));

    JournalHeader after;
    ASSERT_TRUE(readJournalHeader(memory.data(), memory.size(), base, after));
    EXPECT_EQ(after.entryCount, 1u);

    std::vector<StateRegionTrace> regions;
    journalRegions(memory.data(), base, after, 0u, regions);
    EXPECT_TRUE(regions.empty());
}

TEST(WasmContracts, StateJournalMergesAdjacentBlocksAndSeparatesDistantOnes)
{
    std::vector<unsigned char> memory(64u * 1024u, 0);
    const unsigned int base = 8192u;
    const JournalHeader header = initJournal(memory.data(), base, 2048u, 8u);

    noteAndWrite(memory.data(), base, header, 0u, JOURNAL_BLOCK_BYTES, filled(4u, 0x11));
    noteAndWrite(memory.data(), base, header, 0u, JOURNAL_BLOCK_BYTES * 2u, filled(4u, 0x22));
    noteAndWrite(memory.data(), base, header, 0u, JOURNAL_BLOCK_BYTES * 5u, filled(4u, 0x33));

    JournalHeader after;
    ASSERT_TRUE(readJournalHeader(memory.data(), memory.size(), base, after));

    std::vector<StateRegionTrace> regions;
    journalRegions(memory.data(), base, after, 0u, regions);
    ASSERT_EQ(regions.size(), 2u);
    EXPECT_EQ(regions[0].offset, JOURNAL_BLOCK_BYTES);
    EXPECT_EQ(regions[0].before.size(), (size_t)JOURNAL_BLOCK_BYTES * 2u * 2u);
    EXPECT_EQ(regions[1].offset, JOURNAL_BLOCK_BYTES * 5u);
}

TEST(WasmContracts, StateJournalFlagsOverflowOncePastCapacity)
{
    std::vector<unsigned char> memory(64u * 1024u, 0);
    const unsigned int base = 8192u;
    const JournalHeader header = initJournal(memory.data(), base, 4096u, 2u);

    for (unsigned int block = 0u; block < 4u; block++)
    {
        noteAndWrite(memory.data(), base, header, 0u, block * JOURNAL_BLOCK_BYTES, filled(4u, (unsigned char)(0x10 + block)));
    }

    JournalHeader after;
    ASSERT_TRUE(readJournalHeader(memory.data(), memory.size(), base, after));
    EXPECT_EQ(after.entryCount, 2u);
    EXPECT_TRUE(after.overflowed);
}

TEST(WasmContracts, StateJournalResetRetiresTheGenerationAndClearsOnWrap)
{
    std::vector<unsigned char> memory(64u * 1024u, 0);
    const unsigned int base = 4096u;
    JournalHeader header = initJournal(memory.data(), base, 2048u, 8u);

    noteAndWrite(memory.data(), base, header, 0u, 0u, filled(4u, 0x11));
    resetJournal(memory.data(), base, header);

    JournalHeader afterReset;
    ASSERT_TRUE(readJournalHeader(memory.data(), memory.size(), base, afterReset));
    EXPECT_EQ(afterReset.entryCount, 0u);
    EXPECT_EQ(afterReset.generation, JOURNAL_FIRST_GENERATION + 1u);

    // The block was recorded under the retired generation, so the next dispatch records it again.
    noteAndWrite(memory.data(), base, afterReset, 0u, 0u, filled(4u, 0x22));
    JournalHeader afterSecond;
    ASSERT_TRUE(readJournalHeader(memory.data(), memory.size(), base, afterSecond));
    EXPECT_EQ(afterSecond.entryCount, 1u);

    // Wrapping back onto the first generation must clear leftovers stamped with it.
    writeJournalU32(memory.data(), base + JournalHeaderOffset::GENERATION, 0xffffffffu);
    resetJournal(memory.data(), base, afterSecond);
    JournalHeader afterWrap;
    ASSERT_TRUE(readJournalHeader(memory.data(), memory.size(), base, afterWrap));
    EXPECT_EQ(afterWrap.generation, JOURNAL_FIRST_GENERATION);

    const unsigned int tableAt = base + JOURNAL_HEADER_BYTES;
    for (unsigned int slot = 0u; slot < afterWrap.tableSlots; slot++)
    {
        EXPECT_EQ(readJournalU32(memory.data(), tableAt + slot * JOURNAL_SLOT_BYTES), 0u);
    }
}

// The journal has to reproduce what the page tracker reports, region for region. The tracker clips to
// page boundaries and the journal to 256-byte blocks, so an unaligned state is where they could differ.
TEST(WasmContracts, StateJournalMatchesTheWriteTrackerRegions)
{
    const size_t pageSize = journalTestPageSize();
    const unsigned int stateSize = (unsigned int)(pageSize + 300u);
    const unsigned int journalBase = (unsigned int)(((stateSize + pageSize - 1u) / pageSize) * pageSize);
    const unsigned int capacityBlocks = 64u;
    const size_t total = journalBase + 64u * 1024u;

    unsigned char* memory = allocateJournalTestPages(total);
    ASSERT_NE(memory, nullptr);
    memset(memory, 0, total);

    const JournalHeader header = initJournal(memory, journalBase, stateSize, capacityBlocks);

    // Writes chosen to straddle a block boundary, a page boundary, and the short tail block.
    const std::vector<std::pair<unsigned int, unsigned char>> writes = {
        {0u, 0x11},
        {JOURNAL_BLOCK_BYTES - 2u, 0x22},
        {(unsigned int)pageSize - 3u, 0x33},
        {stateSize - 5u, 0x44},
    };

    ASSERT_TRUE(beginStateWriteTracking(memory, stateSize));
    for (const auto& write : writes)
    {
        noteAndWrite(memory, journalBase, header, 0u, write.first, filled(4u, write.second));
    }

    TraceEntry trackerEntry;
    finishStateWriteTracking(trackerEntry, memory, stateSize);

    JournalHeader after;
    ASSERT_TRUE(readJournalHeader(memory, total, journalBase, after));
    std::vector<StateRegionTrace> journalDiff;
    journalRegions(memory, journalBase, after, 0u, journalDiff);

    EXPECT_FALSE(trackerEntry.stateTruncated);
    // Grouping differs by construction — see changedBytes — so agreement is asserted on content.
    EXPECT_EQ(changedBytes(journalDiff), changedBytes(trackerEntry.stateDiff));

    freeJournalTestPages(memory, total);
}

// The dispatch site drives both through their scopes, so the scopes are what the node actually relies
// on: the journal retires a generation on entry and reports the same bytes the tracker does on exit.
TEST(WasmContracts, StateJournalScopeAgreesWithTheWriteScopeAcrossDispatches)
{
    const size_t pageSize = journalTestPageSize();
    const unsigned int stateSize = (unsigned int)(pageSize + 300u);
    const unsigned int journalBase = (unsigned int)(((stateSize + pageSize - 1u) / pageSize) * pageSize);
    const size_t total = journalBase + 64u * 1024u;

    unsigned char* memory = allocateJournalTestPages(total);
    ASSERT_NE(memory, nullptr);
    memset(memory, 0, total);

    const JournalHeader header = initJournal(memory, journalBase, stateSize, 64u);

    for (unsigned int dispatch = 0u; dispatch < 3u; dispatch++)
    {
        StateJournalScope journalScope(true, memory, journalBase, 0u, header);
        StateWriteScope writeScope(true, memory, stateSize);

        const unsigned char value = (unsigned char)(0x40 + dispatch);
        noteAndWrite(memory, journalBase, header, 0u, dispatch * 16u, filled(4u, value));
        noteAndWrite(memory, journalBase, header, 0u, (unsigned int)pageSize - 2u, filled(4u, value));

        TraceEntry entry;
        writeScope.finish(entry);

        std::vector<StateRegionTrace> journalDiff;
        ASSERT_TRUE(journalScope.finish(journalDiff)) << "dispatch " << dispatch;
        EXPECT_FALSE(entry.stateTruncated);
        EXPECT_EQ(changedBytes(journalDiff), changedBytes(entry.stateDiff)) << "dispatch " << dispatch;
    }

    freeJournalTestPages(memory, total);
}

// Past capacity the journal can only report truncation, which is what arms the tracker for the next call.
TEST(WasmContracts, StateJournalScopeReportsNothingOnceItOverflows)
{
    std::vector<unsigned char> memory(256u * 1024u, 0);
    const unsigned int base = 16384u;
    const JournalHeader header = initJournal(memory.data(), base, 8192u, 2u);

    StateJournalScope journalScope(true, memory.data(), base, 0u, header);
    for (unsigned int block = 0u; block < 4u; block++)
    {
        noteAndWrite(memory.data(), base, header, 0u, block * JOURNAL_BLOCK_BYTES, filled(4u, (unsigned char)(0x50 + block)));
    }

    std::vector<StateRegionTrace> regions;
    EXPECT_FALSE(journalScope.finish(regions));
    EXPECT_TRUE(regions.empty());
}

namespace
{

// Counter's dispatch only reaches these; WAMR leaves the rest of the lhost surface unlinked until called.
void stubVoidI(wasm_exec_env_t, uint32_t) {}
uint32_t stubAcquireScratch(wasm_exec_env_t, uint64_t, uint32_t)
{
    return 0u;
}
void stubReleaseScratch(wasm_exec_env_t, uint32_t) {}

} // namespace

/**
 * The journal against a real Qinit-built artifact: found where the node looks for it, and reporting the
 * same bytes the tracker does for the contract's own instrumented stores rather than synthetic writes.
 */
TEST(WasmContracts, StateJournalAttachesToARealArtifactAndAgreesOnItsStores)
{
    // Its own variable: QINIT_WASM already drives the cross-host test, which needs more setup.
    const char* path = getenv("QINIT_JOURNAL_WASM");
    if (!path)
    {
        GTEST_SKIP() << "set QINIT_JOURNAL_WASM to a qinit-built instrumented contract";
    }

    FILE* file = fopen(path, "rb");
    ASSERT_NE(file, nullptr) << "open " << path;
    fseek(file, 0, SEEK_END);
    const long length = ftell(file);
    fseek(file, 0, SEEK_SET);
    std::vector<unsigned char> moduleBytes((size_t)length);
    ASSERT_EQ(fread(moduleBytes.data(), 1, (size_t)length, file), (size_t)length);
    fclose(file);

    RuntimeInitArgs initArgs;
    memset(&initArgs, 0, sizeof initArgs);
    initArgs.mem_alloc_type = Alloc_With_System_Allocator;
    ASSERT_TRUE(wasm_runtime_full_init(&initArgs));

    static NativeSymbol lhostStubs[] = {
        {"beginFn", (void*)stubVoidI, "(i)", nullptr},
        {"endFn", (void*)stubVoidI, "(i)", nullptr},
        {"markDirty", (void*)stubVoidI, "(i)", nullptr},
        {"acquireScratch", (void*)stubAcquireScratch, "(Ii)i", nullptr},
        {"releaseScratch", (void*)stubReleaseScratch, "(i)", nullptr},
    };
    ASSERT_TRUE(wasm_runtime_register_natives("lhost", lhostStubs, 5));

    char error[256];
    wasm_module_t module = wasm_runtime_load(moduleBytes.data(), (uint32_t)length, error, sizeof error);
    ASSERT_NE(module, nullptr) << error;
    wasm_module_inst_t instance = wasm_runtime_instantiate(module, 256u * 1024u, 0u, error, sizeof error);
    ASSERT_NE(instance, nullptr) << error;
    wasm_exec_env_t execEnv = wasm_runtime_create_exec_env(instance, 256u * 1024u);
    ASSERT_NE(execEnv, nullptr);

    auto callExport = [&](const char* name, uint32_t* arguments, uint32_t count) -> uint32_t
    {
        wasm_function_inst_t function = wasm_runtime_lookup_function(instance, name);
        EXPECT_NE(function, nullptr) << name;
        EXPECT_TRUE(function && wasm_runtime_call_wasm(execEnv, function, count, arguments)) << name << ": " << wasm_runtime_get_exception(instance);
        return arguments[0];
    };

    uint32_t arguments[8] = {0};
    const uint32_t ioBase = callExport("io_base", arguments, 0);
    arguments[0] = 0;
    const uint32_t ioSize = callExport("io_size", arguments, 0);
    arguments[0] = 0;
    const uint32_t stateOffset = callExport("state_addr", arguments, 0);
    arguments[0] = 0;
    const uint32_t stateSize = callExport("state_size", arguments, 0);

    // The node derives the journal from its own carve constant, so this is where the two would drift.
    unsigned int journalBaseOffset = 0;
    JournalHeader header;
    ASSERT_TRUE(attachJournal(instance, execEnv, ioBase, ioSize, stateSize, journalBaseOffset, header)) << "no journal at io_base + io_size";
    EXPECT_EQ(journalBaseOffset, ioBase + ioSize);
    EXPECT_EQ(header.stateSize, stateSize);
    EXPECT_GT(header.capacityBlocks, 0u);

    unsigned char* memory = (unsigned char*)wasm_runtime_addr_app_to_native(instance, ioBase) - ioBase;
    unsigned char* state = memory + stateOffset;

    const uint32_t inputOffset = ioBase;
    const uint32_t outputOffset = ioBase + 64u * 1024u;
    const uint32_t localsOffset = outputOffset + 64u * 1024u;

    for (uint32_t call = 0u; call < 3u; call++)
    {
        StateJournalScope journalScope(true, memory, journalBaseOffset, stateOffset, header);
        StateWriteScope writeScope(true, state, stateSize);

        // Kind 1 is a user procedure; Counter's entry 1 increments its state.
        uint32_t dispatchArguments[5] = {1u, 1u, inputOffset, outputOffset, localsOffset};
        wasm_function_inst_t dispatch = wasm_runtime_lookup_function(instance, "dispatch");
        ASSERT_NE(dispatch, nullptr);
        ASSERT_TRUE(wasm_runtime_call_wasm(execEnv, dispatch, 5, dispatchArguments)) << wasm_runtime_get_exception(instance);

        TraceEntry entry;
        writeScope.finish(entry);

        std::vector<StateRegionTrace> journalDiff;
        ASSERT_TRUE(journalScope.finish(journalDiff)) << "call " << call;
        EXPECT_FALSE(entry.stateTruncated);
        EXPECT_FALSE(entry.stateDiff.empty()) << "call " << call << ": the contract wrote nothing";
        EXPECT_EQ(changedBytes(journalDiff), changedBytes(entry.stateDiff)) << "call " << call;
    }

    wasm_runtime_destroy_exec_env(execEnv);
    wasm_runtime_deinstantiate(instance);
    wasm_runtime_unload(module);
    wasm_runtime_destroy();
}

#endif // LITE_WASM_SC

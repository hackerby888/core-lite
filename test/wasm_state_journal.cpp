// The state-write journal an instrumented contract carries, and its agreement with the page write
// tracker it is meant to replace.
#ifdef LITE_WASM_SC

#include "platform/m256.h"
#include "extensions/wasm/runtime/state_write_journal.h"
#include "extensions/wasm/runtime/state_write_tracker.h"
#include "gtest/gtest.h"

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

#endif // LITE_WASM_SC

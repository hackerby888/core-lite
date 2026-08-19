#pragma once

// Reads the state-write journal an instrumented contract carries: the original bytes of every block it
// was first to overwrite, so a traced call reports what changed without keeping a copy of the state.
#ifdef LITE_WASM_SC

#include "extensions/wasm/runtime/trace.h"
#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

namespace Wasm::Runtime
{

// Mirror of packages/core/src/wasm/journal.ts in Qinit. The hash, probe order and insert must stay
// bit-identical: a host note has to land in the slot the module's own note would have taken.
static constexpr unsigned int JOURNAL_BLOCK_BYTES = 256u;
static constexpr unsigned int JOURNAL_BLOCK_SHIFT = 8u;
static constexpr unsigned int JOURNAL_MAGIC = 0x514a524eu; // "QJRN"
static constexpr unsigned int JOURNAL_FORMAT_VERSION = 2u;
static constexpr unsigned int JOURNAL_HEADER_BYTES = 32u;

// Probe-table slot: the generation that claimed it, then the block index it holds.
static constexpr unsigned int JOURNAL_SLOT_BYTES = 8u;
static constexpr unsigned int JOURNAL_SLOT_BLOCK_INDEX_OFFSET = 4u;

// Linear memory starts zeroed, so every slot reads as an older generation than the first one used.
static constexpr unsigned int JOURNAL_FIRST_GENERATION = 1u;

static constexpr unsigned int JOURNAL_ENTRY_DATA_OFFSET = 4u;
static constexpr unsigned int JOURNAL_ENTRY_BYTES = JOURNAL_BLOCK_BYTES + JOURNAL_ENTRY_DATA_OFFSET;
static constexpr unsigned int JOURNAL_OVERFLOW_FLAG = 1u;

// Knuth multiplicative hash, the same constant the module emits (-1640531527 as a signed i32).
static constexpr unsigned int JOURNAL_HASH_MULTIPLIER = 0x9e3779b9u;

struct JournalHeaderOffset
{
    static constexpr unsigned int MAGIC = 0u;
    static constexpr unsigned int VERSION = 4u;
    static constexpr unsigned int FLAGS = 8u;
    static constexpr unsigned int ENTRY_COUNT = 12u;
    static constexpr unsigned int CAPACITY = 16u;
    static constexpr unsigned int STATE_SIZE = 20u;
    static constexpr unsigned int TABLE_MASK = 24u;
    static constexpr unsigned int GENERATION = 28u;
};

struct JournalHeader
{
    unsigned int version = 0u;
    unsigned int flags = 0u;
    unsigned int entryCount = 0u;
    unsigned int capacityBlocks = 0u;
    unsigned int stateSize = 0u;
    unsigned int tableSlots = 0u;
    // Slots stamped with this value are live; anything else is a leftover and may be reused.
    unsigned int generation = 0u;
    // Where the undo entries start, relative to the journal base.
    unsigned int entriesOffset = 0u;
    bool overflowed = false;
};

// memcpy rather than a cast: the journal is byte-addressed and these offsets carry no alignment promise.
static inline unsigned int readJournalU32(const unsigned char* memory, unsigned int at)
{
    unsigned int value = 0u;
    memcpy(&value, memory + at, sizeof(value));
    return value;
}

static inline void writeJournalU32(unsigned char* memory, unsigned int at, unsigned int value)
{
    memcpy(memory + at, &value, sizeof(value));
}

/**
 * Reads the header at `base`, or false when the module carries no journal there. Every geometry value
 * comes from the header, never from build-time constants: the node reads artifacts whose journal
 * capacity it did not choose.
 */
static inline bool readJournalHeader(const unsigned char* memory, size_t memorySize, unsigned int base, JournalHeader& header)
{
    if (!memory || (size_t)base + JOURNAL_HEADER_BYTES > memorySize)
    {
        return false;
    }

    if (readJournalU32(memory, base + JournalHeaderOffset::MAGIC) != JOURNAL_MAGIC)
    {
        return false;
    }

    const unsigned int version = readJournalU32(memory, base + JournalHeaderOffset::VERSION);
    if (version != JOURNAL_FORMAT_VERSION)
    {
        return false;
    }

    const unsigned int tableSlots = readJournalU32(memory, base + JournalHeaderOffset::TABLE_MASK) + 1u;
    const unsigned int flags = readJournalU32(memory, base + JournalHeaderOffset::FLAGS);

    header.version = version;
    header.flags = flags;
    header.entryCount = readJournalU32(memory, base + JournalHeaderOffset::ENTRY_COUNT);
    header.capacityBlocks = readJournalU32(memory, base + JournalHeaderOffset::CAPACITY);
    header.stateSize = readJournalU32(memory, base + JournalHeaderOffset::STATE_SIZE);
    header.tableSlots = tableSlots;
    header.generation = readJournalU32(memory, base + JournalHeaderOffset::GENERATION);
    header.entriesOffset = JOURNAL_HEADER_BYTES + tableSlots * JOURNAL_SLOT_BYTES;
    header.overflowed = (flags & JOURNAL_OVERFLOW_FLAG) != 0u;
    return true;
}

/**
 * Starts the next dispatch empty by retiring the generation the probe table is stamped with, so the
 * table costs nothing to clear however large it is.
 */
static inline void resetJournal(unsigned char* memory, unsigned int base, const JournalHeader& header)
{
    writeJournalU32(memory, base + JournalHeaderOffset::FLAGS, 0u);
    writeJournalU32(memory, base + JournalHeaderOffset::ENTRY_COUNT, 0u);

    const unsigned int next = readJournalU32(memory, base + JournalHeaderOffset::GENERATION) + 1u;
    if (next == 0u)
    {
        // Wrapped: leftovers would read as live again, so this is the one dispatch that pays a clear.
        memset(memory + base + JOURNAL_HEADER_BYTES, 0, (size_t)header.tableSlots * JOURNAL_SLOT_BYTES);
        writeJournalU32(memory, base + JournalHeaderOffset::GENERATION, JOURNAL_FIRST_GENERATION);
        return;
    }

    writeJournalU32(memory, base + JournalHeaderOffset::GENERATION, next);
}

/**
 * Records a host write into guest memory, exactly as an instrumented store would. The module's own
 * stores are wrapped at build time, but a host write through an lhost out-pointer is invisible to them
 * and a contract may aim one at its state.
 */
static inline void noteHostWrite(
    unsigned char* memory, unsigned int base, const JournalHeader& header, unsigned int stateAddr, unsigned int address, unsigned int length)
{
    if (!length || address < stateAddr)
    {
        return;
    }

    const unsigned int relative = address - stateAddr;
    if (relative >= header.stateSize)
    {
        return;
    }

    const unsigned int lastByte = std::min(relative + length - 1u, header.stateSize - 1u);
    const unsigned int mask = header.tableSlots - 1u;
    const unsigned int tableAt = base + JOURNAL_HEADER_BYTES;
    const unsigned int entriesAt = base + header.entriesOffset;
    // Read live, not from the cached header: reset advances it between dispatches.
    const unsigned int generation = readJournalU32(memory, base + JournalHeaderOffset::GENERATION);

    for (unsigned int block = relative >> JOURNAL_BLOCK_SHIFT; block <= (lastByte >> JOURNAL_BLOCK_SHIFT); block++)
    {
        unsigned int slotIndex = (block * JOURNAL_HASH_MULTIPLIER) & mask;
        bool seen = false;

        for (;;)
        {
            const unsigned int slot = tableAt + slotIndex * JOURNAL_SLOT_BYTES;
            if (readJournalU32(memory, slot) != generation)
            {
                break;
            }
            if (readJournalU32(memory, slot + JOURNAL_SLOT_BLOCK_INDEX_OFFSET) == block)
            {
                seen = true;
                break;
            }
            slotIndex = (slotIndex + 1u) & mask;
        }

        if (seen)
        {
            continue;
        }

        const unsigned int count = readJournalU32(memory, base + JournalHeaderOffset::ENTRY_COUNT);
        if (count >= header.capacityBlocks)
        {
            writeJournalU32(memory, base + JournalHeaderOffset::FLAGS, readJournalU32(memory, base + JournalHeaderOffset::FLAGS) | JOURNAL_OVERFLOW_FLAG);
            return;
        }

        writeJournalU32(memory, tableAt + slotIndex * JOURNAL_SLOT_BYTES, generation);
        writeJournalU32(memory, tableAt + slotIndex * JOURNAL_SLOT_BYTES + JOURNAL_SLOT_BLOCK_INDEX_OFFSET, block);

        const unsigned int destination = entriesAt + count * JOURNAL_ENTRY_BYTES;
        const unsigned int offset = block * JOURNAL_BLOCK_BYTES;
        const unsigned int copyLength = std::min(JOURNAL_BLOCK_BYTES, header.stateSize - offset);
        writeJournalU32(memory, destination, block);
        memmove(memory + destination + JOURNAL_ENTRY_DATA_OFFSET, memory + stateAddr + offset, copyLength);
        writeJournalU32(memory, base + JournalHeaderOffset::ENTRY_COUNT, count + 1u);
    }
}

struct JournalChangedBlock
{
    unsigned int block;
    unsigned int offset;
    unsigned int length;
    const unsigned char* before;
};

/**
 * The regions this dispatch changed, in the shape the trace reports. Blocks the contract wrote without
 * changing are dropped before adjacent ones merge, so writing an identical value never widens its
 * neighbour's region.
 */
static inline void journalRegions(
    const unsigned char* memory, unsigned int base, const JournalHeader& header, unsigned int stateAddr, std::vector<StateRegionTrace>& regions)
{
    const unsigned int entriesAt = base + header.entriesOffset;
    std::vector<JournalChangedBlock> changed;
    changed.reserve(header.entryCount);

    for (unsigned int index = 0u; index < header.entryCount; index++)
    {
        const unsigned int at = entriesAt + index * JOURNAL_ENTRY_BYTES;
        const unsigned int block = readJournalU32(memory, at);
        const unsigned int offset = block * JOURNAL_BLOCK_BYTES;
        if (offset >= header.stateSize)
        {
            continue;
        }

        // The last block of a state is short whenever the state is not a multiple of the block.
        const unsigned int length = std::min(JOURNAL_BLOCK_BYTES, header.stateSize - offset);
        const unsigned char* before = memory + at + JOURNAL_ENTRY_DATA_OFFSET;
        if (memcmp(before, memory + stateAddr + offset, length) == 0)
        {
            continue;
        }

        changed.push_back({block, offset, length, before});
    }

    std::sort(changed.begin(), changed.end(), [](const JournalChangedBlock& left, const JournalChangedBlock& right) { return left.block < right.block; });

    for (size_t index = 0u; index < changed.size();)
    {
        size_t end = index;
        while (end + 1u < changed.size() && changed[end + 1u].block == changed[end].block + 1u)
        {
            end++;
        }

        std::string before;
        std::string after;
        for (size_t part = index; part <= end; part++)
        {
            before += hex(changed[part].before, changed[part].length);
            after += hex(memory + stateAddr + changed[part].offset, changed[part].length);
        }

        regions.push_back({changed[index].offset, before, after});
        index = end + 1u;
    }
}

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

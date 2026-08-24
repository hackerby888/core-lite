#pragma once

// Tick rollback for the ant colony, the counterpart of the miner-array and spectrum rollback the
// node already keeps for legacy solutions. Without it a re-run of a tick that carried ant solutions
// leaves the tree, the replay flags and the export set holding state from the discarded pass, while
// resourceTestingDigest is restored wholesale - a silent divergence from quorum.
//
// Scheduled for removal: fork()-based tick rollback makes every buffer below roll back with the
// process image. Set LITE_ANT_TICK_ROLLBACK to 0, confirm the suite is green, then delete this file,
// the friend declaration in ant_colony.h and the guarded call sites.

#ifndef LITE_ANT_TICK_ROLLBACK
#define LITE_ANT_TICK_ROLLBACK 1
#endif

#if LITE_ANT_TICK_ROLLBACK

struct AntColonyTickRollback
{
    typedef AntColonyBpp9000T Colony;

    // One entry per flag bit this tick set, so only bits we actually set are cleared. Both bits of a
    // solution are set unconditionally, and either may already belong to a different solution that
    // collided on it.
    struct FlagUndo
    {
        unsigned int index;
        bool wasSet;
    };

    bool armed;
    unsigned int solutionCount;
    unsigned int tickSlotIndex;
    bool tickSlotValid;
    AntTickSlot tickSlot;
    Colony::ExportSet exportSet;
    AntColonyDiagnostics stats;
    FlagUndo flagJournal[2 * NUMBER_OF_TRANSACTIONS_PER_TICK];
    unsigned int flagJournalCount;

    void disarm()
    {
        armed = false;
        flagJournalCount = 0;
    }

    // Call before any ant transaction of the tick is processed, and only for ticks that carry one.
    void capture(Colony& colony, unsigned int tick)
    {
        solutionCount = colony._solutionCount;

        tickSlotValid = colony.slotOf(tick, tickSlotIndex);
        if (tickSlotValid)
        {
            tickSlot = colony._tickIndex[tickSlotIndex];
        }

        copyMem(&exportSet, colony._exportSet, sizeof(exportSet));
        stats = colony._stats;

        flagJournalCount = 0;
        armed = true;
    }

    // Tick processor only: the journal is per-tick and unsynchronised.
    void noteFlagSet(unsigned int index, bool wasSet)
    {
        if (!armed || flagJournalCount >= (unsigned int)(sizeof(flagJournal) / sizeof(flagJournal[0])))
        {
            return;
        }
        flagJournal[flagJournalCount].index = index;
        flagJournal[flagJournalCount].wasSet = wasSet;
        flagJournalCount++;
    }

    void restore(Colony& colony, unsigned long long* antSolutionFlags)
    {
        if (!armed)
        {
            return;
        }

        // Reverse order: a later record's nextSiblingIdx names an earlier one, so the chain head has
        // to be put back newest-first.
        for (unsigned int i = colony._solutionCount; i-- > solutionCount; )
        {
            const AntSolutionRecord& rec = colony._records[i];

            const AntDedupKey dedupKey{ rec.pubkey, rec.nonce, rec.parentRef };
            colony._dedup->remove(dedupKey);

            LockGuard guard(colony._headMapLock);
            if (rec.parentRef.isRoot())
            {
                if (rec.nextSiblingIdx == NO_SIBLING)
                {
                    colony._childHeadByMiner->removeByKey(rec.pubkey);
                }
                else
                {
                    colony._childHeadByMiner->set(rec.pubkey, rec.nextSiblingIdx);
                }
            }
            else
            {
                if (rec.nextSiblingIdx == NO_SIBLING)
                {
                    colony._childHeadByParent->removeByKey(rec.parentRef);
                }
                else
                {
                    colony._childHeadByParent->set(rec.parentRef, rec.nextSiblingIdx);
                }
            }
        }

        // Records and networks past the count are unreachable: every reader gates on _solutionCount,
        // so truncating is the whole undo for them.
        colony._solutionCount = solutionCount;

        if (tickSlotValid)
        {
            colony._tickIndex[tickSlotIndex] = tickSlot;
        }

        // The anchor ring is deliberately not restored: it is K12(tick || K12(TickData)), which a
        // re-run does not change, and it is written after the capture point. Putting the pre-tick
        // value back would leave a hole for this tick that nothing re-records.
        copyMem(colony._exportSet, &exportSet, sizeof(exportSet));
        colony._stats = stats;

        for (unsigned int k = 0; k < flagJournalCount; k++)
        {
            if (!flagJournal[k].wasSet)
            {
                const unsigned int index = flagJournal[k].index;
                antSolutionFlags[index >> 6] &= ~(1ULL << (index & 63));
            }
        }

        disarm();
    }
};

static AntColonyTickRollback gAntTickRollback;

#endif // LITE_ANT_TICK_ROLLBACK

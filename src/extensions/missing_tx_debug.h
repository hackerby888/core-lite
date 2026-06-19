#pragma once

// Debug instrumentation for next-tick txs that stall tick advancement.
// Low-noise: only logs while a tx is actually missing (watch-list non-empty).

// Master toggle for all missing-tx logging.
static bool gLogMissingTx = true;

// Watch-list of next-tick tx digests currently missing from local storage.
static volatile char gMissingTxLock = 0;
static unsigned int gMissingTxTick = 0;
static unsigned int gMissingTxCount = 0;
static m256i gMissingTxDigests[NUMBER_OF_TRANSACTIONS_PER_TICK];

// Clears the watch-list when the next-tick target changes.
static void missingTxDebug_resetIfNewTick(unsigned int nextTick)
{
    if (gMissingTxTick != nextTick)
    {
        gMissingTxTick = nextTick;
        gMissingTxCount = 0;
    }
}

// Logs every still-missing next-tick tx and seeds the watch-list (deduped per tick).
// Call at end of prepareNextTickTransactions() with the final unknownTransactions bitmask.
static void missingTxDebug_reportMissingSet(unsigned int nextTick, const unsigned long long* unknownBitmask)
{
    if (!gLogMissingTx)
        return;
    LockGuard guard(gMissingTxLock);
    missingTxDebug_resetIfNewTick(nextTick);
    const unsigned int nextTickIndex = ts.tickToIndexCurrentEpoch(nextTick);
    const auto* offsets = ts.tickTransactionOffsets.getByTickIndex(nextTickIndex);
    for (unsigned int i = 0; i < NUMBER_OF_TRANSACTIONS_PER_TICK; i++)
    {
        if (!(unknownBitmask[i >> 6] & (1ULL << (i & 63))))
            continue;
        const m256i& expected = nextTickData.transactionDigests[i];

        bool seen = false;
        for (unsigned int k = 0; k < gMissingTxCount; k++)
            if (gMissingTxDigests[k] == expected) { seen = true; break; }
        if (seen)
            continue;
        if (gMissingTxCount < NUMBER_OF_TRANSACTIONS_PER_TICK)
            gMissingTxDigests[gMissingTxCount++] = expected;

        CHAR16 expChars[60 + 1];
        getIdentity((const unsigned char*)&expected, expChars, true);
        CHAR16 msg[320];
        setText(msg, L"[missing-tx] tick ");
        appendNumber(msg, nextTick, FALSE);
        appendText(msg, L" idx ");
        appendNumber(msg, i, FALSE);
        appendText(msg, L" expect ");
        appendText(msg, expChars);

        ts.tickTransactions.acquireLock();
        const auto off = offsets[i];
        if (off)
        {
            const Transaction* stored = ts.tickTransactions(off);
            unsigned char storedDigest[32];
            KangarooTwelve(stored, stored->totalSize(), storedDigest, sizeof(storedDigest));
            const unsigned int storedTick = stored->tick;
            ts.tickTransactions.releaseLock();
            CHAR16 storedChars[60 + 1];
            getIdentity(storedDigest, storedChars, true);
            appendText(msg, L" -> WRONG TX squats slot, stored ");
            appendText(msg, storedChars);
            appendText(msg, L" storedTick ");
            appendNumber(msg, storedTick, FALSE);
            appendText(msg, L" (broadcast cannot overwrite; needs flush)");
        }
        else
        {
            ts.tickTransactions.releaseLock();
            appendText(msg, L" -> ABSENT (no tx stored, not in pendingPool)");
        }
        logToConsole(msg);
    }
}

// Logs receipt of a broadcast tx whose digest is on the missing watch-list, with its storage outcome.
// Call from processBroadcastTransaction() after the tickData-slot match attempt.
static void missingTxDebug_onBroadcast(unsigned int tick, const m256i& digest, int matchedSlot, bool storedNow, bool storageFull)
{
    if (!gLogMissingTx)
        return;
    LockGuard guard(gMissingTxLock);
    if (gMissingTxCount == 0)
        return;
    bool watched = false;
    for (unsigned int k = 0; k < gMissingTxCount; k++)
        if (gMissingTxDigests[k] == digest) { watched = true; break; }
    if (!watched)
        return;

    CHAR16 idChars[60 + 1];
    getIdentity((const unsigned char*)&digest, idChars, true);
    CHAR16 msg[256];
    setText(msg, L"[missing-tx] tick ");
    appendNumber(msg, tick, FALSE);
    appendText(msg, L" RECEIVED broadcast digest ");
    appendText(msg, idChars);
    if (matchedSlot < 0)
    {
        appendText(msg, L" -> NO matching tickData slot");
    }
    else
    {
        appendText(msg, L" matched slot ");
        appendNumber(msg, (unsigned int)matchedSlot, FALSE);
        if (storedNow)
            appendText(msg, L" -> STORED to tickTx");
        else if (storageFull)
            appendText(msg, L" -> NOT stored (tickTx storage full)");
        else
            appendText(msg, L" -> already in tickTx");
    }
    logToConsole(msg);
}

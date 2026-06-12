#pragma once

// =====================================================================
// Lite-only bulk catch-up over P2P (Qubic TCP), message types 240/241.
//
// A node that is far behind asks a peer for a whole RANGE of ticks in one
// request; the peer streams every tick's data (tickData + votes + txs) back
// as a chunk of standard-framed sub-messages. The requester re-feeds each
// sub-frame to the existing signature-verifying handlers, so no consensus
// rule is bypassed and a malicious peer cannot inject unverified state.
//
// Replaces the per-tick +1/+2 (and fork +20 prefetch) request pattern, which
// caps catch-up at the request pacing rather than the link/CPU. Fork-owned,
// 240/241 are unused upstream so a UEFI node that receives a stray 240 simply
// has no switch case and drops it (nonzero dejavu => no re-dissemination).
//
// Wiring (qubic.cpp): #include after the broadcast handlers + ts; one init()
// at startup; two switch cases (240 -> processRequest, 241 -> onRespondChunk);
// one kicker() call in the main loop's request block.
// =====================================================================

namespace LiteBulkCatchup
{
constexpr unsigned char REQUEST_TYPE = 240;
constexpr unsigned char RESPOND_TYPE = 241;
constexpr unsigned int  VERSION       = 1;

// Responder clamp. <= RequestResponseHeader::max_size (16MB-1); bounds the
// per-peer transmit-buffer copy and the receive-side compaction cost.
constexpr unsigned int  CHUNK_BYTES_MAX = 4u * 1024 * 1024;

// Requester activates when this far behind the network tip, deactivates below the low mark.
constexpr unsigned int  ACTIVATE_BEHIND   = 100;
constexpr unsigned int  DEACTIVATE_BEHIND = 30;
// How far ahead of system.tick a single request may span (bounded so storage stays sane).
constexpr unsigned int  REQUEST_SPAN      = 512;
// Resend an unanswered chunk request after this long (seconds * frequency applied by caller).
constexpr unsigned long long CHUNK_TIMEOUT_SECS = 8;

#pragma pack(push, 1)
struct RequestTickRangeChunk
{
    static constexpr unsigned char type() { return REQUEST_TYPE; }
    unsigned char  version;
    unsigned int   startTick;
    unsigned int   maxBytes;
};
static_assert(sizeof(RequestTickRangeChunk) == 9, "RequestTickRangeChunk layout drifted");

struct RespondTickRangeChunkHeader
{
    static constexpr unsigned char type() { return RESPOND_TYPE; }
    unsigned int   startTick;            // first tick this chunk tried to cover
    unsigned int   endTickFullyIncluded; // last tick fully present; < startTick => responder lacks startTick or it alone exceeds the cap
    unsigned int   flags;                // reserved
    // followed by a sequence of standard RequestResponseHeader-framed sub-messages
};
static_assert(sizeof(RespondTickRangeChunkHeader) == 12, "RespondTickRangeChunkHeader layout drifted");
#pragma pack(pop)

// ---- counters (printed on the status line) ----
static volatile long long gChunksServed = 0, gChunksReceived = 0, gBulkTicksApplied = 0;

// ---- bulk sub-frame work queue ----
// onRespondChunk copies a received chunk's sub-frames here (fast); the requestProcessor pool drains
// and verifies them in parallel, so a chunk's ~thousands of vote/tx signature checks don't serialize
// on the single thread that received the chunk. Variable-size byte ring, mirrors requestQueueBuffer.
#define BULK_QUEUE_BUF_BYTES (256u * 1024 * 1024)
#define BULK_QUEUE_HEADROOM  (MAX_MESSAGE_PAYLOAD_SIZE + (unsigned int)sizeof(RequestResponseHeader))
#define BULK_QUEUE_ELEMS     (1u << 16)
static char*        gBulkQueueBuf = nullptr;
static unsigned int gBulkElemOffset[BULK_QUEUE_ELEMS];
static volatile unsigned int gBulkElemHead = 0, gBulkElemTail = 0;
static volatile unsigned int gBulkBufHead = 0, gBulkBufTail = 0;
static volatile char gBulkProducerLock = 0, gBulkConsumerLock = 0;
static Peer gBulkDummyPeer;   // sub-frames applied from a chunk have no originating request peer
static void storeBulkTransaction(const Transaction* request);  // defined below

// =====================================================================
// Responder (stateless). One scratch buffer per request processor so concurrent
// serves don't collide. Sized for the header + cap + one oversized sub-frame.
// =====================================================================
static char* gChunkBuf[MAX_NUMBER_OF_PROCESSORS] = { 0 };

static bool init()
{
    for (unsigned int i = 0; i < MAX_NUMBER_OF_PROCESSORS; i++)
    {
        if (!allocPoolWithErrorLog(L"LiteBulkCatchup::chunkBuf",
                CHUNK_BYTES_MAX + MAX_MESSAGE_PAYLOAD_SIZE + sizeof(RequestResponseHeader),
                (void**)&gChunkBuf[i], __LINE__))
            return false;
    }
    if (!allocPoolWithErrorLog(L"LiteBulkCatchup::bulkQueueBuf", BULK_QUEUE_BUF_BYTES, (void**)&gBulkQueueBuf, __LINE__))
        return false;
    setMem(&gBulkDummyPeer, sizeof(gBulkDummyPeer), 0);
    return true;
}

// Copy one sub-frame into the work queue (called by onRespondChunk; multi-producer safe). Drops on full.
static void bulkEnqueue(const RequestResponseHeader* sub)
{
    const unsigned int sz = sub->size();
    if (sz < sizeof(RequestResponseHeader) || sz > BULK_QUEUE_HEADROOM) return;
    ACQUIRE(gBulkProducerLock);
    if ((gBulkBufHead >= gBulkBufTail || gBulkBufHead + sz < gBulkBufTail)
        && ((gBulkElemHead + 1) & (BULK_QUEUE_ELEMS - 1)) != gBulkElemTail)
    {
        gBulkElemOffset[gBulkElemHead] = gBulkBufHead;
        copyMem(gBulkQueueBuf + gBulkBufHead, sub, sz);
        gBulkBufHead += sz;
        if (gBulkBufHead > BULK_QUEUE_BUF_BYTES - BULK_QUEUE_HEADROOM) gBulkBufHead = 0;
        gBulkElemHead = (gBulkElemHead + 1) & (BULK_QUEUE_ELEMS - 1);
    }
    RELEASE(gBulkProducerLock);
}

// Drain + apply one sub-frame (called by the requestProcessor pool; multi-consumer safe). `scratch`
// is a per-processor buffer >= one sub-frame. Returns false if the queue is empty.
static bool bulkProcessOne(char* scratch)
{
    ACQUIRE(gBulkConsumerLock);
    if (gBulkElemTail == gBulkElemHead) { RELEASE(gBulkConsumerLock); return false; }
    const RequestResponseHeader* sub = (const RequestResponseHeader*)(gBulkQueueBuf + gBulkElemOffset[gBulkElemTail]);
    const unsigned int sz = sub->size();
    copyMem(scratch, sub, sz);                // copy out so the dispatch runs unlocked
    gBulkBufTail += sz;
    if (gBulkBufTail > BULK_QUEUE_BUF_BYTES - BULK_QUEUE_HEADROOM) gBulkBufTail = 0;
    gBulkElemTail = (gBulkElemTail + 1) & (BULK_QUEUE_ELEMS - 1);
    RELEASE(gBulkConsumerLock);

    RequestResponseHeader* h = (RequestResponseHeader*)scratch;
    switch (h->type())
    {
    case BroadcastFutureTickData::type(): processBroadcastFutureTickData(&gBulkDummyPeer, h); break;
    case BroadcastTick::type():           processBroadcastTick(&gBulkDummyPeer, h);           break;
    case BROADCAST_TRANSACTION:
    {
        Transaction* tx = h->getPayload<Transaction>();
        if (tx->checkValidity() && tx->totalSize() == h->size() - sizeof(RequestResponseHeader))
        {
            unsigned char d[32];
            KangarooTwelve(tx, tx->totalSize() - SIGNATURE_SIZE, d, sizeof(d));
            if (verify(tx->sourcePublicKey.m256i_u8, d, tx->signaturePtr()))
                storeBulkTransaction(tx);
        }
        break;
    }
    default: break;
    }
    _InterlockedIncrement64(&gBulkTicksApplied);
    return true;
}

// Append one standard-framed sub-message into buf at cursor. dejavu is the chunk's
// (nonzero) dejavu so the existing handlers treat it as already-seen (no re-broadcast).
static inline void appendSubFrame(char* buf, unsigned int& cursor, unsigned char type,
                                  unsigned int dejavu, const void* payload, unsigned int payloadSize)
{
    RequestResponseHeader* h = (RequestResponseHeader*)(buf + cursor);
    h->checkAndSetSize(sizeof(RequestResponseHeader) + payloadSize);
    h->setType(type);
    h->setDejavu(dejavu);
    copyMem(buf + cursor + sizeof(RequestResponseHeader), payload, payloadSize);
    cursor += sizeof(RequestResponseHeader) + payloadSize;
}

// Append every stored sub-frame for tick t (tickData, then votes, then txs) into buf.
// Returns false (and leaves cursor untouched) if t is not in storage at all.
// Returns 0 = tick not in storage; 1 = tick fully appended; 2 = tick does not fit in the
// remaining cap (everything appended for it is rolled back, so a partial tick is never shipped).
static int appendTick(char* buf, unsigned int& cursor, unsigned int t, unsigned int dejavu, unsigned int cap)
{
    unsigned short epoch = 0;
    bool current = false;
    if (ts.tickInCurrentEpochStorage(t)) { epoch = system.epoch; current = true; }
    else if (ts.tickInPreviousEpochStorage(t)) { epoch = system.epoch - 1; current = false; }
    else return 0;

    const unsigned int tickStart = cursor;
    // Each sub-frame is header + payload; refuse to write past cap (gChunkBuf has one extra max frame of headroom).
    #define BULK_FITS(sz) (cursor + sizeof(RequestResponseHeader) + (unsigned int)(sz) <= cap)

    // tickData
    ts.tickData.acquireLock();
    const TickData* td = current ? &ts.tickData.getByTickInCurrentEpoch(t)
                                 : &ts.tickData.getByTickInPreviousEpoch(t);
    if (td->epoch == epoch)
    {
        if (!BULK_FITS(sizeof(TickData))) { ts.tickData.releaseLock(); cursor = tickStart; return 2; }
        appendSubFrame(buf, cursor, BroadcastFutureTickData::type(), dejavu, td, sizeof(TickData));
    }
    ts.tickData.releaseLock();

    // votes (one Tick per computor that voted)
    const Tick* tsTicks = current ? ts.ticks.getByTickInCurrentEpoch(t)
                                  : ts.ticks.getByTickInPreviousEpoch(t);
    for (unsigned int ci = 0; ci < NUMBER_OF_COMPUTORS; ci++)
    {
        if (tsTicks[ci].epoch == epoch)
        {
            if (!BULK_FITS(sizeof(Tick))) { cursor = tickStart; return 2; }
            ts.ticks.acquireLock(ci);
            if (tsTicks[ci].epoch == epoch)
                appendSubFrame(buf, cursor, BroadcastTick::type(), dejavu, &tsTicks[ci], sizeof(Tick));
            ts.ticks.releaseLock(ci);
        }
    }

    // transactions
    const unsigned long long* offs = current ? ts.tickTransactionOffsets.getByTickInCurrentEpoch(t)
                                             : ts.tickTransactionOffsets.getByTickInPreviousEpoch(t);
    for (unsigned int i = 0; i < NUMBER_OF_TRANSACTIONS_PER_TICK; i++)
    {
        const unsigned long long off = offs[i];
        if (off)
        {
            const Transaction* tx = ts.tickTransactions(off);
            if (tx->tick == t && tx->checkValidity())
            {
                const unsigned int sz = tx->totalSize();
                if (!BULK_FITS(sz)) { cursor = tickStart; return 2; }
                appendSubFrame(buf, cursor, BROADCAST_TRANSACTION, dejavu, tx, sz);
            }
        }
    }
    #undef BULK_FITS
    return 1;
}

static void processRequest(Peer* peer, RequestResponseHeader* header, unsigned long long processorNumber)
{
    if (!header->checkPayloadSize(sizeof(RequestTickRangeChunk)))
        return;
    const RequestTickRangeChunk* req = header->getPayload<RequestTickRangeChunk>();
    const unsigned int dejavu = header->dejavu();
    if (req->version != VERSION)
    {
        enqueueResponse(peer, 0, EndResponse::type(), dejavu, NULL);
        return;
    }

    char* buf = gChunkBuf[processorNumber];
    unsigned int cap = req->maxBytes < CHUNK_BYTES_MAX ? req->maxBytes : CHUNK_BYTES_MAX;
    if (cap < sizeof(RespondTickRangeChunkHeader) + MAX_MESSAGE_PAYLOAD_SIZE)
        cap = sizeof(RespondTickRangeChunkHeader) + MAX_MESSAGE_PAYLOAD_SIZE;

    unsigned int cursor = sizeof(RespondTickRangeChunkHeader);
    const unsigned int startTick = req->startTick;
    unsigned int endIncluded = startTick - 1;

    for (unsigned int t = startTick; t < startTick + REQUEST_SPAN; t++)
    {
        const int r = appendTick(buf, cursor, t, dejavu, cap);
        if (r != 1) break;               // 0 = no more ticks in storage; 2 = won't fit (already rolled back)
        endIncluded = t;
    }

    RespondTickRangeChunkHeader* rh = (RespondTickRangeChunkHeader*)buf;
    rh->startTick = startTick;
    rh->endTickFullyIncluded = endIncluded;
    rh->flags = 0;

    enqueueResponse(peer, cursor, RESPOND_TYPE, dejavu, buf);
    _InterlockedIncrement64(&gChunksServed);
}

// =====================================================================
// Direct-store for a bulk transaction. Mirrors the qubic.cpp store path used
// for system.tick+1 txs, minus the +1 gate: a bulk chunk delivers txs for ticks
// far ahead, whose tickData (with the matching digest) is in the same chunk
// ordered before them. Only stores if a slot's digest matches and is empty.
// =====================================================================
static void storeBulkTransaction(const Transaction* request)
{
    const unsigned int transactionSize = request->totalSize();
    const unsigned int t = request->tick;
    if (!ts.tickInCurrentEpochStorage(t))
        return;

    unsigned char digest[32];
    KangarooTwelve(request, transactionSize, digest, sizeof(digest));

    const unsigned int tickIndex = ts.tickToIndexCurrentEpoch(t);
    ts.tickData.acquireLock();
    const bool hasTickData = (ts.tickData[tickIndex].epoch == system.epoch);
    if (hasTickData)
    {
        auto* offsets = ts.tickTransactionOffsets.getByTickIndex(tickIndex);
        for (unsigned int i = 0; i < NUMBER_OF_TRANSACTIONS_PER_TICK; i++)
        {
            if (ts.tickData[tickIndex].transactionDigests[i] == *(const m256i*)digest)
            {
                ts.tickTransactions.acquireLock();
                if (!offsets[i] && ts.nextTickTransactionOffset + transactionSize <= ts.tickTransactions.storageSpaceCurrentEpoch)
                {
                    offsets[i] = ts.nextTickTransactionOffset;
                    copyMem(ts.tickTransactions(ts.nextTickTransactionOffset), request, transactionSize);
                    ts.nextTickTransactionOffset += transactionSize;
                }
                ts.tickTransactions.releaseLock();
                break;
            }
        }
    }
    ts.tickData.releaseLock();
}

// =====================================================================
// Requester. Sequential single-outstanding-chunk pull from one capable peer.
// (Striping across peers is a later optimization; this already removes the
// request-pacing cap.) Runs only on AUX while far behind.
// =====================================================================
static bool          gActive       = false;
static unsigned int  gReqFrontier  = 0;             // next tick to request
static unsigned int  gReqDejavu    = 0;
static unsigned int  gAvgChunkTicks = 32;           // running estimate of ticks delivered per chunk
static unsigned long long gLastProbeTsc = 0;
constexpr unsigned int BULK_MAX_AHEAD = 8000;       // don't fetch more than this far past system.tick
constexpr unsigned long long PROBE_INTERVAL_SECS = 2;

// Capable peers (run the 240/241 protocol); striped across so a behind node pulls from several
// archives and the responders' pools serve in parallel.
constexpr int BULK_MAX_CAPABLE = 16;
static unsigned int gCapablePeers[BULK_MAX_CAPABLE];
static int gCapableCount = 0;
static unsigned int gRrCursor = 0;

// Concurrent in-flight chunk requests so the responder pool(s) serve them in parallel.
constexpr int BULK_INFLIGHT = 8;
struct ReqSlot { bool active; unsigned int startTick; unsigned long long tsc; };
static ReqSlot gSlots[BULK_INFLIGHT];
static volatile char gSlotsLock = 0;

static void addCapablePeer(unsigned int addr)
{
    for (int i = 0; i < gCapableCount; i++) if (gCapablePeers[i] == addr) return;
    if (gCapableCount < BULK_MAX_CAPABLE) gCapablePeers[gCapableCount++] = addr;
}

// Highest tick any connected peer reports (network tip estimate).
static unsigned int networkTip()
{
    unsigned int tip = 0;
    for (unsigned int i = 0; i < NUMBER_OF_OUTGOING_CONNECTIONS + NUMBER_OF_INCOMING_CONNECTIONS; i++)
    {
        if (peers[i].tcp4Protocol && peers[i].isConnectedAccepted && !peers[i].isClosing
            && peers[i].peerReportedTick > tip)
            tip = peers[i].peerReportedTick;
    }
    return tip;
}

// Find a connected peer with the given address.u32, or -1.
static int peerIndexForAddr(unsigned int addr)
{
    for (unsigned int i = 0; i < NUMBER_OF_OUTGOING_CONNECTIONS + NUMBER_OF_INCOMING_CONNECTIONS; i++)
        if (peers[i].tcp4Protocol && peers[i].isConnectedAccepted && !peers[i].isClosing
            && peers[i].address.u32 == addr)
            return (int)i;
    return -1;
}

// Send a tiny bulk request to every connected peer to discover which ones run the protocol.
// A non-lite peer has no 240 handler and silently drops it; the first 241 reply marks the peer capable.
static void probeAllPeers(unsigned long long nowTsc)
{
    _rdrand32_step(&gReqDejavu);
    if (!gReqDejavu) gReqDejavu = 1;
    RequestTickRangeChunk req;
    req.version = VERSION;
    req.startTick = system.tick + 1;
    req.maxBytes = 64 * 1024;
    for (unsigned int i = 0; i < NUMBER_OF_OUTGOING_CONNECTIONS + NUMBER_OF_INCOMING_CONNECTIONS; i++)
        if (peers[i].tcp4Protocol && peers[i].isConnectedAccepted && !peers[i].isClosing
            && (i < NUMBER_OF_OUTGOING_CONNECTIONS || peers[i].exchangedPublicPeers))
            enqueueResponse(&peers[i], sizeof(req), REQUEST_TYPE, gReqDejavu, &req);
    gLastProbeTsc = nowTsc;
}

// Single-outstanding contiguous pull: one request in flight, frontier advances ONLY by each
// response's confirmed end tick. An optimistic multi-slot stride is unusable here because tick
// size varies ~100x (empty/void vs full-tx ticks) so a guessed stride overshoots dense regions
// and leaves gaps the ticker stalls on. 4MB/chunk per RTT is ample bandwidth for the target rate;
// provider-side parallelism, if needed, must come from gap-free fixed-count requests, not a guess.
static void fillSlots(unsigned long long nowTsc, unsigned long long freq)
{
    if (gCapableCount == 0) return;
    const unsigned int queued = (gBulkElemHead - gBulkElemTail) & (BULK_QUEUE_ELEMS - 1);
    ACQUIRE(gSlotsLock);
    if (gSlots[0].active && (nowTsc - gSlots[0].tsc) > CHUNK_TIMEOUT_SECS * freq)
        gSlots[0].active = false;                           // timed out -> reissue
    if (!gSlots[0].active
        && queued <= BULK_QUEUE_ELEMS * 3 / 4               // work queue has room
        && gReqFrontier <= system.tick + BULK_MAX_AHEAD)    // don't run too far ahead of the ticker
    {
        int pi = -1;
        for (int tries = 0; tries < gCapableCount && pi < 0; tries++)
            pi = peerIndexForAddr(gCapablePeers[(gRrCursor++) % gCapableCount]);
        if (pi < 0) gCapableCount = 0;                      // all capable peers gone; re-discover
        else
        {
            RequestTickRangeChunk req;
            req.version = VERSION;
            req.startTick = gReqFrontier;
            req.maxBytes = CHUNK_BYTES_MAX;
            _rdrand32_step(&gReqDejavu);
            if (!gReqDejavu) gReqDejavu = 1;
            enqueueResponse(&peers[pi], sizeof(req), REQUEST_TYPE, gReqDejavu, &req);
            gSlots[0].active = true;
            gSlots[0].startTick = gReqFrontier;
            gSlots[0].tsc = nowTsc;
        }
    }
    RELEASE(gSlotsLock);
}

// Receive a chunk: advance the frontier + chain the next request (continuous delivery), then copy
// the sub-frames into the work queue for the requestProcessor pool to verify + store in parallel.
static void onRespondChunk(Peer* peer, RequestResponseHeader* header, unsigned long long processorNumber)
{
    _InterlockedIncrement64(&gChunksReceived);
    addCapablePeer(peer->address.u32);   // this peer speaks the bulk protocol
    const unsigned int total = header->size();
    if (total < sizeof(RequestResponseHeader) + sizeof(RespondTickRangeChunkHeader))
        return;
    char* base = (char*)header->getPayload<char>();
    const unsigned int payloadSize = total - sizeof(RequestResponseHeader);

    // Free the slot that owned this range, update the chunk-size estimate, keep the frontier honest.
    const RespondTickRangeChunkHeader* rh = (const RespondTickRangeChunkHeader*)base;
    ACQUIRE(gSlotsLock);
    for (int s = 0; s < BULK_INFLIGHT; s++)
        if (gSlots[s].active && gSlots[s].startTick == rh->startTick) { gSlots[s].active = false; break; }
    if (rh->endTickFullyIncluded >= rh->startTick)
    {
        const unsigned int got = rh->endTickFullyIncluded - rh->startTick + 1;
        gAvgChunkTicks = (gAvgChunkTicks * 3 + got) / 4;            // smooth the stride estimate
        if (rh->endTickFullyIncluded + 1 > gReqFrontier) gReqFrontier = rh->endTickFullyIncluded + 1;
    }
    RELEASE(gSlotsLock);

    fillSlots(__rdtsc(), frequency);     // refill in parallel (continuous, multi-peer delivery)

    unsigned int cursor = sizeof(RespondTickRangeChunkHeader);
    while (cursor + sizeof(RequestResponseHeader) <= payloadSize)
    {
        RequestResponseHeader* sub = (RequestResponseHeader*)(base + cursor);
        const unsigned int subSize = sub->size();
        if (subSize < sizeof(RequestResponseHeader) || cursor + subSize > payloadSize)
            break;                       // truncated/garbage tail: stop
        bulkEnqueue(sub);                // verified + stored later by the pool (bulkProcessOne)
        cursor += subSize;
    }
}

// Called from the main loop's request block (~2x/sec). Drives activation and the
// single in-flight request. `nowTsc` and `freq` are __rdtsc()/frequency from the caller.
static void kicker(unsigned long long nowTsc, unsigned long long freq)
{
    if (isMainMode()) { gActive = false; return; }

    const unsigned int tip = networkTip();
    const unsigned int behind = tip > system.tick ? (tip - system.tick) : 0;
    if (!gActive && behind >= ACTIVATE_BEHIND) gActive = true;
    else if (gActive && behind <= DEACTIVATE_BEHIND) { gActive = false; return; }
    if (!gActive) return;

    // No capable bulk peer yet: probe everyone periodically until one answers.
    if (gCapableCount == 0)
    {
        if (nowTsc - gLastProbeTsc > PROBE_INTERVAL_SECS * freq)
            probeAllPeers(nowTsc);
        return;
    }

    // Keep the frontier at/ahead of where we've ticked to (recovers if it stalled or fell behind).
    if (gReqFrontier <= system.tick) gReqFrontier = system.tick + 1;

    fillSlots(nowTsc, freq);            // kickstart / recover / top up the in-flight slots
}

} // namespace LiteBulkCatchup

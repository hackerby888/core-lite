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
static bool appendTick(char* buf, unsigned int& cursor, unsigned int t, unsigned int dejavu)
{
    unsigned short epoch = 0;
    bool current = false;
    if (ts.tickInCurrentEpochStorage(t)) { epoch = system.epoch; current = true; }
    else if (ts.tickInPreviousEpochStorage(t)) { epoch = system.epoch - 1; current = false; }
    else return false;

    // tickData
    ts.tickData.acquireLock();
    const TickData* td = current ? &ts.tickData.getByTickInCurrentEpoch(t)
                                 : &ts.tickData.getByTickInPreviousEpoch(t);
    if (td->epoch == epoch)
        appendSubFrame(buf, cursor, BroadcastFutureTickData::type(), dejavu, td, sizeof(TickData));
    ts.tickData.releaseLock();

    // votes (one Tick per computor that voted)
    const Tick* tsTicks = current ? ts.ticks.getByTickInCurrentEpoch(t)
                                  : ts.ticks.getByTickInPreviousEpoch(t);
    for (unsigned int ci = 0; ci < NUMBER_OF_COMPUTORS; ci++)
    {
        if (tsTicks[ci].epoch == epoch)
        {
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
                appendSubFrame(buf, cursor, BROADCAST_TRANSACTION, dejavu, tx, tx->totalSize());
        }
    }
    return true;
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
        const unsigned int before = cursor;
        if (!appendTick(buf, cursor, t, dejavu))
            break;                       // responder has no more ticks from here
        if (cursor > cap)                // this tick overflowed the chunk
        {
            cursor = before;             // roll it back; never ship a partial tick
            break;
        }
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
static int           gOutstanding  = -1;            // peer index of the in-flight request, or -1
static unsigned int  gReqStartTick = 0;             // startTick of the in-flight request
static unsigned long long gReqTsc  = 0;
static unsigned int  gReqDejavu    = 0;

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

static int pickPeer()
{
    int candidates[NUMBER_OF_OUTGOING_CONNECTIONS + NUMBER_OF_INCOMING_CONNECTIONS];
    int n = 0;
    for (unsigned int i = 0; i < NUMBER_OF_OUTGOING_CONNECTIONS + NUMBER_OF_INCOMING_CONNECTIONS; i++)
    {
        if (peers[i].tcp4Protocol && peers[i].isConnectedAccepted && !peers[i].isClosing
            && (i < NUMBER_OF_OUTGOING_CONNECTIONS || peers[i].exchangedPublicPeers))
            candidates[n++] = (int)i;
    }
    if (!n) return -1;
    return candidates[random(n)];
}

// Apply one received chunk: walk its sub-frames and feed each to the existing
// verifying handler. Bounds every sub-frame against the payload before use.
static void onRespondChunk(Peer* peer, RequestResponseHeader* header, unsigned long long processorNumber)
{
    _InterlockedIncrement64(&gChunksReceived);
    const unsigned int total = header->size();
    if (total < sizeof(RequestResponseHeader) + sizeof(RespondTickRangeChunkHeader))
    {
        gOutstanding = -1;
        return;
    }
    char* base = (char*)header->getPayload<char>();
    const unsigned int payloadSize = total - sizeof(RequestResponseHeader);

    unsigned int cursor = sizeof(RespondTickRangeChunkHeader);
    while (cursor + sizeof(RequestResponseHeader) <= payloadSize)
    {
        RequestResponseHeader* sub = (RequestResponseHeader*)(base + cursor);
        const unsigned int subSize = sub->size();
        if (subSize < sizeof(RequestResponseHeader) || cursor + subSize > payloadSize)
            break;                       // truncated/garbage tail: stop
        switch (sub->type())
        {
        case BroadcastFutureTickData::type(): processBroadcastFutureTickData(peer, sub); break;
        case BroadcastTick::type():           processBroadcastTick(peer, sub);           break;
        case BROADCAST_TRANSACTION:
        {
            Transaction* tx = sub->getPayload<Transaction>();
            if (tx->checkValidity() && tx->totalSize() == subSize - sizeof(RequestResponseHeader))
            {
                unsigned char d[32];
                KangarooTwelve(tx, tx->totalSize() - SIGNATURE_SIZE, d, sizeof(d));
                if (verify(tx->sourcePublicKey.m256i_u8, d, tx->signaturePtr()))
                    storeBulkTransaction(tx);
            }
            break;
        }
        default: break;                  // ignore unknown sub-frames
        }
        cursor += subSize;
        _InterlockedIncrement64(&gBulkTicksApplied);
    }
    gOutstanding = -1;                    // free the slot to request the next range
}

// Called from the main loop's request block (~2x/sec). Drives activation and the
// single in-flight request. `nowTsc` and `freq` are __rdtsc()/frequency from the caller.
static void kicker(unsigned long long nowTsc, unsigned long long freq)
{
    if (isMainMode()) { gActive = false; gOutstanding = -1; return; }

    const unsigned int tip = networkTip();
    const unsigned int behind = tip > system.tick ? (tip - system.tick) : 0;
    if (!gActive && behind >= ACTIVATE_BEHIND) gActive = true;
    else if (gActive && behind <= DEACTIVATE_BEHIND) { gActive = false; gOutstanding = -1; return; }
    if (!gActive) return;

    // Time out a stuck request.
    if (gOutstanding >= 0 && (nowTsc - gReqTsc) > CHUNK_TIMEOUT_SECS * freq)
        gOutstanding = -1;

    if (gOutstanding < 0)
    {
        const int pi = pickPeer();
        if (pi < 0) return;
        RequestTickRangeChunk req;
        req.version = VERSION;
        req.startTick = system.tick + 1;
        req.maxBytes = CHUNK_BYTES_MAX;
        _rdrand32_step(&gReqDejavu);
        if (!gReqDejavu) gReqDejavu = 1;
        enqueueResponse(&peers[pi], sizeof(req), REQUEST_TYPE, gReqDejavu, &req);
        gOutstanding = pi;
        gReqStartTick = req.startTick;
        gReqTsc = nowTsc;
    }
}

} // namespace LiteBulkCatchup

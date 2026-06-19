#pragma once

// Super-prefetch: a node >= TRIGGER ticks behind asks lite peers for a leased
// per-IP slot, then deep-pulls up to DEPTH ticks ahead (beyond depth-20 prefetch).

#include <chrono>

namespace SuperPrefetch
{

constexpr unsigned char REQUEST_TYPE = 232;
constexpr unsigned char RESPOND_TYPE = 233;
constexpr unsigned char DONE_TYPE = 234;

constexpr unsigned int TRIGGER_TICKS_BEHIND = 128; // enter super-prefetch at/after this lag
constexpr unsigned int STOP_TICKS_BEHIND = 16;     // release slots once within this lag
constexpr unsigned int SHALLOW_DEPTH = 20;         // must equal opt CATCHUP_MAX_PREFETCH; super pulls beyond it
constexpr unsigned short DEPTH = 64;               // ticks ahead a granted source is pulled to
constexpr unsigned int MAX_SLOTS = 16;             // hard upper bound for the runtime slot cap
constexpr unsigned long long LEASE_MS = 8000;      // a grant lapses this long after the last handshake
constexpr unsigned long long RENEW_MS = 2500;      // asker re-handshakes a source this often
constexpr unsigned int FANOUT = 3;                 // max simultaneous sources an asker uses

#pragma pack(push, 1)
struct RequestSuperPrefetch
{
    static constexpr unsigned char type() { return REQUEST_TYPE; }
    unsigned int askerTick;
    unsigned short desiredDepth;
};
static_assert(sizeof(RequestSuperPrefetch) == 6, "RequestSuperPrefetch layout drifted");

struct RespondSuperPrefetch
{
    static constexpr unsigned char type() { return RESPOND_TYPE; }
    unsigned char granted;
    unsigned short grantedDepth;
    unsigned short leaseSeconds;
};
static_assert(sizeof(RespondSuperPrefetch) == 5, "RespondSuperPrefetch layout drifted");

struct RequestSuperPrefetchDone
{
    static constexpr unsigned char type() { return DONE_TYPE; }
    unsigned int finalTick;
};
static_assert(sizeof(RequestSuperPrefetchDone) == 4, "RequestSuperPrefetchDone layout drifted");

struct WireSuperPrefetch { RequestResponseHeader header; RequestSuperPrefetch body; };
struct WireSuperPrefetchDone { RequestResponseHeader header; RequestSuperPrefetchDone body; };
#pragma pack(pop)

// Natural layout (NOT packed): payload size must equal the server's sizeof() (checkPayloadSize).
struct WireRequestTickData { RequestResponseHeader header; RequestTickData body; };
struct WireRequestQuorumTick { RequestResponseHeader header; RequestQuorumTick body; };
struct WireRequestTickTxs { RequestResponseHeader header; RequestTickTransactions body; };

static bool gEnabled = true;
static unsigned int gSlotCap = 4;

// Server: leased grant slots (written on request-processor threads, expired on main).
struct Slot { bool active; unsigned int peerAddrU32; unsigned long long leaseExpiryMs; };
static Slot slots[MAX_SLOTS] = {};
static volatile char slotsLock = 0;

// Asker: granted sources indexed by peer slot (written on request-processor threads, read on main).
static constexpr unsigned int NUM_PEERS = NUMBER_OF_OUTGOING_CONNECTIONS + NUMBER_OF_INCOMING_CONNECTIONS;
struct Source { bool active; bool granted; unsigned int peerAddrU32; unsigned short depth; unsigned long long lastSentMs; };
static Source sources[NUM_PEERS] = {};
static volatile char sourcesLock = 0;

static inline unsigned long long nowMs()
{
    return (unsigned long long)std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

// push() is main-thread only; these run from requesterTick.

static void sendHandshake(Peer* peer, unsigned int askerTick)
{
    WireSuperPrefetch m;
    m.header.setSize<sizeof(WireSuperPrefetch)>();
    m.header.setType(REQUEST_TYPE);
    m.header.randomizeDejavu();
    m.body.askerTick = askerTick;
    m.body.desiredDepth = DEPTH;
    push(peer, &m.header);
}

static void sendDone(Peer* peer, unsigned int finalTick)
{
    WireSuperPrefetchDone m;
    m.header.setSize<sizeof(WireSuperPrefetchDone)>();
    m.header.setType(DONE_TYPE);
    m.header.randomizeDejavu();
    m.body.finalTick = finalTick;
    push(peer, &m.header);
}

// Request tickData + votes + missing txs for one future tick from a granted peer
// (mirrors opt_future_tick_prefetch's construction, targeted at one peer).
static void deepPullTick(Peer* peer, unsigned int futureTick)
{
    if (!ts.tickInCurrentEpochStorage(futureTick))
        return;

    ts.tickData.acquireLock();
    const bool haveTickData = ts.tickData[futureTick - system.initialTick].epoch == system.epoch;
    ts.tickData.releaseLock();

    if (!haveTickData)
    {
        WireRequestTickData m;
        m.header.setSize<sizeof(WireRequestTickData)>();
        m.header.setType(RequestTickData::type());
        m.header.randomizeDejavu();
        m.body.requestedTickData.tick = futureTick;
        push(peer, &m.header);
    }

    {
        WireRequestQuorumTick m;
        m.header.setSize<sizeof(WireRequestQuorumTick)>();
        m.header.setType(RequestQuorumTick::type());
        m.header.randomizeDejavu();
        m.body.quorumTick.tick = futureTick;
        setMem(&m.body.quorumTick.voteFlags, sizeof(m.body.quorumTick.voteFlags), 0);
        const Tick* tsCompTicks = ts.ticks.getByTickInCurrentEpoch(futureTick);
        for (unsigned int i = 0; i < NUMBER_OF_COMPUTORS; i++)
            if (tsCompTicks[i].epoch == system.epoch)
                m.body.quorumTick.voteFlags[i >> 3] |= (1 << (i & 7));
        push(peer, &m.header);
    }

    if (haveTickData)
    {
        WireRequestTickTxs m;
        bool anyMissing = false;
        ts.tickData.acquireLock();
        if (ts.tickData[futureTick - system.initialTick].epoch == system.epoch)
        {
            const TickData& td = ts.tickData[futureTick - system.initialTick];
            const unsigned long long* offsets = ts.tickTransactionOffsets.getByTickInCurrentEpoch(futureTick);
            setMem(m.body.transactionFlags, sizeof(m.body.transactionFlags), 0xff);
            for (unsigned int i = 0; i < NUMBER_OF_TRANSACTIONS_PER_TICK; i++)
                if (!offsets[i] && !isZero(td.transactionDigests[i]))
                {
                    m.body.transactionFlags[i >> 3] &= ~(1 << (i & 7));
                    anyMissing = true;
                }
        }
        ts.tickData.releaseLock();
        if (anyMissing)
        {
            m.header.setSize<sizeof(WireRequestTickTxs)>();
            m.header.setType(RequestTickTransactions::type());
            m.header.randomizeDejavu();
            m.body.tick = futureTick;
            push(peer, &m.header);
        }
    }
}

// ---- server side (the node being asked) ----

inline void serverOnRequest(Peer* peer, RequestResponseHeader* header)
{
    if (!gEnabled || !header->checkPayloadSize(sizeof(RequestSuperPrefetch)))
        return;
    const RequestSuperPrefetch* req = header->getPayload<RequestSuperPrefetch>();
    const unsigned int addr = peer->address.u32;
    const unsigned long long now = nowMs();
    const unsigned int cap = gSlotCap <= MAX_SLOTS ? gSlotCap : MAX_SLOTS;
    const unsigned short depth = req->desiredDepth < DEPTH ? req->desiredDepth : DEPTH;

    bool granted = false;
    ACQUIRE(slotsLock);
    int mine = -1, freeSlot = -1;
    unsigned int active = 0;
    for (unsigned int i = 0; i < MAX_SLOTS; i++)
    {
        if (slots[i].active && slots[i].leaseExpiryMs < now)
            slots[i] = Slot{};
        if (slots[i].active)
        {
            active++;
            if (slots[i].peerAddrU32 == addr)
                mine = (int)i;
        }
        else if (freeSlot < 0)
            freeSlot = (int)i;
    }
    if (mine >= 0)
    {
        slots[mine].leaseExpiryMs = now + LEASE_MS; // renew
        granted = true;
    }
    else if (active < cap && freeSlot >= 0)
    {
        slots[freeSlot].active = true;
        slots[freeSlot].peerAddrU32 = addr;
        slots[freeSlot].leaseExpiryMs = now + LEASE_MS;
        granted = true;
    }
    RELEASE(slotsLock);

    RespondSuperPrefetch resp{};
    resp.granted = granted ? 1 : 0;
    resp.grantedDepth = granted ? depth : 0;
    resp.leaseSeconds = (unsigned short)(LEASE_MS / 1000);
    enqueueResponse(peer, sizeof(resp), RESPOND_TYPE, header->dejavu(), &resp);
}

inline void serverOnDone(Peer* peer, RequestResponseHeader* header)
{
    if (!header->checkPayloadSize(sizeof(RequestSuperPrefetchDone)))
        return;
    const unsigned int addr = peer->address.u32;
    ACQUIRE(slotsLock);
    for (unsigned int i = 0; i < MAX_SLOTS; i++)
        if (slots[i].active && slots[i].peerAddrU32 == addr)
            slots[i] = Slot{};
    RELEASE(slotsLock);
}

// Reclaim slots whose holder stopped renewing (dead peer). Main thread.
inline void serverTick()
{
    if (!gEnabled)
        return;
    const unsigned long long now = nowMs();
    ACQUIRE(slotsLock);
    for (unsigned int i = 0; i < MAX_SLOTS; i++)
        if (slots[i].active && slots[i].leaseExpiryMs < now)
            slots[i] = Slot{};
    RELEASE(slotsLock);
}

// Append " SP serve=N(ips) pull=M(ips)" to the periodic status line (main thread):
// provider slots we serve + consumer sources we pull from. Omitted when idle.
inline void appendStatus(CHAR16* message)
{
    if (!gEnabled)
        return;
    // Snapshot each set under a single brief lock so the printed count and the
    // listed IPs stay consistent, and no formatting runs while holding the lock.
    unsigned int serveAddrs[MAX_SLOTS], pullAddrs[NUM_PEERS];
    unsigned int serve = 0, pull = 0;
    ACQUIRE(slotsLock);
    for (unsigned int i = 0; i < MAX_SLOTS; i++)
        if (slots[i].active) serveAddrs[serve++] = slots[i].peerAddrU32;
    RELEASE(slotsLock);
    ACQUIRE(sourcesLock);
    for (unsigned int i = 0; i < NUM_PEERS; i++)
        if (sources[i].active && sources[i].granted) pullAddrs[pull++] = sources[i].peerAddrU32;
    RELEASE(sourcesLock);
    if (!serve && !pull)
        return;

    appendText(message, L" SP serve=");
    appendNumber(message, serve, FALSE);
    if (serve)
    {
        appendText(message, L"(");
        for (unsigned int k = 0; k < serve; k++)
        {
            if (k) appendText(message, L" ");
            IPv4Address a; a.u32 = serveAddrs[k];
            appendIPv4Address(message, a);
        }
        appendText(message, L")");
    }
    appendText(message, L" pull=");
    appendNumber(message, pull, FALSE);
    if (pull)
    {
        appendText(message, L"(");
        for (unsigned int k = 0; k < pull; k++)
        {
            if (k) appendText(message, L" ");
            IPv4Address a; a.u32 = pullAddrs[k];
            appendIPv4Address(message, a);
        }
        appendText(message, L")");
    }
}

// ---- asker side (the behind node) ----

inline void requesterOnRespond(Peer* peer, RequestResponseHeader* header)
{
    if (!header->checkPayloadSize(sizeof(RespondSuperPrefetch)))
        return;
    const RespondSuperPrefetch* r = header->getPayload<RespondSuperPrefetch>();
    const unsigned int idx = (unsigned int)(peer - peers);
    if (idx >= NUM_PEERS)
        return;
    ACQUIRE(sourcesLock);
    if (sources[idx].active && sources[idx].peerAddrU32 == peer->address.u32)
    {
        if (r->granted)
        {
            sources[idx].granted = true;
            sources[idx].depth = r->grantedDepth < DEPTH ? r->grantedDepth : DEPTH;
        }
        else
        {
            sources[idx] = Source{}; // denied: stop bothering this peer
        }
    }
    RELEASE(sourcesLock);
}

// Detect lag, (re)handshake up to FANOUT lite sources, deep-pull from granted
// ones, release on catch-up. Main thread (push() is main-thread only).
inline void requesterTick()
{
    if (!gEnabled)
        return;

    const unsigned int myTick = system.tick;
    unsigned int tip = 0;
    for (unsigned int i = 0; i < NUM_PEERS; i++)
        if (peers[i].tcp4Protocol && peers[i].isConnectedAccepted && !peers[i].isClosing
            && peers[i].peerReportedTick > tip)
            tip = peers[i].peerReportedTick;
    const unsigned int behind = tip > myTick ? tip - myTick : 0;
    const unsigned long long now = nowMs();

    unsigned short handshakeIdx[NUM_PEERS];
    unsigned short grantedIdx[NUM_PEERS];
    unsigned short doneIdx[NUM_PEERS];
    unsigned int nHandshake = 0, nGranted = 0, nDone = 0;

    ACQUIRE(sourcesLock);
    for (unsigned int i = 0; i < NUM_PEERS; i++)
        if (sources[i].active
            && (!peers[i].isConnectedAccepted || peers[i].isClosing
                || peers[i].address.u32 != sources[i].peerAddrU32))
            sources[i] = Source{}; // peer gone or slot recycled to a different IP

    if (behind < STOP_TICKS_BEHIND)
    {
        for (unsigned int i = 0; i < NUM_PEERS; i++)
            if (sources[i].active && sources[i].granted)
                doneIdx[nDone++] = (unsigned short)i;
        for (unsigned int i = 0; i < NUM_PEERS; i++)
            sources[i] = Source{};
    }
    else if (behind >= TRIGGER_TICKS_BEHIND)
    {
        unsigned int active = 0;
        for (unsigned int i = 0; i < NUM_PEERS; i++)
        {
            if (!sources[i].active)
                continue;
            active++;
            if (now - sources[i].lastSentMs >= RENEW_MS)
            {
                handshakeIdx[nHandshake++] = (unsigned short)i;
                sources[i].lastSentMs = now;
            }
        }
        for (unsigned int i = 0; i < NUM_PEERS && active < FANOUT; i++)
            if (!sources[i].active
                && peers[i].tcp4Protocol && peers[i].isConnectedAccepted && !peers[i].isClosing
                && peers[i].peerReportedTick >= myTick)
            {
                sources[i].active = true;
                sources[i].granted = false;
                sources[i].peerAddrU32 = peers[i].address.u32;
                sources[i].depth = DEPTH;
                sources[i].lastSentMs = now;
                handshakeIdx[nHandshake++] = (unsigned short)i;
                active++;
            }
        for (unsigned int i = 0; i < NUM_PEERS; i++)
            if (sources[i].active && sources[i].granted)
                grantedIdx[nGranted++] = (unsigned short)i;
    }
    RELEASE(sourcesLock);

    for (unsigned int k = 0; k < nHandshake; k++)
        sendHandshake(&peers[handshakeIdx[k]], myTick);
    for (unsigned int k = 0; k < nDone; k++)
        sendDone(&peers[doneIdx[k]], myTick);

    if (nGranted > 0)
    {
        unsigned int rr = 0;
        for (unsigned int d = SHALLOW_DEPTH + 1; d <= DEPTH; d++, rr++)
            deepPullTick(&peers[grantedIdx[rr % nGranted]], myTick + d);
    }
}

} // namespace SuperPrefetch

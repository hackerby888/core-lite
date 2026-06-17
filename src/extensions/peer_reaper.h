#pragma once

// Auto-reap dead/zombie regular-outgoing peers. Detects two failure modes per slot:
//   (A) connect-stall: dialed but never reaches ConnectedAccepted within a timeout.
//   (B) rx-stall: established but no bytes received for a while (frozen/half-open/blackholed).
// On reap we forgetPublicPeer() the dead IP (keeps the >=10 floor and never drops seeded/
// static peers) then closePeer(); peerReconnectIfInactive() redials a fresh pool peer.
// Runs in the efi_main peer loop (main processor) where closePeer() is legal.

#include <atomic>

namespace PeerReaper
{
// Per-reason reap totals are exposed at rpc/stats via PeerDisc (closePeer records the reason).
static volatile bool gEnabled = true;                            // runtime kill-switch
static constexpr unsigned long long CONNECT_TIMEOUT_SECS = 10;   // stuck connecting -> reap
static constexpr unsigned long long RX_STALL_SECS = 60;          // established, no rx -> reap

// Reaper-side per-slot state; no core fields added. Zero-initialized (static storage).
static bool sActive[NUMBER_OF_OUTGOING_CONNECTIONS];                     // slot was live last scan
static unsigned long long sConnStartTsc[NUMBER_OF_OUTGOING_CONNECTIONS]; // rising-edge connect time
static unsigned long long sLastRxBytes[NUMBER_OF_OUTGOING_CONNECTIONS];  // last observed gSlotRxBytes
static unsigned long long sRxProgressTsc[NUMBER_OF_OUTGOING_CONNECTIONS];// when rx last advanced

// True if the IP came from CLI --peers; keep these in the pool so a false positive cannot evict our bootstrap set.
static bool isCliSeedPeer(const IPv4Address& address)
{
    for (unsigned int k = 0; k < knownPublicPeersDynamic.size(); k++)
        if (knownPublicPeersDynamic[k] == address)
            return true;
    return false;
}

// Drop a dead peer and let the redial pick a new one.
static void reap(unsigned int i, unsigned int discReason)
{
    // forgetPublicPeer already spares baked-in seeds and keeps the >=10 floor; also spare CLI --peers here.
    if (!isCliSeedPeer(peers[i].address))
        forgetPublicPeer(peers[i].address);   // before close: close resets address
    closePeer(&peers[i], 0, discReason);  // 0 retries, matches churn/OM-timeout closes; records discReason
    logToConsole(discReason == PeerDisc::ZOMBIE_CONNECT
        ? L"Reaped dead outgoing peer (connect timeout)"
        : L"Reaped zombie outgoing peer (rx stall)");
}

// Called once per peer-loop iteration for every slot; acts only on regular-outgoing slots.
static void checkSlot(unsigned int i, unsigned long long frequency)
{
    if (!gEnabled || i >= NUMBER_OF_OUTGOING_CONNECTIONS || peers[i].isOMNode)
        return;

    const unsigned long long conn = (unsigned long long)peers[i].tcp4Protocol;
    const bool live = conn > 1 && !peers[i].isClosing;   // real conn object, not tearing down
    const unsigned long long now = __rdtsc();

    // Idle / connect-failed / closing slot -> nothing to judge; arm the rising edge.
    if (!live)
    {
        sActive[i] = false;
        return;
    }

    // Rising edge into a live state marks a new connection (a slot always passes through a
    // not-live state between connections, so this is immune to tcp4Protocol pointer reuse).
    if (!sActive[i])
    {
        sActive[i] = true;
        sConnStartTsc[i] = now;
        sLastRxBytes[i] = PeerDisc::gSlotRxBytes[i].load(std::memory_order_relaxed);
        sRxProgressTsc[i] = now;
        return;
    }

    // (A) connect-stall: still connecting past the timeout.
    if (!peers[i].isConnectedAccepted)
    {
        if ((now - sConnStartTsc[i]) / frequency > CONNECT_TIMEOUT_SECS)
            reap(i, PeerDisc::ZOMBIE_CONNECT);
        return;
    }

    // (B) rx-stall: established but cumulative received bytes have not moved.
    const unsigned long long rx = PeerDisc::gSlotRxBytes[i].load(std::memory_order_relaxed);
    if (rx != sLastRxBytes[i])
    {
        sLastRxBytes[i] = rx;
        sRxProgressTsc[i] = now;
        return;
    }
    if ((now - sRxProgressTsc[i]) / frequency > RX_STALL_SECS)
        reap(i, PeerDisc::ZOMBIE_RXSTALL);
}
} // namespace PeerReaper

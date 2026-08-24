#pragma once

#include "platform/m256.h"
#include "mining/mining.h"
#include "mining/ant_colony/ant_colony_bpp9000.h"
#include "mining/score_common.h"
#include "spectrum/special_entities.h"
#include "network_core/peers.h"
#include "network_messages/network_message_type.h"
#include "network_messages/transactions.h"
#include "kangaroo_twelve.h"
#include "four_q.h"

namespace TestInvalidSolution
{

namespace detail
{

inline void broadcastTransfer(unsigned int sourceComputorIdx,
                              const m256i& destinationPublicKey,
                              long long amount,
                              unsigned int txTick)
{
    struct
    {
        Transaction transaction;
        unsigned char signature[SIGNATURE_SIZE];
    } payload;
    static_assert(sizeof(payload) == sizeof(Transaction) + SIGNATURE_SIZE,
                  "TestInvalidSolution transfer payload layout drifted");

    payload.transaction.sourcePublicKey      = computorPublicKeys[sourceComputorIdx];
    payload.transaction.destinationPublicKey = destinationPublicKey;
    payload.transaction.amount               = amount;
    payload.transaction.tick                 = txTick;
    payload.transaction.inputType            = 0;
    payload.transaction.inputSize            = 0;

    unsigned char digest[32];
    KangarooTwelve(&payload.transaction,
                   sizeof(payload.transaction),
                   digest,
                   sizeof(digest));
    sign(computorSubseeds[sourceComputorIdx].m256i_u8,
         computorPublicKeys[sourceComputorIdx].m256i_u8,
         digest,
         payload.signature);

    enqueueResponse(NULL, sizeof(payload), BROADCAST_TRANSACTION, 0, &payload);
}

} // namespace detail

inline bool broadcastRandom(const m256i& currentMiningSeed, unsigned int txTick, unsigned int claimedScore)
{
    if (computorSeedsCount == 0)
    {
        return false;
    }

    // Pick a random one of our computors.
    m256i rnd;
    rnd.setRandomValue();
    const unsigned int computorIdx = (unsigned int)(rnd.m256i_u64[0] % computorSeedsCount);

    // ---- 1) Invalid solution tx ----
    {
        MiningSolutionTransaction payload;
        payload.sourcePublicKey      = computorPublicKeys[computorIdx];
        payload.destinationPublicKey = m256i::zero();
        payload.amount               = MiningSolutionTransaction::minAmount();
        payload.tick                 = txTick;
        payload.inputType            = MiningSolutionTransaction::transactionType();
        payload.inputSize            = MiningSolutionTransaction::minInputSize();

        payload.miningSeed = currentMiningSeed;
        payload.miningSeed.m256i_u64[0] ^= 1;
        payload.nonce.setRandomValue();
        payload.nonce.m256i_u8[0] = (unsigned char)score_engine::AlgoType::Bpp9000;
        payload.score = claimedScore;
        payload.reserved = 0;

        unsigned char digest[32];
        KangarooTwelve(&payload,
                       sizeof(Transaction) + MiningSolutionTransaction::minInputSize(),
                       digest,
                       sizeof(digest));
        sign(computorSubseeds[computorIdx].m256i_u8,
             computorPublicKeys[computorIdx].m256i_u8,
             digest,
             payload.signature);

        enqueueResponse(NULL, sizeof(payload), BROADCAST_TRANSACTION, 0, &payload);
    }

    // ---- 2) Standard QU transfer to the id that signed the wrong sol ----
    const long long transferAmount = 1;
    detail::broadcastTransfer(computorIdx,
                              computorPublicKeys[computorIdx],
                              transferAmount,
                              txTick);

    // ---- 3) Standard QU transfer to a random network computor ----
    m256i randomComputorRnd;
    randomComputorRnd.setRandomValue();
    const unsigned int randomComputorIdx =
        (unsigned int)(randomComputorRnd.m256i_u64[0] % NUMBER_OF_COMPUTORS);
    detail::broadcastTransfer(computorIdx,
                              broadcastedComputors.computors.publicKeys[randomComputorIdx],
                              transferAmount,
                              txTick);

    // ---- 4) Standard QU transfer to a fully random id ----
    m256i randomId;
    randomId.setRandomValue();
    detail::broadcastTransfer(computorIdx,
                              randomId,
                              transferAmount,
                              txTick);

    return true;
}


// Ant-colony injector. The node mines against its own colony, so this drives the whole inputType-12
// path on one machine: broadcast, pre-score, publish, commit, deposit, ranking. Each mode aims at one
// branch of the accept rules, so every ValidityResult is reachable without a second node.
enum class AntInjectMode
{
    Valid,          // honest solution: accepted, deposit refunded, ranked
    BadClaim,       // right nonce, wrong claimedScore: committed and folded, deposit kept
    NonCanonical,   // nonce[1] out of range
    WrongTree,      // parent belonging to another identity
    Stale,          // anchor older than the publish window
    FutureParent,   // parent ref into the current tick
    LeParent,       // child that does not beat its parent
};

namespace detail
{

inline void signAndBroadcastAntSolution(unsigned int computorIdx,
                                        const SolutionRef& parentRef,
                                        unsigned int anchorTick,
                                        unsigned int claimedScore,
                                        const m256i& nonce,
                                        unsigned int txTick)
{
    AntColonyMiningSolutionTransaction payload;
    setMem(&payload, sizeof(payload), 0);
    payload.sourcePublicKey           = computorPublicKeys[computorIdx];
    payload.destinationPublicKey      = m256i::zero();
    payload.amount                    = AntColonyMiningSolutionTransaction::minAmount();
    payload.tick                      = txTick;
    payload.inputType                 = AntColonyMiningSolutionTransaction::transactionType();
    payload.inputSize                 = AntColonyMiningSolutionTransaction::minInputSize();
    payload.parentTick                = parentRef.tick;
    payload.parentSolutionIndexInTick = parentRef.solutionIndexInTick;
    payload.anchorTick                = anchorTick;
    payload.claimedScore              = claimedScore;
    payload.nonce                     = nonce;

    unsigned char digest[32];
    KangarooTwelve(&payload,
                   sizeof(Transaction) + AntColonyMiningSolutionTransaction::minInputSize(),
                   digest,
                   sizeof(digest));
    sign(computorSubseeds[computorIdx].m256i_u8,
         computorPublicKeys[computorIdx].m256i_u8,
         digest,
         payload.signature);

    enqueueResponse(NULL, sizeof(payload), BROADCAST_TRANSACTION, 0, &payload);
}

} // namespace detail

// ColonyT and ScoreT stay template parameters so this header keeps compiling where it is included,
// which is before qubic.cpp declares gAntColony and score.
template<typename ColonyT, typename ScoreT>
inline bool broadcastAntSolution(ColonyT& colony,
                                 ScoreT& scoreFn,
                                 unsigned long long processorNumber,
                                 unsigned int txTick,
                                 unsigned int anchorTick,
                                 AntInjectMode mode,
                                 unsigned int attempts = 8)
{
    if (computorSeedsCount == 0)
    {
        return false;
    }

    m256i rnd;
    rnd.setRandomValue();
    const unsigned int computorIdx = (unsigned int)(rnd.m256i_u64[0] % computorSeedsCount);
    const m256i& minerKey = computorPublicKeys[computorIdx];

    // Extend this identity's best node when it has one, otherwise start its tree from the root.
    SolutionRef parentRef = ROOT_REF;
    const AntSolutionRecord* parentRec = nullptr;
    unsigned int parentScore = 0xFFFFFFFFU;
    for (unsigned int i = 0; i < colony.solutionCount(); i++)
    {
        const AntSolutionRecord* rec = colony.recordAt((long long)i);
        if (rec != nullptr && rec->pubkey == minerKey && rec->score < parentScore)
        {
            parentScore = rec->score;
            parentRef = rec->selfRef;
            parentRec = rec;
        }
    }

    if (mode == AntInjectMode::WrongTree)
    {
        // Any node owned by somebody else. Without one the rule is not reachable yet.
        parentRec = nullptr;
        for (unsigned int i = 0; i < colony.solutionCount(); i++)
        {
            const AntSolutionRecord* rec = colony.recordAt((long long)i);
            if (rec != nullptr && !(rec->pubkey == minerKey))
            {
                parentRef = rec->selfRef;
                parentRec = rec;
                break;
            }
        }
        if (parentRec == nullptr)
        {
            return false;
        }
    }
    else if (mode == AntInjectMode::FutureParent)
    {
        parentRef.tick = txTick;
        parentRef.solutionIndexInTick = 0;
        parentRec = nullptr;
    }

    unsigned int usedAnchorTick = anchorTick;
    if (mode == AntInjectMode::Stale)
    {
        // Far enough back that the ring no longer holds it.
        usedAnchorTick = (anchorTick > ANT_PUBLISH_WINDOW_TICKS + 1)
            ? (anchorTick - ANT_PUBLISH_WINDOW_TICKS - 1)
            : 0;
    }

    m256i anchorDigest = m256i::zero();
    if (!colony.getAnchorDigest(usedAnchorTick, anchorDigest) && mode != AntInjectMode::Stale)
    {
        return false;
    }

    // The parent's network is what the child inherits; the scorer derives the root itself when the
    // parent is ROOT_REF.
    typename ColonyT::Ann parentAnn;
    typename ColonyT::Ann childAnn;
    const typename ColonyT::Ann* parentAnnPtr = nullptr;
    if (parentRec != nullptr)
    {
        if (!colony.annOfNonRoot(*parentRec, parentAnn))
        {
            return false;
        }
        parentAnnPtr = &parentAnn;
    }

    m256i nonce;
    nonce.setRandomValue();
    nonce.m256i_u8[0] = (unsigned char)score_engine::AlgoType::Bpp9000;

    if (mode == AntInjectMode::NonCanonical)
    {
        nonce.m256i_u8[1] = 0;   // L below range; the scorer refuses before walking
        detail::signAndBroadcastAntSolution(computorIdx, parentRef, usedAnchorTick, 0, nonce, txTick);
        return true;
    }

    unsigned int childScore = score_engine::INVALID_SCORE_VALUE;
    bool found = false;
    for (unsigned int attempt = 0; attempt < attempts && !found; attempt++)
    {
        nonce.setRandomValue();
        nonce.m256i_u8[0] = (unsigned char)score_engine::AlgoType::Bpp9000;
        nonce.m256i_u8[1] = (unsigned char)(1 + (nonce.m256i_u8[1] % score_engine::MAX_LUT_ENTRIES_PER_STEP));
        nonce.m256i_u8[2] = (unsigned char)(nonce.m256i_u8[2] % 64);

        childScore = scoreFn.computeAntChildScore(processorNumber, parentAnnPtr, minerKey, nonce,
                                                  anchorDigest, childAnn);
        if (childScore == score_engine::INVALID_SCORE_VALUE)
        {
            continue;
        }

        const bool beatsParent = (childScore < parentScore);
        const bool clearsThreshold = (childScore <= colony.errorThreshold());
        found = (mode == AntInjectMode::LeParent)
            ? (clearsThreshold && !beatsParent)
            : (beatsParent && clearsThreshold);
    }

    if (!found)
    {
        return false;
    }

    // BadClaim keeps the honest nonce so the node's recompute succeeds and then disagrees, which is
    // what forfeits the deposit.
    const unsigned int claimedScore =
        (mode == AntInjectMode::BadClaim) ? (childScore + 1) : childScore;

    detail::signAndBroadcastAntSolution(computorIdx, parentRef, usedAnchorTick, claimedScore, nonce, txTick);
    return true;
}

} // namespace TestInvalidSolution

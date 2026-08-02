#pragma once

#include "platform/m256.h"
#include "mining/mining.h"
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

inline void broadcastSolution(unsigned int computorIdx,
                              const m256i& currentMiningSeed,
                              unsigned int txTick,
                              unsigned int claimedScore)
{
    MiningSolutionTransaction payload{};
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

} // namespace detail

inline bool broadcastN(const m256i& currentMiningSeed,
                       unsigned int txTick,
                       unsigned int count,
                       bool sameComputor,
                       unsigned int claimedScore)
{
    if (computorSeedsCount == 0)
        return false;

    m256i randomValue;
    randomValue.setRandomValue();
    const unsigned int firstComputorIndex =
        (unsigned int)(randomValue.m256i_u64[0] % computorSeedsCount);
    for (unsigned int solutionIndex = 0; solutionIndex < count; solutionIndex++)
    {
        const unsigned int computorIndex = sameComputor
            ? firstComputorIndex
            : (unsigned int)((firstComputorIndex + solutionIndex) % computorSeedsCount);
        detail::broadcastSolution(computorIndex, currentMiningSeed, txTick, claimedScore);
    }
    return true;
}

inline bool broadcastRandom(const m256i& currentMiningSeed, unsigned int txTick, unsigned int claimedScore)
{
    if (computorSeedsCount == 0)
    {
        return false;
    }

    m256i randomValue;
    randomValue.setRandomValue();
    const unsigned int computorIndex =
        (unsigned int)(randomValue.m256i_u64[0] % computorSeedsCount);

    // Exercise rollback with an invalid solution followed by ordinary transfers that must survive.
    detail::broadcastSolution(computorIndex, currentMiningSeed, txTick, claimedScore);

    const long long transferAmount = 1;
    detail::broadcastTransfer(computorIndex,
                              computorPublicKeys[computorIndex],
                              transferAmount,
                              txTick);

    m256i randomComputorSelector;
    randomComputorSelector.setRandomValue();
    const unsigned int randomComputorIndex =
        (unsigned int)(randomComputorSelector.m256i_u64[0] % NUMBER_OF_COMPUTORS);
    detail::broadcastTransfer(computorIndex,
                              broadcastedComputors.computors.publicKeys[randomComputorIndex],
                              transferAmount,
                              txTick);

    m256i randomDestination;
    randomDestination.setRandomValue();
    detail::broadcastTransfer(computorIndex,
                              randomDestination,
                              transferAmount,
                              txTick);

    return true;
}

} // namespace TestInvalidSolution

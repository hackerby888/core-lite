#pragma once

// Host side of the wasm asset iterators. The contract's iterator object holds the native iterator's indices;
// every call resumes a native iterator from them and writes the advanced indices back.

#include "extensions/wasm/shared/abi_types.h"

static_assert(WASM_NO_ASSET_INDEX == NO_ASSET_INDEX, "the sdk end-of-walk sentinel must match the universe's");

namespace Wasm::Runtime
{

// Reaches the protected iterator fields so a walk can continue from the indices the contract holds.
struct ResumedAssetIterator : QPI::AssetPossessionIterator
{
    ResumedAssetIterator(const void* issuance, const void* ownership, const void* possession, unsigned int issuanceIdx, unsigned int ownershipIdx,
        unsigned int possessionIdx)
    {
        _issuance = *(const QPI::Asset*)issuance;
        _ownership = *(const QPI::AssetOwnershipSelect*)ownership;
        _possession = *(const QPI::AssetPossessionSelect*)possession;
        _issuanceIdx = issuanceIdx;
        _ownershipIdx = ownershipIdx;
        _possessionIdx = possessionIdx;
    }
};

static void assetIterBegin(const void*, unsigned int kind, const void* issuance, const void* ownership, const void* possession, unsigned int* issuanceIdx,
    unsigned int* ownershipIdx, unsigned int* possessionIdx)
{
    if (kind == 1)
    {
        QPI::AssetPossessionIterator iterator(*(const QPI::Asset*)issuance, *(const QPI::AssetOwnershipSelect*)ownership, *(const QPI::AssetPossessionSelect*)possession);

        *issuanceIdx = iterator.issuanceIndex();
        *ownershipIdx = iterator.ownershipIndex();
        *possessionIdx = iterator.possessionIndex();
    }
    else
    {
        QPI::AssetOwnershipIterator iterator(*(const QPI::Asset*)issuance, *(const QPI::AssetOwnershipSelect*)ownership);

        *issuanceIdx = iterator.issuanceIndex();
        *ownershipIdx = iterator.ownershipIndex();
    }
}

// Returns whether a record is selected. The native next() indexes the universe with the indices unchecked, so a
// walk that already ended (possession) or holds an index outside the universe stays ended instead.
static unsigned int assetIterNext(const void*, unsigned int kind, const void* issuance, const void* ownership, const void* possession,
    unsigned int* issuanceIdx, unsigned int* ownershipIdx, unsigned int* possessionIdx)
{
    const bool possessionWalk = kind == 1;
    const unsigned int possessionIndex = possessionWalk ? *possessionIdx : NO_ASSET_INDEX;
    const bool ownershipResumable = *issuanceIdx < ASSETS_CAPACITY && (*ownershipIdx < ASSETS_CAPACITY || *ownershipIdx == NO_ASSET_INDEX);
    const bool possessionResumable = *ownershipIdx < ASSETS_CAPACITY && (possessionIndex < ASSETS_CAPACITY || possessionIndex == NO_ASSET_INDEX);

    if (!ownershipResumable || (possessionWalk && !possessionResumable))
    {
        *ownershipIdx = NO_ASSET_INDEX;
        if (possessionWalk)
        {
            *possessionIdx = NO_ASSET_INDEX;
        }
        return 0;
    }

    ResumedAssetIterator iterator(issuance, ownership, possession, *issuanceIdx, *ownershipIdx, possessionIndex);
    const bool selected = possessionWalk ? iterator.next() : iterator.AssetOwnershipIterator::next();

    *ownershipIdx = iterator.ownershipIndex();
    if (possessionWalk)
    {
        *possessionIdx = iterator.possessionIndex();
    }
    return selected ? 1u : 0u;
}

// Fills the entry for the current record, or zeroes it when the indices do not name a record of the expected kind.
static void assetIterRecord(const void*, unsigned int kind, unsigned int ownershipIdx, unsigned int possessionIdx, void* entry)
{
    AssetEntry* output = (AssetEntry*)entry;
    setMem(output, sizeof(AssetEntry), 0);

    if (ownershipIdx >= ASSETS_CAPACITY || assets[ownershipIdx].varStruct.ownership.type != OWNERSHIP)
    {
        return;
    }
    const auto& ownershipRecord = assets[ownershipIdx].varStruct.ownership;

    if (kind == 1)
    {
        if (possessionIdx >= ASSETS_CAPACITY || assets[possessionIdx].varStruct.possession.type != POSSESSION
            || assets[possessionIdx].varStruct.possession.ownershipIndex != ownershipIdx)
        {
            return;
        }
        const auto& possessionRecord = assets[possessionIdx].varStruct.possession;

        copyMem(output->possessor, &possessionRecord.publicKey, 32);
        output->shares = possessionRecord.numberOfShares;
        output->possessionManagingContract = possessionRecord.managingContractIndex;
    }
    else
    {
        copyMem(output->possessor, &ownershipRecord.publicKey, 32);
        output->shares = ownershipRecord.numberOfShares;
    }

    copyMem(output->owner, &ownershipRecord.publicKey, 32);
    output->ownershipManagingContract = ownershipRecord.managingContractIndex;
}

} // namespace Wasm::Runtime

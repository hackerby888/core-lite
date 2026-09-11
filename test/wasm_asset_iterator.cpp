#define NO_UEFI

#if defined(__linux__) || defined(__APPLE__)
#include "platform/msvc_polyfill.h"
#endif

#include "gtest/gtest.h"
#include "test_util.h"

#include "logging_test.h"

#include "assets/assets.h"
#include "contract_core/contract_exec.h"
#include "qpi/impl/qpi_spectrum_impl.h"
#include "qpi/impl/qpi_assets_impl.h"

#include "extensions/wasm/runtime/asset_iterator.h"

#include <cstring>
#include <vector>

// The wasm host walks an issuance from indices the contract holds. Every step must land on the record the native
// iterator lands on, for every selector, and an inner walk must leave an outer one untouched.
class WasmAssetIteratorUniverse : public AssetStorage, LoggingTest
{
public:
    WasmAssetIteratorUniverse()
    {
        initAssets();
        commonBuffers.init(1, universeSizeInBytes);
        memset(assets, 0, ASSETS_CAPACITY * sizeof(assets[0]));
        as.indexLists.reset();
    }

    ~WasmAssetIteratorUniverse()
    {
        commonBuffers.deinit();
        deinitAssets();
    }
};

struct WalkStep
{
    unsigned int ownershipIdx;
    unsigned int possessionIdx;
    Wasm::AssetEntry entry;
};

static void expectSameWalk(const std::vector<WalkStep>& native, const std::vector<WalkStep>& host)
{
    ASSERT_EQ(host.size(), native.size());
    for (size_t step = 0; step < native.size(); ++step)
    {
        EXPECT_EQ(host[step].ownershipIdx, native[step].ownershipIdx) << "step " << step;
        EXPECT_EQ(host[step].possessionIdx, native[step].possessionIdx) << "step " << step;
        EXPECT_EQ(memcmp(&host[step].entry, &native[step].entry, sizeof(Wasm::AssetEntry)), 0) << "step " << step;
    }
}

static std::vector<WalkStep> nativeOwnershipWalk(const QPI::Asset& asset, const QPI::AssetOwnershipSelect& select)
{
    std::vector<WalkStep> steps;

    for (QPI::AssetOwnershipIterator iterator(asset, select); !iterator.reachedEnd(); iterator.next())
    {
        WalkStep step{ iterator.ownershipIndex(), NO_ASSET_INDEX, {} };
        const QPI::id owner = iterator.owner();

        copyMem(step.entry.owner, &owner, 32);
        copyMem(step.entry.possessor, &owner, 32);
        step.entry.shares = iterator.numberOfOwnedShares();
        step.entry.ownershipManagingContract = iterator.ownershipManagingContract();
        steps.push_back(step);
    }
    return steps;
}

static std::vector<WalkStep> hostOwnershipWalk(const QPI::Asset& asset, const QPI::AssetOwnershipSelect& select)
{
    std::vector<WalkStep> steps;
    unsigned int issuanceIdx = 0;
    unsigned int ownershipIdx = 0;

    Wasm::Runtime::assetIterBegin(nullptr, 0, &asset, &select, &select, &issuanceIdx, &ownershipIdx, nullptr);
    while (ownershipIdx != NO_ASSET_INDEX)
    {
        WalkStep step{ ownershipIdx, NO_ASSET_INDEX, {} };

        Wasm::Runtime::assetIterRecord(nullptr, 0, ownershipIdx, NO_ASSET_INDEX, &step.entry);
        steps.push_back(step);
        Wasm::Runtime::assetIterNext(nullptr, 0, &asset, &select, &select, &issuanceIdx, &ownershipIdx, nullptr);
    }
    return steps;
}

static std::vector<WalkStep> nativePossessionWalk(const QPI::Asset& asset, const QPI::AssetOwnershipSelect& ownership, const QPI::AssetPossessionSelect& possession)
{
    std::vector<WalkStep> steps;

    for (QPI::AssetPossessionIterator iterator(asset, ownership, possession); !iterator.reachedEnd(); iterator.next())
    {
        WalkStep step{ iterator.ownershipIndex(), iterator.possessionIndex(), {} };
        const QPI::id owner = iterator.owner();
        const QPI::id possessor = iterator.possessor();

        copyMem(step.entry.owner, &owner, 32);
        copyMem(step.entry.possessor, &possessor, 32);
        step.entry.shares = iterator.numberOfPossessedShares();
        step.entry.ownershipManagingContract = iterator.ownershipManagingContract();
        step.entry.possessionManagingContract = iterator.possessionManagingContract();
        steps.push_back(step);
    }
    return steps;
}

static std::vector<WalkStep> hostPossessionWalk(const QPI::Asset& asset, const QPI::AssetOwnershipSelect& ownership, const QPI::AssetPossessionSelect& possession)
{
    std::vector<WalkStep> steps;
    unsigned int issuanceIdx = 0;
    unsigned int ownershipIdx = 0;
    unsigned int possessionIdx = 0;

    Wasm::Runtime::assetIterBegin(nullptr, 1, &asset, &ownership, &possession, &issuanceIdx, &ownershipIdx, &possessionIdx);
    while (possessionIdx != NO_ASSET_INDEX)
    {
        WalkStep step{ ownershipIdx, possessionIdx, {} };

        Wasm::Runtime::assetIterRecord(nullptr, 1, ownershipIdx, possessionIdx, &step.entry);
        steps.push_back(step);
        Wasm::Runtime::assetIterNext(nullptr, 1, &asset, &ownership, &possession, &issuanceIdx, &ownershipIdx, &possessionIdx);
    }
    return steps;
}

// Two issuances, each split across a different number of holders.
static void seedUniverse(QPI::Asset assets2[2])
{
    const char* names[2] = { "ALPHA", "BETA" };

    for (int i = 0; i < 2; ++i)
    {
        assets2[i].issuer = QPI::id(100 + i, 2, 3, 4);
        assets2[i].assetName = assetNameFromString(names[i]);

        int issuanceIdx = -1;
        int firstOwnershipIdx = -1;
        int firstPossessionIdx = -1;
        ASSERT_EQ(issueAsset(assets2[i].issuer, names[i], 0, CONTRACT_ASSET_UNIT_OF_MEASUREMENT, 10000, 1, &issuanceIdx, &firstOwnershipIdx, &firstPossessionIdx), 10000);

        for (int holder = 1; holder <= 3 + i; ++holder)
        {
            int destinationOwnershipIdx = -1;
            int destinationPossessionIdx = -1;
            ASSERT_TRUE(transferShareOwnershipAndPossession(firstOwnershipIdx, firstPossessionIdx, QPI::id(10 * holder, 9, 8, 7), 500 * holder,
                &destinationOwnershipIdx, &destinationPossessionIdx, false));
        }
    }
}

TEST(WasmAssetIterator, OwnershipWalkMatchesNativeIterator)
{
    WasmAssetIteratorUniverse universe;
    QPI::Asset assets2[2];
    seedUniverse(assets2);

    const QPI::AssetOwnershipSelect selects[] = {
        QPI::AssetOwnershipSelect::any(),
        QPI::AssetOwnershipSelect::byOwner(QPI::id(20, 9, 8, 7)),
        QPI::AssetOwnershipSelect::byOwner(QPI::id(77, 9, 8, 7)),
        QPI::AssetOwnershipSelect::byManagingContract(1),
        QPI::AssetOwnershipSelect::byManagingContract(2),
    };
    for (const QPI::Asset& asset : assets2)
    {
        for (const QPI::AssetOwnershipSelect& select : selects)
        {
            expectSameWalk(nativeOwnershipWalk(asset, select), hostOwnershipWalk(asset, select));
        }
    }
    EXPECT_EQ(hostOwnershipWalk(assets2[0], selects[0]).size(), 4u);

    const QPI::Asset unknown{ QPI::id(9, 9, 9, 9), assetNameFromString("NONE") };
    EXPECT_TRUE(hostOwnershipWalk(unknown, selects[0]).empty());
}

TEST(WasmAssetIterator, PossessionWalkMatchesNativeIterator)
{
    WasmAssetIteratorUniverse universe;
    QPI::Asset assets2[2];
    seedUniverse(assets2);

    const QPI::AssetOwnershipSelect ownerships[] = { QPI::AssetOwnershipSelect::any(), QPI::AssetOwnershipSelect::byOwner(QPI::id(30, 9, 8, 7)) };
    const QPI::AssetPossessionSelect possessions[] = {
        QPI::AssetPossessionSelect::any(),
        QPI::AssetPossessionSelect::byPossessor(QPI::id(10, 9, 8, 7)),
        QPI::AssetPossessionSelect::byManagingContract(1),
    };
    for (const QPI::Asset& asset : assets2)
    {
        for (const QPI::AssetOwnershipSelect& ownership : ownerships)
        {
            for (const QPI::AssetPossessionSelect& possession : possessions)
            {
                expectSameWalk(nativePossessionWalk(asset, ownership, possession), hostPossessionWalk(asset, ownership, possession));
            }
        }
    }
    EXPECT_EQ(hostPossessionWalk(assets2[1], ownerships[0], possessions[0]).size(), 5u);
}

TEST(WasmAssetIterator, NestedWalksDoNotShareState)
{
    WasmAssetIteratorUniverse universe;
    QPI::Asset assets2[2];
    seedUniverse(assets2);

    const QPI::AssetOwnershipSelect any = QPI::AssetOwnershipSelect::any();
    const std::vector<WalkStep> outerNative = nativeOwnershipWalk(assets2[0], any);
    const size_t innerCount = nativeOwnershipWalk(assets2[1], any).size();
    unsigned int issuanceIdx = 0;
    unsigned int ownershipIdx = 0;
    size_t pairs = 0;
    size_t outerStep = 0;

    Wasm::Runtime::assetIterBegin(nullptr, 0, &assets2[0], &any, &any, &issuanceIdx, &ownershipIdx, nullptr);
    while (ownershipIdx != NO_ASSET_INDEX)
    {
        pairs += hostOwnershipWalk(assets2[1], any).size();

        Wasm::AssetEntry entry;
        Wasm::Runtime::assetIterRecord(nullptr, 0, ownershipIdx, NO_ASSET_INDEX, &entry);
        ASSERT_LT(outerStep, outerNative.size());
        EXPECT_EQ(memcmp(&entry, &outerNative[outerStep].entry, sizeof(entry)), 0) << "outer step " << outerStep;

        Wasm::Runtime::assetIterNext(nullptr, 0, &assets2[0], &any, &any, &issuanceIdx, &ownershipIdx, nullptr);
        ++outerStep;
    }
    EXPECT_EQ(outerStep, outerNative.size());
    EXPECT_EQ(pairs, outerNative.size() * innerCount);
}

TEST(WasmAssetIterator, IndicesOutsideTheUniverseEndTheWalk)
{
    WasmAssetIteratorUniverse universe;
    QPI::Asset assets2[2];
    seedUniverse(assets2);

    const QPI::AssetOwnershipSelect any = QPI::AssetOwnershipSelect::any();
    const QPI::AssetPossessionSelect anyPossession = QPI::AssetPossessionSelect::any();
    unsigned int issuanceIdx = 0;
    unsigned int ownershipIdx = 0;
    Wasm::Runtime::assetIterBegin(nullptr, 0, &assets2[0], &any, &any, &issuanceIdx, &ownershipIdx, nullptr);
    ASSERT_NE(ownershipIdx, NO_ASSET_INDEX);

    unsigned int corruptOwnershipIdx = ASSETS_CAPACITY + 5;
    EXPECT_EQ(Wasm::Runtime::assetIterNext(nullptr, 0, &assets2[0], &any, &any, &issuanceIdx, &corruptOwnershipIdx, nullptr), 0u);
    EXPECT_EQ(corruptOwnershipIdx, NO_ASSET_INDEX);

    unsigned int corruptIssuanceIdx = NO_ASSET_INDEX;
    unsigned int liveOwnershipIdx = ownershipIdx;
    EXPECT_EQ(Wasm::Runtime::assetIterNext(nullptr, 0, &assets2[0], &any, &any, &corruptIssuanceIdx, &liveOwnershipIdx, nullptr), 0u);
    EXPECT_EQ(liveOwnershipIdx, NO_ASSET_INDEX);

    // A possession walk that already ended has no ownership to continue from and stays ended.
    unsigned int endedOwnershipIdx = NO_ASSET_INDEX;
    unsigned int endedPossessionIdx = NO_ASSET_INDEX;
    EXPECT_EQ(Wasm::Runtime::assetIterNext(nullptr, 1, &assets2[0], &any, &anyPossession, &issuanceIdx, &endedOwnershipIdx, &endedPossessionIdx), 0u);
    EXPECT_EQ(endedOwnershipIdx, NO_ASSET_INDEX);
    EXPECT_EQ(endedPossessionIdx, NO_ASSET_INDEX);

    Wasm::AssetEntry entry;
    memset(&entry, 0xab, sizeof(entry));
    Wasm::Runtime::assetIterRecord(nullptr, 0, ASSETS_CAPACITY + 5, NO_ASSET_INDEX, &entry);
    const Wasm::AssetEntry zero{};
    EXPECT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);

    memset(&entry, 0xab, sizeof(entry));
    Wasm::Runtime::assetIterRecord(nullptr, 1, ownershipIdx, ownershipIdx, &entry);
    EXPECT_EQ(memcmp(&entry, &zero, sizeof(entry)), 0);
}

#pragma once
// Host-side deployment and QPI adapters for runtime Wasm contracts.
#ifdef LITE_WASM_SC

#include "extensions/wasm/lite_dyn_abi.h"
#include "extensions/wasm/lite_oracle_bridge.h"

#ifdef _MSC_VER
// The Windows SDK defines a SAL macro that conflicts with the QPI method.
#undef __transfer
#endif

#ifndef LITE_DYN_MAX_MODULE
#define LITE_DYN_MAX_MODULE (4u * 1024u * 1024u)
#endif

// Must match the LITEDYN0..N block in contract_def.h.
#define LITE_DYN_SLOT_COUNT 4

void logToConsole(const CHAR16* message);

static void liteHostLogBytes(
    unsigned int contractIndex,
    unsigned char type,
    const void* message,
    unsigned int size)
{
    *((unsigned int*)(void*)message) = contractIndex;
    qLogger::logMessage(size, type, message);
}

static unsigned int liteHostEnumerateAssets(
    const void*,
    unsigned int kind,
    const void* issuance,
    const void* ownership,
    const void* possession,
    void* outputBuffer,
    unsigned int capacity)
{
    LiteAssetEntry* output = (LiteAssetEntry*)outputBuffer;
    unsigned int count = 0;

    if (kind == 1)
    {
        QPI::AssetPossessionIterator iterator(
            *(const QPI::Asset*)issuance,
            *(const QPI::AssetOwnershipSelect*)ownership,
            *(const QPI::AssetPossessionSelect*)possession);

        while (!iterator.reachedEnd() && count < capacity)
        {
            QPI::id owner = iterator.owner();
            QPI::id possessor = iterator.possessor();

            copyMem(output[count].owner, &owner, 32);
            copyMem(output[count].possessor, &possessor, 32);
            output[count].shares = iterator.numberOfPossessedShares();
            output[count].ownershipManagingContract = iterator.ownershipManagingContract();
            output[count].possessionManagingContract = 0;

            iterator.next();
            count++;
        }
    }
    else
    {
        QPI::AssetOwnershipIterator iterator(
            *(const QPI::Asset*)issuance,
            *(const QPI::AssetOwnershipSelect*)ownership);

        while (!iterator.reachedEnd() && count < capacity)
        {
            QPI::id owner = iterator.owner();

            copyMem(output[count].owner, &owner, 32);
            copyMem(output[count].possessor, &owner, 32);
            output[count].shares = iterator.numberOfOwnedShares();
            output[count].ownershipManagingContract = iterator.ownershipManagingContract();
            output[count].possessionManagingContract = 0;

            iterator.next();
            count++;
        }
    }

    return count;
}

static int liteHostCallFunction(
    const void* callerContext,
    unsigned int contractIndex,
    unsigned short inputType,
    const void* input,
    unsigned int,
    void* output,
    unsigned int)
{
    if (contractIndex >= contractCount || !contractUserFunctions[contractIndex][inputType])
    {
        return (int)QPI::CallErrorContractInactive;
    }

    auto* caller = (QPI::QpiContextFunctionCall*)callerContext;
    QPI::InterContractCallError error = QPI::NoCallError;
    const QPI::QpiContextFunctionCall* calleeContext =
        caller->__qpiConstructContextOtherContractFunctionCall(contractIndex, error);
    if (!calleeContext)
    {
        return (int)error;
    }

    void* state = caller->__qpiAcquireStateForReading(contractIndex);
    void* locals = caller->__qpiAllocLocals(
        contractUserFunctionLocalsSizes[contractIndex][inputType]);

    contractUserFunctions[contractIndex][inputType](
        *calleeContext,
        state,
        (void*)input,
        output,
        locals);

    caller->__qpiFreeLocals();
    caller->__qpiReleaseStateForReading(contractIndex);
    caller->__qpiFreeContext();
    return (int)QPI::NoCallError;
}

static int liteHostInvokeProcedure(
    const void* callerContext,
    unsigned int contractIndex,
    unsigned short inputType,
    const void* input,
    unsigned int,
    void* output,
    unsigned int,
    long long invocationReward)
{
    if (contractIndex >= contractCount || !contractUserProcedures[contractIndex][inputType])
    {
        return (int)QPI::CallErrorContractInactive;
    }

    auto* caller = (QPI::QpiContextProcedureCall*)callerContext;
    QPI::InterContractCallError error = QPI::NoCallError;
    const QPI::QpiContextProcedureCall* calleeContext =
        caller->__qpiConstructProcedureCallContext(contractIndex, invocationReward, error, false);
    if (!calleeContext)
    {
        return (int)error;
    }

    void* state = caller->__qpiAcquireStateForWriting(contractIndex);
    void* locals = caller->__qpiAllocLocals(
        contractUserProcedureLocalsSizes[contractIndex][inputType]);

    contractUserProcedures[contractIndex][inputType](
        *calleeContext,
        state,
        (void*)input,
        output,
        locals);

    caller->__qpiFreeLocals();
    caller->__qpiReleaseStateForWriting(contractIndex);
    caller->__qpiFreeContext();
    return (int)QPI::NoCallError;
}

static unsigned short liteHostSetShareholderProposal(
    const void* context,
    unsigned int contractIndex,
    const void* proposal,
    long long invocationReward)
{
    return ((QPI::QpiContextProcedureCall*)context)->setShareholderProposal(
        (unsigned short)contractIndex,
        *(const QPI::Array<QPI::uint8, 1024>*)proposal,
        invocationReward);
}

static unsigned char liteHostSetShareholderVotes(
    const void* context,
    unsigned int contractIndex,
    const void* voteData,
    unsigned int,
    long long invocationReward)
{
    return (unsigned char)((QPI::QpiContextProcedureCall*)context)->setShareholderVotes(
        (unsigned short)contractIndex,
        *(const QPI::ProposalMultiVoteDataV1*)voteData,
        invocationReward);
}

static QPI::QpiContextFunctionCall* liteHostFunctionContext(const void* context)
{
    return (QPI::QpiContextFunctionCall*)context;
}

static QPI::QpiContextProcedureCall* liteHostProcedureContext(const void* context)
{
    return (QPI::QpiContextProcedureCall*)context;
}

static void liteHostBeginFunction(unsigned int id)
{
    __beginFunctionOrProcedure(id);
}

static void liteHostEndFunction(unsigned int id)
{
    __endFunctionOrProcedure(id);
}

static void liteHostMarkDirty(unsigned int contractIndex)
{
    __markContractStateDirty(contractIndex);
}

static void liteHostPauseLog()
{
    __pauseLogMessage();
}

static void liteHostResumeLog()
{
    __resumeLogMessage();
}

static void* liteHostAcquireScratch(
    unsigned long long size,
    bool initializeToZero)
{
    return __acquireScratchpad(size, initializeToZero);
}

static void liteHostReleaseScratch(void* pointer)
{
    __releaseScratchpad(pointer);
}

static void liteHostK12(
    const void* input,
    unsigned int length,
    void* output)
{
    KangarooTwelve(input, length, output, 32);
}

static long long liteHostTransfer(
    const void* context,
    const void* destination,
    long long amount)
{
    return liteHostProcedureContext(context)->transfer(
        *(const m256i*)destination,
        amount);
}

static long long liteHostTransferTyped(
    const void* context,
    const void* destination,
    long long amount,
    unsigned char transferType)
{
    return liteHostProcedureContext(context)->__transfer(
        *(const m256i*)destination,
        amount,
        transferType);
}

static void liteHostAbort(const void* context, unsigned int errorCode)
{
    liteHostProcedureContext(context)->__qpiAbort(errorCode);
}

static long long liteHostBurn(
    const void* context,
    long long amount,
    unsigned int contractIndex)
{
    return liteHostProcedureContext(context)->burn(amount, contractIndex);
}

static unsigned short liteHostEpoch(const void* context)
{
    return liteHostFunctionContext(context)->epoch();
}

static unsigned int liteHostTick(const void* context)
{
    return liteHostFunctionContext(context)->tick();
}

static int liteHostNumberOfTickTransactions(const void* context)
{
    return liteHostFunctionContext(context)->numberOfTickTransactions();
}

static unsigned char liteHostGetEntity(
    const void* context,
    const void* id,
    void* entity)
{
    return (unsigned char)liteHostFunctionContext(context)->getEntity(
        *(const m256i*)id,
        *(QPI::Entity*)entity);
}

static long long liteHostQueryFeeReserve(
    const void* context,
    unsigned int contractIndex)
{
    return liteHostFunctionContext(context)->queryFeeReserve(contractIndex);
}

static void liteHostNextId(const void* context, const void* id, void* output)
{
    *(m256i*)output = liteHostFunctionContext(context)->nextId(*(const m256i*)id);
}

static void liteHostPreviousId(const void* context, const void* id, void* output)
{
    *(m256i*)output = liteHostFunctionContext(context)->prevId(*(const m256i*)id);
}

static unsigned char liteHostIsContractId(const void* context, const void* id)
{
    return (unsigned char)liteHostFunctionContext(context)->isContractId(
        *(const m256i*)id);
}

static void liteHostArbitrator(const void* context, void* output)
{
    *(m256i*)output = liteHostFunctionContext(context)->arbitrator();
}

static void liteHostComputor(
    const void* context,
    unsigned short index,
    void* output)
{
    *(m256i*)output = liteHostFunctionContext(context)->computor(index);
}

static unsigned char liteHostDay(const void* context)
{
    return liteHostFunctionContext(context)->day();
}

static unsigned char liteHostYear(const void* context)
{
    return liteHostFunctionContext(context)->year();
}

static unsigned char liteHostHour(const void* context)
{
    return liteHostFunctionContext(context)->hour();
}

static unsigned char liteHostMinute(const void* context)
{
    return liteHostFunctionContext(context)->minute();
}

static unsigned char liteHostMonth(const void* context)
{
    return liteHostFunctionContext(context)->month();
}

static unsigned char liteHostSecond(const void* context)
{
    return liteHostFunctionContext(context)->second();
}

static unsigned short liteHostMillisecond(const void* context)
{
    return liteHostFunctionContext(context)->millisecond();
}

static void liteHostNow(const void* context, void* output)
{
    *(QPI::DateAndTime*)output = liteHostFunctionContext(context)->now();
}

static void liteHostPreviousSpectrumDigest(const void* context, void* output)
{
    *(m256i*)output = liteHostFunctionContext(context)->getPrevSpectrumDigest();
}

static void liteHostPreviousUniverseDigest(const void* context, void* output)
{
    *(m256i*)output = liteHostFunctionContext(context)->getPrevUniverseDigest();
}

static void liteHostPreviousComputerDigest(const void* context, void* output)
{
    *(m256i*)output = liteHostFunctionContext(context)->getPrevComputerDigest();
}

static unsigned char liteHostIsAssetIssued(
    const void* context,
    const void* issuer,
    unsigned long long name)
{
    return (unsigned char)liteHostFunctionContext(context)->isAssetIssued(
        *(const m256i*)issuer,
        name);
}

static long long liteHostIssueAsset(
    const void* context,
    unsigned long long name,
    const void* issuer,
    signed char decimals,
    long long shares,
    unsigned long long unit)
{
    return liteHostProcedureContext(context)->issueAsset(
        name,
        *(const QPI::id*)issuer,
        decimals,
        shares,
        unit);
}

static long long liteHostNumberOfShares(
    const void* context,
    const void* asset,
    const void* ownership,
    const void* possession)
{
    return liteHostFunctionContext(context)->numberOfShares(
        *(const QPI::Asset*)asset,
        *(const QPI::AssetOwnershipSelect*)ownership,
        *(const QPI::AssetPossessionSelect*)possession);
}

static long long liteHostNumberOfPossessedShares(
    const void* context,
    unsigned long long name,
    const void* issuer,
    const void* owner,
    const void* possessor,
    unsigned short ownershipManagement,
    unsigned short possessionManagement)
{
    return liteHostFunctionContext(context)->numberOfPossessedShares(
        name,
        *(const m256i*)issuer,
        *(const m256i*)owner,
        *(const m256i*)possessor,
        ownershipManagement,
        possessionManagement);
}

static long long liteHostTransferShareOwnershipAndPossession(
    const void* context,
    unsigned long long name,
    const void* issuer,
    const void* owner,
    const void* possessor,
    long long shares,
    const void* newOwner)
{
    return liteHostProcedureContext(context)->transferShareOwnershipAndPossession(
        name,
        *(const m256i*)issuer,
        *(const m256i*)owner,
        *(const m256i*)possessor,
        shares,
        *(const m256i*)newOwner);
}

static long long liteHostAcquireShares(
    const void* context,
    unsigned long long name,
    const void* issuer,
    const void* owner,
    const void* possessor,
    long long shares,
    unsigned short sourceOwnershipManagement,
    unsigned short sourcePossessionManagement,
    long long fee)
{
    return liteHostProcedureContext(context)->acquireShares(
        QPI::Asset{ *(const m256i*)issuer, name },
        *(const m256i*)owner,
        *(const m256i*)possessor,
        shares,
        sourceOwnershipManagement,
        sourcePossessionManagement,
        fee);
}

static long long liteHostReleaseShares(
    const void* context,
    unsigned long long name,
    const void* issuer,
    const void* owner,
    const void* possessor,
    long long shares,
    unsigned short destinationOwnershipManagement,
    unsigned short destinationPossessionManagement,
    long long fee)
{
    return liteHostProcedureContext(context)->releaseShares(
        QPI::Asset{ *(const m256i*)issuer, name },
        *(const m256i*)owner,
        *(const m256i*)possessor,
        shares,
        destinationOwnershipManagement,
        destinationPossessionManagement,
        fee);
}

static unsigned char liteHostDayOfWeek(
    const void* context,
    unsigned char year,
    unsigned char month,
    unsigned char day)
{
    return liteHostFunctionContext(context)->dayOfWeek(year, month, day);
}

static unsigned char liteHostSignatureValidity(
    const void* context,
    const void* entity,
    const void* digest,
    const void* signature)
{
    return (unsigned char)liteHostFunctionContext(context)->signatureValidity(
        *(const m256i*)entity,
        *(const m256i*)digest,
        *(const QPI::Array<QPI::sint8, 64>*)signature);
}

static long long liteHostBidInIPO(
    const void* context,
    unsigned int contractIndex,
    long long price,
    unsigned int quantity)
{
    return liteHostProcedureContext(context)->bidInIPO(
        contractIndex,
        price,
        quantity);
}

static void liteHostIpoBidId(
    const void* context,
    unsigned int contractIndex,
    unsigned int bidIndex,
    void* output)
{
    *(m256i*)output = liteHostFunctionContext(context)->ipoBidId(
        contractIndex,
        bidIndex);
}

static long long liteHostIpoBidPrice(
    const void* context,
    unsigned int contractIndex,
    unsigned int bidIndex)
{
    return liteHostFunctionContext(context)->ipoBidPrice(contractIndex, bidIndex);
}

static void liteHostComputeMiningFunction(
    const void* context,
    const void* seed,
    const void* publicKey,
    const void* nonce,
    void* output)
{
    *(m256i*)output = liteHostFunctionContext(context)->computeMiningFunction(
        *(const m256i*)seed,
        *(const m256i*)publicKey,
        *(const m256i*)nonce);
}

static void liteHostInitMiningSeed(const void* context, const void* seed)
{
    liteHostFunctionContext(context)->initMiningSeed(*(const m256i*)seed);
}

static unsigned char liteHostGetOracleQueryStatus(
    const void* context,
    long long queryId)
{
    return liteHostFunctionContext(context)->getOracleQueryStatus(queryId);
}

static unsigned char liteHostUnsubscribeOracle(
    const void* context,
    int subscriptionId)
{
    return (unsigned char)liteHostProcedureContext(context)->unsubscribeOracle(
        subscriptionId);
}

static unsigned char liteHostDistributeDividends(
    const void* context,
    long long amountPerShare)
{
    return (unsigned char)liteHostProcedureContext(context)->distributeDividends(
        amountPerShare);
}

// Designated initialization keeps member/order drift a compile-time error.
static LiteHostServices g_liteHostServices =
{
    .abiVersion = LITE_DYN_ABI_VERSION,
    .beginFn = &liteHostBeginFunction,
    .endFn = &liteHostEndFunction,
    .markDirty = &liteHostMarkDirty,
    .pauseLog = &liteHostPauseLog,
    .resumeLog = &liteHostResumeLog,
    .acquireScratch = &liteHostAcquireScratch,
    .releaseScratch = &liteHostReleaseScratch,
    .logBytes = &liteHostLogBytes,
    .k12 = &liteHostK12,
    .transfer = &liteHostTransfer,
    .transferTyped = &liteHostTransferTyped,
    .abort = &liteHostAbort,
    .burn = &liteHostBurn,
    .epoch = &liteHostEpoch,
    .tick = &liteHostTick,
    .numberOfTickTransactions = &liteHostNumberOfTickTransactions,
    .getEntity = &liteHostGetEntity,
    .queryFeeReserve = &liteHostQueryFeeReserve,
    .nextId = &liteHostNextId,
    .prevId = &liteHostPreviousId,
    .isContractId = &liteHostIsContractId,
    .arbitrator = &liteHostArbitrator,
    .computor = &liteHostComputor,
    .day = &liteHostDay,
    .year = &liteHostYear,
    .hour = &liteHostHour,
    .minute = &liteHostMinute,
    .month = &liteHostMonth,
    .second = &liteHostSecond,
    .millisecond = &liteHostMillisecond,
    .now = &liteHostNow,
    .prevSpectrumDigest = &liteHostPreviousSpectrumDigest,
    .prevUniverseDigest = &liteHostPreviousUniverseDigest,
    .prevComputerDigest = &liteHostPreviousComputerDigest,
    .isAssetIssued = &liteHostIsAssetIssued,
    .issueAsset = &liteHostIssueAsset,
    .numberOfShares = &liteHostNumberOfShares,
    .numberOfPossessedShares = &liteHostNumberOfPossessedShares,
    .assetEnumerate = &liteHostEnumerateAssets,
    .transferShareOwnershipAndPossession = &liteHostTransferShareOwnershipAndPossession,
    .acquireShares = &liteHostAcquireShares,
    .releaseShares = &liteHostReleaseShares,
    .dayOfWeek = &liteHostDayOfWeek,
    .signatureValidity = &liteHostSignatureValidity,
    .bidInIPO = &liteHostBidInIPO,
    .ipoBidId = &liteHostIpoBidId,
    .ipoBidPrice = &liteHostIpoBidPrice,
    .computeMiningFunction = &liteHostComputeMiningFunction,
    .initMiningSeed = &liteHostInitMiningSeed,
    .getOracleQueryStatus = &liteHostGetOracleQueryStatus,
    .unsubscribeOracle = &liteHostUnsubscribeOracle,
    .queryOracle = &liteWasmQueryOracle,
    .subscribeOracle = &liteWasmSubscribeOracle,
    .getOracleQuery = &liteWasmGetOracleQuery,
    .getOracleReply = &liteWasmGetOracleReply,
    .distributeDividends = &liteHostDistributeDividends,
    .liteCallFunction = &liteHostCallFunction,
    .liteInvokeProcedure = &liteHostInvokeProcedure,
    .setShareholderProposal = &liteHostSetShareholderProposal,
    .setShareholderVotes = &liteHostSetShareholderVotes,
};

struct LiteDynSlot
{
    bool armed = false;
    bool constructed = false;
    bool everInitialized = false;
    bool needsMigrate = false;
    unsigned char codeHash[32] = {};
    unsigned int activationTick = 0;
    unsigned int version = 0;
    char name[32] = {};
    std::string sourceH;
};

static LiteDynSlot g_liteDynSlots[LITE_DYN_SLOT_COUNT];

struct LiteDynUpload
{
    bool active = false;
    unsigned long long sessionId = 0;
    unsigned int totalSize = 0;
    unsigned int chunkCount = 0;
    unsigned int receivedCount = 0;
    unsigned char finalHash[32] = {};
};

static LiteDynUpload g_liteDynUpload;
static unsigned char g_liteDynBuf[LITE_DYN_MAX_MODULE];
static unsigned char g_liteDynSeqSeen[(LITE_DYN_MAX_MODULE / 1008u) / 8u + 1u];

static inline unsigned int liteDynSlotBase()
{
    return LITEDYN0_CONTRACT_INDEX;
}

static inline int liteDynSlotLocal(unsigned int contractIndex)
{
    const int slotOffset = (int)contractIndex - (int)LITEDYN0_CONTRACT_INDEX;
    if (slotOffset < 0 || slotOffset >= (int)LITE_DYN_SLOT_COUNT)
    {
        return -1;
    }

    return slotOffset;
}

[[maybe_unused]] static void liteDynOnUploadBegin(
    unsigned long long sessionId,
    unsigned int totalSize,
    unsigned int chunkCount,
    const unsigned char* finalHash)
{
    if (totalSize > LITE_DYN_MAX_MODULE)
    {
        return;
    }

    g_liteDynUpload.active = true;
    g_liteDynUpload.sessionId = sessionId;
    g_liteDynUpload.totalSize = totalSize;
    g_liteDynUpload.chunkCount = chunkCount;
    g_liteDynUpload.receivedCount = 0;
    copyMem(g_liteDynUpload.finalHash, finalHash, 32);
    setMem(g_liteDynSeqSeen, sizeof(g_liteDynSeqSeen), 0);
    logToConsole(L"LITEDYN: UploadBegin received");
}

[[maybe_unused]] static void liteDynOnUploadChunk(
    unsigned long long sessionId,
    unsigned int sequence,
    const unsigned char* data,
    unsigned int dataLength)
{
    if (!g_liteDynUpload.active || sessionId != g_liteDynUpload.sessionId)
    {
        return;
    }

    const unsigned long long destinationOffset = (unsigned long long)sequence * 1008ull;
    if (destinationOffset + dataLength > LITE_DYN_MAX_MODULE)
    {
        return;
    }

    if (sequence >= g_liteDynUpload.chunkCount)
    {
        return;
    }

    const unsigned int sequenceByte = sequence >> 3;
    const unsigned int sequenceBit = 1u << (sequence & 7);
    if (!(g_liteDynSeqSeen[sequenceByte] & sequenceBit))
    {
        g_liteDynSeqSeen[sequenceByte] |= sequenceBit;
        g_liteDynUpload.receivedCount++;
    }

    copyMem(g_liteDynBuf + destinationOffset, data, dataLength);
}

static bool liteDynUploadComplete()
{
    if (!g_liteDynUpload.active
        || g_liteDynUpload.receivedCount != g_liteDynUpload.chunkCount)
    {
        return false;
    }

    unsigned char calculatedHash[32];

    KangarooTwelve(g_liteDynBuf, g_liteDynUpload.totalSize, calculatedHash, 32);
    for (int index = 0; index < 32; index++)
    {
        if (calculatedHash[index] != g_liteDynUpload.finalHash[index])
        {
            return false;
        }
    }

    return true;
}

// Defined by lite_wasm_contracts.h later in the same translation unit.
static bool liteWasmLoadFromBytes(
    unsigned int contractIndex,
    const unsigned char* bytes,
    unsigned int length);
static bool liteWasmIsWasm(unsigned int contractIndex);
static bool liteWasmHasPendingMigrate(unsigned int contractIndex);
static void liteWasmRunPendingMigrate(unsigned int contractIndex);

[[maybe_unused]] static void liteDynOnDeploy(
    unsigned long long sessionId,
    unsigned int targetSlot,
    const unsigned char* finalHash,
    unsigned int /*abiVersion*/,
    unsigned int /*stateLayoutVersion*/,
    const char* name)
{
    const int slotOffset = liteDynSlotLocal(targetSlot);
    if (slotOffset < 0)
    {
        return;
    }

    if (sessionId != g_liteDynUpload.sessionId || !liteDynUploadComplete())
    {
        return;
    }

    for (int index = 0; index < 32; index++)
    {
        if (finalHash[index] != g_liteDynUpload.finalHash[index])
        {
            return;
        }
    }

    LiteDynSlot& slot = g_liteDynSlots[slotOffset];

    copyMem(slot.codeHash, finalHash, 32);
    if (name)
    {
        copyMem(slot.name, name, 32);
        slot.name[31] = 0;
    }

    slot.armed = true;
    logToConsole(L"LITEDYN: Deploy accepted, slot armed");
    slot.constructed = slot.everInitialized;
    slot.version++;

    bool loadOk = false;
    const unsigned char* artifact = g_liteDynBuf;
    const bool hasWasmMagic = g_liteDynUpload.totalSize >= 4
        && artifact[0] == 0x00
        && artifact[1] == 0x61
        && artifact[2] == 0x73
        && artifact[3] == 0x6d;

    if (hasWasmMagic)
    {
        loadOk = liteWasmLoadFromBytes(targetSlot, g_liteDynBuf, g_liteDynUpload.totalSize);
        logToConsole(loadOk ? L"LITEDYN: wasm contract loaded" : L"LITEDYN: ERROR wasm load failed");
    }
    else
    {
        logToConsole(L"LITEDYN: ERROR upload is not a wasm module ('\\0asm' expected)");
    }

    if (!loadOk)
    {
        logToConsole(L"LITEDYN: ERROR load failed - slot armed but not runnable");
    }

    if (loadOk && liteWasmHasPendingMigrate(targetSlot))
    {
        slot.needsMigrate = true;
        logToConsole(L"LITEDYN: migrate scheduled for next tick");
    }

    g_liteDynUpload.active = false;
}

// These input types and offsets are part of the @qinit/proto deployment wire format.
#define LITE_TX_UPLOAD_BEGIN 240
#define LITE_TX_UPLOAD_CHUNK 241
#define LITE_TX_DEPLOY 242

namespace LiteDynWire
{
constexpr unsigned int SessionIdOffset = 0;
constexpr unsigned int UploadTotalSizeOffset = 8;
constexpr unsigned int UploadChunkCountOffset = 12;
constexpr unsigned int UploadHashOffset = 16;
constexpr unsigned int UploadBeginSize = 48;
constexpr unsigned int ChunkSequenceOffset = 8;
constexpr unsigned int ChunkLengthOffset = 12;
constexpr unsigned int ChunkDataOffset = 14;
constexpr unsigned int ChunkHeaderSize = 14;
constexpr unsigned int DeploySlotOffset = 8;
constexpr unsigned int DeployHashOffset = 12;
constexpr unsigned int DeployAbiVersionOffset = 44;
constexpr unsigned int DeployStateLayoutVersionOffset = 48;
constexpr unsigned int DeployNameOffset = 52;
constexpr unsigned int DeployBaseSize = 52;
constexpr unsigned int DeployNamedSize = 84;
}

static unsigned long long liteDynReadU64(const unsigned char* input, unsigned int offset)
{
    unsigned long long value = 0;

    for (int byteIndex = 0; byteIndex < 8; byteIndex++)
    {
        value |= (unsigned long long)input[offset + byteIndex] << (8 * byteIndex);
    }

    return value;
}

static unsigned int liteDynReadU32(const unsigned char* input, unsigned int offset)
{
    unsigned int value = 0;

    for (int byteIndex = 0; byteIndex < 4; byteIndex++)
    {
        value |= (unsigned int)input[offset + byteIndex] << (8 * byteIndex);
    }

    return value;
}

static unsigned int liteDynReadU16(const unsigned char* input, unsigned int offset)
{
    return (unsigned int)input[offset] | ((unsigned int)input[offset + 1] << 8);
}

[[maybe_unused]] static void liteDynDispatchTx(
    unsigned short inputType,
    const unsigned char* input,
    unsigned int size)
{
    if (inputType == LITE_TX_UPLOAD_BEGIN)
    {
        if (size < LiteDynWire::UploadBeginSize)
        {
            return;
        }

        liteDynOnUploadBegin(
            liteDynReadU64(input, LiteDynWire::SessionIdOffset),
            liteDynReadU32(input, LiteDynWire::UploadTotalSizeOffset),
            liteDynReadU32(input, LiteDynWire::UploadChunkCountOffset),
            input + LiteDynWire::UploadHashOffset);
    }
    else if (inputType == LITE_TX_UPLOAD_CHUNK)
    {
        if (size < LiteDynWire::ChunkHeaderSize)
        {
            return;
        }

        const unsigned int dataLength = liteDynReadU16(input, LiteDynWire::ChunkLengthOffset);
        if (LiteDynWire::ChunkDataOffset + dataLength > size)
        {
            return;
        }

        liteDynOnUploadChunk(
            liteDynReadU64(input, LiteDynWire::SessionIdOffset),
            liteDynReadU32(input, LiteDynWire::ChunkSequenceOffset),
            input + LiteDynWire::ChunkDataOffset,
            dataLength);
    }
    else if (inputType == LITE_TX_DEPLOY)
    {
        if (size < LiteDynWire::DeployBaseSize)
        {
            return;
        }

        const char* name = nullptr;
        if (size >= LiteDynWire::DeployNamedSize)
        {
            name = (const char*)(input + LiteDynWire::DeployNameOffset);
        }

        liteDynOnDeploy(
            liteDynReadU64(input, LiteDynWire::SessionIdOffset),
            liteDynReadU32(input, LiteDynWire::DeploySlotOffset),
            input + LiteDynWire::DeployHashOffset,
            liteDynReadU32(input, LiteDynWire::DeployAbiVersionOffset),
            liteDynReadU32(input, LiteDynWire::DeployStateLayoutVersionOffset),
            name);
    }
}

static bool liteDynPendingForTick(unsigned int /*tick*/)
{
    for (unsigned int slotOffset = 0; slotOffset < LITE_DYN_SLOT_COUNT; slotOffset++)
    {
        const LiteDynSlot& slot = g_liteDynSlots[slotOffset];
        if (slot.armed && (!slot.constructed || slot.needsMigrate))
        {
            return true;
        }
    }

    return false;
}

[[maybe_unused]] static void liteDynConstructPending()
{
    for (unsigned int slotOffset = 0; slotOffset < LITE_DYN_SLOT_COUNT; slotOffset++)
    {
        LiteDynSlot& slot = g_liteDynSlots[slotOffset];
        if (!slot.armed)
        {
            continue;
        }

        const unsigned int contractIndex = LITEDYN0_CONTRACT_INDEX + slotOffset;
        if (slot.needsMigrate)
        {
            liteWasmRunPendingMigrate(contractIndex);
            slot.needsMigrate = false;
            continue;
        }

        if (slot.constructed)
        {
            continue;
        }

        if (contractSystemProcedures[contractIndex][INITIALIZE])
        {
            QpiContextSystemProcedureCall qpiContext(contractIndex, INITIALIZE);

            qpiContext.call();
            slot.everInitialized = true;
            logToConsole(L"LITEDYN: slot constructed (INITIALIZE ran)");
        }
        else
        {
            logToConsole(L"LITEDYN: ERROR construct skipped - tables unpatched (load failed)");
        }

        slot.constructed = true;
    }
}

[[maybe_unused]] static void liteDynBootDeploy()
{
    logToConsole(L"LITEWASM: runtime deployment enabled for testnet lite RAM");
    logToConsole(L"LITEWASM: deploy address id(99999,0,0,0)");

    for (unsigned int slotOffset = 0; slotOffset < LITE_DYN_SLOT_COUNT; slotOffset++)
    {
        const unsigned int contractIndex = LITEDYN0_CONTRACT_INDEX + slotOffset;

        contractError[contractIndex] = NoContractError;
        if (getContractFeeReserve(contractIndex) <= 0)
        {
            setContractFeeReserve(contractIndex, 1000000000000ll);
        }
    }
}

#endif // LITE_WASM_SC

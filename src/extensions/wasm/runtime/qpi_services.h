#pragma once

// Node QPI adapters used by runtime-deployed contracts.
#ifdef LITE_WASM_SC

#include "extensions/wasm/shared/abi_types.h"

#ifdef _MSC_VER
#undef __transfer
#endif

void logToConsole(const CHAR16* message);

namespace Wasm::Runtime
{

static void logBytes(
    unsigned int contractIndex,
    unsigned char type,
    const void* message,
    unsigned int size)
{
    *((unsigned int*)(void*)message) = contractIndex;
    qLogger::logMessage(size, type, message);
}

static unsigned int enumerateAssets(
    const void*,
    unsigned int kind,
    const void* issuance,
    const void* ownership,
    const void* possession,
    void* outputBuffer,
    unsigned int capacity)
{
    AssetEntry* output = (AssetEntry*)outputBuffer;
    unsigned int count = 0;

    if (kind == 1)
    {
        QPI::AssetPossessionIterator iterator(*(const QPI::Asset*)issuance, *(const QPI::AssetOwnershipSelect*)ownership, *(const QPI::AssetPossessionSelect*)possession);

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
        QPI::AssetOwnershipIterator iterator(*(const QPI::Asset*)issuance, *(const QPI::AssetOwnershipSelect*)ownership);

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

static int callContractFunction(
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
    const QPI::QpiContextFunctionCall* calleeContext = caller->__qpiConstructContextOtherContractFunctionCall(contractIndex, error);
    if (!calleeContext)
    {
        return (int)error;
    }

    void* state = caller->__qpiAcquireStateForReading(contractIndex);
    void* locals = caller->__qpiAllocLocals(contractUserFunctionLocalsSizes[contractIndex][inputType]);

    contractUserFunctions[contractIndex][inputType](*calleeContext, state, (void*)input, output, locals);

    caller->__qpiFreeLocals();
    caller->__qpiReleaseStateForReading(contractIndex);
    caller->__qpiFreeContext();
    return (int)QPI::NoCallError;
}

static int invokeContractProcedure(
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
    const QPI::QpiContextProcedureCall* calleeContext = caller->__qpiConstructProcedureCallContext(contractIndex, invocationReward, error, false);
    if (!calleeContext)
    {
        return (int)error;
    }

    void* state = caller->__qpiAcquireStateForWriting(contractIndex);
    void* locals = caller->__qpiAllocLocals(contractUserProcedureLocalsSizes[contractIndex][inputType]);

    contractUserProcedures[contractIndex][inputType](*calleeContext, state, (void*)input, output, locals);

    caller->__qpiFreeLocals();
    caller->__qpiReleaseStateForWriting(contractIndex);
    caller->__qpiFreeContext();
    return (int)QPI::NoCallError;
}

static unsigned short setShareholderProposal(
    const void* context,
    unsigned int contractIndex,
    const void* proposal,
    long long invocationReward)
{
    return ((QPI::QpiContextProcedureCall*)context)->setShareholderProposal((unsigned short)contractIndex, *(const QPI::Array<QPI::uint8, 1024>*)proposal, invocationReward);
}

static unsigned char setShareholderVotes(
    const void* context,
    unsigned int contractIndex,
    const void* voteData,
    unsigned int,
    long long invocationReward)
{
    return (unsigned char)((QPI::QpiContextProcedureCall*)context)->setShareholderVotes((unsigned short)contractIndex, *(const QPI::ProposalMultiVoteDataV1*)voteData, invocationReward);
}

static QPI::QpiContextFunctionCall* functionContext(const void* context)
{
    return (QPI::QpiContextFunctionCall*)context;
}

static QPI::QpiContextProcedureCall* procedureContext(const void* context)
{
    return (QPI::QpiContextProcedureCall*)context;
}

static void beginFunction(unsigned int id)
{
    __beginFunctionOrProcedure(id);
}

static void endFunction(unsigned int id)
{
    __endFunctionOrProcedure(id);
}

static void markContractDirty(unsigned int contractIndex)
{
    __markContractStateDirty(contractIndex);
}

static void pauseLog()
{
    __pauseLogMessage();
}

static void resumeLog()
{
    __resumeLogMessage();
}

static void* acquireScratch(
    unsigned long long size,
    bool initializeToZero)
{
    return __acquireScratchpad(size, initializeToZero);
}

static void releaseScratch(void* pointer)
{
    __releaseScratchpad(pointer);
}

static void hashK12(
    const void* input,
    unsigned int length,
    void* output)
{
    KangarooTwelve(input, length, output, 32);
}

static long long transfer(
    const void* context,
    const void* destination,
    long long amount)
{
    return procedureContext(context)->transfer(*(const m256i*)destination, amount);
}

static long long transferTyped(
    const void* context,
    const void* destination,
    long long amount,
    unsigned char transferType)
{
    return procedureContext(context)->__transfer(*(const m256i*)destination, amount, transferType);
}

static void abortCall(const void* context, unsigned int errorCode)
{
    procedureContext(context)->__qpiAbort(errorCode);
}

static long long burn(
    const void* context,
    long long amount,
    unsigned int contractIndex)
{
    return procedureContext(context)->burn(amount, contractIndex);
}

static unsigned short epoch(const void* context)
{
    return functionContext(context)->epoch();
}

static unsigned int tick(const void* context)
{
    return functionContext(context)->tick();
}

static int numberOfTickTransactions(const void* context)
{
    return functionContext(context)->numberOfTickTransactions();
}

static unsigned char getEntity(
    const void* context,
    const void* id,
    void* entity)
{
    return (unsigned char)functionContext(context)->getEntity(*(const m256i*)id, *(QPI::Entity*)entity);
}

static long long queryFeeReserve(
    const void* context,
    unsigned int contractIndex)
{
    return functionContext(context)->queryFeeReserve(contractIndex);
}

static void nextId(const void* context, const void* id, void* output)
{
    *(m256i*)output = functionContext(context)->nextId(*(const m256i*)id);
}

static void previousId(const void* context, const void* id, void* output)
{
    *(m256i*)output = functionContext(context)->prevId(*(const m256i*)id);
}

static unsigned char isContractId(const void* context, const void* id)
{
    return (unsigned char)functionContext(context)->isContractId(*(const m256i*)id);
}

static void arbitrator(const void* context, void* output)
{
    *(m256i*)output = functionContext(context)->arbitrator();
}

static void computor(
    const void* context,
    unsigned short index,
    void* output)
{
    *(m256i*)output = functionContext(context)->computor(index);
}

static unsigned char day(const void* context)
{
    return functionContext(context)->day();
}

static unsigned char year(const void* context)
{
    return functionContext(context)->year();
}

static unsigned char hour(const void* context)
{
    return functionContext(context)->hour();
}

static unsigned char minute(const void* context)
{
    return functionContext(context)->minute();
}

static unsigned char month(const void* context)
{
    return functionContext(context)->month();
}

static unsigned char second(const void* context)
{
    return functionContext(context)->second();
}

static unsigned short millisecond(const void* context)
{
    return functionContext(context)->millisecond();
}

static void now(const void* context, void* output)
{
    *(QPI::DateAndTime*)output = functionContext(context)->now();
}

static void previousSpectrumDigest(const void* context, void* output)
{
    *(m256i*)output = functionContext(context)->getPrevSpectrumDigest();
}

static void previousUniverseDigest(const void* context, void* output)
{
    *(m256i*)output = functionContext(context)->getPrevUniverseDigest();
}

static void previousComputerDigest(const void* context, void* output)
{
    *(m256i*)output = functionContext(context)->getPrevComputerDigest();
}

static unsigned char isAssetIssued(
    const void* context,
    const void* issuer,
    unsigned long long name)
{
    return (unsigned char)functionContext(context)->isAssetIssued(*(const m256i*)issuer, name);
}

static long long issueAsset(
    const void* context,
    unsigned long long name,
    const void* issuer,
    signed char decimals,
    long long shares,
    unsigned long long unit)
{
    return procedureContext(context)->issueAsset(name, *(const QPI::id*)issuer, decimals, shares, unit);
}

static long long numberOfShares(
    const void* context,
    const void* asset,
    const void* ownership,
    const void* possession)
{
    return functionContext(context)->numberOfShares(*(const QPI::Asset*)asset, *(const QPI::AssetOwnershipSelect*)ownership, *(const QPI::AssetPossessionSelect*)possession);
}

static long long numberOfPossessedShares(
    const void* context,
    unsigned long long name,
    const void* issuer,
    const void* owner,
    const void* possessor,
    unsigned short ownershipManagement,
    unsigned short possessionManagement)
{
    return functionContext(context)->numberOfPossessedShares(name, *(const m256i*)issuer, *(const m256i*)owner, *(const m256i*)possessor, ownershipManagement, possessionManagement);
}

static long long transferShareOwnershipAndPossession(
    const void* context,
    unsigned long long name,
    const void* issuer,
    const void* owner,
    const void* possessor,
    long long shares,
    const void* newOwner)
{
    return procedureContext(context)->transferShareOwnershipAndPossession(name, *(const m256i*)issuer, *(const m256i*)owner, *(const m256i*)possessor, shares, *(const m256i*)newOwner);
}

static long long acquireShares(
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
    return procedureContext(context)->acquireShares(QPI::Asset{ *(const m256i*)issuer, name }, *(const m256i*)owner, *(const m256i*)possessor, shares, sourceOwnershipManagement, sourcePossessionManagement, fee);
}

static long long releaseShares(
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
    return procedureContext(context)->releaseShares(QPI::Asset{ *(const m256i*)issuer, name }, *(const m256i*)owner, *(const m256i*)possessor, shares, destinationOwnershipManagement, destinationPossessionManagement, fee);
}

static unsigned char dayOfWeek(
    const void* context,
    unsigned char year,
    unsigned char month,
    unsigned char day)
{
    return functionContext(context)->dayOfWeek(year, month, day);
}

static unsigned char signatureValidity(
    const void* context,
    const void* entity,
    const void* digest,
    const void* signature)
{
    return (unsigned char)functionContext(context)->signatureValidity(*(const m256i*)entity, *(const m256i*)digest, *(const QPI::Array<QPI::sint8, 64>*)signature);
}

static long long bidInIPO(
    const void* context,
    unsigned int contractIndex,
    long long price,
    unsigned int quantity)
{
    return procedureContext(context)->bidInIPO(contractIndex, price, quantity);
}

static void ipoBidId(
    const void* context,
    unsigned int contractIndex,
    unsigned int bidIndex,
    void* output)
{
    *(m256i*)output = functionContext(context)->ipoBidId(contractIndex, bidIndex);
}

static long long ipoBidPrice(
    const void* context,
    unsigned int contractIndex,
    unsigned int bidIndex)
{
    return functionContext(context)->ipoBidPrice(contractIndex, bidIndex);
}

static void computeMiningFunction(
    const void* context,
    const void* seed,
    const void* publicKey,
    const void* nonce,
    void* output)
{
    *(m256i*)output = functionContext(context)->computeMiningFunction(*(const m256i*)seed, *(const m256i*)publicKey, *(const m256i*)nonce);
}

static void initMiningSeed(const void* context, const void* seed)
{
    functionContext(context)->initMiningSeed(*(const m256i*)seed);
}

static unsigned char getOracleQueryStatus(
    const void* context,
    long long queryId)
{
    return functionContext(context)->getOracleQueryStatus(queryId);
}

static unsigned char getOcInvocationStatus(
    const void* context,
    long long invocationId)
{
    return functionContext(context)->getOcInvocationStatus(invocationId);
}

static long long invokeOc(
    const void* context,
    unsigned int interfaceIndex,
    const void* request,
    unsigned int requestSize)
{
    static_assert(OCI::ocInterfacesCount == 1, "add Wasm OC dispatch case");

    if (!context
        || !request
        || interfaceIndex >= OCI::ocInterfacesCount
        || requestSize != OCI::ocInterfaces[interfaceIndex].requestSize)
    {
        return -1;
    }

    switch (interfaceIndex)
    {
    case OCI::Mock::ocInterfaceIndex:
    {
        OCI::Mock::OcRequest typedRequest;
        copyMem(&typedRequest, request, sizeof(typedRequest));
        return procedureContext(context)->__qpiInvokeOC<OCI::Mock>(typedRequest);
    }
    default:
        return -1;
    }
}

static unsigned char unsubscribeOracle(
    const void* context,
    int subscriptionId)
{
    return (unsigned char)procedureContext(context)->unsubscribeOracle(subscriptionId);
}

static unsigned char distributeDividends(
    const void* context,
    long long amountPerShare)
{
    return (unsigned char)procedureContext(context)->distributeDividends(amountPerShare);
}

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

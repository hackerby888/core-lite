#pragma once
// Contract-side binding compiled into contract.wasm by Qinit.
// Host imports omit QpiContext because the node binds it to the active call.
#ifdef LITE_WASM_TU_BUILD

#define LH_IMPORT(name) __attribute__((import_module("lhost"), import_name(#name)))
#define LH_EXPORT(name) __attribute__((export_name(#name)))

void setMem(void* buffer, unsigned long long size, unsigned char value)
{
    __builtin_memset(buffer, value, size);
}

void copyMem(void* destination, const void* source, unsigned long long length)
{
    __builtin_memcpy(destination, source, length);
}

bool allocatePool(unsigned long long size, void** buffer)
{
    *buffer = __builtin_malloc(size);
    return *buffer != nullptr;
}

void freePool(void* buffer)
{
    __builtin_free(buffer);
}

// These signatures are the contract side of the stable "lhost" ABI.
extern "C" {
LH_IMPORT(beginFn)        void  lh_beginFn(unsigned int id);
LH_IMPORT(endFn)          void  lh_endFn(unsigned int id);
LH_IMPORT(markDirty)      void  lh_markDirty(unsigned int contractIndex);
LH_IMPORT(pauseLog)       void  lh_pauseLog();
LH_IMPORT(resumeLog)      void  lh_resumeLog();
LH_IMPORT(acquireScratch) void* lh_acquireScratch(unsigned long long size, unsigned int initZero);
LH_IMPORT(releaseScratch) void  lh_releaseScratch(void* ptr);
LH_IMPORT(logBytes)       void  lh_logBytes(unsigned int ci, unsigned int level, const void* msg, unsigned int size);
LH_IMPORT(k12)            void  lh_k12(const void* in, unsigned int len, void* out32);
LH_IMPORT(transfer)       long long lh_transfer(const void* dest32, long long amount);
LH_IMPORT(transferTyped)  long long lh_transferTyped(const void* dest32, long long amount, unsigned int transferType);
LH_IMPORT(abort)          void  lh_abort(unsigned int errorCode);
LH_IMPORT(burn)           long long lh_burn(long long amount, unsigned int contractIndexBurnedFor);
LH_IMPORT(epoch)          unsigned int lh_epoch();
LH_IMPORT(tick)           unsigned int lh_tick();
LH_IMPORT(numberOfTickTransactions) int lh_numberOfTickTransactions();
LH_IMPORT(getEntity)      unsigned int lh_getEntity(const void* id32, void* entityOut);
LH_IMPORT(queryFeeReserve) long long lh_queryFeeReserve(unsigned int contractIndex);
LH_IMPORT(nextId)         void  lh_nextId(const void* id32, void* out32);
LH_IMPORT(prevId)         void  lh_prevId(const void* id32, void* out32);
LH_IMPORT(isContractId)   unsigned int lh_isContractId(const void* id32);
LH_IMPORT(arbitrator)     void  lh_arbitrator(void* out32);
LH_IMPORT(computor)       void  lh_computor(unsigned int index, void* out32);
LH_IMPORT(day)            unsigned int lh_day();
LH_IMPORT(year)           unsigned int lh_year();
LH_IMPORT(hour)           unsigned int lh_hour();
LH_IMPORT(minute)         unsigned int lh_minute();
LH_IMPORT(month)          unsigned int lh_month();
LH_IMPORT(second)         unsigned int lh_second();
LH_IMPORT(millisecond)    unsigned int lh_millisecond();
LH_IMPORT(now)            void  lh_now(void* dateAndTimeOut);
LH_IMPORT(prevSpectrumDigest) void lh_prevSpectrumDigest(void* out32);
LH_IMPORT(prevUniverseDigest) void lh_prevUniverseDigest(void* out32);
LH_IMPORT(prevComputerDigest) void lh_prevComputerDigest(void* out32);
LH_IMPORT(isAssetIssued)  unsigned int lh_isAssetIssued(const void* issuer32, unsigned long long assetName);
LH_IMPORT(issueAsset)     long long lh_issueAsset(unsigned long long name, const void* issuer32, unsigned int decimals, long long shares, unsigned long long unit);
LH_IMPORT(numberOfShares) long long lh_numberOfShares(const void* asset, const void* ownSel, const void* posSel);
LH_IMPORT(numberOfPossessedShares) long long lh_numberOfPossessedShares(unsigned long long name, const void* issuer32, const void* owner32, const void* possessor32, unsigned int om, unsigned int pm);
LH_IMPORT(assetEnumerate) unsigned int lh_assetEnumerate(unsigned int kind, const void* issuance, const void* ownership, const void* possession, void* out, unsigned int capacity);
LH_IMPORT(transferShareOwnershipAndPossession) long long lh_transferShares(unsigned long long name, const void* issuer32, const void* owner32, const void* possessor32, long long shares, const void* newOwner32);
LH_IMPORT(acquireShares) long long lh_acquireShares(unsigned long long name, const void* issuer32, const void* owner32, const void* possessor32, long long shares, unsigned int srcOwnMgmt, unsigned int srcPosMgmt, long long offeredFee);
LH_IMPORT(releaseShares) long long lh_releaseShares(unsigned long long name, const void* issuer32, const void* owner32, const void* possessor32, long long shares, unsigned int dstOwnMgmt, unsigned int dstPosMgmt, long long offeredFee);
LH_IMPORT(dayOfWeek) unsigned int lh_dayOfWeek(unsigned int year, unsigned int month, unsigned int day);
LH_IMPORT(signatureValidity) unsigned int lh_signatureValidity(const void* entity32, const void* digest32, const void* signature64);
LH_IMPORT(bidInIPO) long long lh_bidInIPO(unsigned int ipoContractIndex, long long price, unsigned int quantity);
LH_IMPORT(ipoBidId) void lh_ipoBidId(unsigned int ipoContractIndex, unsigned int ipoBidIndex, void* out32);
LH_IMPORT(ipoBidPrice) long long lh_ipoBidPrice(unsigned int ipoContractIndex, unsigned int ipoBidIndex);
LH_IMPORT(computeMiningFunction) void lh_computeMiningFunction(const void* miningSeed32, const void* publicKey32, const void* nonce32, void* out32);
LH_IMPORT(initMiningSeed) void lh_initMiningSeed(const void* miningSeed32);
LH_IMPORT(getOracleQueryStatus) unsigned int lh_getOracleQueryStatus(long long queryId);
LH_IMPORT(unsubscribeOracle) unsigned int lh_unsubscribeOracle(int oracleSubscriptionId);
LH_IMPORT(queryOracle) long long lh_queryOracle(unsigned int interfaceIndex, const void* query, unsigned int querySize, unsigned int notificationProcId, unsigned int timeoutMillisec, long long fee);
LH_IMPORT(subscribeOracle) int lh_subscribeOracle(unsigned int interfaceIndex, const void* query, unsigned int querySize, unsigned int notificationProcId, unsigned int periodMillisec, unsigned int notifyPrev, long long fee);
LH_IMPORT(getOracleQuery) unsigned int lh_getOracleQuery(long long queryId, void* out, unsigned int size);
LH_IMPORT(getOracleReply) unsigned int lh_getOracleReply(long long queryId, void* out, unsigned int size);
LH_IMPORT(distributeDividends) unsigned int lh_distributeDividends(long long amountPerShare);
LH_IMPORT(liteCallFunction) int lh_liteCallFunction(unsigned int calleeIdx, unsigned int inputType, const void* in, unsigned int inSize, void* out, unsigned int outSize);
LH_IMPORT(liteInvokeProcedure) int lh_liteInvokeProcedure(unsigned int calleeIdx, unsigned int inputType, const void* in, unsigned int inSize, void* out, unsigned int outSize, long long invocationReward);
LH_IMPORT(liteSetShareholderProposal) unsigned int lh_liteSetShareholderProposal(unsigned int calleeIdx, const void* proposal1024, long long invocationReward);
LH_IMPORT(liteSetShareholderVotes) unsigned int lh_liteSetShareholderVotes(unsigned int calleeIdx, const void* voteData, unsigned int voteSize, long long invocationReward);
} // extern "C"

int liteCallFunction(
    const void*,
    unsigned int calleeIndex,
    unsigned short inputType,
    const void* input,
    unsigned int inputSize,
    void* output,
    unsigned int outputSize)
{
    return lh_liteCallFunction(
        calleeIndex,
        inputType,
        input,
        inputSize,
        output,
        outputSize);
}

int liteInvokeProcedure(
    const void*,
    unsigned int calleeIndex,
    unsigned short inputType,
    const void* input,
    unsigned int inputSize,
    void* output,
    unsigned int outputSize,
    long long invocationReward)
{
    return lh_liteInvokeProcedure(
        calleeIndex,
        inputType,
        input,
        inputSize,
        output,
        outputSize,
        invocationReward);
}

static void __markContractStateDirty(unsigned int contractIndex)
{
    lh_markDirty(contractIndex);
}

static void __beginFunctionOrProcedure(const unsigned int id)
{
    lh_beginFn(id);
}

static void __endFunctionOrProcedure(const unsigned int id)
{
    lh_endFn(id);
}

static void __pauseLogMessage()
{
    lh_pauseLog();
}

static void __resumeLogMessage()
{
    lh_resumeLog();
}

static void* __acquireScratchpad(unsigned long long size, bool initializeToZero)
{
    return lh_acquireScratch(size, initializeToZero ? 1u : 0u);
}

static void __releaseScratchpad(void* pointer)
{
    lh_releaseScratch(pointer);
}

template <typename T>
static void __logContractDebugMessage(unsigned int contractIndex, T& message)
{
    lh_logBytes(
        contractIndex,
        7,
        &message,
        (unsigned int)__builtin_offsetof(T, _terminator));
}

template <typename T>
static void __logContractErrorMessage(unsigned int contractIndex, T& message)
{
    lh_logBytes(
        contractIndex,
        4,
        &message,
        (unsigned int)__builtin_offsetof(T, _terminator));
}

template <typename T>
static void __logContractInfoMessage(unsigned int contractIndex, T& message)
{
    lh_logBytes(
        contractIndex,
        6,
        &message,
        (unsigned int)__builtin_offsetof(T, _terminator));
}

template <typename T>
static void __logContractWarningMessage(unsigned int contractIndex, T& message)
{
    lh_logBytes(
        contractIndex,
        5,
        &message,
        (unsigned int)__builtin_offsetof(T, _terminator));
}

// ---- QpiContext method forwarders (stable host ABI) ----
template <typename T>
QPI::id QPI::QpiContextFunctionCall::K12(const T& data) const
{
    QPI::id digest;

    lh_k12(&data, sizeof(T), &digest);
    return digest;
}

long long QPI::QpiContextProcedureCall::transfer(
    const m256i& destination,
    long long amount) const
{
    return lh_transfer(&destination, amount);
}

long long QPI::QpiContextProcedureCall::__transfer(
    const m256i& destination,
    long long amount,
    unsigned char transferType) const
{
    return lh_transferTyped(&destination, amount, transferType);
}

void QPI::QpiContextFunctionCall::__qpiAbort(unsigned int errorCode) const
{
    lh_abort(errorCode);
}

long long QPI::QpiContextProcedureCall::burn(
    long long amount,
    unsigned int contractIndex) const
{
    return lh_burn(amount, contractIndex);
}

unsigned short QPI::QpiContextFunctionCall::epoch() const
{
    return (unsigned short)lh_epoch();
}

unsigned int QPI::QpiContextFunctionCall::tick() const
{
    return lh_tick();
}

int QPI::QpiContextFunctionCall::numberOfTickTransactions() const
{
    return lh_numberOfTickTransactions();
}

QPI::bit QPI::QpiContextFunctionCall::getEntity(
    const m256i& id,
    QPI::Entity& entity) const
{
    return (QPI::bit)lh_getEntity(&id, &entity);
}

long long QPI::QpiContextFunctionCall::queryFeeReserve(
    unsigned int contractIndex) const
{
    return lh_queryFeeReserve(contractIndex);
}

m256i QPI::QpiContextFunctionCall::nextId(const m256i& current) const
{
    m256i next;

    lh_nextId(&current, &next);
    return next;
}

m256i QPI::QpiContextFunctionCall::prevId(const m256i& current) const
{
    m256i previous;

    lh_prevId(&current, &previous);
    return previous;
}

QPI::bit QPI::QpiContextFunctionCall::isContractId(const QPI::id& id) const
{
    return (QPI::bit)lh_isContractId(&id);
}

QPI::id QPI::QpiContextFunctionCall::arbitrator() const
{
    m256i id;

    lh_arbitrator(&id);
    return id;
}

QPI::id QPI::QpiContextFunctionCall::computor(unsigned short index) const
{
    m256i id;

    lh_computor(index, &id);
    return id;
}

unsigned char QPI::QpiContextFunctionCall::day() const
{
    return (unsigned char)lh_day();
}

unsigned char QPI::QpiContextFunctionCall::year() const
{
    return (unsigned char)lh_year();
}

unsigned char QPI::QpiContextFunctionCall::hour() const
{
    return (unsigned char)lh_hour();
}

unsigned char QPI::QpiContextFunctionCall::minute() const
{
    return (unsigned char)lh_minute();
}

unsigned char QPI::QpiContextFunctionCall::month() const
{
    return (unsigned char)lh_month();
}

unsigned char QPI::QpiContextFunctionCall::second() const
{
    return (unsigned char)lh_second();
}

unsigned short QPI::QpiContextFunctionCall::millisecond() const
{
    return (unsigned short)lh_millisecond();
}

QPI::DateAndTime QPI::QpiContextFunctionCall::now() const
{
    QPI::DateAndTime dateAndTime;

    lh_now(&dateAndTime);
    return dateAndTime;
}

m256i QPI::QpiContextFunctionCall::getPrevSpectrumDigest() const
{
    m256i digest;

    lh_prevSpectrumDigest(&digest);
    return digest;
}

m256i QPI::QpiContextFunctionCall::getPrevUniverseDigest() const
{
    m256i digest;

    lh_prevUniverseDigest(&digest);
    return digest;
}

m256i QPI::QpiContextFunctionCall::getPrevComputerDigest() const
{
    m256i digest;

    lh_prevComputerDigest(&digest);
    return digest;
}

bool QPI::QpiContextFunctionCall::isAssetIssued(
    const m256i& issuer,
    unsigned long long assetName) const
{
    return lh_isAssetIssued(&issuer, assetName);
}

long long QPI::QpiContextProcedureCall::issueAsset(
    unsigned long long assetName,
    const QPI::id& issuer,
    signed char decimals,
    long long numberOfShares,
    unsigned long long unitOfMeasurement) const
{
    return lh_issueAsset(
        assetName,
        &issuer,
        (unsigned int)(unsigned char)decimals,
        numberOfShares,
        unitOfMeasurement);
}

long long QPI::QpiContextFunctionCall::numberOfShares(
    const QPI::Asset& asset,
    const QPI::AssetOwnershipSelect& ownership,
    const QPI::AssetPossessionSelect& possession) const
{
    return lh_numberOfShares(&asset, &ownership, &possession);
}

long long QPI::QpiContextFunctionCall::numberOfPossessedShares(
    unsigned long long assetName,
    const m256i& issuer,
    const m256i& owner,
    const m256i& possessor,
    unsigned short ownershipManagingContractIndex,
    unsigned short possessionManagingContractIndex) const
{
    return lh_numberOfPossessedShares(
        assetName,
        &issuer,
        &owner,
        &possessor,
        ownershipManagingContractIndex,
        possessionManagingContractIndex);
}

long long QPI::QpiContextProcedureCall::transferShareOwnershipAndPossession(
    unsigned long long assetName,
    const m256i& issuer,
    const m256i& owner,
    const m256i& possessor,
    long long numberOfShares,
    const m256i& newOwnerAndPossessor) const
{
    return lh_transferShares(
        assetName,
        &issuer,
        &owner,
        &possessor,
        numberOfShares,
        &newOwnerAndPossessor);
}

long long QPI::QpiContextProcedureCall::acquireShares(
    const QPI::Asset& asset,
    const m256i& owner,
    const m256i& possessor,
    long long numberOfShares,
    unsigned short sourceOwnershipManagingContractIndex,
    unsigned short sourcePossessionManagingContractIndex,
    long long offeredFee) const
{
    return lh_acquireShares(
        asset.assetName,
        &asset.issuer,
        &owner,
        &possessor,
        numberOfShares,
        sourceOwnershipManagingContractIndex,
        sourcePossessionManagingContractIndex,
        offeredFee);
}

long long QPI::QpiContextProcedureCall::releaseShares(
    const QPI::Asset& asset,
    const m256i& owner,
    const m256i& possessor,
    long long numberOfShares,
    unsigned short destinationOwnershipManagingContractIndex,
    unsigned short destinationPossessionManagingContractIndex,
    long long offeredFee) const
{
    return lh_releaseShares(
        asset.assetName,
        &asset.issuer,
        &owner,
        &possessor,
        numberOfShares,
        destinationOwnershipManagingContractIndex,
        destinationPossessionManagingContractIndex,
        offeredFee);
}

unsigned char QPI::QpiContextFunctionCall::dayOfWeek(
    unsigned char year,
    unsigned char month,
    unsigned char day) const
{
    return (unsigned char)lh_dayOfWeek(year, month, day);
}

QPI::bit QPI::QpiContextFunctionCall::signatureValidity(
    const m256i& entity,
    const m256i& digest,
    const QPI::Array<QPI::sint8, 64>& signature) const
{
    return lh_signatureValidity(&entity, &digest, &signature) != 0;
}

long long QPI::QpiContextProcedureCall::bidInIPO(
    unsigned int ipoContractIndex,
    long long price,
    unsigned int quantity) const
{
    return lh_bidInIPO(ipoContractIndex, price, quantity);
}

m256i QPI::QpiContextFunctionCall::ipoBidId(
    unsigned int ipoContractIndex,
    unsigned int ipoBidIndex) const
{
    m256i id;

    lh_ipoBidId(ipoContractIndex, ipoBidIndex, &id);
    return id;
}

long long QPI::QpiContextFunctionCall::ipoBidPrice(
    unsigned int ipoContractIndex,
    unsigned int ipoBidIndex) const
{
    return lh_ipoBidPrice(ipoContractIndex, ipoBidIndex);
}

m256i QPI::QpiContextFunctionCall::computeMiningFunction(
    const m256i miningSeed,
    const m256i publicKey,
    const m256i nonce) const
{
    m256i result;

    lh_computeMiningFunction(&miningSeed, &publicKey, &nonce, &result);
    return result;
}

void QPI::QpiContextFunctionCall::initMiningSeed(const m256i miningSeed) const
{
    lh_initMiningSeed(&miningSeed);
}

unsigned char QPI::QpiContextFunctionCall::getOracleQueryStatus(
    long long queryId) const
{
    return (unsigned char)lh_getOracleQueryStatus(queryId);
}

bool QPI::QpiContextProcedureCall::unsubscribeOracle(
    int oracleSubscriptionId) const
{
    return lh_unsubscribeOracle(oracleSubscriptionId) != 0;
}

template <typename OracleInterface, typename ContractStateType, typename LocalsType>
QPI::sint64 QPI::QpiContextProcedureCall::__qpiQueryOracle(
    const typename OracleInterface::OracleQuery& query,
    void (*)(
        const QPI::QpiContextProcedureCall&,
        ContractStateType&,
        QPI::OracleNotificationInput<OracleInterface>&,
        QPI::NoData&,
        LocalsType&),
    unsigned int notificationProcedureId,
    unsigned int timeoutMilliseconds) const
{
    return lh_queryOracle(
        OracleInterface::oracleInterfaceIndex,
        &query,
        (unsigned int)sizeof(typename OracleInterface::OracleQuery),
        notificationProcedureId,
        timeoutMilliseconds,
        OracleInterface::getQueryFee(query));
}

template <typename OracleInterface, typename ContractStateType, typename LocalsType>
QPI::sint32 QPI::QpiContextProcedureCall::__qpiSubscribeOracle(
    const typename OracleInterface::OracleQuery& query,
    void (*)(
        const QPI::QpiContextProcedureCall&,
        ContractStateType&,
        QPI::OracleNotificationInput<OracleInterface>&,
        QPI::NoData&,
        LocalsType&),
    unsigned int notificationProcedureId,
    unsigned int notificationPeriodInMilliseconds,
    bool notifyWithPreviousReply) const
{
    return lh_subscribeOracle(
        OracleInterface::oracleInterfaceIndex,
        &query,
        (unsigned int)sizeof(typename OracleInterface::OracleQuery),
        notificationProcedureId,
        notificationPeriodInMilliseconds,
        notifyWithPreviousReply ? 1u : 0u,
        OracleInterface::getSubscriptionFee(
            query,
            notificationPeriodInMilliseconds));
}

template <typename OracleInterface>
bool QPI::QpiContextFunctionCall::getOracleQuery(
    QPI::sint64 queryId,
    typename OracleInterface::OracleQuery& query) const
{
    return lh_getOracleQuery(
        queryId,
        &query,
        (unsigned int)sizeof(typename OracleInterface::OracleQuery)) != 0;
}

template <typename OracleInterface>
bool QPI::QpiContextFunctionCall::getOracleReply(
    QPI::sint64 queryId,
    typename OracleInterface::OracleReply& reply) const
{
    return lh_getOracleReply(
        queryId,
        &reply,
        (unsigned int)sizeof(typename OracleInterface::OracleReply)) != 0;
}

bool QPI::QpiContextProcedureCall::distributeDividends(
    long long amountPerShare) const
{
    return lh_distributeDividends(amountPerShare);
}

QPI::uint16 QPI::QpiContextProcedureCall::setShareholderProposal(
    QPI::uint16 contractIndex,
    const QPI::Array<QPI::uint8, 1024>& proposalDataBuffer,
    QPI::sint64 invocationReward) const
{
    return (QPI::uint16)lh_liteSetShareholderProposal(
        contractIndex,
        &proposalDataBuffer,
        invocationReward);
}

bool QPI::QpiContextProcedureCall::setShareholderVotes(
    QPI::uint16 contractIndex,
    const QPI::ProposalMultiVoteDataV1& voteData,
    QPI::sint64 invocationReward) const
{
    return lh_liteSetShareholderVotes(
        contractIndex,
        &voteData,
        sizeof(voteData),
        invocationReward) != 0;
}

// ---- registration capture (read by reg_info) ----
#ifndef LITE_MAX_USER_ENTRIES
#define LITE_MAX_USER_ENTRIES 1024
#endif
struct LiteWasmTuEntry
{
    unsigned short inputType;
    LiteWasmDispatchKind kind;
    unsigned short inputSize;
    unsigned short outputSize;
    unsigned int localsSize;
    void* function;
};
static LiteWasmTuEntry g_wasmTuEntries[LITE_MAX_USER_ENTRIES];
static unsigned int g_wasmTuEntryCount = 0;

QPI::QpiContextForInit::QpiContextForInit(unsigned int contractIndex)
    : QpiContext(contractIndex, QPI::NULL_ID, QPI::NULL_ID, 0, 0)
{
}

void QPI::QpiContextForInit::__registerUserFunction(
    USER_FUNCTION function,
    unsigned short inputType,
    unsigned short inputSize,
    unsigned short outputSize,
    unsigned int localsSize) const
{
    if (g_wasmTuEntryCount >= LITE_MAX_USER_ENTRIES)
    {
        return;
    }

    g_wasmTuEntries[g_wasmTuEntryCount++] = {
        inputType,
        LiteWasmDispatchKind::UserFunction,
        inputSize,
        outputSize,
        localsSize,
        (void*)function,
    };
}

void QPI::QpiContextForInit::__registerUserProcedure(
    USER_PROCEDURE procedure,
    unsigned short inputType,
    unsigned short inputSize,
    unsigned short outputSize,
    unsigned int localsSize) const
{
    if (g_wasmTuEntryCount >= LITE_MAX_USER_ENTRIES)
    {
        return;
    }

    g_wasmTuEntries[g_wasmTuEntryCount++] = {
        inputType,
        LiteWasmDispatchKind::UserProcedure,
        inputSize,
        outputSize,
        localsSize,
        (void*)procedure,
    };
}

// Oracle notification dispatch uses the low 16 bits of its synthetic procedure ID.
void QPI::QpiContextForInit::__registerUserProcedureNotification(
    USER_PROCEDURE procedure,
    unsigned int procedureId,
    unsigned short inputSize,
    unsigned short outputSize,
    unsigned int localsSize) const
{
    if (g_wasmTuEntryCount >= LITE_MAX_USER_ENTRIES)
    {
        return;
    }

    g_wasmTuEntries[g_wasmTuEntryCount++] = {
        (unsigned short)procedureId,
        LiteWasmDispatchKind::UserProcedure,
        inputSize,
        outputSize,
        localsSize,
        (void*)procedure,
    };
}

// The node calls these exports when the contract type is available.
#ifdef CONTRACT_STATE_TYPE
typedef void (*LiteWasmUserFunction)(
    const QPI::QpiContextFunctionCall&,
    void*,
    void*,
    void*,
    void*);
typedef void (*LiteWasmUserProcedure)(
    const QPI::QpiContextProcedureCall&,
    void*,
    void*,
    void*,
    void*);
typedef void (*LiteWasmSystemProcedure)(
    const QPI::QpiContextProcedureCall&,
    void*,
    void*,
    void*,
    void*);
typedef void (*LiteWasmMigrateProcedure)(
    const QPI::QpiContextFunctionCall&,
    void*,
    void*,
    void*);

// Alignment keeps debug page protection for state separate from context and IO.
// Raw storage avoids running constructors that native contract state never runs.
#ifdef QINIT_CORPUS_RUNNER
// Corpus runs use engine-deployed state, so the local region only reserves one page.
alignas(65536) static unsigned char g_wasmStateBuf[65536];
#else
alignas(65536) static unsigned char g_wasmStateBuf[
    sizeof(CONTRACT_STATE_TYPE::StateData)];
#endif
static CONTRACT_STATE_TYPE::StateData& g_wasmState =
    *reinterpret_cast<CONTRACT_STATE_TYPE::StateData*>(g_wasmStateBuf);
alignas(65536) static unsigned char g_wasmCtxBuf[256];
#ifndef LITE_WASM_ARENA_SZ
#define LITE_WASM_ARENA_SZ (1024 * 1024 * 1024)
#endif
// Layout is input, output, locals, then scratch arena; it must match the node carve.
alignas(65536) static unsigned char g_wasmIo[
    (64 * 1024) + (64 * 1024) + (32 * 1024) + LITE_WASM_ARENA_SZ];

static bool g_wasmRegistered = false;
static void liteWasmTuEnsureRegistered()
{
    if (g_wasmRegistered)
    {
        return;
    }

    g_wasmRegistered = true;
    QPI::QpiContextForInit qpi(CONTRACT_INDEX);

    CONTRACT_STATE_TYPE::__registerUserFunctionsAndProcedures(qpi);
}

extern "C"
{
LH_EXPORT(state_addr)
unsigned int state_addr()
{
    return (unsigned int)(unsigned long)&g_wasmState;
}

LH_EXPORT(state_size)
unsigned int state_size()
{
    return (unsigned int)sizeof(g_wasmState);
}

LH_EXPORT(io_base)
unsigned int io_base()
{
    return (unsigned int)(unsigned long)&g_wasmIo[0];
}

LH_EXPORT(io_size)
unsigned int io_size()
{
    return (unsigned int)sizeof(g_wasmIo);
}

LH_EXPORT(ctx_addr)
unsigned int ctx_addr()
{
    return (unsigned int)(unsigned long)&g_wasmCtxBuf[0];
}

LH_EXPORT(reg_count)
unsigned int reg_count()
{
    liteWasmTuEnsureRegistered();
    return g_wasmTuEntryCount;
}

struct LiteWasmTuInfo
{
    unsigned int inputType;
    unsigned int kind;
    unsigned int inputSize;
    unsigned int outputSize;
};

LH_EXPORT(reg_info)
void reg_info(unsigned int entryIndex, LiteWasmTuInfo* output)
{
    liteWasmTuEnsureRegistered();
    if (entryIndex >= g_wasmTuEntryCount)
    {
        setMem(output, sizeof(*output), 0);
        return;
    }

    const LiteWasmTuEntry& entry = g_wasmTuEntries[entryIndex];

    output->inputType = entry.inputType;
    output->kind = (unsigned int)entry.kind;
    output->inputSize = entry.inputSize;
    output->outputSize = entry.outputSize;
}

// System procedure bits use the IDs declared in lite_dyn_abi.h.
LH_EXPORT(reg_sysproc_mask)
unsigned int reg_sysproc_mask()
{
    unsigned int mask = 0;
#define LITE_SYS_PROC_MASK(symbol, id, method, emptyMember) \
    if (!CONTRACT_STATE_TYPE::emptyMember)                 \
    {                                                      \
        mask |= (1u << id);                                \
    }
    LITE_SYSTEM_PROCEDURE_ROWS(LITE_SYS_PROC_MASK)
#undef LITE_SYS_PROC_MASK
    return mask;
}

LH_EXPORT(sysproc_locals_size)
unsigned int sysproc_locals_size(unsigned int systemProcedure)
{
    switch (systemProcedure)
    {
        case 0:
            return (unsigned int)CONTRACT_STATE_TYPE::__initializeLocalsSize;
        case 1:
            return (unsigned int)CONTRACT_STATE_TYPE::__beginEpochLocalsSize;
        case 2:
            return (unsigned int)CONTRACT_STATE_TYPE::__endEpochLocalsSize;
        case 3:
            return (unsigned int)CONTRACT_STATE_TYPE::__beginTickLocalsSize;
        case 4:
            return (unsigned int)CONTRACT_STATE_TYPE::__endTickLocalsSize;
        case 5:
            return (unsigned int)CONTRACT_STATE_TYPE::__preReleaseSharesLocalsSize;
        case 6:
            return (unsigned int)CONTRACT_STATE_TYPE::__preAcquireSharesLocalsSize;
        case 7:
            return (unsigned int)CONTRACT_STATE_TYPE::__postReleaseSharesLocalsSize;
        case 8:
            return (unsigned int)CONTRACT_STATE_TYPE::__postAcquireSharesLocalsSize;
        case 9:
            return (unsigned int)CONTRACT_STATE_TYPE::__postIncomingTransferLocalsSize;
        case 10:
            return (unsigned int)CONTRACT_STATE_TYPE::__setShareholderProposalLocalsSize;
        case 11:
            return (unsigned int)CONTRACT_STATE_TYPE::__setShareholderVotesLocalsSize;
    }

    return 0;
}

LH_EXPORT(sysproc_in_size)
unsigned int sysproc_in_size(unsigned int systemProcedure)
{
    switch (systemProcedure)
    {
        case 5:
        case 6:
            return (unsigned int)sizeof(QPI::PreManagementRightsTransfer_input);
        case 7:
        case 8:
            return (unsigned int)sizeof(QPI::PostManagementRightsTransfer_input);
        case 9:
            return (unsigned int)sizeof(QPI::PostIncomingTransfer_input);
        case 10:
            return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_PROPOSAL_input);
        case 11:
            return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_VOTES_input);
    }

    return 0;
}

LH_EXPORT(sysproc_out_size)
unsigned int sysproc_out_size(unsigned int systemProcedure)
{
    switch (systemProcedure)
    {
        case 5:
        case 6:
            return (unsigned int)sizeof(QPI::PreManagementRightsTransfer_output);
        case 10:
            return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_PROPOSAL_output);
        case 11:
            return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_VOTES_output);
    }

    return 0;
}

LH_EXPORT(has_migrate)
unsigned int has_migrate()
{
    return CONTRACT_STATE_TYPE::__migrateEmpty ? 0u : 1u;
}

LH_EXPORT(migrate_old_state_size)
unsigned int migrate_old_state_size()
{
    return (unsigned int)CONTRACT_STATE_TYPE::__migrateOldStateSize;
}

LH_EXPORT(migrate_locals_size)
unsigned int migrate_locals_size()
{
    return (unsigned int)CONTRACT_STATE_TYPE::__migrateLocalsSize;
}
} // extern "C"

static const LiteWasmTuEntry* liteWasmFindUserEntry(
    unsigned int inputType,
    LiteWasmDispatchKind kind)
{
    for (unsigned int entryIndex = 0; entryIndex < g_wasmTuEntryCount; entryIndex++)
    {
        const LiteWasmTuEntry& entry = g_wasmTuEntries[entryIndex];
        if (entry.inputType == (unsigned short)inputType && entry.kind == kind)
        {
            return &entry;
        }
    }

    return nullptr;
}

static void liteWasmCallSystemProcedure(
    LiteWasmSystemProcedure procedure,
    void* input,
    void* output,
    void* locals)
{
    auto& context = *reinterpret_cast<QPI::QpiContextProcedureCall*>(&g_wasmCtxBuf[0]);
    procedure(context, &g_wasmState, input, output, locals);
}

static void liteWasmDispatchSystemProcedure(
    unsigned int systemProcedureId,
    void* input,
    void* output,
    void* locals)
{
    switch (systemProcedureId)
    {
        case LITE_SP_INITIALIZE:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__initialize,
                input,
                output,
                locals);
            break;
        case LITE_SP_BEGIN_EPOCH:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__beginEpoch,
                input,
                output,
                locals);
            break;
        case LITE_SP_END_EPOCH:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__endEpoch,
                input,
                output,
                locals);
            break;
        case LITE_SP_BEGIN_TICK:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__beginTick,
                input,
                output,
                locals);
            break;
        case LITE_SP_END_TICK:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__endTick,
                input,
                output,
                locals);
            break;
        case LITE_SP_PRE_RELEASE_SHARES:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__preReleaseShares,
                input,
                output,
                locals);
            break;
        case LITE_SP_PRE_ACQUIRE_SHARES:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__preAcquireShares,
                input,
                output,
                locals);
            break;
        case LITE_SP_POST_RELEASE_SHARES:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__postReleaseShares,
                input,
                output,
                locals);
            break;
        case LITE_SP_POST_ACQUIRE_SHARES:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__postAcquireShares,
                input,
                output,
                locals);
            break;
        case LITE_SP_POST_INCOMING_TRANSFER:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__postIncomingTransfer,
                input,
                output,
                locals);
            break;
        case LITE_SP_SET_SHAREHOLDER_PROPOSAL:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__setShareholderProposal,
                input,
                output,
                locals);
            break;
        case LITE_SP_SET_SHAREHOLDER_VOTES:
            liteWasmCallSystemProcedure(
                (LiteWasmSystemProcedure)(void*)CONTRACT_STATE_TYPE::__setShareholderVotes,
                input,
                output,
                locals);
            break;
        default:
            break;
    }
}

static void liteWasmDispatchMigration(void* oldState, void* locals)
{
    auto& context = *reinterpret_cast<QPI::QpiContextFunctionCall*>(&g_wasmCtxBuf[0]);
    auto migrate = (LiteWasmMigrateProcedure)(void*)CONTRACT_STATE_TYPE::__migrate;

    migrate(context, &g_wasmState, oldState, locals);
}

static void liteWasmDispatchUserFunction(
    unsigned int inputType,
    void* input,
    void* output,
    void* locals)
{
    const LiteWasmTuEntry* entry =
        liteWasmFindUserEntry(inputType, LiteWasmDispatchKind::UserFunction);
    if (!entry)
    {
        return;
    }

    auto& context = *reinterpret_cast<QPI::QpiContextFunctionCall*>(&g_wasmCtxBuf[0]);
    auto function = (LiteWasmUserFunction)entry->function;

    function(context, &g_wasmState, input, output, locals);
}

static void liteWasmDispatchUserProcedure(
    unsigned int inputType,
    void* input,
    void* output,
    void* locals)
{
    const LiteWasmTuEntry* entry =
        liteWasmFindUserEntry(inputType, LiteWasmDispatchKind::UserProcedure);
    if (!entry)
    {
        return;
    }

    auto& context = *reinterpret_cast<QPI::QpiContextProcedureCall*>(&g_wasmCtxBuf[0]);
    auto procedure = (LiteWasmUserProcedure)entry->function;

    procedure(context, &g_wasmState, input, output, locals);
}

extern "C"
{
LH_EXPORT(dispatch)
void dispatch(
    unsigned int kindValue,
    unsigned int inputType,
    unsigned int inputOffset,
    unsigned int outputOffset,
    unsigned int localsOffset)
{
    liteWasmTuEnsureRegistered();

    const LiteWasmDispatchKind kind = (LiteWasmDispatchKind)kindValue;
    void* input = (void*)(unsigned long)inputOffset;
    void* output = (void*)(unsigned long)outputOffset;
    void* locals = (void*)(unsigned long)localsOffset;

    switch (kind)
    {
        case LiteWasmDispatchKind::UserFunction:
            liteWasmDispatchUserFunction(inputType, input, output, locals);
            break;
        case LiteWasmDispatchKind::UserProcedure:
            liteWasmDispatchUserProcedure(inputType, input, output, locals);
            break;
        case LiteWasmDispatchKind::SystemProcedure:
            liteWasmDispatchSystemProcedure(inputType, input, output, locals);
            break;
        case LiteWasmDispatchKind::Migration:
            liteWasmDispatchMigration(input, locals);
            break;
    }
}
} // extern "C"

#endif // CONTRACT_STATE_TYPE

#undef LH_IMPORT
#undef LH_EXPORT
#endif // LITE_WASM_TU_BUILD

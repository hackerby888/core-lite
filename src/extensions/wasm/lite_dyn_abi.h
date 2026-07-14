#pragma once
// Shared binary declarations for runtime-deployed Wasm contracts.

#include <cstdint>
#include "extensions/wasm/lite_abi_metadata.h"

// This exchange layout is consumed by the node, contract iterator, and SDK metadata.
struct LiteAssetEntry
{
    unsigned char owner[32];
    unsigned char possessor[32];
    long long shares;
    unsigned short ownershipManagingContract;
    unsigned short possessionManagingContract;
    unsigned char padding[4];
};
#define LITE_ASSET_ENTRY_CAPACITY 1024u

// System-procedure slots must match SystemProcedureID in contract_def.h.
#define LITE_SYS_PROC_ENUM(symbol, id, method, emptyMember) LITE_SP_##symbol = id,
enum LiteSysProcId : uint32_t
{
    LITE_SYSTEM_PROCEDURE_ROWS(LITE_SYS_PROC_ENUM)
    LITE_SP_COUNT,
};
#undef LITE_SYS_PROC_ENUM

enum LiteEntryKind : uint8_t
{
    LITE_KIND_FUNCTION = 0,
    LITE_KIND_PROCEDURE = 1,
};

enum class LiteWasmDispatchKind : uint8_t
{
    UserFunction = 0,
    UserProcedure = 1,
    SystemProcedure = 2,
    Migration = 3,
};

// The host fills this vtable and exposes it through the stable "lhost" import surface.
struct LiteHostServices
{
    uint32_t abiVersion;

    void (*beginFn)(unsigned int id);
    void (*endFn)(unsigned int id);
    void (*markDirty)(unsigned int contractIndex);
    void (*pauseLog)();
    void (*resumeLog)();
    void* (*acquireScratch)(unsigned long long size, bool initZero);
    void (*releaseScratch)(void* ptr);
    void (*logBytes)(unsigned int contractIndex, unsigned char level, const void* msg, unsigned int size);
    void (*k12)(const void* in, unsigned int len, void* out32);

    // Context parameters point to the host-owned QpiContext for the active call.
    long long (*transfer)(const void* ctx, const void* dest32, long long amount);
    long long (*transferTyped)(const void* ctx, const void* dest32, long long amount, unsigned char transferType);
    void      (*abort)(const void* ctx, unsigned int errorCode);
    long long (*burn)(const void* ctx, long long amount, unsigned int contractIndexBurnedFor);
    unsigned short (*epoch)(const void* ctx);
    unsigned int   (*tick)(const void* ctx);
    int            (*numberOfTickTransactions)(const void* ctx);
    unsigned char (*getEntity)(const void* ctx, const void* id32, void* entityOut);
    long long (*queryFeeReserve)(const void* ctx, unsigned int contractIndex);
    void (*nextId)(const void* ctx, const void* id32, void* out32);
    void (*prevId)(const void* ctx, const void* id32, void* out32);
    unsigned char (*isContractId)(const void* ctx, const void* id32);
    void (*arbitrator)(const void* ctx, void* out32);
    void (*computor)(const void* ctx, unsigned short index, void* out32);

    unsigned char  (*day)(const void* ctx);
    unsigned char  (*year)(const void* ctx);
    unsigned char  (*hour)(const void* ctx);
    unsigned char  (*minute)(const void* ctx);
    unsigned char  (*month)(const void* ctx);
    unsigned char  (*second)(const void* ctx);
    unsigned short (*millisecond)(const void* ctx);
    void           (*now)(const void* ctx, void* dateAndTimeOut);
    void (*prevSpectrumDigest)(const void* ctx, void* out32);
    void (*prevUniverseDigest)(const void* ctx, void* out32);
    void (*prevComputerDigest)(const void* ctx, void* out32);

    unsigned char (*isAssetIssued)(const void* ctx, const void* issuer32, unsigned long long assetName);
    long long (*issueAsset)(const void* ctx, unsigned long long name, const void* issuer32, signed char decimals, long long shares, unsigned long long unit);
    long long (*numberOfShares)(const void* ctx, const void* asset, const void* ownSel, const void* posSel);
    long long (*numberOfPossessedShares)(const void* ctx, unsigned long long assetName, const void* issuer32, const void* owner32, const void* possessor32, unsigned short ownMgmt, unsigned short posMgmt);
    unsigned int (*assetEnumerate)(const void* ctx, unsigned int kind, const void* issuance, const void* ownSel, const void* posSel, void* outBuf, unsigned int maxEntries);
    long long (*transferShareOwnershipAndPossession)(const void* ctx, unsigned long long assetName, const void* issuer32, const void* owner32, const void* possessor32, long long shares, const void* newOwner32);
    long long (*acquireShares)(const void* ctx, unsigned long long assetName, const void* issuer32, const void* owner32, const void* possessor32, long long shares, unsigned short srcOwnMgmt, unsigned short srcPosMgmt, long long offeredFee);
    long long (*releaseShares)(const void* ctx, unsigned long long assetName, const void* issuer32, const void* owner32, const void* possessor32, long long shares, unsigned short dstOwnMgmt, unsigned short dstPosMgmt, long long offeredFee);
    unsigned char (*dayOfWeek)(const void* ctx, unsigned char year, unsigned char month, unsigned char day);
    unsigned char (*signatureValidity)(const void* ctx, const void* entity32, const void* digest32, const void* signature64);
    long long (*bidInIPO)(const void* ctx, unsigned int ipoContractIndex, long long price, unsigned int quantity);
    void (*ipoBidId)(const void* ctx, unsigned int ipoContractIndex, unsigned int ipoBidIndex, void* out32);
    long long (*ipoBidPrice)(const void* ctx, unsigned int ipoContractIndex, unsigned int ipoBidIndex);
    void (*computeMiningFunction)(const void* ctx, const void* miningSeed32, const void* publicKey32, const void* nonce32, void* out32);
    void (*initMiningSeed)(const void* ctx, const void* miningSeed32);
    unsigned char (*getOracleQueryStatus)(const void* ctx, long long queryId);
    unsigned char (*unsubscribeOracle)(const void* ctx, int oracleSubscriptionId);
    long long (*queryOracle)(const void* ctx, unsigned int interfaceIndex, const void* query, unsigned int querySize, unsigned int notificationProcId, unsigned int timeoutMillisec, long long fee);
    int (*subscribeOracle)(const void* ctx, unsigned int interfaceIndex, const void* query, unsigned int querySize, unsigned int notificationProcId, unsigned int periodMillisec, unsigned int notifyPrev, long long fee);
    unsigned int (*getOracleQuery)(const void* ctx, long long queryId, void* out, unsigned int size);
    unsigned int (*getOracleReply)(const void* ctx, long long queryId, void* out, unsigned int size);
    unsigned char (*distributeDividends)(const void* ctx, long long amountPerShare);
    // Inter-contract calls return an InterContractCallError value.
    int (*liteCallFunction)(const void* callerCtx, unsigned int calleeIdx, unsigned short inputType,
                            const void* in, unsigned int inSize, void* out, unsigned int outSize);
    int (*liteInvokeProcedure)(const void* callerCtx, unsigned int calleeIdx, unsigned short inputType,
                               const void* in, unsigned int inSize, void* out, unsigned int outSize,
                               long long invocationReward);
    // Governance callbacks target another deployed contract.
    unsigned short (*setShareholderProposal)(const void* callerCtx, unsigned int calleeIdx, const void* proposal1024, long long invocationReward);
    unsigned char  (*setShareholderVotes)(const void* callerCtx, unsigned int calleeIdx, const void* voteData, unsigned int voteSize, long long invocationReward);
};

#define LITE_MAX_USER_ENTRIES 1024

struct LiteContractDescriptor
{
    uint32_t abiVersion;
    char name[16];
    uint64_t stateSize;
    uint32_t stateLayoutVersion;
    uint16_t entryCount;
};

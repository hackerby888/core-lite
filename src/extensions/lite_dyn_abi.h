#pragma once
// Shared ABI for runtime-deployed wasm contracts. See WASM_CONTRACTS.md.
// Defines the LiteHostServices vtable (the host's QPI surface), the system-procedure id enum,
// and small shared structs. The node (lite_dynamic_contracts.h) fills the vtable; the wasm
// engine (lite_wasm_imports.h) exposes the same surface to the module as "lhost" imports.

#include <cstdint>

// ---------------------------------------------------------------------------
// Shared ABI — primitives only, no QPI types, so both sides agree byte-for-byte.
// ---------------------------------------------------------------------------

#define LITE_DYN_ABI_VERSION 1u

// System-procedure slots — MUST match SystemProcedureID order in contract_def.h.
enum LiteSysProcId : uint32_t {
    LITE_SP_INITIALIZE = 0,
    LITE_SP_BEGIN_EPOCH,
    LITE_SP_END_EPOCH,
    LITE_SP_BEGIN_TICK,
    LITE_SP_END_TICK,
    LITE_SP_PRE_RELEASE_SHARES,
    LITE_SP_PRE_ACQUIRE_SHARES,
    LITE_SP_POST_RELEASE_SHARES,
    LITE_SP_POST_ACQUIRE_SHARES,
    LITE_SP_POST_INCOMING_TRANSFER,
    LITE_SP_SET_SHAREHOLDER_PROPOSAL,
    LITE_SP_SET_SHAREHOLDER_VOTES,
    LITE_SP_COUNT,
};

enum LiteEntryKind : uint8_t { LITE_KIND_FUNCTION = 0, LITE_KIND_PROCEDURE = 1 };

// The QPI surface the host fills; the wasm engine exposes each member to the contract as an "lhost"
// import (lite_wasm_imports.h). The host wraps each QpiContext method in a thin free function (the
// single-TU host inlines the methods away, so a wrapper forces emission and gives a stable entry).
struct LiteHostServices {
    uint32_t abiVersion;

    // Infra (the static QPI hooks from pre_qpi_def.h + K12 + logging).
    void  (*beginFn)(unsigned int id);
    void  (*endFn)(unsigned int id);
    void  (*markDirty)(unsigned int contractIndex);
    void  (*pauseLog)();
    void  (*resumeLog)();
    void* (*acquireScratch)(unsigned long long size, bool initZero);
    void  (*releaseScratch)(void* ptr);
    void  (*logBytes)(unsigned int contractIndex, unsigned char level, const void* msg, unsigned int size);
    void  (*k12)(const void* in, unsigned int len, void* out32);

    // QpiContext method backends. ctx = the QpiContext* (host casts back).
    // Extend as deployed contracts require — this set is the codegen target.
    long long (*transfer)(const void* ctx, const void* dest32, long long amount);
    long long (*transferTyped)(const void* ctx, const void* dest32, long long amount, unsigned char transferType);
    void      (*abort)(const void* ctx, unsigned int errorCode);
    long long (*burn)(const void* ctx, long long amount, unsigned int contractIndexBurnedFor);
    unsigned short (*epoch)(const void* ctx);
    unsigned int   (*tick)(const void* ctx);
    int            (*numberOfTickTransactions)(const void* ctx);
    // spectrum / identity reads (struct returns via out-ptr).
    unsigned char  (*getEntity)(const void* ctx, const void* id32, void* entityOut); // bit; entityOut = QPI::Entity*
    long long      (*queryFeeReserve)(const void* ctx, unsigned int contractIndex);
    void           (*nextId)(const void* ctx, const void* id32, void* out32);
    void           (*prevId)(const void* ctx, const void* id32, void* out32);
    unsigned char  (*isContractId)(const void* ctx, const void* id32);
    void           (*arbitrator)(const void* ctx, void* out32);
    void           (*computor)(const void* ctx, unsigned short index, void* out32);
    // time (from the tick's timestamp).
    unsigned char  (*day)(const void* ctx);
    unsigned char  (*year)(const void* ctx);
    unsigned char  (*hour)(const void* ctx);
    unsigned char  (*minute)(const void* ctx);
    unsigned char  (*month)(const void* ctx);
    unsigned char  (*second)(const void* ctx);
    unsigned short (*millisecond)(const void* ctx);
    void           (*now)(const void* ctx, void* dateAndTimeOut);
    // etalon-tick digests (m256i via out-ptr).
    void           (*prevSpectrumDigest)(const void* ctx, void* out32);
    void           (*prevUniverseDigest)(const void* ctx, void* out32);
    void           (*prevComputerDigest)(const void* ctx, void* out32);
    // assets / shares.
    unsigned char  (*isAssetIssued)(const void* ctx, const void* issuer32, unsigned long long assetName);
    long long      (*issueAsset)(const void* ctx, unsigned long long name, const void* issuer32, signed char decimals, long long shares, unsigned long long unit);
    long long      (*numberOfShares)(const void* ctx, const void* asset, const void* ownSel, const void* posSel);
    long long      (*numberOfPossessedShares)(const void* ctx, unsigned long long assetName, const void* issuer32, const void* owner32, const void* possessor32, unsigned short ownMgmt, unsigned short posMgmt);
    // Enumerate matching asset records for the contract-side AssetOwnership/PossessionIterator (kind 0 = ownership,
    // 1 = possession). Writes 80-byte records (owner[32], possessor[32], sint64 shares, uint16 ownMgmt, uint16
    // posMgmt, pad[4]) to outBuf, returns the count (capped at maxEntries).
    unsigned int   (*assetEnumerate)(const void* ctx, unsigned int kind, const void* issuance, const void* ownSel, const void* posSel, void* outBuf, unsigned int maxEntries);
    long long      (*transferShareOwnershipAndPossession)(const void* ctx, unsigned long long assetName, const void* issuer32, const void* owner32, const void* possessor32, long long shares, const void* newOwner32);
    long long      (*acquireShares)(const void* ctx, unsigned long long assetName, const void* issuer32, const void* owner32, const void* possessor32, long long shares, unsigned short srcOwnMgmt, unsigned short srcPosMgmt, long long offeredFee);
    long long      (*releaseShares)(const void* ctx, unsigned long long assetName, const void* issuer32, const void* owner32, const void* possessor32, long long shares, unsigned short dstOwnMgmt, unsigned short dstPosMgmt, long long offeredFee);
    unsigned char  (*dayOfWeek)(const void* ctx, unsigned char year, unsigned char month, unsigned char day);
    unsigned char  (*signatureValidity)(const void* ctx, const void* entity32, const void* digest32, const void* signature64);
    long long      (*bidInIPO)(const void* ctx, unsigned int ipoContractIndex, long long price, unsigned int quantity);
    void           (*ipoBidId)(const void* ctx, unsigned int ipoContractIndex, unsigned int ipoBidIndex, void* out32);
    long long      (*ipoBidPrice)(const void* ctx, unsigned int ipoContractIndex, unsigned int ipoBidIndex);
    void           (*computeMiningFunction)(const void* ctx, const void* miningSeed32, const void* publicKey32, const void* nonce32, void* out32);
    void           (*initMiningSeed)(const void* ctx, const void* miningSeed32);
    unsigned char  (*getOracleQueryStatus)(const void* ctx, long long queryId);
    unsigned char  (*unsubscribeOracle)(const void* ctx, int oracleSubscriptionId);
    long long      (*queryOracle)(const void* ctx, unsigned int interfaceIndex, const void* query, unsigned int querySize, unsigned int notificationProcId, unsigned int timeoutMillisec, long long fee);
    int            (*subscribeOracle)(const void* ctx, unsigned int interfaceIndex, const void* query, unsigned int querySize, unsigned int notificationProcId, unsigned int periodMillisec, unsigned int notifyPrev, long long fee);
    unsigned int   (*getOracleQuery)(const void* ctx, long long queryId, void* out, unsigned int size);
    unsigned int   (*getOracleReply)(const void* ctx, long long queryId, void* out, unsigned int size);
    unsigned char  (*distributeDividends)(const void* ctx, long long amountPerShare);
    // inter-contract calls (late-bound): run the callee's DEPLOYED code via the host dispatch tables.
    // returns 0 (NoCallError) on success, else an InterContractCallError code.
    int (*liteCallFunction)(const void* callerCtx, unsigned int calleeIdx, unsigned short inputType,
                            const void* in, unsigned int inSize, void* out, unsigned int outSize);
    int (*liteInvokeProcedure)(const void* callerCtx, unsigned int calleeIdx, unsigned short inputType,
                               const void* in, unsigned int inSize, void* out, unsigned int outSize,
                               long long invocationReward);
    // shareholder governance: invoke another contract's SET_SHAREHOLDER_PROPOSAL / SET_SHAREHOLDER_VOTES callback.
    unsigned short (*setShareholderProposal)(const void* callerCtx, unsigned int calleeIdx, const void* proposal1024, long long invocationReward);
    unsigned char  (*setShareholderVotes)(const void* callerCtx, unsigned int calleeIdx, const void* voteData, unsigned int voteSize, long long invocationReward);
};

// Max user functions+procedures a contract may register (wasm reg table bound).
#define LITE_MAX_USER_ENTRIES 1024

// IDL seed (names + sizes). Names are best-effort; the build tool emits richer IDL JSON.
struct LiteContractDescriptor {
    uint32_t abiVersion;
    char     name[16];
    uint64_t stateSize;
    uint32_t stateLayoutVersion;
    uint16_t entryCount;
};

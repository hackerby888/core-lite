#pragma once
// ABI bridge for runtime-deployed contract .so files. See DYNAMIC_CONTRACTS.md.
//
// Two roles, one file:
//   * Shared ABI structs (always) — the host and the .so must agree on these layouts.
//   * .so-side forwarders + entry points (under LITE_DYN_SO_BUILD) — provide definitions
//     for the QPI surface so the .so binds to the host's QPI via a vtable at load time,
//     WITHOUT -rdynamic. The .so build defines LITE_DYN_SO_BUILD and includes this AFTER
//     qpi.h and the contract source.
//
// The host (lite_dynamic_contracts.h) includes this WITHOUT LITE_DYN_SO_BUILD to get the
// structs, builds a LiteHostServices vtable of thin wrappers, and at deploy calls the .so's
// liteSetHostServices() + liteContractRegister().

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

// Vtable the host fills and the .so calls through. Bind-by-pointer (not -rdynamic):
// the single-TU host inlines most QpiContext methods away, so name-binding fails; the
// host wraps each needed method in a thin free function that forces emission.
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
};

// One registered user function/procedure (filled by the .so during liteContractRegister).
struct LiteUserEntry {
    uint16_t inputType;
    uint8_t  kind;        // LiteEntryKind
    uint16_t inputSize;
    uint16_t outputSize;
    uint32_t localsSize;
    void*    fn;          // USER_FUNCTION / USER_PROCEDURE
};

#define LITE_MAX_USER_ENTRIES 1024

// Output of liteContractRegister: everything the host needs to patch a slot's tables.
struct LiteRegistration {
    uint32_t           abiVersion;
    uint64_t           stateSize;
    void*              systemProcedures[LITE_SP_COUNT];     // SYSTEM_PROCEDURE; null if empty
    uint16_t           systemProcedureLocalsSizes[LITE_SP_COUNT];
    void*              expandProcedure;                     // EXPAND_PROCEDURE; null if empty
    uint32_t           userEntryCount;
    LiteUserEntry      userEntries[LITE_MAX_USER_ENTRIES];
};

// IDL seed (names + sizes). Names are best-effort; the build tool emits richer IDL JSON.
struct LiteContractDescriptor {
    uint32_t abiVersion;
    char     name[16];
    uint64_t stateSize;
    uint32_t stateLayoutVersion;
    uint16_t entryCount;
};

// ===========================================================================
// .so-side definitions. Only compiled into the dynamic contract object.
// Requires qpi.h + pre_qpi_def.h (declarations) already included, and the
// contract included with CONTRACT_INDEX / CONTRACT_STATE_TYPE defined.
// ===========================================================================
#ifdef LITE_DYN_SO_BUILD

// QPI trivial mem templates: declared in qpi.h, normally defined in qpi_trivial_impl.h
// (too heavy to include here — pulls four_q/k12/globals). Define them locally so the .so
// self-resolves contract state/array assignment (else dlopen RTLD_NOW fails on copyMemory).
namespace QPI
{
    template <typename T1, typename T2>
    inline void copyMemory(T1& dst, const T2& src)
    {
        static_assert(sizeof(dst) == sizeof(src), "copyMemory size mismatch");
        __builtin_memcpy(&dst, &src, sizeof(dst));
    }
    template <typename T1, typename T2>
    inline void copyToBuffer(T1& dst, const T2& src, bool setTailToZero)
    {
        __builtin_memcpy(&dst, &src, sizeof(src));
        if (sizeof(dst) > sizeof(src) && setTailToZero)
            __builtin_memset(reinterpret_cast<unsigned char*>(&dst) + sizeof(src), 0, sizeof(dst) - sizeof(src));
    }
    template <typename T1, typename T2>
    inline void copyFromBuffer(T1& dst, const T2& src)
    {
        __builtin_memcpy(&dst, &src, sizeof(dst));
    }
    template <typename T>
    inline void setMemory(T& dst, unsigned char value)
    {
        __builtin_memset(&dst, value, sizeof(dst));
    }
}

// Set by the host at dlopen, before any contract code runs.
inline LiteHostServices* g_liteHost = nullptr;
inline LiteRegistration* g_liteReg = nullptr;   // active during liteContractRegister()

extern "C" void liteSetHostServices(LiteHostServices* services) { g_liteHost = services; }

// ---- static QPI hooks (declared static in pre_qpi_def.h) ----
static void __markContractStateDirty(unsigned int contractIndex) { g_liteHost->markDirty(contractIndex); }
static void __beginFunctionOrProcedure(const unsigned int id) { g_liteHost->beginFn(id); }
static void __endFunctionOrProcedure(const unsigned int id) { g_liteHost->endFn(id); }
static void __pauseLogMessage() { g_liteHost->pauseLog(); }
static void __resumeLogMessage() { g_liteHost->resumeLog(); }
static void* __acquireScratchpad(unsigned long long size, bool initZero) { return g_liteHost->acquireScratch(size, initZero); }
static void __releaseScratchpad(void* ptr) { g_liteHost->releaseScratch(ptr); }

template <typename T> static void __logContractDebugMessage(unsigned int ci, T& m)   { g_liteHost->logBytes(ci, 0, &m, sizeof(T)); }
template <typename T> static void __logContractErrorMessage(unsigned int ci, T& m)   { g_liteHost->logBytes(ci, 1, &m, sizeof(T)); }
template <typename T> static void __logContractInfoMessage(unsigned int ci, T& m)    { g_liteHost->logBytes(ci, 2, &m, sizeof(T)); }
template <typename T> static void __logContractWarningMessage(unsigned int ci, T& m) { g_liteHost->logBytes(ci, 3, &m, sizeof(T)); }

// ---- templated QpiContext method (host-TU template; define locally, forward to backend) ----
template <typename T>
QPI::id QPI::QpiContextFunctionCall::K12(const T& data) const {
    QPI::id out;
    g_liteHost->k12(&data, sizeof(T), &out);
    return out;
}

// ---- QpiContext method forwarders (extend as contracts require / codegen) ----
long long QPI::QpiContextProcedureCall::transfer(const m256i& destination, long long amount) const {
    return g_liteHost->transfer(this, &destination, amount);
}
long long QPI::QpiContextProcedureCall::__transfer(const m256i& destination, long long amount, unsigned char transferType) const {
    return g_liteHost->transferTyped(this, &destination, amount, transferType);
}
void QPI::QpiContextFunctionCall::__qpiAbort(unsigned int errorCode) const {
    g_liteHost->abort(this, errorCode);
}

// ---- registration: record into g_liteReg instead of host tables ----
// entryPoint value must match REGISTER_USER_FUNCTIONS_AND_PROCEDURES_CALL in contract_def.h
// (= contractSystemProcedureCount + 3 = LITE_SP_COUNT + 3).
QPI::QpiContextForInit::QpiContextForInit(unsigned int contractIndex)
    : QpiContext(contractIndex, QPI::NULL_ID, QPI::NULL_ID, 0, (unsigned char)(LITE_SP_COUNT + 3)) {}

void QPI::QpiContextForInit::__registerUserFunction(USER_FUNCTION userFunction, unsigned short inputType,
        unsigned short inputSize, unsigned short outputSize, unsigned int localsSize) const {
    if (!g_liteReg || g_liteReg->userEntryCount >= LITE_MAX_USER_ENTRIES) return;
    LiteUserEntry& e = g_liteReg->userEntries[g_liteReg->userEntryCount++];
    e.inputType = inputType; e.kind = LITE_KIND_FUNCTION;
    e.inputSize = inputSize; e.outputSize = outputSize; e.localsSize = localsSize;
    e.fn = (void*)userFunction;
}

void QPI::QpiContextForInit::__registerUserProcedure(USER_PROCEDURE userProcedure, unsigned short inputType,
        unsigned short inputSize, unsigned short outputSize, unsigned int localsSize) const {
    if (!g_liteReg || g_liteReg->userEntryCount >= LITE_MAX_USER_ENTRIES) return;
    LiteUserEntry& e = g_liteReg->userEntries[g_liteReg->userEntryCount++];
    e.inputType = inputType; e.kind = LITE_KIND_PROCEDURE;
    e.inputSize = inputSize; e.outputSize = outputSize; e.localsSize = localsSize;
    e.fn = (void*)userProcedure;
}

// ---- exported entry points the host calls at deploy ----
#ifdef CONTRACT_STATE_TYPE

#define LITE_SP(empty, fn, id) \
    if (!CONTRACT_STATE_TYPE::empty) out->systemProcedures[id] = (void*)CONTRACT_STATE_TYPE::fn; \
    out->systemProcedureLocalsSizes[id] = (uint16_t)CONTRACT_STATE_TYPE::fn##LocalsSize;

extern "C" void liteContractRegister(LiteRegistration* out) {
    g_liteReg = out;
    out->abiVersion = LITE_DYN_ABI_VERSION;
    out->stateSize = sizeof(CONTRACT_STATE_TYPE::StateData);
    out->userEntryCount = 0;

    LITE_SP(__initializeEmpty,           __initialize,           LITE_SP_INITIALIZE)
    LITE_SP(__beginEpochEmpty,           __beginEpoch,           LITE_SP_BEGIN_EPOCH)
    LITE_SP(__endEpochEmpty,             __endEpoch,             LITE_SP_END_EPOCH)
    LITE_SP(__beginTickEmpty,            __beginTick,            LITE_SP_BEGIN_TICK)
    LITE_SP(__endTickEmpty,              __endTick,              LITE_SP_END_TICK)
    LITE_SP(__preReleaseSharesEmpty,     __preReleaseShares,     LITE_SP_PRE_RELEASE_SHARES)
    LITE_SP(__preAcquireSharesEmpty,     __preAcquireShares,     LITE_SP_PRE_ACQUIRE_SHARES)
    LITE_SP(__postReleaseSharesEmpty,    __postReleaseShares,    LITE_SP_POST_RELEASE_SHARES)
    LITE_SP(__postAcquireSharesEmpty,    __postAcquireShares,    LITE_SP_POST_ACQUIRE_SHARES)
    LITE_SP(__postIncomingTransferEmpty, __postIncomingTransfer, LITE_SP_POST_INCOMING_TRANSFER)
    LITE_SP(__setShareholderProposalEmpty, __setShareholderProposal, LITE_SP_SET_SHAREHOLDER_PROPOSAL)
    LITE_SP(__setShareholderVotesEmpty,  __setShareholderVotes,  LITE_SP_SET_SHAREHOLDER_VOTES)

    if (!CONTRACT_STATE_TYPE::__expandEmpty) out->expandProcedure = (void*)CONTRACT_STATE_TYPE::__expand;

    QPI::QpiContextForInit qpi(CONTRACT_INDEX);
    CONTRACT_STATE_TYPE::__registerUserFunctionsAndProcedures(qpi);
    g_liteReg = nullptr;
}

#undef LITE_SP
#endif // CONTRACT_STATE_TYPE
#endif // LITE_DYN_SO_BUILD

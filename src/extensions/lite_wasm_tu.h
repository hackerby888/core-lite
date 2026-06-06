#pragma once
// Contract-side wasm binding (compiled INTO contract.wasm by qinit's clang.wasm, NOT into the node).
// The wasm analog of lite_dyn_abi.h's LITE_DYN_SO_BUILD block: the contract's qpi.X() calls resolve to wasm
// IMPORTS from module "lhost" (matching lite_wasm_imports.h on the node); the contract's entry points become
// wasm EXPORTS (dispatch / reg_count / reg_info / state_addr / state_size / io_base). Pointers cross as i32
// linear-mem offsets; the import DROPS the QpiContext (host binds it out-of-band). See WASM_CONTRACTS.md §13.11.
#ifdef LITE_WASM_TU_BUILD

#define LH_IMPORT(name) __attribute__((import_module("lhost"), import_name(#name)))
#define LH_EXPORT(name) __attribute__((export_name(#name)))

// ---- core mem ops (the contract's qpi.h collections need them; no libc in the freestanding wasm) ----
void setMem(void* buffer, unsigned long long size, unsigned char value) { __builtin_memset(buffer, value, size); }
void copyMem(void* destination, const void* source, unsigned long long length) { __builtin_memcpy(destination, source, length); }
bool allocatePool(unsigned long long size, void** buffer) { *buffer = __builtin_malloc(size); return *buffer != nullptr; }
void freePool(void* buffer) { __builtin_free(buffer); }

// ---- host imports (module "lhost"); ctx is host-bound so NOT passed; pointers are i32 offsets ----
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
LH_IMPORT(transferShareOwnershipAndPossession) long long lh_transferShares(unsigned long long name, const void* issuer32, const void* owner32, const void* possessor32, long long shares, const void* newOwner32);
LH_IMPORT(distributeDividends) unsigned int lh_distributeDividends(long long amountPerShare);
LH_IMPORT(liteCallFunction) int lh_liteCallFunction(unsigned int calleeIdx, unsigned int inputType, const void* in, unsigned int inSize, void* out, unsigned int outSize);
LH_IMPORT(liteInvokeProcedure) int lh_liteInvokeProcedure(unsigned int calleeIdx, unsigned int inputType, const void* in, unsigned int inSize, void* out, unsigned int outSize, long long invocationReward);
LH_IMPORT(liteSetShareholderProposal) unsigned int lh_liteSetShareholderProposal(unsigned int calleeIdx, const void* proposal1024, long long invocationReward);
LH_IMPORT(liteSetShareholderVotes) unsigned int lh_liteSetShareholderVotes(unsigned int calleeIdx, const void* voteData, unsigned int voteSize, long long invocationReward);
} // extern "C"

// inter-contract helpers (called by the redefined CALL_OTHER_CONTRACT_* macros from contract code).
int liteCallFunction(const void*, unsigned int calleeIdx, unsigned short inputType,
                     const void* in, unsigned int inSize, void* out, unsigned int outSize) {
    return lh_liteCallFunction(calleeIdx, inputType, in, inSize, out, outSize);
}
int liteInvokeProcedure(const void*, unsigned int calleeIdx, unsigned short inputType,
                        const void* in, unsigned int inSize, void* out, unsigned int outSize, long long reward) {
    return lh_liteInvokeProcedure(calleeIdx, inputType, in, inSize, out, outSize, reward);
}

// ---- static QPI hooks (declared static in pre_qpi_def.h) ----
static void __markContractStateDirty(unsigned int ci) { lh_markDirty(ci); }
static void __beginFunctionOrProcedure(const unsigned int id) { lh_beginFn(id); }
static void __endFunctionOrProcedure(const unsigned int id) { lh_endFn(id); }
static void __pauseLogMessage() { lh_pauseLog(); }
static void __resumeLogMessage() { lh_resumeLog(); }
static void* __acquireScratchpad(unsigned long long size, bool initZero) { return lh_acquireScratch(size, initZero ? 1u : 0u); }
static void __releaseScratchpad(void* ptr) { lh_releaseScratch(ptr); }
template <typename T> static void __logContractDebugMessage(unsigned int ci, T& m)   { lh_logBytes(ci, 7, &m, (unsigned int)__builtin_offsetof(T, _terminator)); }
template <typename T> static void __logContractErrorMessage(unsigned int ci, T& m)   { lh_logBytes(ci, 4, &m, (unsigned int)__builtin_offsetof(T, _terminator)); }
template <typename T> static void __logContractInfoMessage(unsigned int ci, T& m)    { lh_logBytes(ci, 6, &m, (unsigned int)__builtin_offsetof(T, _terminator)); }
template <typename T> static void __logContractWarningMessage(unsigned int ci, T& m) { lh_logBytes(ci, 5, &m, (unsigned int)__builtin_offsetof(T, _terminator)); }

// ---- QpiContext method forwarders (mirror lite_dyn_abi.h, but call imports; ctx dropped) ----
template <typename T>
QPI::id QPI::QpiContextFunctionCall::K12(const T& data) const { QPI::id out; lh_k12(&data, sizeof(T), &out); return out; }

long long QPI::QpiContextProcedureCall::transfer(const m256i& d, long long a) const { return lh_transfer(&d, a); }
long long QPI::QpiContextProcedureCall::__transfer(const m256i& d, long long a, unsigned char t) const { return lh_transferTyped(&d, a, t); }
void QPI::QpiContextFunctionCall::__qpiAbort(unsigned int e) const { lh_abort(e); }
long long QPI::QpiContextProcedureCall::burn(long long a, unsigned int idx) const { return lh_burn(a, idx); }
unsigned short QPI::QpiContextFunctionCall::epoch() const { return (unsigned short)lh_epoch(); }
unsigned int QPI::QpiContextFunctionCall::tick() const { return lh_tick(); }
int QPI::QpiContextFunctionCall::numberOfTickTransactions() const { return lh_numberOfTickTransactions(); }
QPI::bit QPI::QpiContextFunctionCall::getEntity(const m256i& id, QPI::Entity& e) const { return (QPI::bit)lh_getEntity(&id, &e); }
long long QPI::QpiContextFunctionCall::queryFeeReserve(unsigned int ci) const { return lh_queryFeeReserve(ci); }
m256i QPI::QpiContextFunctionCall::nextId(const m256i& c) const { m256i r; lh_nextId(&c, &r); return r; }
m256i QPI::QpiContextFunctionCall::prevId(const m256i& c) const { m256i r; lh_prevId(&c, &r); return r; }
QPI::bit QPI::QpiContextFunctionCall::isContractId(const QPI::id& id) const { return (QPI::bit)lh_isContractId(&id); }
QPI::id QPI::QpiContextFunctionCall::arbitrator() const { m256i r; lh_arbitrator(&r); return r; }
QPI::id QPI::QpiContextFunctionCall::computor(unsigned short i) const { m256i r; lh_computor(i, &r); return r; }
unsigned char QPI::QpiContextFunctionCall::day() const { return (unsigned char)lh_day(); }
unsigned char QPI::QpiContextFunctionCall::year() const { return (unsigned char)lh_year(); }
unsigned char QPI::QpiContextFunctionCall::hour() const { return (unsigned char)lh_hour(); }
unsigned char QPI::QpiContextFunctionCall::minute() const { return (unsigned char)lh_minute(); }
unsigned char QPI::QpiContextFunctionCall::month() const { return (unsigned char)lh_month(); }
unsigned char QPI::QpiContextFunctionCall::second() const { return (unsigned char)lh_second(); }
unsigned short QPI::QpiContextFunctionCall::millisecond() const { return (unsigned short)lh_millisecond(); }
QPI::DateAndTime QPI::QpiContextFunctionCall::now() const { QPI::DateAndTime d; lh_now(&d); return d; }
m256i QPI::QpiContextFunctionCall::getPrevSpectrumDigest() const { m256i r; lh_prevSpectrumDigest(&r); return r; }
m256i QPI::QpiContextFunctionCall::getPrevUniverseDigest() const { m256i r; lh_prevUniverseDigest(&r); return r; }
m256i QPI::QpiContextFunctionCall::getPrevComputerDigest() const { m256i r; lh_prevComputerDigest(&r); return r; }
bool QPI::QpiContextFunctionCall::isAssetIssued(const m256i& iss, unsigned long long n) const { return lh_isAssetIssued(&iss, n); }
long long QPI::QpiContextProcedureCall::issueAsset(unsigned long long n, const QPI::id& iss, signed char dec, long long sh, unsigned long long u) const { return lh_issueAsset(n, &iss, (unsigned int)(unsigned char)dec, sh, u); }
long long QPI::QpiContextFunctionCall::numberOfShares(const QPI::Asset& a, const QPI::AssetOwnershipSelect& o, const QPI::AssetPossessionSelect& p) const { return lh_numberOfShares(&a, &o, &p); }
long long QPI::QpiContextFunctionCall::numberOfPossessedShares(unsigned long long n, const m256i& iss, const m256i& ow, const m256i& po, unsigned short om, unsigned short pm) const { return lh_numberOfPossessedShares(n, &iss, &ow, &po, om, pm); }
long long QPI::QpiContextProcedureCall::transferShareOwnershipAndPossession(unsigned long long n, const m256i& iss, const m256i& ow, const m256i& po, long long sh, const m256i& no) const { return lh_transferShares(n, &iss, &ow, &po, sh, &no); }
bool QPI::QpiContextProcedureCall::distributeDividends(long long a) const { return lh_distributeDividends(a); }
QPI::uint16 QPI::QpiContextProcedureCall::setShareholderProposal(QPI::uint16 idx, const QPI::Array<QPI::uint8, 1024>& proposalDataBuffer, QPI::sint64 reward) const { return (QPI::uint16)lh_liteSetShareholderProposal(idx, &proposalDataBuffer, reward); }
bool QPI::QpiContextProcedureCall::setShareholderVotes(QPI::uint16 idx, const QPI::ProposalMultiVoteDataV1& voteData, QPI::sint64 reward) const { return lh_liteSetShareholderVotes(idx, &voteData, sizeof(voteData), reward) != 0; }

// ---- registration capture (the contract's __registerUserFunctionsAndProcedures fills this) ----
#ifndef LITE_MAX_USER_ENTRIES
#define LITE_MAX_USER_ENTRIES 1024
#endif
struct LiteWasmTuEntry { unsigned short it; unsigned char kind; unsigned short inSize, outSize; unsigned int localsSize; void* fn; };
static LiteWasmTuEntry g_wasmTuEntries[LITE_MAX_USER_ENTRIES];
static unsigned int    g_wasmTuEntryCount = 0;

QPI::QpiContextForInit::QpiContextForInit(unsigned int contractIndex)
    : QpiContext(contractIndex, QPI::NULL_ID, QPI::NULL_ID, 0, 0) {}
void QPI::QpiContextForInit::__registerUserFunction(USER_FUNCTION fn, unsigned short it,
        unsigned short inSize, unsigned short outSize, unsigned int localsSize) const {
    if (g_wasmTuEntryCount >= LITE_MAX_USER_ENTRIES) return;
    g_wasmTuEntries[g_wasmTuEntryCount++] = { it, 0 /*FUNCTION*/, inSize, outSize, localsSize, (void*)fn };
}
void QPI::QpiContextForInit::__registerUserProcedure(USER_PROCEDURE fn, unsigned short it,
        unsigned short inSize, unsigned short outSize, unsigned int localsSize) const {
    if (g_wasmTuEntryCount >= LITE_MAX_USER_ENTRIES) return;
    g_wasmTuEntries[g_wasmTuEntryCount++] = { it, 1 /*PROCEDURE*/, inSize, outSize, localsSize, (void*)fn };
}

// ---- exported entry points the node calls (compiled when the contract type is defined) ----
#ifdef CONTRACT_STATE_TYPE
typedef void (*LiteWasmUserFn)(const QPI::QpiContextFunctionCall&, void*, void*, void*, void*);

static CONTRACT_STATE_TYPE::StateData g_wasmState;   // resident state, copied in/out by the host per call
alignas(16) static unsigned char     g_wasmCtxBuf[256];   // QpiContext scalar header; host populates per call
alignas(16) static unsigned char     g_wasmIo[3 * (32 * 1024) + 16 * 1024];   // [in|out|locals|arena]; slot/arena sizes MUST match LITE_WASM_IO_SLOT/ARENA_SZ in lite_wasm_contracts.h

static bool g_wasmRegistered = false;
static void liteWasmTuEnsureRegistered() {
    if (g_wasmRegistered) return;
    g_wasmRegistered = true;
    QPI::QpiContextForInit qpi(CONTRACT_INDEX);
    CONTRACT_STATE_TYPE::__registerUserFunctionsAndProcedures(qpi);
}

extern "C" {
LH_EXPORT(state_addr) unsigned int state_addr() { return (unsigned int)(unsigned long)&g_wasmState; }
LH_EXPORT(state_size) unsigned int state_size() { return (unsigned int)sizeof(g_wasmState); }
LH_EXPORT(io_base)    unsigned int io_base()    { return (unsigned int)(unsigned long)&g_wasmIo[0]; }
LH_EXPORT(ctx_addr)   unsigned int ctx_addr()   { return (unsigned int)(unsigned long)&g_wasmCtxBuf[0]; }

LH_EXPORT(reg_count)  unsigned int reg_count()  { liteWasmTuEnsureRegistered(); return g_wasmTuEntryCount; }

struct LiteWasmTuInfo { unsigned int inputType, kind, inSize, outSize; };
LH_EXPORT(reg_info)
void reg_info(unsigned int i, LiteWasmTuInfo* out) {
    liteWasmTuEnsureRegistered();
    if (i >= g_wasmTuEntryCount) { setMem(out, sizeof(*out), 0); return; }
    const LiteWasmTuEntry& e = g_wasmTuEntries[i];
    out->inputType = e.it; out->kind = e.kind; out->inSize = e.inSize; out->outSize = e.outSize;
}

// System procedures. Bit i (= LITE_SP_* id) set if the contract defines it. Order matches lite_dyn_abi.h:
// 0 INITIALIZE, 1 BEGIN_EPOCH, 2 END_EPOCH, 3 BEGIN_TICK, 4 END_TICK, 5 PRE_RELEASE_SHARES,
// 6 PRE_ACQUIRE_SHARES, 7 POST_RELEASE_SHARES, 8 POST_ACQUIRE_SHARES, 9 POST_INCOMING_TRANSFER,
// 10 SET_SHAREHOLDER_PROPOSAL, 11 SET_SHAREHOLDER_VOTES.
LH_EXPORT(reg_sysproc_mask)
unsigned int reg_sysproc_mask() {
    unsigned int m = 0;
    if (!CONTRACT_STATE_TYPE::__initializeEmpty)            m |= (1u << 0);
    if (!CONTRACT_STATE_TYPE::__beginEpochEmpty)            m |= (1u << 1);
    if (!CONTRACT_STATE_TYPE::__endEpochEmpty)              m |= (1u << 2);
    if (!CONTRACT_STATE_TYPE::__beginTickEmpty)             m |= (1u << 3);
    if (!CONTRACT_STATE_TYPE::__endTickEmpty)               m |= (1u << 4);
    if (!CONTRACT_STATE_TYPE::__preReleaseSharesEmpty)      m |= (1u << 5);
    if (!CONTRACT_STATE_TYPE::__preAcquireSharesEmpty)      m |= (1u << 6);
    if (!CONTRACT_STATE_TYPE::__postReleaseSharesEmpty)     m |= (1u << 7);
    if (!CONTRACT_STATE_TYPE::__postAcquireSharesEmpty)     m |= (1u << 8);
    if (!CONTRACT_STATE_TYPE::__postIncomingTransferEmpty)  m |= (1u << 9);
    if (!CONTRACT_STATE_TYPE::__setShareholderProposalEmpty)m |= (1u << 10);
    if (!CONTRACT_STATE_TYPE::__setShareholderVotesEmpty)   m |= (1u << 11);
    return m;
}
LH_EXPORT(sysproc_locals_size)
unsigned int sysproc_locals_size(unsigned int sp) {
    switch (sp) {
        case 0:  return (unsigned int)CONTRACT_STATE_TYPE::__initializeLocalsSize;
        case 1:  return (unsigned int)CONTRACT_STATE_TYPE::__beginEpochLocalsSize;
        case 2:  return (unsigned int)CONTRACT_STATE_TYPE::__endEpochLocalsSize;
        case 3:  return (unsigned int)CONTRACT_STATE_TYPE::__beginTickLocalsSize;
        case 4:  return (unsigned int)CONTRACT_STATE_TYPE::__endTickLocalsSize;
        case 5:  return (unsigned int)CONTRACT_STATE_TYPE::__preReleaseSharesLocalsSize;
        case 6:  return (unsigned int)CONTRACT_STATE_TYPE::__preAcquireSharesLocalsSize;
        case 7:  return (unsigned int)CONTRACT_STATE_TYPE::__postReleaseSharesLocalsSize;
        case 8:  return (unsigned int)CONTRACT_STATE_TYPE::__postAcquireSharesLocalsSize;
        case 9:  return (unsigned int)CONTRACT_STATE_TYPE::__postIncomingTransferLocalsSize;
        case 10: return (unsigned int)CONTRACT_STATE_TYPE::__setShareholderProposalLocalsSize;
        case 11: return (unsigned int)CONTRACT_STATE_TYPE::__setShareholderVotesLocalsSize;
    }
    return 0;
}
// QPI-defined input/output sizes for the share-management sysprocs (lifecycle ones = NoData).
LH_EXPORT(sysproc_in_size)
unsigned int sysproc_in_size(unsigned int sp) {
    switch (sp) {
        case 5: case 6: return (unsigned int)sizeof(QPI::PreManagementRightsTransfer_input);
        case 7: case 8: return (unsigned int)sizeof(QPI::PostManagementRightsTransfer_input);
        case 9:  return (unsigned int)sizeof(QPI::PostIncomingTransfer_input);
        case 10: return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_PROPOSAL_input);
        case 11: return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_VOTES_input);
    }
    return 0;
}
LH_EXPORT(sysproc_out_size)
unsigned int sysproc_out_size(unsigned int sp) {
    switch (sp) {
        case 5: case 6: return (unsigned int)sizeof(QPI::PreManagementRightsTransfer_output);
        case 10: return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_PROPOSAL_output);
        case 11: return (unsigned int)sizeof(QPI::SET_SHAREHOLDER_VOTES_output);
    }
    return 0;   // POST_* outputs are NoData
}

// kind/it select the entry; in/out/locals are linear-mem offsets (== ptrs in wasm32). The host has already
// copied state -> g_wasmState and the input bytes -> inOff, and populated the ctx header at ctx_addr.
LH_EXPORT(dispatch)
void dispatch(unsigned int kind, unsigned int it, unsigned int inOff, unsigned int outOff, unsigned int localsOff) {
    liteWasmTuEnsureRegistered();
    void* in = (void*)(unsigned long)inOff; void* out = (void*)(unsigned long)outOff; void* lo = (void*)(unsigned long)localsOff;
    if (kind == 2) {   // system procedure; it = LITE_SP_* id (lifecycle set)
        // the generated __initialize/etc have typed signatures (ContractState&, varying arity); cast through
        // void* to the uniform SYSTEM_PROCEDURE shape + call with 5 args, exactly as lite_dyn_abi.h's table does.
        typedef void (*LiteWasmSysProc)(const QPI::QpiContextProcedureCall&, void*, void*, void*, void*);
        auto& pctx = *reinterpret_cast<QPI::QpiContextProcedureCall*>(&g_wasmCtxBuf[0]);
        #define LITE_WASM_SP_CALL(fn) ((LiteWasmSysProc)(void*)CONTRACT_STATE_TYPE::fn)(pctx, &g_wasmState, in, out, lo)
        switch (it) {
            case 0:  LITE_WASM_SP_CALL(__initialize);             break;
            case 1:  LITE_WASM_SP_CALL(__beginEpoch);             break;
            case 2:  LITE_WASM_SP_CALL(__endEpoch);               break;
            case 3:  LITE_WASM_SP_CALL(__beginTick);              break;
            case 4:  LITE_WASM_SP_CALL(__endTick);                break;
            case 5:  LITE_WASM_SP_CALL(__preReleaseShares);       break;
            case 6:  LITE_WASM_SP_CALL(__preAcquireShares);       break;
            case 7:  LITE_WASM_SP_CALL(__postReleaseShares);      break;
            case 8:  LITE_WASM_SP_CALL(__postAcquireShares);      break;
            case 9:  LITE_WASM_SP_CALL(__postIncomingTransfer);   break;
            case 10: LITE_WASM_SP_CALL(__setShareholderProposal); break;
            case 11: LITE_WASM_SP_CALL(__setShareholderVotes);    break;
        }
        #undef LITE_WASM_SP_CALL
        return;
    }
    auto& ctx = *reinterpret_cast<QPI::QpiContextFunctionCall*>(&g_wasmCtxBuf[0]);
    for (unsigned int i = 0; i < g_wasmTuEntryCount; i++) {
        const LiteWasmTuEntry& e = g_wasmTuEntries[i];
        if (e.it == (unsigned short)it && e.kind == (unsigned char)kind) {
            ((LiteWasmUserFn)e.fn)(ctx, &g_wasmState, (void*)(unsigned long)inOff,
                                   (void*)(unsigned long)outOff, (void*)(unsigned long)localsOff);
            return;
        }
    }
}
} // extern "C"

#endif // CONTRACT_STATE_TYPE

#undef LH_IMPORT
#undef LH_EXPORT
#endif // LITE_WASM_TU_BUILD

#pragma once
// Host-side runtime dynamic-contract deploy subsystem (testnet, LITE_DYNAMIC_CONTRACTS).
// See extensions/WASM_CONTRACTS.md. Included by qubic.cpp AFTER the contract machinery
// (contract_def.h + contract_exec.h). Owns the deploy registry + uploaded wasm bytes as
// EXTENSION state (never in contract StateData). Builds the LiteHostServices vtable, hands
// the uploaded module to the wasm engine, then constructs the deployable slot.
#ifdef LITE_DYNAMIC_CONTRACTS

#include "extensions/lite_dyn_abi.h" // shared ABI structs (LiteHostServices vtable etc)

#ifdef _MSC_VER
// The Windows SDK's specstrings.h (pulled in by <windows.h> via overload.h) defines a function-like
// SAL macro `__transfer(formal)` that mangles the QPI __transfer member calls below. We never use SAL.
#undef __transfer
#endif

// Upper bound on a deployable contract module. Bytes live here (extension state), not in any StateData.
#ifndef LITE_DYN_MAX_MODULE
#define LITE_DYN_MAX_MODULE (4u * 1024u * 1024u)
#endif

// Number of reserved deployable slots — must match the LITEDYN0..N block in contract_def.h.
#define LITE_DYN_SLOT_COUNT 4

// logToConsole is defined later in qubic.cpp (same TU); this header is included before that point.
void logToConsole(const CHAR16* message);

// ---------------------------------------------------------------------------
// Host-services vtable — the host's QPI surface. The wasm engine exposes these
// to the module as "lhost" imports (lite_wasm_imports.h mirrors this table).
// ---------------------------------------------------------------------------
// Designated initializers (C++20): each lambda binds to its vtable member BY NAME, so the struct and this
// table can never silently desync by order (a reorder/insert is a compile error, not a wrong-fn-at-runtime).
static LiteHostServices g_liteHostServices = {
    .abiVersion = LITE_DYN_ABI_VERSION,
    .beginFn = +[](unsigned int id) { __beginFunctionOrProcedure(id); },
    .endFn = +[](unsigned int id) { __endFunctionOrProcedure(id); },
    .markDirty = +[](unsigned int ci) { __markContractStateDirty(ci); },
    .pauseLog = +[]() { __pauseLogMessage(); },
    .resumeLog = +[]() { __resumeLogMessage(); },
    .acquireScratch = +[](unsigned long long s, bool z) -> void* { return __acquireScratchpad(s, z); },
    .releaseScratch = +[](void* p) { __releaseScratchpad(p); },
    .logBytes = +[](unsigned int ci, unsigned char type, const void* msg, unsigned int size) {
        *((unsigned int*)(void*)msg) = ci;           // contractIndex into first 4 bytes (host convention)
        qLogger::logMessage(size, type, msg);        // type = CONTRACT_{ERROR,WARNING,INFORMATION,DEBUG}_MESSAGE
    },
    .k12 = +[](const void* in, unsigned int len, void* out32) { KangarooTwelve(in, len, out32, 32); },
    .transfer = +[](const void* ctx, const void* d, long long a) -> long long {
        return ((QPI::QpiContextProcedureCall*)ctx)->transfer(*(const m256i*)d, a);
    },
    .transferTyped = +[](const void* ctx, const void* d, long long a, unsigned char t) -> long long {
        return ((QPI::QpiContextProcedureCall*)ctx)->__transfer(*(const m256i*)d, a, t);
    },
    .abort = +[](const void* ctx, unsigned int e) { ((QPI::QpiContextProcedureCall*)ctx)->__qpiAbort(e); },
    .burn = +[](const void* ctx, long long a, unsigned int idx) -> long long { return ((QPI::QpiContextProcedureCall*)ctx)->burn(a, idx); },
    .epoch = +[](const void* ctx) -> unsigned short { return ((QPI::QpiContextFunctionCall*)ctx)->epoch(); },
    .tick = +[](const void* ctx) -> unsigned int { return ((QPI::QpiContextFunctionCall*)ctx)->tick(); },
    .numberOfTickTransactions = +[](const void* ctx) -> int { return ((QPI::QpiContextFunctionCall*)ctx)->numberOfTickTransactions(); },
    .getEntity = +[](const void* c, const void* id32, void* eo) -> unsigned char { return (unsigned char)((QPI::QpiContextFunctionCall*)c)->getEntity(*(const m256i*)id32, *(QPI::Entity*)eo); },
    .queryFeeReserve = +[](const void* c, unsigned int ci) -> long long { return ((QPI::QpiContextFunctionCall*)c)->queryFeeReserve(ci); },
    .nextId = +[](const void* c, const void* id32, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->nextId(*(const m256i*)id32); },
    .prevId = +[](const void* c, const void* id32, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->prevId(*(const m256i*)id32); },
    .isContractId = +[](const void* c, const void* id32) -> unsigned char { return (unsigned char)((QPI::QpiContextFunctionCall*)c)->isContractId(*(const m256i*)id32); },
    .arbitrator = +[](const void* c, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->arbitrator(); },
    .computor = +[](const void* c, unsigned short i, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->computor(i); },
    .day = +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->day(); },
    .year = +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->year(); },
    .hour = +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->hour(); },
    .minute = +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->minute(); },
    .month = +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->month(); },
    .second = +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->second(); },
    .millisecond = +[](const void* c) -> unsigned short { return ((QPI::QpiContextFunctionCall*)c)->millisecond(); },
    .now = +[](const void* c, void* o) { *(QPI::DateAndTime*)o = ((QPI::QpiContextFunctionCall*)c)->now(); },
    .prevSpectrumDigest = +[](const void* c, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->getPrevSpectrumDigest(); },
    .prevUniverseDigest = +[](const void* c, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->getPrevUniverseDigest(); },
    .prevComputerDigest = +[](const void* c, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->getPrevComputerDigest(); },
    .isAssetIssued = +[](const void* c, const void* i, unsigned long long n) -> unsigned char { return (unsigned char)((QPI::QpiContextFunctionCall*)c)->isAssetIssued(*(const m256i*)i, n); },
    .issueAsset = +[](const void* c, unsigned long long n, const void* i, signed char d, long long s, unsigned long long u) -> long long { return ((QPI::QpiContextProcedureCall*)c)->issueAsset(n, *(const QPI::id*)i, d, s, u); },
    .numberOfShares = +[](const void* c, const void* a, const void* o, const void* p) -> long long { return ((QPI::QpiContextFunctionCall*)c)->numberOfShares(*(const QPI::Asset*)a, *(const QPI::AssetOwnershipSelect*)o, *(const QPI::AssetPossessionSelect*)p); },
    .numberOfPossessedShares = +[](const void* c, unsigned long long n, const void* i, const void* o, const void* p, unsigned short om, unsigned short pm) -> long long { return ((QPI::QpiContextFunctionCall*)c)->numberOfPossessedShares(n, *(const m256i*)i, *(const m256i*)o, *(const m256i*)p, om, pm); },
    .transferShareOwnershipAndPossession = +[](const void* c, unsigned long long n, const void* i, const void* o, const void* p, long long s, const void* no) -> long long { return ((QPI::QpiContextProcedureCall*)c)->transferShareOwnershipAndPossession(n, *(const m256i*)i, *(const m256i*)o, *(const m256i*)p, s, *(const m256i*)no); },
    .acquireShares = +[](const void* c, unsigned long long n, const void* i, const void* o, const void* p, long long s, unsigned short so, unsigned short sp, long long f) -> long long { return ((QPI::QpiContextProcedureCall*)c)->acquireShares(QPI::Asset{*(const m256i*)i, n}, *(const m256i*)o, *(const m256i*)p, s, so, sp, f); },
    .releaseShares = +[](const void* c, unsigned long long n, const void* i, const void* o, const void* p, long long s, unsigned short dno, unsigned short dp, long long f) -> long long { return ((QPI::QpiContextProcedureCall*)c)->releaseShares(QPI::Asset{*(const m256i*)i, n}, *(const m256i*)o, *(const m256i*)p, s, dno, dp, f); },
    .distributeDividends = +[](const void* c, long long a) -> unsigned char { return (unsigned char)((QPI::QpiContextProcedureCall*)c)->distributeDividends(a); },
    // liteCallFunction: run the callee's DEPLOYED function (table dispatch) under a nested context.
    .liteCallFunction = +[](const void* cc, unsigned int idx, unsigned short it, const void* in, unsigned int, void* out, unsigned int) -> int {
        if (idx >= contractCount || !contractUserFunctions[idx][it]) return (int)QPI::CallErrorContractInactive;
        auto* caller = (QPI::QpiContextFunctionCall*)cc;
        QPI::InterContractCallError err = QPI::NoCallError;
        const QPI::QpiContextFunctionCall* cctx = caller->__qpiConstructContextOtherContractFunctionCall(idx, err);
        if (!cctx) return (int)err;
        void* st = caller->__qpiAcquireStateForReading(idx);
        void* lo = caller->__qpiAllocLocals(contractUserFunctionLocalsSizes[idx][it]);
        contractUserFunctions[idx][it](*cctx, st, (void*)in, out, lo);
        caller->__qpiFreeLocals();
        caller->__qpiReleaseStateForReading(idx);
        caller->__qpiFreeContext();
        return (int)QPI::NoCallError;
    },
    // liteInvokeProcedure: run the callee's DEPLOYED procedure (table dispatch) + transfer invocationReward.
    .liteInvokeProcedure = +[](const void* cc, unsigned int idx, unsigned short it, const void* in, unsigned int, void* out, unsigned int, long long reward) -> int {
        if (idx >= contractCount || !contractUserProcedures[idx][it]) return (int)QPI::CallErrorContractInactive;
        auto* caller = (QPI::QpiContextProcedureCall*)cc;
        QPI::InterContractCallError err = QPI::NoCallError;
        const QPI::QpiContextProcedureCall* cctx = caller->__qpiConstructProcedureCallContext(idx, reward, err, false);
        if (!cctx) return (int)err;
        void* st = caller->__qpiAcquireStateForWriting(idx);
        void* lo = caller->__qpiAllocLocals(contractUserProcedureLocalsSizes[idx][it]);
        contractUserProcedures[idx][it](*cctx, st, (void*)in, out, lo);
        caller->__qpiFreeLocals();
        caller->__qpiReleaseStateForWriting(idx);
        caller->__qpiFreeContext();
        return (int)QPI::NoCallError;
    },
    // setShareholderProposal / setShareholderVotes: invoke the callee's governance callback via the QPI method.
    .setShareholderProposal = +[](const void* c, unsigned int idx, const void* p, long long reward) -> unsigned short {
        return ((QPI::QpiContextProcedureCall*)c)->setShareholderProposal((unsigned short)idx, *(const QPI::Array<QPI::uint8, 1024>*)p, reward);
    },
    .setShareholderVotes = +[](const void* c, unsigned int idx, const void* v, unsigned int, long long reward) -> unsigned char {
        return (unsigned char)((QPI::QpiContextProcedureCall*)c)->setShareholderVotes((unsigned short)idx, *(const QPI::ProposalMultiVoteDataV1*)v, reward);
    },
};

// ---------------------------------------------------------------------------
// Extension-owned state: per-slot registry + a single active upload session.
// ---------------------------------------------------------------------------
struct LiteDynSlot {
    bool armed = false;
    bool constructed = false;
    bool everInitialized = false; // INITIALIZE has run at least once -> upgrades skip it (preserve state)
    unsigned char codeHash[32] = {};
    unsigned int activationTick = 0;
    unsigned int version = 0;
    char name[32] = {}; // contract name from the deploy tx; lets tooling resolve name -> slot
    std::string sourceH; // contract .h source (dev-only, node-local, off-chain) for inter-contract callee resolution
};
static LiteDynSlot g_liteDynSlots[LITE_DYN_SLOT_COUNT];

struct LiteDynUpload {
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

static inline unsigned int liteDynSlotBase() { return LITEDYN0_CONTRACT_INDEX; }
static inline int liteDynSlotLocal(unsigned int contractIndex) {
    int i = (int)contractIndex - (int)LITEDYN0_CONTRACT_INDEX;
    return (i >= 0 && i < (int)LITE_DYN_SLOT_COUNT) ? i : -1;
}

// ---------------------------------------------------------------------------
// Upload/deploy transaction handlers (called from processTickTransaction).
// Inputs are consensus-ordered txs; assembly is order-independent (scatter-write).
// ---------------------------------------------------------------------------
[[maybe_unused]] static void liteDynOnUploadBegin(unsigned long long sessionId, unsigned int totalSize,
        unsigned int chunkCount, const unsigned char* finalHash) {
    if (totalSize > LITE_DYN_MAX_MODULE) return;
    g_liteDynUpload.active = true;
    g_liteDynUpload.sessionId = sessionId;
    g_liteDynUpload.totalSize = totalSize;
    g_liteDynUpload.chunkCount = chunkCount;
    g_liteDynUpload.receivedCount = 0;
    copyMem(g_liteDynUpload.finalHash, finalHash, 32);
    setMem(g_liteDynSeqSeen, sizeof(g_liteDynSeqSeen), 0);
    logToConsole(L"LITEDYN: UploadBegin received");
}

[[maybe_unused]] static void liteDynOnUploadChunk(unsigned long long sessionId, unsigned int seq,
        const unsigned char* data, unsigned int dataLen) {
    if (!g_liteDynUpload.active || sessionId != g_liteDynUpload.sessionId) return;
    unsigned long long off = (unsigned long long)seq * 1008ull;
    if (off + dataLen > LITE_DYN_MAX_MODULE) return;
    if (seq >= g_liteDynUpload.chunkCount) return;
    const unsigned int byteIdx = seq >> 3, bit = 1u << (seq & 7);
    if (!(g_liteDynSeqSeen[byteIdx] & bit)) {
        g_liteDynSeqSeen[byteIdx] |= bit;
        g_liteDynUpload.receivedCount++;
    }
    copyMem(g_liteDynBuf + off, data, dataLen);
}

static bool liteDynUploadComplete() {
    if (!g_liteDynUpload.active || g_liteDynUpload.receivedCount != g_liteDynUpload.chunkCount) return false;
    unsigned char h[32];
    KangarooTwelve(g_liteDynBuf, g_liteDynUpload.totalSize, h, 32);
    for (int i = 0; i < 32; i++) if (h[i] != g_liteDynUpload.finalHash[i]) return false;
    return true;
}

#ifdef LITE_WASM_CONTRACTS
// Defined later in the same TU (extensions/lite_wasm_contracts.h, included after this header in qubic.cpp).
static bool liteWasmLoadFromBytes(unsigned int idx, const unsigned char* bytes, unsigned int len);
static bool liteWasmIsWasm(unsigned int idx);
#endif

[[maybe_unused]] static void liteDynOnDeploy(unsigned long long sessionId, unsigned int targetSlot,
        const unsigned char* finalHash, unsigned int /*abiVersion*/, unsigned int /*stateLayoutVersion*/,
        const char* name) {
    int local = liteDynSlotLocal(targetSlot);
    if (local < 0) return;
    if (sessionId != g_liteDynUpload.sessionId || !liteDynUploadComplete()) return;
    for (int i = 0; i < 32; i++) if (finalHash[i] != g_liteDynUpload.finalHash[i]) return;

    LiteDynSlot& s = g_liteDynSlots[local];
    copyMem(s.codeHash, finalHash, 32);
    if (name) { copyMem(s.name, name, 32); s.name[31] = 0; }
    s.armed = true;
    logToConsole(L"LITEDYN: Deploy accepted, slot armed");
    s.constructed = s.everInitialized;   // first deploy -> run INITIALIZE; upgrade -> skip it (state preserved at load)
    s.version++;
    // Load the code now (node-local, non-consensus); construction (state init) is deferred to a framed tick
    // step (liteDynConstructPending). The uploaded artifact must be a wasm module ('\0asm' magic).
    bool loadOk = false;
#ifdef LITE_WASM_CONTRACTS
    const unsigned char* art = g_liteDynBuf;
    if (g_liteDynUpload.totalSize >= 4 && art[0] == 0x00 && art[1] == 0x61 && art[2] == 0x73 && art[3] == 0x6d) {
        loadOk = liteWasmLoadFromBytes(targetSlot, g_liteDynBuf, g_liteDynUpload.totalSize);
        logToConsole(loadOk ? L"LITEDYN: wasm contract loaded" : L"LITEDYN: ERROR wasm load failed");
    } else
        logToConsole(L"LITEDYN: ERROR upload is not a wasm module ('\\0asm' expected)");
#else
    logToConsole(L"LITEDYN: ERROR no contract engine built (enable LITE_WASM_CONTRACTS)");
#endif
    if (!loadOk)
        logToConsole(L"LITEDYN: ERROR load failed - slot armed but not runnable");
    g_liteDynUpload.active = false;
}

// Lite deploy tx inputTypes (system destination). Must match @qinit/proto LITE_TX.
#define LITE_TX_UPLOAD_BEGIN 240
#define LITE_TX_UPLOAD_CHUNK 241
#define LITE_TX_DEPLOY 242

// Decode a lite deploy tx payload (little-endian, matches @qinit/proto) and dispatch.
// Called from processTickTransaction for the system-destination lite inputTypes.
[[maybe_unused]] static void liteDynDispatchTx(unsigned short inputType, const unsigned char* in, unsigned int size) {
    auto rdU64 = [&](unsigned o) { unsigned long long v = 0; for (int i = 0; i < 8; i++) v |= (unsigned long long)in[o + i] << (8 * i); return v; };
    auto rdU32 = [&](unsigned o) { unsigned int v = 0; for (int i = 0; i < 4; i++) v |= (unsigned int)in[o + i] << (8 * i); return v; };
    auto rdU16 = [&](unsigned o) { return (unsigned int)in[o] | ((unsigned int)in[o + 1] << 8); };
    if (inputType == LITE_TX_UPLOAD_BEGIN) {
        if (size < 48) return;
        liteDynOnUploadBegin(rdU64(0), rdU32(8), rdU32(12), in + 16);
    } else if (inputType == LITE_TX_UPLOAD_CHUNK) {
        if (size < 14) return;
        unsigned int len = rdU16(12);
        if (14u + len > size) return;
        liteDynOnUploadChunk(rdU64(0), rdU32(8), in + 14, len);
    } else if (inputType == LITE_TX_DEPLOY) {
        if (size < 52) return;
        liteDynOnDeploy(rdU64(0), rdU32(8), in + 12, rdU32(44), rdU32(48),
                        (size >= 84) ? (const char*)(in + 52) : nullptr);
    }
}

// ---------------------------------------------------------------------------
// Framed construction (B'): runs INITIALIZE under SC_INITIALIZE_TX framing.
// liteDynPendingForTick() is checked in processTick; liteDynConstructPending() runs the
// INITIALIZE on armed-but-unconstructed slots.
// ---------------------------------------------------------------------------
static bool liteDynPendingForTick(unsigned int /*tick*/) {
    for (unsigned int i = 0; i < LITE_DYN_SLOT_COUNT; i++)
        if (g_liteDynSlots[i].armed && !g_liteDynSlots[i].constructed) return true;
    return false;
}

[[maybe_unused]] static void liteDynConstructPending() {
    for (unsigned int i = 0; i < LITE_DYN_SLOT_COUNT; i++) {
        LiteDynSlot& s = g_liteDynSlots[i];
        if (!s.armed || s.constructed) continue;
        unsigned int contractIndex = LITEDYN0_CONTRACT_INDEX + i;
        // wasm slots patch contractSystemProcedures[][INITIALIZE] with a closure at load (lite_wasm_contracts.h),
        // so the normal path below runs INITIALIZE through the wasm engine — same as native.
        if (contractSystemProcedures[contractIndex][INITIALIZE]) {
            QpiContextSystemProcedureCall qpiContext(contractIndex, INITIALIZE);
            qpiContext.call();
            s.everInitialized = true;   // never re-run INITIALIZE for this slot (upgrades preserve state)
            logToConsole(L"LITEDYN: slot constructed (INITIALIZE ran)");
        } else {
            logToConsole(L"LITEDYN: ERROR construct skipped - tables unpatched (load failed)");
        }
        s.constructed = true;
    }
}

// ---------------------------------------------------------------------------
// Boot: clear the IPO-failed error stamp + seed fee reserve for dev slots so they can run.
// (Reloading persisted blobs across restart is a TODO.)
// ---------------------------------------------------------------------------
[[maybe_unused]] static void liteDynBootDeploy() {
    logToConsole(L"========================================================================");
    logToConsole(L"  LITE_DYNAMIC_CONTRACTS ENABLED - runtime wasm contract deploy active");
    logToConsole(L"  TESTNET DEV FEATURE ONLY - deploy address id(99999,0,0,0)");
    logToConsole(L"  Runs uploaded wasm in the embedded engine. NEVER enable on mainnet.");
    logToConsole(L"========================================================================");
    for (unsigned int i = 0; i < LITE_DYN_SLOT_COUNT; i++) {
        unsigned int contractIndex = LITEDYN0_CONTRACT_INDEX + i;
        contractError[contractIndex] = NoContractError;
        if (getContractFeeReserve(contractIndex) <= 0)
            setContractFeeReserve(contractIndex, 1000000000000ll);
    }
}

#endif // LITE_DYNAMIC_CONTRACTS

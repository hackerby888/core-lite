#pragma once
// Host-side runtime dynamic-contract deploy subsystem (testnet, LITE_DYNAMIC_CONTRACTS).
// See extensions/DYNAMIC_CONTRACTS.md. Included by qubic.cpp AFTER the contract machinery
// (contract_def.h + contract_exec.h). Owns the deploy registry + uploaded .so bytes as
// EXTENSION state (never in contract StateData). Builds the LiteHostServices vtable, and
// dlopens / registers / patches / constructs deployable slots.
#ifdef LITE_DYNAMIC_CONTRACTS

#include <dlfcn.h>
#include <sys/stat.h> // mkdir for the .so cache dir
#include "extensions/lite_dyn_abi.h" // shared ABI structs (no LITE_DYN_SO_BUILD here -> structs only)

// Upper bound on a deployable .so. Bytes live here (extension state), not in any StateData.
#ifndef LITE_DYN_MAX_SO
#define LITE_DYN_MAX_SO (4u * 1024u * 1024u)
#endif

// Number of reserved deployable slots — must match the LITEDYN0..N block in contract_def.h.
#define LITE_DYN_SLOT_COUNT 4

// logToConsole is defined later in qubic.cpp (same TU); this header is included before that point.
void logToConsole(const CHAR16* message);

// ---------------------------------------------------------------------------
// Host-services vtable — thin wrappers force emission of the host QPI surface so
// the .so binds by pointer (not -rdynamic). Extend the method backends as deployed
// contracts require (Counter needs none beyond the infra hooks).
// ---------------------------------------------------------------------------
static LiteHostServices g_liteHostServices = {
    LITE_DYN_ABI_VERSION,
    +[](unsigned int id) { __beginFunctionOrProcedure(id); },
    +[](unsigned int id) { __endFunctionOrProcedure(id); },
    +[](unsigned int ci) { __markContractStateDirty(ci); },
    +[]() { __pauseLogMessage(); },
    +[]() { __resumeLogMessage(); },
    +[](unsigned long long s, bool z) -> void* { return __acquireScratchpad(s, z); },
    +[](void* p) { __releaseScratchpad(p); },
    +[](unsigned int ci, unsigned char type, const void* msg, unsigned int size) {
        *((unsigned int*)(void*)msg) = ci;           // contractIndex into first 4 bytes (host convention)
        qLogger::logMessage(size, type, msg);        // type = CONTRACT_{ERROR,WARNING,INFORMATION,DEBUG}_MESSAGE
    },
    +[](const void* in, unsigned int len, void* out32) { KangarooTwelve(in, len, out32, 32); },
    +[](const void* ctx, const void* d, long long a) -> long long {
        return ((QPI::QpiContextProcedureCall*)ctx)->transfer(*(const m256i*)d, a);
    },
    +[](const void* ctx, const void* d, long long a, unsigned char t) -> long long {
        return ((QPI::QpiContextProcedureCall*)ctx)->__transfer(*(const m256i*)d, a, t);
    },
    +[](const void* ctx, unsigned int e) { ((QPI::QpiContextProcedureCall*)ctx)->__qpiAbort(e); },
    +[](const void* ctx, long long a, unsigned int idx) -> long long { return ((QPI::QpiContextProcedureCall*)ctx)->burn(a, idx); },
    +[](const void* ctx) -> unsigned short { return ((QPI::QpiContextFunctionCall*)ctx)->epoch(); },
    +[](const void* ctx) -> unsigned int { return ((QPI::QpiContextFunctionCall*)ctx)->tick(); },
    +[](const void* ctx) -> int { return ((QPI::QpiContextFunctionCall*)ctx)->numberOfTickTransactions(); },
    +[](const void* c, const void* id32, void* eo) -> unsigned char { return (unsigned char)((QPI::QpiContextFunctionCall*)c)->getEntity(*(const m256i*)id32, *(QPI::Entity*)eo); },
    +[](const void* c, unsigned int ci) -> long long { return ((QPI::QpiContextFunctionCall*)c)->queryFeeReserve(ci); },
    +[](const void* c, const void* id32, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->nextId(*(const m256i*)id32); },
    +[](const void* c, const void* id32, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->prevId(*(const m256i*)id32); },
    +[](const void* c, const void* id32) -> unsigned char { return (unsigned char)((QPI::QpiContextFunctionCall*)c)->isContractId(*(const m256i*)id32); },
    +[](const void* c, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->arbitrator(); },
    +[](const void* c, unsigned short i, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->computor(i); },
    +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->day(); },
    +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->year(); },
    +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->hour(); },
    +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->minute(); },
    +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->month(); },
    +[](const void* c) -> unsigned char { return ((QPI::QpiContextFunctionCall*)c)->second(); },
    +[](const void* c) -> unsigned short { return ((QPI::QpiContextFunctionCall*)c)->millisecond(); },
    +[](const void* c, void* o) { *(QPI::DateAndTime*)o = ((QPI::QpiContextFunctionCall*)c)->now(); },
    +[](const void* c, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->getPrevSpectrumDigest(); },
    +[](const void* c, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->getPrevUniverseDigest(); },
    +[](const void* c, void* o) { *(m256i*)o = ((QPI::QpiContextFunctionCall*)c)->getPrevComputerDigest(); },
    +[](const void* c, const void* i, unsigned long long n) -> unsigned char { return (unsigned char)((QPI::QpiContextFunctionCall*)c)->isAssetIssued(*(const m256i*)i, n); },
    +[](const void* c, unsigned long long n, const void* i, signed char d, long long s, unsigned long long u) -> long long { return ((QPI::QpiContextProcedureCall*)c)->issueAsset(n, *(const QPI::id*)i, d, s, u); },
    +[](const void* c, const void* a, const void* o, const void* p) -> long long { return ((QPI::QpiContextFunctionCall*)c)->numberOfShares(*(const QPI::Asset*)a, *(const QPI::AssetOwnershipSelect*)o, *(const QPI::AssetPossessionSelect*)p); },
    +[](const void* c, unsigned long long n, const void* i, const void* o, const void* p, unsigned short om, unsigned short pm) -> long long { return ((QPI::QpiContextFunctionCall*)c)->numberOfPossessedShares(n, *(const m256i*)i, *(const m256i*)o, *(const m256i*)p, om, pm); },
    +[](const void* c, unsigned long long n, const void* i, const void* o, const void* p, long long s, const void* no) -> long long { return ((QPI::QpiContextProcedureCall*)c)->transferShareOwnershipAndPossession(n, *(const m256i*)i, *(const m256i*)o, *(const m256i*)p, s, *(const m256i*)no); },
    +[](const void* c, long long a) -> unsigned char { return (unsigned char)((QPI::QpiContextProcedureCall*)c)->distributeDividends(a); },
    // liteCallFunction: run the callee's DEPLOYED function (table dispatch) under a nested context.
    +[](const void* cc, unsigned int idx, unsigned short it, const void* in, unsigned int, void* out, unsigned int) -> int {
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
    +[](const void* cc, unsigned int idx, unsigned short it, const void* in, unsigned int, void* out, unsigned int, long long reward) -> int {
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
};

// ---------------------------------------------------------------------------
// Extension-owned state: per-slot registry + a single active upload session.
// ---------------------------------------------------------------------------
struct LiteDynSlot {
    bool armed = false;
    bool constructed = false;
    unsigned char codeHash[32] = {};
    unsigned int activationTick = 0;
    unsigned int version = 0;
    void* soHandle = nullptr;
    char name[32] = {}; // contract name from the deploy tx; lets tooling resolve name -> slot
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
static unsigned char g_liteDynBuf[LITE_DYN_MAX_SO];
static unsigned char g_liteDynSeqSeen[(LITE_DYN_MAX_SO / 1008u) / 8u + 1u];

static inline unsigned int liteDynSlotBase() { return LITEDYN0_CONTRACT_INDEX; }
static inline int liteDynSlotLocal(unsigned int contractIndex) {
    int i = (int)contractIndex - (int)LITEDYN0_CONTRACT_INDEX;
    return (i >= 0 && i < (int)LITE_DYN_SLOT_COUNT) ? i : -1;
}

// ---------------------------------------------------------------------------
// Copy a .so registration into the host dispatch tables for one slot.
// ---------------------------------------------------------------------------
static void liteDynPatchSlot(unsigned int contractIndex, const LiteRegistration& reg) {
    contractStateLock[contractIndex].acquireWrite();
    setMem(contractStates[contractIndex], (unsigned long long)contractDescriptions[contractIndex].stateSize, 0);

    for (unsigned int i = 0; i < contractSystemProcedureCount && i < LITE_SP_COUNT; i++) {
        contractSystemProcedures[contractIndex][i] = (SYSTEM_PROCEDURE)reg.systemProcedures[i];
        contractSystemProcedureLocalsSizes[contractIndex][i] = reg.systemProcedureLocalsSizes[i];
    }
    contractExpandProcedures[contractIndex] = (EXPAND_PROCEDURE)reg.expandProcedure;

    for (unsigned int e = 0; e < reg.userEntryCount; e++) {
        const LiteUserEntry& ue = reg.userEntries[e];
        if (ue.kind == LITE_KIND_FUNCTION) {
            contractUserFunctions[contractIndex][ue.inputType] = (USER_FUNCTION)ue.fn;
            contractUserFunctionInputSizes[contractIndex][ue.inputType] = ue.inputSize;
            contractUserFunctionOutputSizes[contractIndex][ue.inputType] = ue.outputSize;
            contractUserFunctionLocalsSizes[contractIndex][ue.inputType] = (unsigned short)ue.localsSize;
        } else {
            contractUserProcedures[contractIndex][ue.inputType] = (USER_PROCEDURE)ue.fn;
            contractUserProcedureInputSizes[contractIndex][ue.inputType] = ue.inputSize;
            contractUserProcedureOutputSizes[contractIndex][ue.inputType] = ue.outputSize;
            contractUserProcedureLocalsSizes[contractIndex][ue.inputType] = (unsigned short)ue.localsSize;
        }
    }
    contractError[contractIndex] = NoContractError;
    contractStateLock[contractIndex].releaseWrite();
}

// dlopen the .so bytes (written to a temp file), hand it the vtable, run its registration,
// patch the slot's tables. Returns true on success. NOT consensus — local code load.
[[maybe_unused]] static bool liteDynLoadAndPatch(unsigned int contractIndex, const unsigned char* bytes, unsigned int len) {
    int local = liteDynSlotLocal(contractIndex);
    unsigned int ver = (local >= 0) ? g_liteDynSlots[local].version : 0;

    // Path: contracts_dyn/<slot>_<version>.so. The version suffix means a redeploy never overwrites a
    // file still mmap'd by a prior dlopen (truncating a mapped image crashes the node on its next call).
    char p[64]; int n = 0;
    auto putUint = [&](unsigned int v) { char num[12]; int nn = 0; do { num[nn++] = (char)('0' + (v % 10)); v /= 10; } while (v); while (nn) p[n++] = num[--nn]; };
    for (const char* c = "contracts_dyn/"; *c; ++c) p[n++] = *c;
    putUint(contractIndex); p[n++] = '_'; putUint(ver);
    for (const char* c = ".so"; *c; ++c) p[n++] = *c; p[n] = 0;

    mkdir("contracts_dyn", 0755); // create cache dir; ignore EEXIST
    FILE* f = fopen(p, "wb");
    if (!f) { logToConsole(L"LITEDYN: ERROR cannot write .so (fopen failed)"); return false; }
    fwrite(bytes, 1, len, f);
    fclose(f);

    void* h = dlopen(p, RTLD_NOW | RTLD_LOCAL);
    if (!h) { logToConsole(L"LITEDYN: ERROR dlopen failed"); return false; }
    auto setSvc = (void (*)(LiteHostServices*))dlsym(h, "liteSetHostServices");
    auto reg = (void (*)(LiteRegistration*))dlsym(h, "liteContractRegister");
    if (!setSvc || !reg) { logToConsole(L"LITEDYN: ERROR .so missing entry points"); dlclose(h); return false; }
    setSvc(&g_liteHostServices);

    static LiteRegistration registration; // large; keep static
    setMem(&registration, sizeof(registration), 0);
    reg(&registration);
    liteDynPatchSlot(contractIndex, registration);

    if (local >= 0) {
        void* old = g_liteDynSlots[local].soHandle;
        g_liteDynSlots[local].soHandle = h;
        if (old && old != h) dlclose(old); // free prior mapping AFTER tables point to the new .so
    }
    return true;
}

// ---------------------------------------------------------------------------
// Upload/deploy transaction handlers (called from processTickTransaction).
// Inputs are consensus-ordered txs; assembly is order-independent (scatter-write).
// ---------------------------------------------------------------------------
[[maybe_unused]] static void liteDynOnUploadBegin(unsigned long long sessionId, unsigned int totalSize,
        unsigned int chunkCount, const unsigned char* finalHash) {
    if (totalSize > LITE_DYN_MAX_SO) return;
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
    if (off + dataLen > LITE_DYN_MAX_SO) return;
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
    s.constructed = false;
    s.version++;
    // Load the code now (node-local, non-consensus); construction (state init) is deferred to a framed tick
    // step (liteDynConstructPending). Route by the uploaded artifact's magic: '\0asm' -> wasm engine,
    // ELF/Mach-O -> native .so engine (WASM_CONTRACTS.md §13.9). qinit signals the engine by what it uploads.
    bool loadOk;
    const unsigned char* art = g_liteDynBuf;
#ifdef LITE_WASM_CONTRACTS
    if (g_liteDynUpload.totalSize >= 4 && art[0] == 0x00 && art[1] == 0x61 && art[2] == 0x73 && art[3] == 0x6d) {
        loadOk = liteWasmLoadFromBytes(targetSlot, g_liteDynBuf, g_liteDynUpload.totalSize);
        logToConsole(loadOk ? L"LITEDYN: wasm contract loaded" : L"LITEDYN: ERROR wasm load failed");
    } else
#endif
    {
        loadOk = liteDynLoadAndPatch(targetSlot, g_liteDynBuf, g_liteDynUpload.totalSize);
    }
    if (!loadOk)
        logToConsole(L"LITEDYN: ERROR load/patch failed - slot armed but not runnable");
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
#ifdef LITE_WASM_CONTRACTS
        if (liteWasmIsWasm(contractIndex)) {
            // wasm slot: user fn/proc dispatch is live; INITIALIZE (a system procedure) on wasm isn't wired
            // yet, so state relies on zero-init. TODO: wasm system-procedure dispatch.
            logToConsole(L"LITEDYN: wasm slot armed (INITIALIZE skipped; zero-init state)");
            s.constructed = true;
            continue;
        }
#endif
        if (contractSystemProcedures[contractIndex][INITIALIZE]) {
            QpiContextSystemProcedureCall qpiContext(contractIndex, INITIALIZE);
            qpiContext.call();
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
    logToConsole(L"  LITE_DYNAMIC_CONTRACTS ENABLED - runtime .so contract deploy active");
    logToConsole(L"  TESTNET DEV FEATURE ONLY - deploy address id(99999,0,0,0)");
    logToConsole(L"  Loads native code at runtime. NEVER enable on mainnet.");
    logToConsole(L"========================================================================");
    for (unsigned int i = 0; i < LITE_DYN_SLOT_COUNT; i++) {
        unsigned int contractIndex = LITEDYN0_CONTRACT_INDEX + i;
        contractError[contractIndex] = NoContractError;
        if (getContractFeeReserve(contractIndex) <= 0)
            setContractFeeReserve(contractIndex, 1000000000000ll);
    }
}

#endif // LITE_DYNAMIC_CONTRACTS

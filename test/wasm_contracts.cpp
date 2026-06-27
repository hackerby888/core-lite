// Regression test for the wasm-contract dispatch/reg/state ABI (WASM_CONTRACTS.md §13). Loads the embedded
// fixture contract.wasm (engine ABI: state_addr/state_size/io_base/reg_count/reg_info/dispatch) in WAMR and
// checks registration + dispatch (function + procedure) + state round-trip. This is the contract<->runtime
// contract that both the node engine (lite_wasm_contracts.h) and the contract binding (lite_wasm_tu.h) rely on.
// Built only with -DLITE_WASM_CONTRACTS (the test CMake adds WAMR/vmlib + this source then).
#ifdef LITE_WASM_CONTRACTS

#include <cstring>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include "gtest/gtest.h"
#include "wasm_export.h"
#include "wasm_contract_fixture.h"

namespace {
enum { KIND_FUNCTION = 0, KIND_PROCEDURE = 1 };

struct WasmFixture {
    wasm_module_t mod = nullptr;
    wasm_module_inst_t inst = nullptr;
    wasm_exec_env_t env = nullptr;
    unsigned char buf[8192];

    bool load() {
        static char heap[8 * 1024 * 1024];
        RuntimeInitArgs ia; memset(&ia, 0, sizeof ia);
        ia.mem_alloc_type = Alloc_With_Pool;
        ia.mem_alloc_option.pool.heap_buf = heap;
        ia.mem_alloc_option.pool.heap_size = sizeof heap;
        if (!wasm_runtime_full_init(&ia)) return false;
        if (g_wasmFixtureLen > sizeof buf) return false;
        memcpy(buf, g_wasmFixture, g_wasmFixtureLen);   // WAMR mutates the load buffer
        char err[192];
        mod = wasm_runtime_load(buf, g_wasmFixtureLen, err, sizeof err);
        if (!mod) return false;
        inst = wasm_runtime_instantiate(mod, 64 * 1024, 1024 * 1024, err, sizeof err);
        if (!inst) return false;
        env = wasm_runtime_create_exec_env(inst, 64 * 1024);
        return env != nullptr;
    }
    ~WasmFixture() {
        if (env) wasm_runtime_destroy_exec_env(env);
        if (inst) wasm_runtime_deinstantiate(inst);
        if (mod) wasm_runtime_unload(mod);
        wasm_runtime_destroy();
    }
    uint32_t call(const char* fn, uint32_t* argv, uint32_t n) {
        wasm_function_inst_t f = wasm_runtime_lookup_function(inst, fn);
        EXPECT_NE(f, nullptr) << fn;
        EXPECT_TRUE(wasm_runtime_call_wasm(env, f, n, argv)) << fn << ": " << wasm_runtime_get_exception(inst);
        return argv[0];
    }
    void* nat(uint32_t off) { return wasm_runtime_addr_app_to_native(inst, off); }
};
} // namespace

TEST(WasmContracts, RegistrationDispatchAndStateRoundTrip) {
    WasmFixture w;
    ASSERT_TRUE(w.load());

    // registration: 2 entries — G (function, it=1) + INC (procedure, it=2)
    uint32_t a[5] = { 0 };
    EXPECT_EQ(w.call("reg_count", a, 0), 2u);

    a[0] = 0; uint32_t io = w.call("io_base", a, 0);
    struct EntryInfo { uint32_t inputType, kind, inSize, outSize; };
    a[0] = 0; a[1] = io; w.call("reg_info", a, 2);
    EntryInfo* e0 = (EntryInfo*)w.nat(io);
    EXPECT_EQ(e0->inputType, 1u); EXPECT_EQ(e0->kind, (uint32_t)KIND_FUNCTION); EXPECT_EQ(e0->outSize, 8u);
    a[0] = 1; a[1] = io; w.call("reg_info", a, 2);
    EntryInfo* e1 = (EntryInfo*)w.nat(io);
    EXPECT_EQ(e1->inputType, 2u); EXPECT_EQ(e1->kind, (uint32_t)KIND_PROCEDURE); EXPECT_EQ(e1->inSize, 8u);

    const uint32_t IN = io, OUT = io + 64, LOCALS = io + 128;

    // INC (procedure) x3, by=1 each -> count 1,2,3 (state persists in the instance across calls)
    for (uint64_t i = 1; i <= 3; i++) {
        *(uint64_t*)w.nat(IN) = 1;
        a[0] = KIND_PROCEDURE; a[1] = 2; a[2] = IN; a[3] = OUT; a[4] = LOCALS;
        w.call("dispatch", a, 5);
        EXPECT_EQ(*(uint64_t*)w.nat(OUT), i) << "INC returns running count";
    }

    // G (function) -> reads the persisted count
    a[0] = KIND_FUNCTION; a[1] = 1; a[2] = IN; a[3] = OUT; a[4] = LOCALS;
    w.call("dispatch", a, 5);
    EXPECT_EQ(*(uint64_t*)w.nat(OUT), 3u) << "get_count after 3 increments";
}

TEST(WasmContracts, SystemProceduresMaskAndDispatch) {
    WasmFixture w;
    ASSERT_TRUE(w.load());
    enum { KIND_SYSPROC = 2, SP_INITIALIZE = 0, SP_POST_INCOMING_TRANSFER = 9 };

    uint32_t a[5] = { 0 };
    // mask reports INITIALIZE + POST_INCOMING_TRANSFER; the input sysproc declares its 8-byte input size.
    EXPECT_EQ(w.call("reg_sysproc_mask", a, 0), (1u << SP_INITIALIZE) | (1u << SP_POST_INCOMING_TRANSFER));
    a[0] = SP_POST_INCOMING_TRANSFER; EXPECT_EQ(w.call("sysproc_in_size", a, 1), 8u);
    a[0] = SP_INITIALIZE;             EXPECT_EQ(w.call("sysproc_in_size", a, 1), 0u);

    a[0] = 0; uint32_t io = w.call("io_base", a, 0);
    a[0] = 0; uint32_t st = w.call("state_addr", a, 0);
    const uint32_t IN = io, OUT = io + 64, LOCALS = io + 128;

    // INITIALIZE (kind=sysproc, sp 0): writes a sentinel into state.count
    a[0] = KIND_SYSPROC; a[1] = SP_INITIALIZE; a[2] = IN; a[3] = OUT; a[4] = LOCALS;
    w.call("dispatch", a, 5);
    EXPECT_EQ(((uint64_t*)w.nat(st))[0], 4242u) << "INITIALIZE sysproc ran via kind=2 dispatch";

    // POST_INCOMING_TRANSFER (sp 9): the 8-byte input is marshalled in and stored into state.sum
    *(uint64_t*)w.nat(IN) = 777;
    a[0] = KIND_SYSPROC; a[1] = SP_POST_INCOMING_TRANSFER; a[2] = IN; a[3] = OUT; a[4] = LOCALS;
    w.call("dispatch", a, 5);
    EXPECT_EQ(((uint64_t*)w.nat(st))[1], 777u) << "input sysproc read its marshalled input";
}

// Cross-host state equivalence: load a qinit-BUILT wasm (env QINIT_WASM, e.g. the DigestProbe fixture) under the
// node's WAMR, run INITIALIZE + Inc x QINIT_OPS, and print the post-op StateData bytes. The qinit driver
// (cross-host.test.ts) runs the SAME wasm on its VirtualNode and asserts byte-equality — the proof that the TS
// engine port marshals + mutates contract state identically to the node, which is what makes the state digest
// (K12 of these bytes) match across hosts. A pure-state contract needs no real host services, so the few lhost
// infra imports (beginFn/endFn/markDirty), the assert, and wasi fd_* are stubbed no-op here.
namespace {
void hs_void_i(wasm_exec_env_t, uint32_t) {}
void hs_assert(wasm_exec_env_t, uint32_t, uint32_t, uint32_t) {}
uint32_t hs_fd_write(wasm_exec_env_t, uint32_t, uint32_t, uint32_t, uint32_t) { return 0; }
uint32_t hs_fd_close(wasm_exec_env_t, uint32_t) { return 0; }
uint32_t hs_fd_seek(wasm_exec_env_t, uint32_t, uint64_t, uint32_t, uint32_t) { return 0; }
// Scratch (stack-locals frame) for contracts that use them (e.g. a Collection): back it with the WAMR app
// heap — a real wasm-app pointer the contract writes through, freed on release. No node scratchpad needed.
uint32_t hs_acquireScratch(wasm_exec_env_t e, uint64_t size, uint32_t initZero) {
    wasm_module_inst_t inst = wasm_runtime_get_module_inst(e);
    void* native = nullptr;
    uint32_t off = (uint32_t)wasm_runtime_module_malloc(inst, (uint32_t)size, &native);
    if (off && initZero && native) memset(native, 0, (size_t)size);
    return off;
}
void hs_releaseScratch(wasm_exec_env_t e, uint32_t off) {
    if (off) wasm_runtime_module_free(wasm_runtime_get_module_inst(e), off);
}
// Parse one hex byte pair; -1 on a non-hex nibble.
int hexNibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}
} // namespace

TEST(WasmContracts, CrossHostStateEquivalence) {
    const char* path = getenv("QINIT_WASM");
    if (!path) GTEST_SKIP() << "set QINIT_WASM to a qinit-built pure-state wasm (e.g. DigestProbe)";
    const int ops = getenv("QINIT_OPS") ? atoi(getenv("QINIT_OPS")) : 1;

    FILE* fp = fopen(path, "rb");
    ASSERT_NE(fp, nullptr) << "open " << path;
    fseek(fp, 0, SEEK_END);
    long flen = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    std::vector<unsigned char> buf(flen);
    ASSERT_EQ(fread(buf.data(), 1, flen, fp), (size_t)flen);
    fclose(fp);

    static char heap[32 * 1024 * 1024];
    RuntimeInitArgs ia;
    memset(&ia, 0, sizeof ia);
    ia.mem_alloc_type = Alloc_With_Pool;
    ia.mem_alloc_option.pool.heap_buf = heap;
    ia.mem_alloc_option.pool.heap_size = sizeof heap;
    ASSERT_TRUE(wasm_runtime_full_init(&ia));

    static NativeSymbol lhostNs[] = {
        { "beginFn", (void*)hs_void_i, "(i)", nullptr },
        { "endFn", (void*)hs_void_i, "(i)", nullptr },
        { "markDirty", (void*)hs_void_i, "(i)", nullptr },
        { "acquireScratch", (void*)hs_acquireScratch, "(Ii)i", nullptr },
        { "releaseScratch", (void*)hs_releaseScratch, "(i)", nullptr },
    };
    static NativeSymbol envNs[] = {
        { "_ZL21addDebugMessageAssertPKcS0_j", (void*)hs_assert, "(iii)", nullptr },
    };
    static NativeSymbol wasiNs[] = {
        { "fd_write", (void*)hs_fd_write, "(iiii)i", nullptr },
        { "fd_close", (void*)hs_fd_close, "(i)i", nullptr },
        { "fd_seek", (void*)hs_fd_seek, "(iIii)i", nullptr },
    };
    ASSERT_TRUE(wasm_runtime_register_natives("lhost", lhostNs, 5));
    ASSERT_TRUE(wasm_runtime_register_natives("env", envNs, 1));
    ASSERT_TRUE(wasm_runtime_register_natives("wasi_snapshot_preview1", wasiNs, 3));

    char err[256];
    wasm_module_t mod = wasm_runtime_load(buf.data(), (uint32_t)flen, err, sizeof err);
    ASSERT_NE(mod, nullptr) << err;
    wasm_module_inst_t inst = wasm_runtime_instantiate(mod, 256 * 1024, 4 * 1024 * 1024, err, sizeof err);
    ASSERT_NE(inst, nullptr) << err;
    wasm_exec_env_t env = wasm_runtime_create_exec_env(inst, 256 * 1024);
    ASSERT_NE(env, nullptr);

    auto call = [&](const char* fn, uint32_t* a, uint32_t n) -> uint32_t {
        wasm_function_inst_t f = wasm_runtime_lookup_function(inst, fn);
        EXPECT_NE(f, nullptr) << fn;
        EXPECT_TRUE(wasm_runtime_call_wasm(env, f, n, a)) << fn << ": " << wasm_runtime_get_exception(inst);
        return a[0];
    };

    uint32_t a[5] = { 0 };
    uint32_t io = call("io_base", a, 0);
    a[0] = 0;
    uint32_t st = call("state_addr", a, 0);
    a[0] = 0;
    uint32_t ss = call("state_size", a, 0);
    const uint32_t IN = io, OUT = io + 64, LOCALS = io + 128;
    enum { KIND_PROCEDURE = 1, KIND_SYSPROC = 2, SP_INITIALIZE = 0 };

    a[0] = KIND_SYSPROC; a[1] = SP_INITIALIZE; a[2] = IN; a[3] = OUT; a[4] = LOCALS;
    call("dispatch", a, 5);

    const char* script = getenv("QINIT_SCRIPT");
    if (script && *script) {
        // QINIT_SCRIPT = "it:inputhex;it:inputhex;..." — one procedure call per op, input marshalled into IN.
        // Recompute the native IN pointer each op: a contract's scratch allocation can grow linear memory.
        std::string s(script);
        size_t p = 0;
        while (p < s.size()) {
            size_t semi = s.find(';', p);
            std::string tok = s.substr(p, semi == std::string::npos ? std::string::npos : semi - p);
            p = (semi == std::string::npos) ? s.size() : semi + 1;
            if (tok.empty()) continue;
            size_t colon = tok.find(':');
            int it = atoi(tok.substr(0, colon).c_str());
            std::string hex = colon == std::string::npos ? std::string() : tok.substr(colon + 1);
            unsigned char* inN = (unsigned char*)wasm_runtime_addr_app_to_native(inst, IN);
            memset(inN, 0, 64);
            for (size_t i = 0; i + 1 < hex.size() && i / 2 < 64; i += 2) {
                int hi = hexNibble(hex[i]), lo = hexNibble(hex[i + 1]);
                if (hi >= 0 && lo >= 0) inN[i / 2] = (unsigned char)((hi << 4) | lo);
            }
            a[0] = KIND_PROCEDURE; a[1] = (uint32_t)it; a[2] = IN; a[3] = OUT; a[4] = LOCALS;
            call("dispatch", a, 5);
        }
    } else {
        for (int i = 0; i < ops; i++) {
            a[0] = KIND_PROCEDURE; a[1] = 1; a[2] = IN; a[3] = OUT; a[4] = LOCALS;
            call("dispatch", a, 5);
        }
    }

    const unsigned char* s = (const unsigned char*)wasm_runtime_addr_app_to_native(inst, st);
    std::string hex;
    hex.reserve(ss * 2);
    char b[3];
    for (uint32_t i = 0; i < ss; i++) {
        sprintf(b, "%02x", s[i]);
        hex += b;
    }
    printf("CROSSHOST_STATE=%s\n", hex.c_str());

    wasm_runtime_destroy_exec_env(env);
    wasm_runtime_deinstantiate(inst);
    wasm_runtime_unload(mod);
    wasm_runtime_destroy();
}

#ifdef LITE_WASM_TRAP_BACKTRACE
#include "wasm_trap_fixture.h"
#include <unistd.h>
#include <cstdio>
#include <string>
// Classic interp + DUMP_CALL_STACK: on a contract trap WAMR auto-prints a backtrace ("#NN: 0xOFF - name")
// whose offsets are original-wasm bytes -> source file:line via the DWARF sidecar (qinit side). The node
// can't capture it structurally (copy_callstack carries no offset + the frames unwind before we regain
// control), so it flows stdout -> node.log -> qinit. This locks that the auto-dump fires with a byte offset
// under the dev build. Only runs with -DLITE_WASM_TRAP_BACKTRACE=ON (fast-interp offsets are unmappable).
TEST(WasmContracts, TrapAutoDumpHasMappableOffset) {
    static char heap[8 * 1024 * 1024];
    RuntimeInitArgs ia; memset(&ia, 0, sizeof ia);
    ia.mem_alloc_type = Alloc_With_Pool;
    ia.mem_alloc_option.pool.heap_buf = heap;
    ia.mem_alloc_option.pool.heap_size = sizeof heap;
    ASSERT_TRUE(wasm_runtime_full_init(&ia));
    unsigned char buf[8192]; ASSERT_LE(g_wasmTrapFixtureLen, sizeof buf);
    memcpy(buf, g_wasmTrapFixture, g_wasmTrapFixtureLen);
    char err[192];
    wasm_module_t mod = wasm_runtime_load(buf, g_wasmTrapFixtureLen, err, sizeof err);
    ASSERT_NE(mod, nullptr) << err;
    wasm_module_inst_t inst = wasm_runtime_instantiate(mod, 64 * 1024, 1024 * 1024, err, sizeof err);
    ASSERT_NE(inst, nullptr) << err;
    wasm_exec_env_t env = wasm_runtime_create_exec_env(inst, 64 * 1024);
    ASSERT_NE(env, nullptr);
    wasm_function_inst_t f = wasm_runtime_lookup_function(inst, "dispatch");
    ASSERT_NE(f, nullptr);

    // capture stdout (WAMR auto-dumps the backtrace there during the trap); single-threaded test -> safe.
    int saved = dup(fileno(stdout));
    int pfd[2]; ASSERT_EQ(pipe(pfd), 0);
    fflush(stdout); dup2(pfd[1], fileno(stdout)); close(pfd[1]);

    uint32_t a[5] = { 0, 0, 0, 0, 0 };   // it=0 -> do_div(7,0) -> divide-by-zero trap
    bool ok = wasm_runtime_call_wasm(env, f, 5, a);

    fflush(stdout); dup2(saved, fileno(stdout)); close(saved);
    char cap[8192]; ssize_t n = read(pfd[0], cap, sizeof(cap) - 1); close(pfd[0]);
    cap[n > 0 ? n : 0] = '\0';
    printf("--- captured WAMR auto-dump ---\n%s\n-------------------------------\n", cap);

    EXPECT_FALSE(ok) << "dispatch must trap (divide by zero)";
    std::string out(cap);
    EXPECT_NE(out.find("#0"), std::string::npos) << "WAMR backtrace frame markers present in the auto-dump";
    EXPECT_NE(out.find("0x"), std::string::npos) << "the backtrace carries a (DWARF-mappable) byte offset";

    wasm_runtime_destroy_exec_env(env);
    wasm_runtime_deinstantiate(inst);
    wasm_runtime_unload(mod);
    wasm_runtime_destroy();
}
#endif // LITE_WASM_TRAP_BACKTRACE

#endif // LITE_WASM_CONTRACTS

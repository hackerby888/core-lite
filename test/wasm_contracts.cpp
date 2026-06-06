// Regression test for the wasm-contract dispatch/reg/state ABI (WASM_CONTRACTS.md §13). Loads the embedded
// fixture contract.wasm (engine ABI: state_addr/state_size/io_base/reg_count/reg_info/dispatch) in WAMR and
// checks registration + dispatch (function + procedure) + state round-trip. This is the contract<->runtime
// contract that both the node engine (lite_wasm_contracts.h) and the contract binding (lite_wasm_tu.h) rely on.
// Built only with -DLITE_WASM_CONTRACTS (the test CMake adds WAMR/vmlib + this source then).
#ifdef LITE_WASM_CONTRACTS

#include <cstring>
#include <cstdint>
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

#endif // LITE_WASM_CONTRACTS

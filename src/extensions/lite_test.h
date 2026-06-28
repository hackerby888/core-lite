#pragma once
// Engine-run contract tests: a googletest-compatible shim (TEST / EXPECT_* / ASSERT_*) compiled INTO the
// contract wasm by qinit's wasi-sdk clang. The qinit/IDE Virtual Node instantiates the module, runs each
// registered test, and reports results — so a contract's C++ tests run in Bun and the browser with no native
// toolchain. Real googletest is unavailable here (it needs RTTI + exceptions; the contract wasm builds
// -fno-rtti -fno-exceptions), so this is a self-contained reimplementation of the macro surface.
//
// The contract under test is the SAME module: its qpi.* host calls resolve through the engine's "lhost"
// imports, and the test drives it through a tiny "thost" (test-host) the engine binds to its in-process
// Virtual Node (a fresh, isolated genesis ledger — never the live session). The matching engine side is
// @qinit/engine runTests(); the ABI below is the contract between the two.
#ifdef LITE_TEST_BUILD

// ---- thost imports: engine-owned operations the test wasm can't perform itself (bound by @qinit/engine) ----
// Pointers cross as i32 linear-memory offsets; amounts as i64. The engine reads/writes the test module's memory.
#define TH_IMPORT(name) __attribute__((import_module("thost"), import_name(#name)))
extern "C" {
TH_IMPORT(t_reset)   void         th_reset();
TH_IMPORT(t_invoke)  unsigned int th_invoke(unsigned int it, const void* in, unsigned int inLen, long long amount, const void* origin32, void* out, unsigned int outCap);
TH_IMPORT(t_query)   unsigned int th_query(unsigned int it, const void* in, unsigned int inLen, void* out, unsigned int outCap);
TH_IMPORT(t_fund)    void         th_fund(const void* id32, long long amount);
TH_IMPORT(t_balance) long long    th_balance(const void* id32);
TH_IMPORT(t_derive)  void         th_derive(const void* seed, unsigned int seedLen, void* out32);
TH_IMPORT(t_tick)    void         th_tick(unsigned int n);
TH_IMPORT(t_report)  void         th_report(const void* name, unsigned int nameLen, unsigned int passed, const void* msg, unsigned int msgLen);
}
#undef TH_IMPORT

namespace litetest {

// ---- minimal string ops + value formatting (no libc <stdio>/<string>; freestanding wasm) ----
inline unsigned int slen(const char* s) {
    unsigned int n = 0;
    while (s[n]) {
        ++n;
    }
    return n;
}

// Per-test failure accumulator. One test runs at a time, so a single static buffer is enough.
struct Ctx {
    char         msg[2048];
    unsigned int msgLen;
    bool         failed;
};
static Ctx g_ctx;

inline void appendBytes(const char* s, unsigned int n) {
    for (unsigned int i = 0; i < n && g_ctx.msgLen < sizeof(g_ctx.msg) - 1; ++i) {
        g_ctx.msg[g_ctx.msgLen++] = s[i];
    }
}
inline void appendStr(const char* s) {
    appendBytes(s, slen(s));
}

inline void appendI64(long long v) {
    char buf[24];
    int  i = 0;
    bool neg = v < 0;
    unsigned long long u = neg ? (unsigned long long)(-(v + 1)) + 1ull : (unsigned long long)v;
    if (u == 0) {
        buf[i++] = '0';
    }
    while (u) {
        buf[i++] = (char)('0' + (int)(u % 10));
        u /= 10;
    }
    if (neg) {
        buf[i++] = '-';
    }
    while (i > 0) {
        char c = buf[--i];
        appendBytes(&c, 1);
    }
}
inline void appendU64(unsigned long long u) {
    char buf[24];
    int  i = 0;
    if (u == 0) {
        buf[i++] = '0';
    }
    while (u) {
        buf[i++] = (char)('0' + (int)(u % 10));
        u /= 10;
    }
    while (i > 0) {
        char c = buf[--i];
        appendBytes(&c, 1);
    }
}

// Render a compared value into the failure message: bool -> true/false, integral -> decimal, else -> "(value)".
template <typename T>
inline void appendVal(const T& v) {
    if constexpr (std::is_same_v<T, bool>) {
        appendStr(v ? "true" : "false");
    } else if constexpr (std::is_integral_v<T>) {
        if constexpr (std::is_signed_v<T>) {
            appendI64((long long)v);
        } else {
            appendU64((unsigned long long)v);
        }
    } else {
        appendStr("(value)");
    }
}

// ---- test registry: populated at module init (reactor _initialize runs the Registrar ctors) ----
typedef void (*TestFn)();
struct Entry {
    const char* name;
    TestFn      fn;
};
#ifndef LITE_TEST_MAX
#define LITE_TEST_MAX 512
#endif
static Entry        g_tests[LITE_TEST_MAX];
static unsigned int g_testCount = 0;

struct Registrar {
    Registrar(const char* name, TestFn fn) {
        if (g_testCount < LITE_TEST_MAX) {
            g_tests[g_testCount].name = name;
            g_tests[g_testCount].fn = fn;
            ++g_testCount;
        }
    }
};

inline void failAt(const char* file, int line, const char* what) {
    g_ctx.failed = true;
    appendStr("\n  ");
    appendStr(file);
    appendStr(":");
    appendI64(line);
    appendStr(": ");
    appendStr(what);
}

} // namespace litetest

// ---- googletest-compatible macros ----
#define TEST(suite, name)                                                                       \
    static void litetest_body_##suite##_##name();                                               \
    static ::litetest::Registrar litetest_reg_##suite##_##name(#suite "." #name,                \
                                                                &litetest_body_##suite##_##name); \
    static void litetest_body_##suite##_##name()

#define LITE_TEST_BOOL(cond, what, fatal)                                                       \
    do {                                                                                        \
        if (!(cond)) {                                                                          \
            ::litetest::failAt(__FILE__, __LINE__, what);                                       \
            if (fatal) return;                                                                  \
        }                                                                                       \
    } while (0)

#define LITE_TEST_CMP(a, b, op, label, fatal)                                                   \
    do {                                                                                        \
        auto litetest_va = (a);                                                                 \
        auto litetest_vb = (b);                                                                 \
        if (!(litetest_va op litetest_vb)) {                                                    \
            ::litetest::failAt(__FILE__, __LINE__, label "(" #a ", " #b ")");                   \
            ::litetest::appendStr(" (");                                                        \
            ::litetest::appendVal(litetest_va);                                                 \
            ::litetest::appendStr(" vs ");                                                      \
            ::litetest::appendVal(litetest_vb);                                                 \
            ::litetest::appendStr(")");                                                         \
            if (fatal) return;                                                                  \
        }                                                                                       \
    } while (0)

#define EXPECT_TRUE(x)  LITE_TEST_BOOL((x), "EXPECT_TRUE(" #x ")", false)
#define EXPECT_FALSE(x) LITE_TEST_BOOL(!(x), "EXPECT_FALSE(" #x ")", false)
#define ASSERT_TRUE(x)  LITE_TEST_BOOL((x), "ASSERT_TRUE(" #x ")", true)
#define ASSERT_FALSE(x) LITE_TEST_BOOL(!(x), "ASSERT_FALSE(" #x ")", true)

#define EXPECT_EQ(a, b) LITE_TEST_CMP(a, b, ==, "EXPECT_EQ", false)
#define EXPECT_NE(a, b) LITE_TEST_CMP(a, b, !=, "EXPECT_NE", false)
#define EXPECT_LT(a, b) LITE_TEST_CMP(a, b, <,  "EXPECT_LT", false)
#define EXPECT_LE(a, b) LITE_TEST_CMP(a, b, <=, "EXPECT_LE", false)
#define EXPECT_GT(a, b) LITE_TEST_CMP(a, b, >,  "EXPECT_GT", false)
#define EXPECT_GE(a, b) LITE_TEST_CMP(a, b, >=, "EXPECT_GE", false)
#define ASSERT_EQ(a, b) LITE_TEST_CMP(a, b, ==, "ASSERT_EQ", true)
#define ASSERT_NE(a, b) LITE_TEST_CMP(a, b, !=, "ASSERT_NE", true)
#define ASSERT_LT(a, b) LITE_TEST_CMP(a, b, <,  "ASSERT_LT", true)
#define ASSERT_LE(a, b) LITE_TEST_CMP(a, b, <=, "ASSERT_LE", true)
#define ASSERT_GT(a, b) LITE_TEST_CMP(a, b, >,  "ASSERT_GT", true)
#define ASSERT_GE(a, b) LITE_TEST_CMP(a, b, >=, "ASSERT_GE", true)

// ---- harness: drive the contract under test through the engine ("thost"), gtest-fixture style ----
// Construct one per TEST (mirrors core's `ContractTestingX x;`): the ctor resets the isolated node to genesis
// + re-runs INITIALIZE, so every test starts from a fresh ledger + fresh contract state.
class ContractTest {
public:
    ContractTest() {
        th_reset();
    }

    void reset() {
        th_reset();
    }

    // Invoke a user PROCEDURE (state-mutating). `amount` is the invocation reward sent with the call; `origin`
    // is the transaction originator. Returns the procedure's output struct.
    template <typename Out, typename In>
    Out invoke(unsigned int it, const In& in, long long amount, const QPI::id& origin) {
        Out out;
        setMem(&out, sizeof(out), 0);
        th_invoke(it, &in, sizeof(in), amount, &origin, &out, sizeof(out));
        return out;
    }

    // Call a user FUNCTION (read-only). Returns the function's output struct.
    template <typename Out, typename In>
    Out call(unsigned int it, const In& in) {
        Out out;
        setMem(&out, sizeof(out), 0);
        th_query(it, &in, sizeof(in), &out, sizeof(out));
        return out;
    }

    // The contract's live StateData (resident in this module's memory).
    template <typename StateData>
    const StateData& state() const {
        return *(const StateData*)(unsigned long)state_addr();
    }

    void fund(const QPI::id& who, long long amount) {
        th_fund(&who, amount);
    }

    long long balance(const QPI::id& who) {
        return th_balance(&who);
    }

    void advanceTick(unsigned int n = 1) {
        th_tick(n);
    }

    // Derive a 32-byte identity from a seed string (FourQ public key), for funding + originator args.
    QPI::id idFromSeed(const char* seed) {
        QPI::id out;
        th_derive(seed, ::litetest::slen(seed), &out);
        return out;
    }
};

// ---- runner exports the engine calls to enumerate + run tests ----
extern "C" {
__attribute__((export_name("test_count")))
unsigned int test_count() {
    return ::litetest::g_testCount;
}

__attribute__((export_name("test_name")))
unsigned int test_name(unsigned int i, void* out, unsigned int cap) {
    if (i >= ::litetest::g_testCount) {
        return 0;
    }
    const char*  nm = ::litetest::g_tests[i].name;
    unsigned int n = ::litetest::slen(nm);
    if (n > cap) {
        n = cap;
    }
    copyMem(out, nm, n);
    return n;
}

__attribute__((export_name("run_test")))
unsigned int run_test(unsigned int i) {
    if (i >= ::litetest::g_testCount) {
        return 0;
    }
    ::litetest::g_ctx.failed = false;
    ::litetest::g_ctx.msgLen = 0;
    ::litetest::g_tests[i].fn();
    const char* nm = ::litetest::g_tests[i].name;
    const unsigned int passed = ::litetest::g_ctx.failed ? 0u : 1u;
    th_report(nm, ::litetest::slen(nm), passed, ::litetest::g_ctx.msg, ::litetest::g_ctx.msgLen);
    return passed;
}
} // extern "C"

#endif // LITE_TEST_BUILD

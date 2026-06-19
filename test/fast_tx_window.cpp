#define NO_UEFI

#include "gtest/gtest.h"

#ifdef __linux__
#include "platform/msvc_polyfill.h"
#endif

// workaround for name clash with stdlib
#define system qubicSystemStruct

#include "src/public_settings.h"
#undef NUMBER_OF_TRANSACTIONS_PER_TICK
#define NUMBER_OF_TRANSACTIONS_PER_TICK 8ULL   // small per-tick slot count (must be 2^N)
#undef FAST_TX_WINDOW_TICKS
#define FAST_TX_WINDOW_TICKS 4                  // small window for ring-reuse testing

#include "src/extensions/fast_tx_window.h"

namespace
{
alignas(32) unsigned char g_buf[MAX_TRANSACTION_SIZE];

// Build a zeroed tx for `tick` with a unique source key `uid` (=> unique digest).
static Transaction* buildTx(unsigned int tick, unsigned long long uid)
{
    setMem(g_buf, sizeof(g_buf), 0);
    Transaction* t = (Transaction*)g_buf;
    t->sourcePublicKey = m256i{ uid, 0, 0, 0 };
    t->destinationPublicKey = m256i{ 0, 0, 0, 0 };
    t->amount = 0;
    t->tick = tick;
    t->inputType = 0;
    t->inputSize = 0;
    return t;
}

static m256i digestOf(const Transaction* t)
{
    m256i d;
    KangarooTwelve(t, t->totalSize(), &d, sizeof(m256i));
    return d;
}
}

class FastTxWindowTest : public ::testing::Test
{
protected:
    static void SetUpTestSuite() { ASSERT_TRUE(FastTxWindow::init()); }
    static void TearDownTestSuite() { FastTxWindow::deinit(); }
};

TEST_F(FastTxWindowTest, AddThenLookup)
{
    const unsigned int currentTick = 0; // window holds ticks 1..4
    Transaction* t = buildTx(1, 0x1001);
    m256i d = digestOf(t);
    EXPECT_TRUE(FastTxWindow::add(t, currentTick));

    const Transaction* got = FastTxWindow::lookup(1, d, currentTick);
    ASSERT_NE(got, nullptr);
    EXPECT_EQ(memcmp(got, t, t->totalSize()), 0);
}

TEST_F(FastTxWindowTest, DedupSecondAddRejected)
{
    const unsigned int currentTick = 0;
    Transaction* t = buildTx(2, 0x2002);
    m256i d = digestOf(t);
    EXPECT_TRUE(FastTxWindow::add(t, currentTick));   // first stored
    EXPECT_FALSE(FastTxWindow::add(t, currentTick));  // duplicate dropped
    EXPECT_NE(FastTxWindow::lookup(2, d, currentTick), nullptr);
}

TEST_F(FastTxWindowTest, OutOfWindowRejected)
{
    const unsigned int currentTick = 0; // window = (0, 4]
    Transaction* past = buildTx(0, 0x3003);   // tick == currentTick -> not future
    EXPECT_FALSE(FastTxWindow::add(past, currentTick));
    Transaction* future = buildTx(5, 0x3004); // tick > currentTick + W
    EXPECT_FALSE(FastTxWindow::add(future, currentTick));
    EXPECT_EQ(FastTxWindow::lookup(5, digestOf(future), currentTick), nullptr);
}

TEST_F(FastTxWindowTest, SlotsFullDrops)
{
    const unsigned int currentTick = 0;
    const unsigned int tick = 3;
    std::vector<m256i> digests;
    for (unsigned int i = 0; i < NUMBER_OF_TRANSACTIONS_PER_TICK; i++)
    {
        Transaction* t = buildTx(tick, 0x4000 + i);
        digests.push_back(digestOf(t));
        EXPECT_TRUE(FastTxWindow::add(t, currentTick));
    }
    // one more distinct tx -> tick is full -> dropped
    Transaction* overflow = buildTx(tick, 0x4FFF);
    EXPECT_FALSE(FastTxWindow::add(overflow, currentTick));
    EXPECT_EQ(FastTxWindow::lookup(tick, digestOf(overflow), currentTick), nullptr);
    // all the accepted ones are still findable
    for (unsigned int i = 0; i < NUMBER_OF_TRANSACTIONS_PER_TICK; i++)
        EXPECT_NE(FastTxWindow::lookup(tick, digests[i], currentTick), nullptr);
}

TEST_F(FastTxWindowTest, RingReuseSelfClears)
{
    // tick 4 and tick 8 map to the same slab (4 % 4 == 8 % 4 == 0).
    Transaction* a = buildTx(4, 0x5005);
    m256i da = digestOf(a);
    EXPECT_TRUE(FastTxWindow::add(a, 0));               // currentTick 0 -> tick 4 in window
    EXPECT_NE(FastTxWindow::lookup(4, da, 0), nullptr);

    // advance: currentTick 4 -> window (4, 8]; tick 4 now out, tick 8 reuses the slab
    Transaction* b = buildTx(8, 0x6006);
    m256i db = digestOf(b);
    EXPECT_TRUE(FastTxWindow::add(b, 4));
    EXPECT_NE(FastTxWindow::lookup(8, db, 4), nullptr); // new owner served
    EXPECT_EQ(FastTxWindow::lookup(4, da, 4), nullptr); // stale tick not returned
}

TEST_F(FastTxWindowTest, SizeNonZero)
{
    EXPECT_GT(FastTxWindow::getSize(), 0u);
}

#define NO_UEFI

#include "gtest/gtest.h"

#ifdef __linux__
#include "platform/msvc_polyfill.h"
#endif

#include "platform/memory_util.h"

#include <cstring>

// A pool above the threshold arrives zeroed, and zeroing an unaligned window inside it must leave
// the bytes on either side untouched.
TEST(DemandZeroPool, UnalignedZeroPoolPreservesNeighbours)
{
    const unsigned long long size = DEMAND_ZERO_POOL_THRESHOLD;
    unsigned char* pool = nullptr;
    ASSERT_TRUE(allocSparsePoolWithErrorLog(L"DemandZeroPool", size, (void**)&pool, __LINE__));
#if defined(__linux__)
    EXPECT_TRUE(qVirtualContains(pool, size));
#endif
    for (unsigned long long i = 0; i < size; i += 4096)
    {
        ASSERT_EQ(pool[i], 0);
    }

    memset(pool, 0xAB, size);
    const unsigned long long from = 4096 * 3 + 100;
    const unsigned long long len = 4096 * 5 + 200;
    zeroPool(pool + from, len);
    EXPECT_EQ(pool[from - 1], 0xAB);
    for (unsigned long long i = from; i < from + len; i++)
    {
        ASSERT_EQ(pool[i], 0);
    }
    EXPECT_EQ(pool[from + len], 0xAB);

    // Whole-pool zero, then a second alloc/free cycle: freePool must release a mapping cleanly.
    zeroPool(pool, size);
    EXPECT_EQ(pool[size - 1], 0);
    freePool(pool);
    EXPECT_FALSE(qVirtualContains(pool, size));
}

TEST(DemandZeroPool, SmallPoolStaysOnHeap)
{
    unsigned char* pool = nullptr;
    ASSERT_TRUE(allocPoolWithErrorLog(L"SmallPool", 1000, (void**)&pool, __LINE__));
    EXPECT_FALSE(qVirtualContains(pool, 1000));
    memset(pool, 0xAB, 1000);
    zeroPool(pool + 10, 500);
    EXPECT_EQ(pool[9], 0xAB);
    EXPECT_EQ(pool[10], 0);
    EXPECT_EQ(pool[509], 0);
    EXPECT_EQ(pool[510], 0xAB);
    freePool(pool);
}

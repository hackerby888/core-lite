#define NO_UEFI

#include "gtest/gtest.h"
#include "../src/network_messages/tick.h"
#include "../src/public_settings.h"
#include "../src/platform/virtual_memory.h"

#include <cstring>
#include <random>
#include <thread>
#include <atomic>
#include <vector>
#if defined(__linux__)
#include <csignal>
#endif

#include "network_messages/transactions.h"

TEST(TestVirtualMemory, TestVirtualMemory_NativeChar) {
    initFilesystem();
    registerAsynFileIO(NULL);
    const unsigned long long name_u64 = 123456789;
    const unsigned long long pageDir = 0;
    VirtualMemory<char, name_u64, pageDir, 1997, 128> test_vm;
    test_vm.init();
    std::vector<char> arr;
    // fill with random value
    const int N = 1000000;
    arr.resize(N);
    srand(0);
    for (int i = 0; i < N; i++)
    {
        arr[i] = int(rand() % 256) - 127;
    }
    // append to virtual memory via single append
    for (int i = 0; i < 113; i++)
    {
        test_vm.append(arr[i]);
    }
    int pos = 113;
    int stride = 1337;
    while (pos < N)
    {
        int s = pos;
        int e = std::min(pos + stride, N);
        int n_item = e - s;
        test_vm.appendMany(arr.data() + s, n_item);
        pos += stride;
    }
    std::vector<char> fetcher;
    
    for (int i = 0; i < 1024; i++)
    {
        int offset = rand() % (N/2);
        int test_len = rand() % (N - offset);
        fetcher.resize(test_len);
        test_vm.getMany(fetcher.data(), offset, test_len);
        EXPECT_TRUE(memcmp(fetcher.data(), arr.data() + offset, test_len) == 0);
    }
    
    for (int i = 0; i < 1024; i++)
    {
        int index = rand() % N;
        EXPECT_TRUE(test_vm[index] == arr[index]);
    }
    test_vm.deinit();
}

#define IMAX_BITS(m) ((m)/((m)%255+1) / 255%255*8 + 7-86/((m)%255+12))
#define RAND_MAX_WIDTH IMAX_BITS(RAND_MAX)
static_assert((RAND_MAX& (RAND_MAX + 1u)) == 0, "RAND_MAX not a Mersenne number");
uint64_t rand64(void) {
    uint64_t r = 0;
    for (int i = 0; i < 64; i += RAND_MAX_WIDTH) {
        r <<= RAND_MAX_WIDTH;
        r ^= (unsigned)rand();
    }
    return r;
}

TEST(TestVirtualMemory, TestVirtualMemory_NativeU64) {
    initFilesystem();
    registerAsynFileIO(NULL);
    const unsigned long long name_u64 = 123456789;
    const unsigned long long pageDir = 0;
    VirtualMemory<unsigned long long, name_u64, pageDir, 2005, 128> test_vm;
    test_vm.init();
    std::vector<unsigned long long> arr;
    // fill with random value
    const int N = 1000000;
    arr.resize(N);
    srand(0);
    for (int i = 0; i < N; i++)
    {
        arr[i] = rand64();
    }
    // append to virtual memory via single append
    for (int i = 0; i < 113; i++)
    {
        test_vm.append(arr[i]);
    }
    int pos = 113;
    int stride = 1337;
    while (pos < N)
    {
        int s = pos;
        int e = std::min(pos + stride, N);
        int n_item = e - s;
        test_vm.appendMany(arr.data() + s, n_item);
        pos += stride;
    }
    std::vector<unsigned long long> fetcher;

    for (int i = 0; i < 1024; i++)
    {
        int offset = rand() % (N/2);
        int test_len = rand() % (N - offset);
        fetcher.resize(test_len);
        test_vm.getMany(fetcher.data(), offset, test_len);
        EXPECT_TRUE(memcmp(fetcher.data(), arr.data() + offset, test_len * sizeof(unsigned long long)) == 0);
    }
    for (int i = 0; i < 1024; i++)
    {
        int index = rand() % N;
        EXPECT_TRUE(test_vm[index] == arr[index]);
    }
    test_vm.deinit();
}

TickData randTick()
{
    TickData res;
    int* ptr = (int*)&res;
    int sz = sizeof(res);
    for (int i = 0; i < sz/4; i++) {
        ptr[i] = rand();
    }
    return res;
}

bool tickEqual(const TickData a, const TickData b)
{
    return memcmp(&a, &b, sizeof(TickData)) == 0;
}

TEST(TestVirtualMemory, TestVirtualMemory_TickStruct) {
    initFilesystem();
    registerAsynFileIO(NULL);
    const unsigned long long name_u64 = 123456789;
    const unsigned long long pageDir = 0;
    VirtualMemory<TickData, name_u64, pageDir, 2005, 128> test_vm;
    test_vm.init();
    std::vector<TickData> arr;
    // fill with random value
    const int N = 1000;
    arr.resize(N);
    srand(0);
    for (int i = 0; i < N; i++)
    {
        arr[i] = randTick();
    }
    // append to virtual memory via single append
    for (int i = 0; i < 113; i++)
    {
        test_vm.append(arr[i]);
    }
    int pos = 113;
    int stride = 1337;
    while (pos < N)
    {
        int s = pos;
        int e = std::min(pos + stride, N);
        int n_item = e - s;
        test_vm.appendMany(arr.data() + s, n_item);
        pos += stride;
    }
    std::vector<TickData> fetcher;

    for (int i = 0; i < 1024; i++)
    {
        int offset = rand() % (N/2);
        int test_len = rand() % (N - offset);
        fetcher.resize(test_len);
        test_vm.getMany(fetcher.data(), offset, test_len);
        EXPECT_TRUE(memcmp(fetcher.data(), arr.data() + offset, test_len * sizeof(TickData)) == 0);
    }
    for (int i = 0; i < 1024; i++)
    {
        int index = rand() % N;
        EXPECT_TRUE(tickEqual(test_vm[index], arr[index]));
    }
    test_vm.deinit();
}

TEST(TestVirtualMemory, TestVirtualMemory_SpecialCases) {
    initFilesystem();
    registerAsynFileIO(NULL);
    const unsigned long long name_u64 = 123456789;
    const unsigned long long pageDir = 0;
    const unsigned long long pageCap = 2001;
    VirtualMemory<char, name_u64, pageDir, pageCap, 128> test_vm;
    test_vm.init();
    std::vector<char> arr;
    // fill with random value
    const int N = 1000000;
    arr.resize(N);
    srand(0);
    for (int i = 0; i < N; i++)
    {
        arr[i] = int(rand() % 256) - 127;
    }
    // append to virtual memory via single append
    for (int i = 0; i < 113; i++)
    {
        test_vm.append(arr[i]);
    }
    int pos = 113;
    int stride = 1337;
    while (pos < N)
    {
        int s = pos;
        int e = std::min(pos + stride, N);
        int n_item = e - s;
        test_vm.appendMany(arr.data() + s, n_item);
        pos += stride;
    }

    // case: head start aligns page
    std::vector<char> fetcher;
    {
        int offset = pageCap;
        int test_len = pageCap * 5 + 1;
        fetcher.resize(test_len);
        test_vm.getMany(fetcher.data(), offset, test_len);
        EXPECT_TRUE(memcmp(fetcher.data(), arr.data() + offset, test_len) == 0);
    }

    // case: tail end aligns page
    {
        int offset = 1337;
        int test_len = pageCap * 5 + (pageCap - 1337);
        fetcher.resize(test_len);
        test_vm.getMany(fetcher.data(), offset, test_len);
        EXPECT_TRUE(memcmp(fetcher.data(), arr.data() + offset, test_len) == 0);
    }

    test_vm.deinit();
}


TEST(TestSwapVirtualMemory, TestSwapVirtualMemory_IndexModeRandomAccess) {
    initFilesystem();
    registerAsynFileIO(NULL);

    struct TxHashMapEntry {
        m256i digest;
        unsigned long long offset;
    };
    SwapVirtualMemory<TxHashMapEntry, wcharToNumber(L"txdi"), wcharToNumber(L"data"), 64, 64, INDEX_MODE, 0> test_vm;
    test_vm.init();

    {
        TxHashMapEntry &entry = test_vm.getRef(1);
        entry.digest = m256i::zero();
        entry.digest.m256i_u64[0] = 1;
        entry.offset = 1;
    }

    {
        TxHashMapEntry& entry = test_vm.getRef(1000);
        entry.digest = m256i::zero();
        entry.digest.m256i_u64[0] = 1000;
        entry.offset = 1000;
    }

    {
        TxHashMapEntry& entry = test_vm.getRef(1'000'000);
        entry.digest = m256i::zero();
        entry.digest.m256i_u64[0] = 1'000'000;
        entry.offset = 1'000'000;
    }

    for (int i = 0; i < 1024 * 128 * 64; i++) {
        PinScope _pinScope; // release this iteration's page pin (no work-unit boundary in tests)
        auto randomRange = m256i::randomValue().m256i_u64[0] % (1024*128*64);
        if (randomRange != 1 && randomRange != 1000 && randomRange != 1'000'000) {
            TxHashMapEntry& entry = test_vm.getRef(randomRange);
            entry.digest = m256i::zero();
            entry.digest.m256i_u64[0] = randomRange;
            entry.offset = randomRange;
        }
    }

    // retest if previous test is ok
    {
        TxHashMapEntry& entry = test_vm.getRef(1);
        EXPECT_TRUE(entry.digest.m256i_u64[0] == 1);
        EXPECT_TRUE(entry.offset == 1);
    }
    {
        TxHashMapEntry& entry = test_vm.getRef(1000);
        EXPECT_TRUE(entry.digest.m256i_u64[0] == 1000);
        EXPECT_TRUE(entry.offset == 1000);
    }
    {
        TxHashMapEntry& entry = test_vm.getRef(1'000'000);
        EXPECT_TRUE(entry.digest.m256i_u64[0] == 1'000'000);
        EXPECT_TRUE(entry.offset == 1'000'000);
    }
}

TEST(TestSwapVirtualMemory, TestSwapVirtualMemory_IndexModeLinearAccess) {
    initFilesystem();
    registerAsynFileIO(NULL);

    struct TxHashMapEntry {
        m256i digest;
        unsigned long long offset;
    };
    SwapVirtualMemory<TxHashMapEntry, wcharToNumber(L"txdi"), wcharToNumber(L"data"), 64, 64, INDEX_MODE, 0> test_vm;
    test_vm.init();

    for (unsigned long long i = 0; i < 1024 * 128 * 64; i++) {
        PinScope _pinScope; // release this iteration's page pin (no work-unit boundary in tests)
        TxHashMapEntry& entry = test_vm.getRef(i);
        entry.digest = m256i::zero();
        entry.digest.m256i_u64[0] = i;
        entry.offset = i;
    }

    for (unsigned long long i = 0; i < 1024 * 128 * 64; i++) {
        PinScope _pinScope; // release this iteration's page pin (no work-unit boundary in tests)
        TxHashMapEntry& entry = test_vm.getRef(i);
        EXPECT_TRUE(entry.digest.m256i_u64[0] == i);
        EXPECT_TRUE(entry.offset == i);
    }
}

// Reproduces the epoch-transition pin-leak: a thread holds pin "notes" in its arena, the VM is
// reset() (as beginEpoch does), then the thread keeps working. Without the generation guard, the
// stale notes (a) suppress fresh pins (under-pin) and (b) make pinnedNow drift; with it, a reset
// cleanly invalidates the stale notes.
TEST(TestSwapVirtualMemory, TestSwapVirtualMemory_ResetInvalidatesStalePins) {
    initFilesystem();
    registerAsynFileIO(NULL);

    releaseThreadPins(); // start from a clean arena

    struct E { unsigned long long a, b; };
    SwapVirtualMemory<E, wcharToNumber(L"rsta"), wcharToNumber(L"data"), 64, 8, INDEX_MODE, 0> vm;
    vm.init();

    // Pin two distinct pages and DON'T release them (simulating a thread that holds notes across
    // the reset, like an in-flight HTTP request during an epoch transition).
    vm.getRef(0);    // page 0
    vm.getRef(64);   // page 1
    EXPECT_EQ(vm.getPinnedNow(), 2);

    // Epoch transition: reset wipes the cache + pin counters (but cannot touch this thread's arena).
    vm.reset();
    EXPECT_EQ(vm.getPinnedNow(), 0);

    // Fresh access to the SAME page must take a real pin again (stale note must not suppress it).
    vm.getRef(0);
    EXPECT_EQ(vm.getPinnedNow(), 1); // without the generation guard this would wrongly be 0

    // Releasing everything (the one fresh pin + the two stale notes) must land back at exactly 0,
    // not underflow or leave a phantom pinned slot.
    releaseThreadPins();
    EXPECT_EQ(vm.getPinnedNow(), 0);
}


TEST(TestSwapVirtualMemory, TestSwapVirtualMemory_OffsetModeLinearAccess) {
    initFilesystem();
    registerAsynFileIO(NULL);

    constexpr unsigned long long maxElementSize = (sizeof(Transaction) + SIGNATURE_SIZE + MAX_INPUT_SIZE);
    SwapVirtualMemory<Transaction, wcharToNumber(L"offs"), wcharToNumber(L"data"), 2, 64, OFFSET_MODE, SIGNATURE_SIZE + MAX_INPUT_SIZE> test_vm;
    test_vm.init();

    // Init
    {
        Transaction *tx = test_vm[0];
        tx->amount = 0;
        tx->inputSize = 0;
    }

    {
        Transaction *tx = test_vm[(maxElementSize)];
        tx->amount = maxElementSize;
        tx->inputSize = 0;
        // Should not use extra buffer
        EXPECT_TRUE(isAllBytesZero((void*)test_vm.getExtraBuffer(0), maxElementSize));
    }

    {
        EXPECT_TRUE(isAllBytesZero((void*)test_vm.getExtraBuffer(0), maxElementSize));
        // Will be added to extra buffer
        Transaction *tx = test_vm[(maxElementSize) + sizeof(Transaction)];
        tx->amount = (maxElementSize) + sizeof(Transaction);
        tx->inputSize = 4;
        EXPECT_FALSE(isAllBytesZero((void*)test_vm.getExtraBuffer(0), maxElementSize));

        // Check if the tx is from extra buffer
        EXPECT_TRUE(memcmp(test_vm.getExtraBuffer(0), (void*)tx, sizeof(Transaction)) == 0);
    }

    {
        Transaction *backupTx = new Transaction();
        memcpy(backupTx, (void*)test_vm.getExtraBuffer(0), sizeof(Transaction));
        // Will remove about tx from extra buffer and add below to extra buffer
        Transaction *tx = test_vm[(maxElementSize) + sizeof(Transaction) * 2 + 8];
        // Extra buffer should be reset
        EXPECT_TRUE(isAllBytesZero((void*)test_vm.getExtraBuffer(0), maxElementSize));
        // Check if the previous tx is written back correctly
        EXPECT_TRUE(memcmp(backupTx, (void*)test_vm[(maxElementSize) + sizeof(Transaction)], sizeof(Transaction)) == 0);
        tx->amount = (maxElementSize) + sizeof(Transaction) * 2 + 8;
        tx->inputSize = 0;
        EXPECT_FALSE(isAllBytesZero((void*)test_vm.getExtraBuffer(0), maxElementSize));
        // Check if the tx is from extra buffer
        EXPECT_TRUE(memcmp(test_vm.getExtraBuffer(0), (void*)tx, sizeof(Transaction)) == 0);
    }

    // Recheck
    {
        Transaction* tx = test_vm[0];
        EXPECT_TRUE(tx->amount == 0);
        EXPECT_TRUE(tx->inputSize == 0);
    }
    {
        Transaction* tx = test_vm[(maxElementSize)];
        EXPECT_TRUE(tx->amount == maxElementSize);
        EXPECT_TRUE(tx->inputSize == 0);
    }
    {
        Transaction* tx = test_vm[(maxElementSize) + sizeof(Transaction)];
        EXPECT_TRUE(tx->amount == (maxElementSize) + sizeof(Transaction));
        EXPECT_TRUE(tx->inputSize == 4);
    }
    {
        Transaction* tx = test_vm[(maxElementSize) + sizeof(Transaction) * 2 + 8];
        EXPECT_TRUE(tx->amount == (maxElementSize) + sizeof(Transaction) * 2 + 8);
        EXPECT_TRUE(tx->inputSize == 0);
    }
}

TEST(TestSwapVirtualMemory, TestSwapVirtualMemory_OffsetModeRandomAccess) {
    initFilesystem();
    registerAsynFileIO(NULL);

    SwapVirtualMemory<char, wcharToNumber(L"offs"), wcharToNumber(L"data"), 1024 * 1024, 64, OFFSET_MODE, 2> test_vm;
    test_vm.init();
    std::map<unsigned long long, char> valueMap;
    for (int i = 0; i < 1024 * 128 * 64; i++) {
        auto randomIndex = rand64() % (1024 * 128 * 64);
        char randomvalue = int(rand() % 256) - 127;
        char* entry = test_vm[randomIndex];
        *entry = randomvalue;
        valueMap[randomIndex] = randomvalue;
    }

    // Recheck
    for (auto& it : valueMap) {
        char* entry = test_vm[it.first];
        EXPECT_TRUE(*entry == it.second);
    }
}

TEST(TestSwapVirtualMemory, TestSwapVirtualMemory_TestCacheBuffer)
{
    initFilesystem();
    registerAsynFileIO(NULL);

    {
        SwapVirtualMemory<Transaction, wcharToNumber(L"cach"), wcharToNumber(L"data"), 1024 * 1024, 64, INDEX_MODE, 0> test_vm;
        test_vm.init();

        for (int i = 0; i < 64; i++)
        {
            Transaction *tx = test_vm.getCacheBuffer(i);
            EXPECT_TRUE(tx != nullptr);
            EXPECT_TRUE((uint64_t)tx == (uint64_t)test_vm.getCacheBuffer(0) + i * sizeof(Transaction) * 1024 * 1024);
        }
    }

    {
        SwapVirtualMemory<Transaction, wcharToNumber(L"cach"), wcharToNumber(L"data"), 1024 * 1024, 64, OFFSET_MODE, 64> test_vm;
        test_vm.init();
        EXPECT_TRUE(test_vm.getPageSize() == (1024 * 1024 * (sizeof(Transaction) + 64)));
        for (int i = 0; i < 64; i++)
        {
            Transaction *tx = test_vm.getCacheBuffer(i);
            EXPECT_TRUE(tx != nullptr);
            EXPECT_TRUE((uint64_t)tx == (uint64_t)test_vm.getCacheBuffer(0) + i * test_vm.getPageSize());
        }
    }
}

namespace {
struct E16 { unsigned long long a, b; }; // 16 bytes; pageCapacity 256 -> pageSize 4096 (one OS page)
}

// Component A: every page must survive eviction+reload even though the working set (64 pages) far
// exceeds the cache (4 slots). Small + fast, exercising the IO-outside-the-lock evict/load path.
TEST(TestSwapVirtualMemory, IndexEvictReloadIntegritySmall) {
    initFilesystem();
    registerAsynFileIO(NULL);
    SwapVirtualMemory<E16, wcharToNumber(L"evrl"), wcharToNumber(L"data"), 256, 4, INDEX_MODE, 0> vm;
    vm.init();
    const unsigned long long NPAGES = 64, CAP = 256;
    for (unsigned long long p = 0; p < NPAGES; p++) {
        PinScope _;
        E16& e = vm.getRef(p * CAP);
        e.a = p + 1; e.b = (p + 1) * 7;
    }
    for (unsigned long long p = 0; p < NPAGES; p++) {
        PinScope _;
        E16& e = vm.getRef(p * CAP);
        EXPECT_EQ(e.a, p + 1);
        EXPECT_EQ(e.b, (p + 1) * 7);
    }
}

// Component A: cache hits must keep working (no corruption, no deadlock) while another thread churns
// misses that evict+load under released memLock. A churner walks 200 pages (constant eviction) while
// four hitters hammer the resident sentinel page 0; page 0 is only ever written 0xABCD, so any other
// value read back signals a torn evict/reload.
TEST(TestSwapVirtualMemory, ConcurrentHitsDuringMiss) {
    initFilesystem();
    registerAsynFileIO(NULL);
    SwapVirtualMemory<E16, wcharToNumber(L"chdm"), wcharToNumber(L"data"), 256, 8, INDEX_MODE, 0> vm;
    vm.init();
    const unsigned long long CAP = 256;
    { PinScope _; vm.getRef(0).a = 0xABCD; }

    std::atomic<bool> stop{false};
    std::atomic<int> errors{0};
    std::thread churner([&]{
        unsigned long long p = 1;
        while (!stop.load(std::memory_order_relaxed)) {
            PinScope _;
            vm.getRef(p * CAP).a = p;
            p = (p % 200) + 1;
        }
    });
    std::vector<std::thread> hitters;
    for (int t = 0; t < 4; t++) hitters.emplace_back([&]{
        for (int i = 0; i < 100000; i++) {
            PinScope _;
            if (vm.getRef(0).a != 0xABCD) errors++;
        }
    });
    for (auto& h : hitters) h.join();
    stop = true; churner.join();
    EXPECT_EQ(errors.load(), 0);
}

#if defined(__linux__)
// Component B: with dirty tracking on, a page only read since load is evicted without a writeback
// (clean), while a written page is written back and survives. Tests install the SIGSEGV fast path
// that the node installs in signalHandler (the test binary has no handler of its own).
TEST(TestSwapVirtualMemory, DirtyTrackSkipsCleanWriteback) {
    initFilesystem();
    registerAsynFileIO(NULL);

    struct sigaction sa, oldSa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = [](int sig, siginfo_t* si, void*) {
        if (sig == SIGSEGV && si && SwapDirtyTrack::tryMarkDirty(si->si_addr)) return;
        signal(SIGSEGV, SIG_DFL); raise(SIGSEGV);
    };
    sa.sa_flags = SA_SIGINFO;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGSEGV, &sa, &oldSa);
    gSwapDirtyTrackEnabled = true;

    {
        SwapVirtualMemory<E16, wcharToNumber(L"dtyt"), wcharToNumber(L"data"), 256, 4, INDEX_MODE, 0> vm;
        vm.init();
        const unsigned long long CAP = 256;
        for (unsigned long long p = 0; p < 16; p++) {
            PinScope _;
            E16& e = vm.getRef(p * CAP);
            if (p % 2 == 0) e.a = p + 100;                          // written -> dirty
            else { volatile unsigned long long r = e.a; (void)r; }  // read only -> clean
        }
        EXPECT_GT(vm.getCleanEvicts(), 0u);
        EXPECT_GT(vm.getDirtyEvicts(), 0u);
        for (unsigned long long p = 0; p < 16; p++) {
            PinScope _;
            E16& e = vm.getRef(p * CAP);
            if (p % 2 == 0) EXPECT_EQ(e.a, p + 100);  // dirty page written back + reloaded
            else EXPECT_EQ(e.a, 0u);                  // clean page never written -> reloads as zero
        }
    }

    gSwapDirtyTrackEnabled = false;
    sigaction(SIGSEGV, &oldSa, nullptr);
}
#endif
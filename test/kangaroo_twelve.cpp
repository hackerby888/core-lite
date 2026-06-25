#define NO_UEFI

#ifdef __linux__
#include <iomanip>
#include "platform/m256.h"
#endif

#include "../src/K12/kangaroo_twelve_xkcp.h"
#include "../src/kangaroo_twelve.h"
#include "../src/platform/memory.h"
#include "../src/extensions/k12_state_digest_cache.h"
#include <lib/platform_common/qintrin.h>
#include "gtest/gtest.h"

#include <chrono>
#include <iostream>
#include <vector>
#include <algorithm>
#include <cstring>


TEST(TestCoreK12, PerformanceDigest32Of1GB)
{
    constexpr size_t bytesPerGigaByte = 1024 * 1024 * 1024;
    constexpr size_t repN = 1;
    constexpr size_t inputN = bytesPerGigaByte;
    constexpr size_t outputN = 32;

    char* inputPtr = new char[inputN];
    for (size_t i = 0; i < 100; ++i)
    {
        unsigned int pos, val;
        _rdrand32_step(&pos);
        _rdrand32_step(&val);
        inputPtr[pos % inputN] = val & 0xff;
    }
    char outputArray[outputN];

    auto startTime = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < repN; ++i)
        XKCP::KangarooTwelve((unsigned char *) inputPtr, inputN, (unsigned char*) outputArray, outputN);
    auto durationMilliSec = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);

    double bytePerMilliSec = double(repN * inputN) / double(durationMilliSec.count());
    double gigaBytePerSec = bytePerMilliSec * (1000.0 / bytesPerGigaByte);
    std::cout << "K12 of 1 GB to 32 Byte digest: " << gigaBytePerSec << " GB/sec = " << 1.0 / gigaBytePerSec << " sec/GB" << std::endl;

    delete [] inputPtr;
}

TEST(TestCoreK12, CompareK12Implementations)
{
    constexpr size_t bytesPerGigaByte = 1024 * 1024 * 1024;
    constexpr size_t repN = 1;
    constexpr size_t inputN = bytesPerGigaByte;
    constexpr size_t outputN = 32;

    char* inputPtr = new char[inputN];
    for (size_t i = 0; i < 100; ++i)
    {
        unsigned int pos, val;
        _rdrand32_step(&pos);
        _rdrand32_step(&val);
        inputPtr[pos % inputN] = val & 0xff;
    }
    char outputArrayXKCP[outputN];

    auto startTime = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < repN; ++i)
        XKCP::KangarooTwelve((unsigned char *) inputPtr, inputN, (unsigned char*) outputArrayXKCP, outputN);
    auto durationMilliSec = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);

    double bytePerMilliSec = double(repN * inputN) / double(durationMilliSec.count());
    double gigaBytePerSec = bytePerMilliSec * (1000.0 / bytesPerGigaByte);
    std::cout << "K12 of 1 GB to 32 Byte digest: " << gigaBytePerSec << " GB/sec = " << 1.0 / gigaBytePerSec << " sec/GB" << std::endl;
    std::cout << "Digest of xkcp implementation: ";
    for (int i = 0; i < sizeof(outputArrayXKCP); i++){
        std::cout << std::hex << std::setfill('0') << std::setw(2)
                  << (static_cast<int>(outputArrayXKCP[i]) & 0xff);
    }
    std::cout << std::endl;

    char outputArray[outputN];
    startTime = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < repN; ++i)
        KangarooTwelve((unsigned char *) inputPtr, inputN, (unsigned char*) outputArray, outputN);
    durationMilliSec = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);

    bytePerMilliSec = double(repN * inputN) / double(durationMilliSec.count());
    gigaBytePerSec = bytePerMilliSec * (1000.0 / bytesPerGigaByte);
    std::cout << "K12 of 1 GB to 32 Byte digest: " << gigaBytePerSec << " GB/sec = " << 1.0 / gigaBytePerSec << " sec/GB" << std::endl;
    std::cout << "Digest of native implementation: ";

    for (int i = 0; i < sizeof(outputArray); i++){
        std::cout << std::hex << std::setfill('0') << std::setw(2) 
                  << (static_cast<int>(outputArray[i]) & 0xff);
    }
    std::cout << std::endl;
    ASSERT_EQ(memcmp(outputArrayXKCP, outputArray, outputN), 0);
    delete [] inputPtr;
}

// ---- K12 incremental state-digest cache (src/extensions/k12_state_digest_cache.h) ----
// incrementalDigest must be byte-identical to the one-shot KangarooTwelve. Dirty bits are injected
// directly (no mprotect needed); the oracle is the native one-shot used by consensus.

static void k12sdc_fillRandom(unsigned char* p, size_t n)
{
    size_t i = 0;
    for (; i + 4 <= n; i += 4) { unsigned int v; _rdrand32_step(&v); memcpy(p + i, &v, 4); }
    for (; i < n; ++i) { unsigned int v; _rdrand32_step(&v); p[i] = (unsigned char)v; }
}

TEST(TestCoreK12, IncrementalAllDirtyEqualsOneShot)
{
    const size_t sizes[] = { 1, 100, 8191, 8192, 8193, 16383, 16384, 16385,
                             8192 * 3, 8192 * 3 + 1, 8192 * 100 + 777, 10u * 1024 * 1024 + 777 };
    for (size_t S : sizes)
    {
        std::vector<unsigned char> buf(S);
        k12sdc_fillRandom(buf.data(), S);
        const size_t numChunks = (S + 8192 - 1) / 8192;
        std::vector<unsigned char> cv(numChunks * 32, 0), dirty(numChunks, 1);
        unsigned char a[32], b[32];
        KangarooTwelve(buf.data(), (unsigned int)S, a, 32);
        K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), b);
        ASSERT_EQ(memcmp(a, b, 32), 0) << "all-dirty mismatch at size " << S;
    }
}

TEST(TestCoreK12, IncrementalSelectiveDirtyAfterMutation)
{
    const size_t S = 10u * 1024 * 1024 + 777;
    std::vector<unsigned char> buf(S);
    k12sdc_fillRandom(buf.data(), S);
    const size_t numChunks = (S + 8192 - 1) / 8192;
    std::vector<unsigned char> cv(numChunks * 32, 0), dirty(numChunks, 1);
    unsigned char d[32];
    K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), d);   // populate cv
    std::fill(dirty.begin(), dirty.end(), 0);
    for (size_t k = 1; k + 1 < numChunks; k += 7)
    {
        unsigned int off; _rdrand32_step(&off);
        buf[k * 8192 + (off % 8192)] ^= 0xA5;
        dirty[k] = 1;                                  // only the touched interior full chunk
    }
    unsigned char c[32], e[32];
    KangarooTwelve(buf.data(), (unsigned int)S, c, 32);
    K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), e);
    ASSERT_EQ(memcmp(c, e, 32), 0);
}

TEST(TestCoreK12, IncrementalCleanReuseNoMutation)
{
    const size_t S = 8192 * 50 + 33;
    std::vector<unsigned char> buf(S);
    k12sdc_fillRandom(buf.data(), S);
    const size_t numChunks = (S + 8192 - 1) / 8192;
    std::vector<unsigned char> cv(numChunks * 32, 0), dirty(numChunks, 1);
    unsigned char first[32];
    K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), first);
    std::fill(dirty.begin(), dirty.end(), 0);          // all clean -> pure reuse
    unsigned char reuse[32], oneShot[32];
    K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), reuse);
    KangarooTwelve(buf.data(), (unsigned int)S, oneShot, 32);
    ASSERT_EQ(memcmp(first, reuse, 32), 0);
    ASSERT_EQ(memcmp(reuse, oneShot, 32), 0);
}

TEST(TestCoreK12, IncrementalStaleDirtyIsHarmless)
{
    const size_t S = 8192 * 20 + 5;
    std::vector<unsigned char> buf(S);
    k12sdc_fillRandom(buf.data(), S);
    const size_t numChunks = (S + 8192 - 1) / 8192;
    std::vector<unsigned char> cv(numChunks * 32, 0), dirty(numChunks, 1);
    unsigned char base[32];
    K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), base);
    std::fill(dirty.begin(), dirty.end(), 0);          // mark some dirty WITHOUT mutating
    for (size_t k = 0; k < numChunks; k += 3) dirty[k] = 1;
    unsigned char again[32], oneShot[32];
    K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), again);
    KangarooTwelve(buf.data(), (unsigned int)S, oneShot, 32);
    ASSERT_EQ(memcmp(again, oneShot, 32), 0);
    ASSERT_EQ(memcmp(again, base, 32), 0);
}

TEST(TestCoreK12, IncrementalChunk0AndTailNeedNoDirtyBit)
{
    const size_t S = 8192 * 8 + 1234;                  // multi-chunk with partial tail
    std::vector<unsigned char> buf(S);
    k12sdc_fillRandom(buf.data(), S);
    const size_t numChunks = (S + 8192 - 1) / 8192;
    std::vector<unsigned char> cv(numChunks * 32, 0), dirty(numChunks, 1);
    unsigned char tmp[32];
    K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), tmp);   // populate
    std::fill(dirty.begin(), dirty.end(), 0);
    buf[123] ^= 0x5A;                                  // mutate chunk 0 only, no dirty bit
    unsigned char r0[32], o0[32];
    K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), r0);
    KangarooTwelve(buf.data(), (unsigned int)S, o0, 32);
    ASSERT_EQ(memcmp(r0, o0, 32), 0) << "chunk 0 not re-absorbed";
    buf[S - 7] ^= 0x3C;                                // mutate partial tail only, no dirty bit
    unsigned char r1[32], o1[32];
    K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), r1);
    KangarooTwelve(buf.data(), (unsigned int)S, o1, 32);
    ASSERT_EQ(memcmp(r1, o1, 32), 0) << "partial tail not re-absorbed";
}

TEST(TestCoreK12, IncrementalSingleChunkPath)
{
    for (size_t S : { (size_t)1, (size_t)8192 })
    {
        std::vector<unsigned char> buf(S);
        k12sdc_fillRandom(buf.data(), S);
        std::vector<unsigned char> cv(32, 0), dirtyOn(1, 1), dirtyOff(1, 0);
        unsigned char a[32], b[32], o[32];
        KangarooTwelve(buf.data(), (unsigned int)S, o, 32);
        K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirtyOn.data(), a);
        K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirtyOff.data(), b);
        ASSERT_EQ(memcmp(a, o, 32), 0) << "single-chunk dirty at size " << S;
        ASSERT_EQ(memcmp(b, o, 32), 0) << "single-chunk clean at size " << S;
    }
}

TEST(TestCoreK12, IncrementalCacheLifecycle)
{
    const size_t S = 8192 * 40 + 99;
    std::vector<unsigned char> buf(S);
    k12sdc_fillRandom(buf.data(), S);
    const size_t numChunks = (S + 8192 - 1) / 8192;
    std::vector<unsigned char> cv(numChunks * 32, 0), dirty(numChunks, 1);
    unsigned char dig[32], oneShot[32];
    K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), dig);   // initial build
    for (int round = 0; round < 5; ++round)
    {
        std::fill(dirty.begin(), dirty.end(), 0);
        for (int m = 0; m < 3; ++m)                    // mutate a few interior full chunks this "tick"
        {
            unsigned int kk, off; _rdrand32_step(&kk); _rdrand32_step(&off);
            const size_t k = 1 + (kk % (numChunks > 2 ? numChunks - 2 : 1));
            const size_t pos = k * 8192 + (off % 8192);
            if (pos < S) { buf[pos] ^= 0x11; dirty[k] = 1; }
        }
        K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), dig);
        KangarooTwelve(buf.data(), (unsigned int)S, oneShot, 32);
        ASSERT_EQ(memcmp(dig, oneShot, 32), 0) << "lifecycle round " << round;
        std::fill(dirty.begin(), dirty.end(), 0);      // clean recompute equals the same
        unsigned char clean[32];
        K12StateDigestCache::incrementalDigest(buf.data(), S, cv.data(), dirty.data(), clean);
        ASSERT_EQ(memcmp(clean, oneShot, 32), 0) << "lifecycle clean round " << round;
    }
}

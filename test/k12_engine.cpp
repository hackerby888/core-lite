// Consensus invariant: the contract-state engine's incremental/cached K12 digest MUST be
// bit-identical to plain KangarooTwelve over the full state. A mismatch forks the chain.
// Linux-only + x86 (k12_engine.h pulls userfaultfd.h; uses rdrand) — gated in test/CMakeLists.txt.
// The uffd-backed ContractStateEngine path is exercised by the running node (needs a uffd-capable
// runtime); here we verify the portable K12Engine base, including the per-chunk change-tracking path.

#define NO_UEFI
#define _GNU_SOURCE

#include <cstdio>
#include <cstring>
#include <string>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/poll.h>

#include "platform/msvc_polyfill.h"
#include "platform/file_io.h"
#include "platform/m256.h"
#include <extensions/k12_engine.h>
#include <gtest/gtest.h>
#include <immintrin.h>
#include <chrono>
#include <iostream>

TEST(K12EngineTest, IncrementalDigestEqualsPlainK12)
{
    const size_t SZ = 256u * 1024 * 1024 - 64; // not a multiple of K12_chunkSize on purpose
    unsigned char* state = new unsigned char[SZ];
    K12Engine k12(state, SZ);

    // deterministic non-zero fill
    for (size_t i = 0; i + 8 <= SZ; i += 8)
    {
        unsigned long long v = (i + 1) * 2654435761ULL;
        std::memcpy(state + i, &v, sizeof(v));
    }

    m256i h1, h2, xkcp;
    k12.getHash(h1.m256i_u8, 32);
    k12.getHash(h2.m256i_u8, 32); // second call hits the all-chunks-unchanged cache
    XKCP::KangarooTwelve(state, SZ, xkcp.m256i_u8, 32);

    EXPECT_EQ(h1, xkcp); // incremental digest == plain KangarooTwelve
    EXPECT_EQ(h1, h2);   // cached output stable

    // mutate scattered chunks, mark them changed, re-hash through the cache path
    for (int i = 0; i < 4096; i++)
    {
        unsigned int c = (unsigned int)((i * 7919ull) % k12.getMaxChunks());
        state[(size_t)c * K12_chunkSize] ^= 0xFF;
        k12.markChunkChanged(c);
    }

    m256i h3, xkcp2;
    k12.getHash(h3.m256i_u8, 32);
    XKCP::KangarooTwelve(state, SZ, xkcp2.m256i_u8, 32);

    EXPECT_NE(h3, h1);     // change detected
    EXPECT_EQ(h3, xkcp2);  // still == plain KangarooTwelve after partial-rehash

    delete[] state;
}

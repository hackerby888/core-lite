// Verify cached K12 remains identical to a full canonical digest.

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
    const size_t SZ = 256u * 1024 * 1024 - 64;
    unsigned char* state = new unsigned char[SZ];
    K12Engine k12(state, SZ);

    // Use deterministic non-zero state.
    for (size_t i = 0; i + 8 <= SZ; i += 8)
    {
        unsigned long long v = (i + 1) * 2654435761ULL;
        std::memcpy(state + i, &v, sizeof(v));
    }

    m256i h1, h2, xkcp;
    k12.getHash(h1.m256i_u8, 32);
    k12.getHash(h2.m256i_u8, 32);
    XKCP::KangarooTwelve(state, SZ, xkcp.m256i_u8, 32);

    EXPECT_EQ(h1, xkcp);
    EXPECT_EQ(h1, h2);

    // Rehash a deterministic set of changed chunks.
    for (int i = 0; i < 4096; i++)
    {
        unsigned int c = (unsigned int)((i * 7919ull) % k12.getMaxChunks());
        state[(size_t)c * K12_chunkSize] ^= 0xFF;
        k12.markChunkChanged(c);
    }

    m256i h3, xkcp2;
    k12.getHash(h3.m256i_u8, 32);
    XKCP::KangarooTwelve(state, SZ, xkcp2.m256i_u8, 32);

    EXPECT_NE(h3, h1);
    EXPECT_EQ(h3, xkcp2);

    delete[] state;
}

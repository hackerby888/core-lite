#pragma once

// Header file for the inclusion of platform specific x86/x64 intrinsics header files.

#if defined(_MSC_VER) && !defined(__clang__)
#include <intrin.h>
#elif defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#include <immintrin.h>
#else
// ---------------------------------------------------------------------------------------------
// Non-x86 (e.g. arm64 / Apple Silicon): SIMULATE the x86 intrinsics in software so the AVX2/SSE
// code compiles + runs unchanged. The SIMD set comes from SIMDe (NEON-backed or scalar); the few
// scalar x86 intrinsics SIMDe doesn't cover get tiny compiler-builtin shims below.
// This is the dev/"simulate" build path — mainnet x86 keeps native AVX2/512 (the branch above).
// NOTE: _umul128 / __shiftleft128 / __shiftright128 are defined (portably) in four_q.h — not here.
// ---------------------------------------------------------------------------------------------
// x86 calling-convention keywords are no-ops on arm (single ABI). The legacy UEFI fn-ptr typedefs
// in uefi.h hardcode __cdecl; arm has no such keyword -> define them empty so those typedefs parse.
// Runtime-neutral: those EFI function pointers are never called on the OS port.
#ifndef __cdecl
#define __cdecl
#endif
#ifndef __stdcall
#define __stdcall
#endif

#define SIMDE_ENABLE_NATIVE_ALIASES
#include "simde/x86/sse2.h"
#include "simde/x86/sse4.2.h"
#include "simde/x86/avx.h"
#include "simde/x86/avx2.h"

#include <cstdint>
#include <cstdlib>

// --- cycle counter -> arm virtual timer ---
static inline unsigned long long __rdtsc(void)
{
#if defined(__aarch64__)
    unsigned long long v;
    __asm__ volatile("mrs %0, cntvct_el0" : "=r"(v));
    return v;
#else
    return 0ULL;
#endif
}

// --- add/sub with carry (used heavily by four_q; not a SIMD intrinsic). __int128 => gcc+clang. ---
static inline unsigned char _addcarry_u64(unsigned char c_in, unsigned long long a, unsigned long long b, unsigned long long* out)
{
    __uint128_t s = (__uint128_t)a + (__uint128_t)b + (__uint128_t)c_in;
    *out = (unsigned long long)s;
    return (unsigned char)(s >> 64);
}
static inline unsigned char _subborrow_u64(unsigned char b_in, unsigned long long a, unsigned long long b, unsigned long long* out)
{
    __uint128_t d = (__uint128_t)a - (__uint128_t)b - (__uint128_t)b_in;
    *out = (unsigned long long)d;
    return (unsigned char)((d >> 64) & 1); // borrow if high bits set (wrapped)
}

// --- bit ops (BMI/ABM/LZCNT scalar forms) ---
static inline unsigned long long _lzcnt_u64(unsigned long long x) { return x ? (unsigned long long)__builtin_clzll(x) : 64ULL; }
static inline unsigned long long _tzcnt_u64(unsigned long long x) { return x ? (unsigned long long)__builtin_ctzll(x) : 64ULL; }
static inline unsigned int _blsr_u32(unsigned int x) { return x & (x - 1u); }
static inline unsigned long long _blsr_u64(unsigned long long x) { return x & (x - 1ULL); }
static inline unsigned int __popcnt(unsigned int x) { return (unsigned int)__builtin_popcount(x); }
static inline unsigned long long __popcnt64(unsigned long long x) { return (unsigned long long)__builtin_popcountll(x); }
static inline unsigned char _BitScanForward(unsigned long* index, unsigned int mask)
{
    if (!mask) return 0;
    *index = (unsigned long)__builtin_ctz(mask);
    return 1;
}
static inline unsigned long long _andn_u64(unsigned long long a, unsigned long long b) { return (~a) & b; }
static inline unsigned int _andn_u32(unsigned int a, unsigned int b) { return (~a) & b; }
// MSVC-style CPUID (x86 feature/TSC-freq query). No x86 features on arm -> zero (TSC freq via __rdtsc).
static inline void __cpuid(int info[4], int leaf) { (void)leaf; info[0] = info[1] = info[2] = info[3] = 0; }

// --- HW RNG: arm has no RDRAND. Dev/simulate build only -> weak xorshift fallback (NOT a CSPRNG).
//     Mainnet uses x86 RDRAND. Fine for a testnet/dev node + the gtests. ---
static inline unsigned long long __qinit_xorshift64(void)
{
    static unsigned long long s = 0x9e3779b97f4a7c15ULL;
    s ^= s << 13; s ^= s >> 7; s ^= s << 17;
    return s ^ __rdtsc();
}
static inline int _rdrand64_step(unsigned long long* out) { *out = __qinit_xorshift64(); return 1; }
static inline int _rdrand32_step(unsigned int* out) { *out = (unsigned int)__qinit_xorshift64(); return 1; }

// Make code that gates on __AVX2__ take the AVX2 path (e.g. score's "AVX2 or AVX512 required"
// static_assert) — SIMDe is already included above, so those _mm256_* calls resolve to its emulation.
// Defined AFTER the SIMDe includes so SIMDe itself still saw __AVX2__ undefined and used emulation.
#ifndef __AVX2__
#define __AVX2__ 1
#endif
#endif

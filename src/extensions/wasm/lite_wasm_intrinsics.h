#pragma once
// wasm32 shims for the x86/MSVC intrinsics the core platform headers (concurrency.h, memory_util.h,
// math_lib.h) reference. Compiled INTO contract.wasm (force-included by qinit's wasm genWrapper before any
// core header). A wasm contract runs single-instance-serialized (WASM_CONTRACTS.md §13.7), so the
// "Interlocked" ops need not be truly atomic — plain read/modify/write is correct here. Inert off wasm.
#if defined(__wasm__)
#include <cstdint>
#include <cstdlib>

static inline char      _InterlockedCompareExchange8(volatile char* d, char e, char c)        { char o = *d; if (o == c) *d = e; return o; }
static inline long      _InterlockedCompareExchange(volatile long* d, long e, long c)          { long o = *d; if (o == c) *d = e; return o; }
static inline long long _InterlockedCompareExchange64(volatile long long* d, long long e, long long c) { long long o = *d; if (o == c) *d = e; return o; }
static inline char      _InterlockedExchange8(volatile char* d, char v)                        { char o = *d; *d = v; return o; }
static inline long      _InterlockedExchange(volatile long* d, long v)                         { long o = *d; *d = v; return o; }
static inline long long _InterlockedExchange64(volatile long long* d, long long v)             { long long o = *d; *d = v; return o; }
static inline long long _InterlockedExchangeAdd64(volatile long long* d, long long v)          { long long o = *d; *d = o + v; return o; }
static inline long long _InterlockedAnd64(volatile long long* d, long long v)                  { long long o = *d; *d = o & v; return o; }
static inline long      _InterlockedIncrement(volatile long* d)                                { return ++*d; }
static inline long      _InterlockedDecrement(volatile long* d)                                { return --*d; }
static inline long long _InterlockedIncrement64(volatile long long* d)                         { return ++*d; }

static inline unsigned long _byteswap_ulong(unsigned long v) { return __builtin_bswap32((uint32_t)v); }
// _mul128 (qintrin.h) + _mm_pause (SIMDe) are already provided portably — don't redefine. _umul128 lives in
// four_q.h, which the contract TU does not pull, so define it here.
static inline unsigned long long _umul128(unsigned long long a, unsigned long long b, unsigned long long* hi) {
    __uint128_t r = (__uint128_t)a * b; *hi = (unsigned long long)(r >> 64); return (unsigned long long)r;
}

static inline void* _aligned_malloc(size_t size, size_t align) {
    if (align < sizeof(void*)) align = sizeof(void*);
    return aligned_alloc(align, (size + align - 1) & ~(align - 1));   // aligned_alloc needs size % align == 0
}
static inline void _aligned_free(void* p) { free(p); }

#endif // __wasm__

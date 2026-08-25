#pragma once
// wasm32 shims for intrinsics referenced by shared core headers.
// Contract instances are serialized, so these interlocked operations need no host atomics.
#if defined(__wasm__)
#include <cstdint>
#include <cstdlib>

static inline char _InterlockedCompareExchange8(volatile char* destination, char exchange, char comparand)
{
    char original = *destination;

    if (original == comparand)
    {
        *destination = exchange;
    }

    return original;
}

static inline long _InterlockedCompareExchange(volatile long* destination, long exchange, long comparand)
{
    long original = *destination;

    if (original == comparand)
    {
        *destination = exchange;
    }

    return original;
}

static inline long long _InterlockedCompareExchange64(volatile long long* destination, long long exchange, long long comparand)
{
    long long original = *destination;

    if (original == comparand)
    {
        *destination = exchange;
    }

    return original;
}

static inline char _InterlockedExchange8(volatile char* destination, char value)
{
    char original = *destination;
    *destination = value;
    return original;
}

static inline long _InterlockedExchange(volatile long* destination, long value)
{
    long original = *destination;
    *destination = value;
    return original;
}

static inline long long _InterlockedExchange64(volatile long long* destination, long long value)
{
    long long original = *destination;
    *destination = value;
    return original;
}

static inline long long _InterlockedExchangeAdd64(volatile long long* destination, long long value)
{
    long long original = *destination;
    *destination = original + value;
    return original;
}

static inline long long _InterlockedAnd64(volatile long long* destination, long long value)
{
    long long original = *destination;
    *destination = original & value;
    return original;
}

static inline long _InterlockedIncrement(volatile long* destination)
{
    return ++*destination;
}

static inline long _InterlockedDecrement(volatile long* destination)
{
    return --*destination;
}

static inline long long _InterlockedIncrement64(volatile long long* destination)
{
    return ++*destination;
}

static inline unsigned long _byteswap_ulong(unsigned long value)
{
    return __builtin_bswap32((uint32_t)value);
}

static inline unsigned long long _umul128(unsigned long long left, unsigned long long right, unsigned long long* high)
{
    __uint128_t result = (__uint128_t)left * right;
    *high = (unsigned long long)(result >> 64);
    return (unsigned long long)result;
}

static inline void* _aligned_malloc(size_t size, size_t alignment)
{
    if (alignment < sizeof(void*))
    {
        alignment = sizeof(void*);
    }

    const size_t alignedSize = (size + alignment - 1) & ~(alignment - 1);
    return aligned_alloc(alignment, alignedSize);
}

static inline void _aligned_free(void* pointer)
{
    free(pointer);
}

#endif // __wasm__

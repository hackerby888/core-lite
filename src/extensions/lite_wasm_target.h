#pragma once

// Contract-side helpers required before a dynamic contract header is parsed.
//
// This file is part of the core-lite Wasm target ABI. Build tools set
// LITE_WASM_TU_BUILD and include it directly instead of carrying private copies
// of QPI helpers, locals behavior, or asset-iterator layouts.
#ifdef LITE_WASM_TU_BUILD

#include "platform/memory.h"
#include "extensions/lite_dyn_abi.h"

// QPI memory helpers

namespace QPI
{

template <typename T1, typename T2>
inline void copyMemory(T1& dst, const T2& src)
{
    static_assert(sizeof(dst) == sizeof(src), "Size of source and destination must match to run copyMemory().");
    copyMem(&dst, &src, sizeof(dst));
}

template <typename T1, typename T2>
inline void copyFromBuffer(T1& dst, const T2& src)
{
    static_assert(sizeof(dst) <= sizeof(src), "Destination object must be at most the size of the source buffer.");
    copyMem(&dst, &src, sizeof(dst));
}

template <typename T>
inline void setMemory(T& dst, uint8 value)
{
    setMem(&dst, sizeof(dst), value);
}

template <typename T, unsigned int I>
void setMemory(ContractState<T, I>&, uint8) = delete;

template <typename T1, unsigned int I, typename T2>
void copyMemory(ContractState<T1, I>&, const T2&) = delete;

template <typename T1, unsigned int I, typename T2>
void copyFromBuffer(ContractState<T1, I>&, const T2&) = delete;

// Saturating arithmetic

inline static sint64 smul(sint64 a, sint64 b)
{
    __int128 r = (__int128)a * (__int128)b;
    if (r < (__int128)(-9223372036854775807LL - 1))
    {
        return -9223372036854775807LL - 1;
    }
    if (r > (__int128)9223372036854775807LL)
    {
        return 9223372036854775807LL;
    }
    return (sint64)r;
}

inline static uint64 smul(uint64 a, uint64 b)
{
    unsigned __int128 r = (unsigned __int128)a * (unsigned __int128)b;
    if (r > (unsigned __int128)18446744073709551615ULL)
    {
        return 18446744073709551615ULL;
    }
    return (uint64)r;
}

inline static sint32 smul(sint32 a, sint32 b)
{
    sint64 r = (sint64)a * (sint64)b;
    if (r < -2147483647LL - 1)
    {
        return -2147483647 - 1;
    }
    if (r > 2147483647LL)
    {
        return 2147483647;
    }
    return (sint32)r;
}

inline static uint32 smul(uint32 a, uint32 b)
{
    uint64 r = (uint64)a * (uint64)b;
    if (r > 4294967295ULL)
    {
        return 4294967295u;
    }
    return (uint32)r;
}

inline static sint64 sadd(sint64 a, sint64 b)
{
    sint64 sum = (sint64)((uint64)a + (uint64)b);
    if (a < 0 && b < 0 && sum > 0)
    {
        return -9223372036854775807LL - 1;
    }
    if (a > 0 && b > 0 && sum < 0)
    {
        return 9223372036854775807LL;
    }
    return sum;
}

inline static uint64 sadd(uint64 a, uint64 b)
{
    if (18446744073709551615ULL - a < b)
    {
        return 18446744073709551615ULL;
    }
    return a + b;
}

inline static sint32 sadd(sint32 a, sint32 b)
{
    sint64 sum = (sint64)a + (sint64)b;
    if (sum < -2147483647LL - 1)
    {
        return -2147483647 - 1;
    }
    if (sum > 2147483647LL)
    {
        return 2147483647;
    }
    return (sint32)sum;
}

inline static uint32 sadd(uint32 a, uint32 b)
{
    uint64 sum = (uint64)a + (uint64)b;
    if (sum > 4294967295ULL)
    {
        return 4294967295u;
    }
    return (uint32)sum;
}

// Array predicates

template <typename T, uint64 L>
bool isArraySorted(const Array<T, L>& array, uint64 beginIdx, uint64 endIdx)
{
    if (endIdx > L || beginIdx > endIdx)
    {
        return false;
    }

    for (uint64 i = beginIdx + 1; i < endIdx; ++i)
    {
        if (array.get(i - 1) > array.get(i))
        {
            return false;
        }
    }
    return true;
}

template <typename T, uint64 L>
bool isArraySortedWithoutDuplicates(const Array<T, L>& array, uint64 beginIdx, uint64 endIdx)
{
    if (endIdx > L || beginIdx > endIdx)
    {
        return false;
    }

    for (uint64 i = beginIdx + 1; i < endIdx; ++i)
    {
        if (array.get(i - 1) >= array.get(i))
        {
            return false;
        }
    }
    return true;
}

} // namespace QPI

// Function-local storage

static constexpr unsigned int LITE_WASM_LOCALS_CAPACITY = 2u << 20;
static constexpr unsigned int LITE_WASM_LOCALS_DEPTH = 256;

namespace
{
unsigned char g_liteWasmLocals[LITE_WASM_LOCALS_CAPACITY];
unsigned long g_liteWasmLocalsTop = 0;
unsigned long g_liteWasmLocalsMarks[LITE_WASM_LOCALS_DEPTH];
unsigned int g_liteWasmLocalsDepth = 0;
} // namespace

void* QPI::QpiContextFunctionCall::__qpiAllocLocals(unsigned int sizeOfLocals) const
{
    const unsigned long off = g_liteWasmLocalsTop;
    if (g_liteWasmLocalsDepth < LITE_WASM_LOCALS_DEPTH)
    {
        g_liteWasmLocalsMarks[g_liteWasmLocalsDepth++] = off;
    }
    g_liteWasmLocalsTop = (off + sizeOfLocals + 7) & ~7ul;
    __builtin_memset(&g_liteWasmLocals[off], 0, sizeOfLocals);
    return (void*)&g_liteWasmLocals[off];
}

void QPI::QpiContextFunctionCall::__qpiFreeLocals() const
{
    if (g_liteWasmLocalsDepth > 0)
    {
        g_liteWasmLocalsTop = g_liteWasmLocalsMarks[--g_liteWasmLocalsDepth];
    }
}

// Asset iterator host bridge

static_assert(sizeof(LiteAssetEntry) == 80, "LiteAssetEntry ABI size");
static_assert(offsetof(LiteAssetEntry, owner) == 0, "LiteAssetEntry owner offset");
static_assert(offsetof(LiteAssetEntry, possessor) == 32, "LiteAssetEntry possessor offset");
static_assert(offsetof(LiteAssetEntry, shares) == 64, "LiteAssetEntry shares offset");
static_assert(offsetof(LiteAssetEntry, ownershipManagingContract) == 72, "LiteAssetEntry ownership-management offset");
static_assert(offsetof(LiteAssetEntry, possessionManagingContract) == 74, "LiteAssetEntry possession-management offset");
__attribute__((import_module("lhost"), import_name("assetEnumerate")))
extern "C" unsigned int lh_assetEnumerate(unsigned int kind, const void* issuance, const void* ownership,
                                          const void* possession, void* out, unsigned int capacity);

namespace
{
LiteAssetEntry g_liteAssetEntries[LITE_ASSET_ENTRY_CAPACITY];
} // namespace

void QPI::AssetOwnershipIterator::begin(
    const QPI::Asset& issuance,
    const QPI::AssetOwnershipSelect& ownership)
{
    _issuance = issuance;
    _ownership = ownership;
    _issuanceIdx = lh_assetEnumerate(
        0,
        &_issuance,
        &_ownership,
        &_ownership,
        g_liteAssetEntries,
        LITE_ASSET_ENTRY_CAPACITY);
    _ownershipIdx = 0;
}

bool QPI::AssetOwnershipIterator::reachedEnd() const
{
    return _ownershipIdx >= _issuanceIdx;
}

bool QPI::AssetOwnershipIterator::next()
{
    ++_ownershipIdx;
    return _ownershipIdx < _issuanceIdx;
}

QPI::id QPI::AssetOwnershipIterator::issuer() const
{
    return _issuance.issuer;
}

QPI::uint64 QPI::AssetOwnershipIterator::assetName() const
{
    return _issuance.assetName;
}

QPI::id QPI::AssetOwnershipIterator::owner() const
{
    QPI::id r;
    copyMem(&r, g_liteAssetEntries[_ownershipIdx].owner, 32);
    return r;
}

QPI::sint64 QPI::AssetOwnershipIterator::numberOfOwnedShares() const
{
    return g_liteAssetEntries[_ownershipIdx].shares;
}

QPI::uint16 QPI::AssetOwnershipIterator::ownershipManagingContract() const
{
    return g_liteAssetEntries[_ownershipIdx].ownershipManagingContract;
}

void QPI::AssetPossessionIterator::begin(
    const QPI::Asset& issuance,
    const QPI::AssetOwnershipSelect& ownership,
    const QPI::AssetPossessionSelect& possession)
{
    _issuance = issuance;
    _ownership = ownership;
    _possession = possession;
    _issuanceIdx = lh_assetEnumerate(
        1,
        &_issuance,
        &_ownership,
        &_possession,
        g_liteAssetEntries,
        LITE_ASSET_ENTRY_CAPACITY);
    _ownershipIdx = 0;
}

bool QPI::AssetPossessionIterator::reachedEnd() const
{
    return _ownershipIdx >= _issuanceIdx;
}

bool QPI::AssetPossessionIterator::next()
{
    ++_ownershipIdx;
    return _ownershipIdx < _issuanceIdx;
}

QPI::id QPI::AssetPossessionIterator::possessor() const
{
    QPI::id r;
    copyMem(&r, g_liteAssetEntries[_ownershipIdx].possessor, 32);
    return r;
}

QPI::sint64 QPI::AssetPossessionIterator::numberOfPossessedShares() const
{
    return g_liteAssetEntries[_ownershipIdx].shares;
}

#endif

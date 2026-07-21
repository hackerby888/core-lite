#pragma once

// Contract-side QPI helpers required before a deployed contract header is parsed.
#ifdef LITE_WASM_TU_BUILD

#include "platform/memory.h"
#include "extensions/wasm/shared/abi_types.h"

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

inline static sint64 smul(sint64 left, sint64 right)
{
    __int128 result = (__int128)left * (__int128)right;
    if (result < (__int128)(-9223372036854775807LL - 1))
    {
        return -9223372036854775807LL - 1;
    }

    if (result > (__int128)9223372036854775807LL)
    {
        return 9223372036854775807LL;
    }

    return (sint64)result;
}

inline static uint64 smul(uint64 left, uint64 right)
{
    unsigned __int128 result = (unsigned __int128)left * (unsigned __int128)right;
    if (result > (unsigned __int128)18446744073709551615ULL)
    {
        return 18446744073709551615ULL;
    }

    return (uint64)result;
}

inline static sint32 smul(sint32 left, sint32 right)
{
    sint64 result = (sint64)left * (sint64)right;
    if (result < -2147483647LL - 1)
    {
        return -2147483647 - 1;
    }

    if (result > 2147483647LL)
    {
        return 2147483647;
    }

    return (sint32)result;
}

inline static uint32 smul(uint32 left, uint32 right)
{
    uint64 result = (uint64)left * (uint64)right;
    if (result > 4294967295ULL)
    {
        return 4294967295u;
    }

    return (uint32)result;
}

inline static sint64 sadd(sint64 left, sint64 right)
{
    sint64 sum = (sint64)((uint64)left + (uint64)right);
    if (left < 0 && right < 0 && sum > 0)
    {
        return -9223372036854775807LL - 1;
    }

    if (left > 0 && right > 0 && sum < 0)
    {
        return 9223372036854775807LL;
    }

    return sum;
}

inline static uint64 sadd(uint64 left, uint64 right)
{
    if (18446744073709551615ULL - left < right)
    {
        return 18446744073709551615ULL;
    }

    return left + right;
}

inline static sint32 sadd(sint32 left, sint32 right)
{
    sint64 sum = (sint64)left + (sint64)right;
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

inline static uint32 sadd(uint32 left, uint32 right)
{
    uint64 sum = (uint64)left + (uint64)right;
    if (sum > 4294967295ULL)
    {
        return 4294967295u;
    }

    return (uint32)sum;
}

// Array predicates

template <typename T, uint64 L>
bool isArraySorted(const Array<T, L>& array, uint64 beginIndex, uint64 endIndex)
{
    if (endIndex > L || beginIndex > endIndex)
    {
        return false;
    }

    for (uint64 index = beginIndex + 1; index < endIndex; ++index)
    {
        if (array.get(index - 1) > array.get(index))
        {
            return false;
        }
    }

    return true;
}

template <typename T, uint64 L>
bool isArraySortedWithoutDuplicates(
    const Array<T, L>& array,
    uint64 beginIndex,
    uint64 endIndex)
{
    if (endIndex > L || beginIndex > endIndex)
    {
        return false;
    }

    for (uint64 index = beginIndex + 1; index < endIndex; ++index)
    {
        if (array.get(index - 1) >= array.get(index))
        {
            return false;
        }
    }

    return true;
}

} // namespace QPI

// Function-local storage

static constexpr unsigned int WASM_LOCALS_DEPTH = 256;

namespace
{
void* localsMarks[WASM_LOCALS_DEPTH];
unsigned int localsDepth = 0;
} // namespace

void* QPI::QpiContextFunctionCall::__qpiAllocLocals(unsigned int sizeOfLocals) const
{
    if (localsDepth >= WASM_LOCALS_DEPTH)
    {
        return nullptr;
    }

    void* locals = __acquireScratchpad(sizeOfLocals, true);
    if (locals)
    {
        localsMarks[localsDepth++] = locals;
    }
    return locals;
}

void QPI::QpiContextFunctionCall::__qpiFreeLocals() const
{
    if (localsDepth > 0)
    {
        __releaseScratchpad(localsMarks[--localsDepth]);
    }
}

// Asset iterator host bridge

static_assert(sizeof(Wasm::AssetEntry) == 80, "AssetEntry ABI size");
static_assert(offsetof(Wasm::AssetEntry, owner) == 0, "AssetEntry owner offset");
static_assert(offsetof(Wasm::AssetEntry, possessor) == 32, "AssetEntry possessor offset");
static_assert(offsetof(Wasm::AssetEntry, shares) == 64, "AssetEntry shares offset");
static_assert(offsetof(Wasm::AssetEntry, ownershipManagingContract) == 72, "AssetEntry ownership-management offset");
static_assert(offsetof(Wasm::AssetEntry, possessionManagingContract) == 74, "AssetEntry possession-management offset");
__attribute__((import_module("lhost"), import_name("assetEnumerate")))
extern "C" unsigned int lh_assetEnumerate(
    unsigned int kind,
    const void* issuance,
    const void* ownership,
    const void* possession,
    void* output,
    unsigned int capacity);

namespace
{
Wasm::AssetEntry assetEntries[WASM_ASSET_ENTRY_CAPACITY];
} // namespace

void QPI::AssetOwnershipIterator::begin(
    const QPI::Asset& issuance,
    const QPI::AssetOwnershipSelect& ownership)
{
    _issuance = issuance;
    _ownership = ownership;
    _issuanceIdx = lh_assetEnumerate(0, &_issuance, &_ownership, &_ownership, assetEntries, WASM_ASSET_ENTRY_CAPACITY);
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
    QPI::id ownerId;

    copyMem(&ownerId, assetEntries[_ownershipIdx].owner, 32);
    return ownerId;
}

QPI::sint64 QPI::AssetOwnershipIterator::numberOfOwnedShares() const
{
    return assetEntries[_ownershipIdx].shares;
}

QPI::uint16 QPI::AssetOwnershipIterator::ownershipManagingContract() const
{
    return assetEntries[_ownershipIdx].ownershipManagingContract;
}

void QPI::AssetPossessionIterator::begin(
    const QPI::Asset& issuance,
    const QPI::AssetOwnershipSelect& ownership,
    const QPI::AssetPossessionSelect& possession)
{
    _issuance = issuance;
    _ownership = ownership;
    _possession = possession;
    _issuanceIdx = lh_assetEnumerate(1, &_issuance, &_ownership, &_possession, assetEntries, WASM_ASSET_ENTRY_CAPACITY);
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
    QPI::id possessorId;

    copyMem(&possessorId, assetEntries[_ownershipIdx].possessor, 32);
    return possessorId;
}

QPI::sint64 QPI::AssetPossessionIterator::numberOfPossessedShares() const
{
    return assetEntries[_ownershipIdx].shares;
}

#endif

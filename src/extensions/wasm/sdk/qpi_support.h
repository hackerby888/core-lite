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
inline void copyToBuffer(T1& dst, const T2& src, bool setTailToZero)
{
    static_assert(sizeof(dst) >= sizeof(src), "Destination buffer must be at least the size of the source object.");
    copyMem(&dst, &src, sizeof(src));

    if (sizeof(dst) > sizeof(src) && setTailToZero)
    {
        uint8* tailPtr = reinterpret_cast<uint8*>(&dst) + sizeof(src);
        const uint64 tailSize = sizeof(dst) - sizeof(src);
        setMem(tailPtr, tailSize, 0);
    }
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
void copyToBuffer(ContractState<T1, I>&, const T2&, bool) = delete;

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
bool isArraySortedWithoutDuplicates(const Array<T, L>& array, uint64 beginIndex, uint64 endIndex)
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

// Asset iterator host bridge: the object holds the native iterator's universe indices and the host advances them,
// so a walk has no record cap and two live iterators never share a buffer.

static_assert(sizeof(Wasm::AssetEntry) == 80, "AssetEntry ABI size");
static_assert(offsetof(Wasm::AssetEntry, owner) == 0, "AssetEntry owner offset");
static_assert(offsetof(Wasm::AssetEntry, possessor) == 32, "AssetEntry possessor offset");
static_assert(offsetof(Wasm::AssetEntry, shares) == 64, "AssetEntry shares offset");
static_assert(offsetof(Wasm::AssetEntry, ownershipManagingContract) == 72, "AssetEntry ownership-management offset");
static_assert(offsetof(Wasm::AssetEntry, possessionManagingContract) == 74, "AssetEntry possession-management offset");
__attribute__((import_module("lhost"), import_name("assetIterBegin")))
extern "C" void lh_assetIterBegin(unsigned int kind, const void* issuance, const void* ownership, const void* possession, unsigned int* issuanceIdx,
    unsigned int* ownershipIdx, unsigned int* possessionIdx);
__attribute__((import_module("lhost"), import_name("assetIterNext")))
extern "C" unsigned int lh_assetIterNext(unsigned int kind, const void* issuance, const void* ownership, const void* possession, unsigned int* issuanceIdx,
    unsigned int* ownershipIdx, unsigned int* possessionIdx);
__attribute__((import_module("lhost"), import_name("assetIterRecord")))
extern "C" void lh_assetIterRecord(unsigned int kind, unsigned int ownershipIdx, unsigned int possessionIdx, void* entry);

// The current record, fetched per accessor so nested walks never share a buffer.
static Wasm::AssetEntry assetIterRecord(unsigned int kind, unsigned int ownershipIdx, unsigned int possessionIdx)
{
    Wasm::AssetEntry entry;

    lh_assetIterRecord(kind, ownershipIdx, possessionIdx, &entry);
    return entry;
}

void QPI::AssetOwnershipIterator::begin(const QPI::Asset& issuance, const QPI::AssetOwnershipSelect& ownership)
{
    _issuance = issuance;
    _ownership = ownership;
    lh_assetIterBegin(0, &_issuance, &_ownership, &_ownership, &_issuanceIdx, &_ownershipIdx, nullptr);
}

bool QPI::AssetOwnershipIterator::reachedEnd() const
{
    return _ownershipIdx == WASM_NO_ASSET_INDEX;
}

bool QPI::AssetOwnershipIterator::next()
{
    return lh_assetIterNext(0, &_issuance, &_ownership, &_ownership, &_issuanceIdx, &_ownershipIdx, nullptr) != 0;
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
    const Wasm::AssetEntry entry = assetIterRecord(0, _ownershipIdx, WASM_NO_ASSET_INDEX);
    QPI::id ownerId;

    copyMem(&ownerId, entry.owner, 32);
    return ownerId;
}

QPI::sint64 QPI::AssetOwnershipIterator::numberOfOwnedShares() const
{
    return assetIterRecord(0, _ownershipIdx, WASM_NO_ASSET_INDEX).shares;
}

QPI::uint16 QPI::AssetOwnershipIterator::ownershipManagingContract() const
{
    return assetIterRecord(0, _ownershipIdx, WASM_NO_ASSET_INDEX).ownershipManagingContract;
}

void QPI::AssetPossessionIterator::begin(const QPI::Asset& issuance, const QPI::AssetOwnershipSelect& ownership, const QPI::AssetPossessionSelect& possession)
{
    _issuance = issuance;
    _ownership = ownership;
    _possession = possession;
    lh_assetIterBegin(1, &_issuance, &_ownership, &_possession, &_issuanceIdx, &_ownershipIdx, &_possessionIdx);
}

bool QPI::AssetPossessionIterator::reachedEnd() const
{
    return _possessionIdx == WASM_NO_ASSET_INDEX;
}

bool QPI::AssetPossessionIterator::next()
{
    return lh_assetIterNext(1, &_issuance, &_ownership, &_possession, &_issuanceIdx, &_ownershipIdx, &_possessionIdx) != 0;
}

QPI::id QPI::AssetPossessionIterator::possessor() const
{
    const Wasm::AssetEntry entry = assetIterRecord(1, _ownershipIdx, _possessionIdx);
    QPI::id possessorId;

    copyMem(&possessorId, entry.possessor, 32);
    return possessorId;
}

QPI::sint64 QPI::AssetPossessionIterator::numberOfPossessedShares() const
{
    return assetIterRecord(1, _ownershipIdx, _possessionIdx).shares;
}

QPI::uint16 QPI::AssetPossessionIterator::possessionManagingContract() const
{
    return assetIterRecord(1, _ownershipIdx, _possessionIdx).possessionManagingContract;
}

#endif

#pragma once

// Pointer alignment on its own, so callers that only need it do not pull in the allocator and its UEFI
// dependencies.

#include <cstddef>
#include <cstdint>

template <typename T>
inline T* alignPointerDown(T* pointer, std::size_t alignment)
{
    return reinterpret_cast<T*>(reinterpret_cast<std::uintptr_t>(pointer) & ~(static_cast<std::uintptr_t>(alignment) - 1));
}

template <typename T>
inline T* alignPointerUp(T* pointer, std::size_t alignment)
{
    return reinterpret_cast<T*>((reinterpret_cast<std::uintptr_t>(pointer) + alignment - 1) & ~(static_cast<std::uintptr_t>(alignment) - 1));
}

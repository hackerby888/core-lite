// KangarooTwelve for the cross-host parity gtest, isolated in its own translation unit.
//
// Why a separate file rather than an include in wasm_contracts.cpp: kangaroo_twelve.h reaches
// platform/memory.h, whose non-NO_UEFI branch resolves setMem/copyMem through the UEFI boot-services
// pointer `bs`, which no test binary links. NO_UEFI selects plain declarations instead — but defining
// that macro inside wasm_contracts.cpp would change the whole translation unit's view of every core
// header it already includes. Keeping it here confines the macro to four functions.
//
// The four memory primitives NO_UEFI leaves undeclared normally come from test/stdlib_impl.cpp; they
// are defined locally so the wasm test target does not have to grow another source file with its own
// dependencies.
#define NO_UEFI

#include <cstdlib>
#include <cstring>

#include "platform/m256.h"

void setMem(void* buffer, unsigned long long size, unsigned char value)
{
    memset(buffer, value, (size_t)size);
}

void copyMem(void* destination, const void* source, unsigned long long length)
{
    memcpy(destination, source, (size_t)length);
}

bool allocatePool(unsigned long long size, void** buffer)
{
    *buffer = malloc((size_t)size);
    return *buffer != nullptr;
}

void freePool(void* buffer)
{
    free(buffer);
}

#include "kangaroo_twelve.h"

// The lhost ABI passes no output length: lh_k12 is declared `void lh_k12(const void*, unsigned, void*)`
// and always produces 32 bytes, matching qpi_services.h's hashK12 and the qinit engine's k12Bytes.
// Verified byte-for-byte against the engine before this shim was trusted.
extern "C" void qinitShimK12(const void* input, unsigned int length, void* output32)
{
    KangarooTwelve(input, length, output32, 32);
}

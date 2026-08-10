// A traced call runs with the contract's state mapped read-only and repairs each page from a fault handler.
#ifdef LITE_WASM_SC
#ifndef _WIN32

#include "platform/m256.h"
#include "extensions/wasm/runtime/state_write_tracker.h"
#include "gtest/gtest.h"

#include <cstring>
#include <sys/mman.h>
#include <unistd.h>

// Linux raises SIGSEGV for the write that hits the read-only mapping, Darwin SIGBUS. The handler has to be
// registered for whichever one the platform delivers, or the write reaches the crash path instead.
TEST(WasmContracts, StateWriteFaultIsRepaired)
{
    const size_t pageSize = (size_t)sysconf(_SC_PAGESIZE);
    unsigned char* state = (unsigned char*)mmap(
        nullptr, pageSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    ASSERT_NE(state, MAP_FAILED);
    memset(state, 0, pageSize);

    Wasm::Runtime::TraceEntry entry;
    Wasm::Runtime::beginStateWriteTracking(state, (unsigned int)pageSize);
    state[8] = 0x42;
    Wasm::Runtime::finishStateWriteTracking(entry, state, (unsigned int)pageSize);

    EXPECT_EQ(state[8], 0x42);
    EXPECT_FALSE(entry.stateTruncated);

    // Only one of the two signals can be exercised on any given host, so the other is checked by
    // registration: whichever one this platform raises, the handler has to be on it.
    for (int signalNumber : { SIGSEGV, SIGBUS })
    {
        struct sigaction installed;
        ASSERT_EQ(sigaction(signalNumber, nullptr, &installed), 0);
        EXPECT_EQ(installed.sa_sigaction, &Wasm::Runtime::handleStateWriteFault)
            << "signal " << signalNumber << " is not routed to the state-write handler";
    }

    // An empty diff also means the protection never engaged, so the write was never tracked at all.
    ASSERT_FALSE(entry.stateDiff.empty());
    EXPECT_NE(entry.stateDiff[0].before, entry.stateDiff[0].after);

    munmap(state, pageSize);
}

#endif // !_WIN32
#endif // LITE_WASM_SC

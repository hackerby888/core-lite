#pragma once

// Linux-only copy-on-write snapshot of large consensus arrays, used to roll a tick back to its
// pre-tick state when an optimistic (AUX) solution acceptance disagrees with the quorum, so the tick
// can be re-run with full verification. Only pages actually written during the tick are saved
// (userfaultfd write-protect), so RAM cost is the tick's dirty set, not a full copy.
//
// Mechanism: registered regions are write-protected at arm(); the first write to each page faults,
// a dedicated reader thread copies the pre-write page into a demand-zero arena and clears the
// write-protect so the (kernel-blocked) writer resumes; rollback() copies saved pages back.

#ifdef __linux__
#include <linux/userfaultfd.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <cstdint>
#include <atomic>
#include <thread>
#include <cstring>

namespace tickRollback
{
    static constexpr unsigned long long PAGE_SIZE = 4096ULL;
    // Hard cap on saved dirty bytes per tick. Touched-page set of a solution tick is far below this;
    // a full spectrum reorg (~1 GB) still fits. Exceeding it sets overflow -> caller falls back.
    static constexpr unsigned long long ARENA_BUDGET = 2ULL << 30; // 2 GB virtual, demand-committed

    struct Region { unsigned char* base; unsigned long long len; };

    inline Region gRegions[2 + MAX_NUMBER_OF_CONTRACTS]; // spectrum, assets, minerSolutionFlags + per-contract
    inline int gNumRegions = 0;
    inline int gUffd = -1;
    inline bool gAvailable = false;          // uffd-WP usable on this kernel/build
    inline bool gWpUnpopulated = false;      // kernel tracks first-write to demand-zero pages (>=6.4)

    // Saved-page arena: parallel arrays addr[i] -> bytes[i*PAGE_SIZE]. Both demand-zero mmap.
    inline unsigned char** gSavedAddr = nullptr;
    inline unsigned char*   gSavedData = nullptr;
    inline unsigned long long gMaxPages = 0;
    inline std::atomic<unsigned long long> gSavedCount{0};
    inline std::atomic<bool> gArmed{false};
    inline std::atomic<bool> gOverflow{false};
    inline std::thread gReader;
    inline std::atomic<bool> gStopReader{false};

    static inline long uffd_syscall() { return syscall(__NR_userfaultfd, O_CLOEXEC | O_NONBLOCK); }

    // Reader thread: drains write-protect faults. Saves the pre-write page, then un-protects it.
    static inline void readerLoop()
    {
        struct pollfd pfd; pfd.fd = gUffd; pfd.events = POLLIN;
        while (!gStopReader.load(std::memory_order_relaxed))
        {
            int pr = poll(&pfd, 1, 200);
            if (pr <= 0) continue;
            struct uffd_msg msg;
            ssize_t n = read(gUffd, &msg, sizeof(msg));
            if (n != sizeof(msg)) continue;
            if (msg.event != UFFD_EVENT_PAGEFAULT) continue;
            unsigned char* pageAddr = (unsigned char*)(uintptr_t)(msg.arg.pagefault.address & ~(PAGE_SIZE - 1));

            if (gArmed.load(std::memory_order_acquire) && !gOverflow.load(std::memory_order_relaxed))
            {
                unsigned long long idx = gSavedCount.fetch_add(1, std::memory_order_acq_rel);
                if (idx < gMaxPages)
                {
                    // Copy the OLD page content before clearing WP (the writer is blocked until we do).
                    memcpy(gSavedData + idx * PAGE_SIZE, pageAddr, PAGE_SIZE);
                    gSavedAddr[idx] = pageAddr;
                }
                else
                {
                    gOverflow.store(true, std::memory_order_release);
                }
            }
            // Clear write-protect on this page so the faulting write proceeds.
            struct uffdio_writeprotect wp;
            wp.range.start = (uintptr_t)pageAddr;
            wp.range.len = PAGE_SIZE;
            wp.mode = 0; // un-protect
            ioctl(gUffd, UFFDIO_WRITEPROTECT, &wp);
        }
    }

    // Register a page-aligned, committed region for COW tracking. Call once per region at startup.
    static inline bool registerRegion(void* base, unsigned long long len)
    {
        if (gUffd < 0 || base == nullptr || len == 0) return false;
        if (((uintptr_t)base & (PAGE_SIZE - 1)) != 0) return false; // must be page-aligned
        unsigned long long alen = (len + PAGE_SIZE - 1) & ~(PAGE_SIZE - 1);
        struct uffdio_register reg;
        reg.range.start = (uintptr_t)base;
        reg.range.len = alen;
        reg.mode = UFFDIO_REGISTER_MODE_WP;
        if (ioctl(gUffd, UFFDIO_REGISTER, &reg) != 0) return false;
        if ((reg.ioctls & (1ULL << _UFFDIO_WRITEPROTECT)) == 0) return false; // WP not supported here
        if (gNumRegions >= (int)(sizeof(gRegions) / sizeof(gRegions[0]))) return false;
        gRegions[gNumRegions].base = (unsigned char*)base;
        gRegions[gNumRegions].len = alen;
        gNumRegions++;
        return true;
    }

    // Create the uffd, the arena, and the reader thread. Call once after the first region exists.
    static inline bool init()
    {
        gUffd = (int)uffd_syscall();
        if (gUffd < 0) return false;
        // WP_UNPOPULATED (kernel >=6.4) makes the first write to a demand-zero page fault too, so pages a tick
        // populates for the first time (spectrum reorg, a new account) are also tracked and rolled back. The
        // granted feature set is reported back in api.features; per-region WP support is checked in registerRegion.
        struct uffdio_api api; api.api = UFFD_API; api.features = 0;
#ifdef UFFD_FEATURE_WP_UNPOPULATED
        api.features |= UFFD_FEATURE_WP_UNPOPULATED;
#endif
        if (ioctl(gUffd, UFFDIO_API, &api) != 0)
        {
            // Kernel older than its headers: retry without the optional feature (COW is then disabled below).
            close(gUffd);
            gUffd = (int)uffd_syscall();
            if (gUffd < 0) return false;
            api.api = UFFD_API; api.features = 0;
            if (ioctl(gUffd, UFFDIO_API, &api) != 0) { close(gUffd); gUffd = -1; return false; }
        }
#ifdef UFFD_FEATURE_WP_UNPOPULATED
        gWpUnpopulated = (api.features & UFFD_FEATURE_WP_UNPOPULATED) != 0;
#endif

        gMaxPages = ARENA_BUDGET / PAGE_SIZE;
        gSavedData = (unsigned char*)mmap(nullptr, gMaxPages * PAGE_SIZE, PROT_READ | PROT_WRITE,
                                          MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        gSavedAddr = (unsigned char**)mmap(nullptr, gMaxPages * sizeof(unsigned char*), PROT_READ | PROT_WRITE,
                                           MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (gSavedData == MAP_FAILED || gSavedAddr == MAP_FAILED) { close(gUffd); gUffd = -1; return false; }

        // Require full tracking (incl. demand-zero pages) so rollback is correct in all cases; otherwise leave COW
        // disabled and the node falls back to permanent score verification (gTickForceVerify), which is always correct.
        if (!gWpUnpopulated) { munmap(gSavedData, gMaxPages * PAGE_SIZE); munmap(gSavedAddr, gMaxPages * sizeof(unsigned char*)); close(gUffd); gUffd = -1; return false; }

        gStopReader.store(false);
        gReader = std::thread(readerLoop);
        gAvailable = true;
        return true;
    }

    static inline void writeProtectAll(bool on)
    {
        for (int i = 0; i < gNumRegions; i++)
        {
            struct uffdio_writeprotect wp;
            wp.range.start = (uintptr_t)gRegions[i].base;
            wp.range.len = gRegions[i].len;
            wp.mode = on ? UFFDIO_WRITEPROTECT_MODE_WP : 0;
            ioctl(gUffd, UFFDIO_WRITEPROTECT, &wp);
        }
    }

    // Begin tracking writes for this tick. (Small/medium scalar state is snapshotted by the caller.)
    static inline void arm()
    {
        if (!gAvailable) return;
        gSavedCount.store(0, std::memory_order_relaxed);
        gOverflow.store(false, std::memory_order_relaxed);
        // Mark armed BEFORE protecting: nothing is write-protected yet, so no fault can occur until writeProtectAll
        // runs, and once it does the reader must already see gArmed==true or it would clear WP without saving the page.
        gArmed.store(true, std::memory_order_release);
        writeProtectAll(true);
    }

    static inline bool isArmed() { return gArmed.load(std::memory_order_acquire); }
    static inline bool overflowed() { return gOverflow.load(std::memory_order_acquire); }
    static inline unsigned long long savedPageCount() { return gSavedCount.load(std::memory_order_acquire); }
    static inline int regionCount() { return gNumRegions; }

    // Restore every dirtied page to its pre-tick content. Caller restores small/medium state separately.
    static inline void rollback()
    {
        if (!gAvailable) return;
        gArmed.store(false, std::memory_order_release);
        writeProtectAll(false); // make all writable again before restoring
        unsigned long long count = gSavedCount.load(std::memory_order_acquire);
        if (count > gMaxPages) count = gMaxPages;
        for (unsigned long long i = 0; i < count; i++)
        {
            if (gSavedAddr[i]) memcpy(gSavedAddr[i], gSavedData + i * PAGE_SIZE, PAGE_SIZE);
        }
        gSavedCount.store(0, std::memory_order_relaxed);
    }

    // Accept the tick: stop tracking, release saved-page RSS.
    static inline void commit()
    {
        if (!gAvailable) return;
        gArmed.store(false, std::memory_order_release);
        writeProtectAll(false);
        unsigned long long count = gSavedCount.load(std::memory_order_acquire);
        if (count > gMaxPages) count = gMaxPages;
        if (count) madvise(gSavedData, count * PAGE_SIZE, MADV_DONTNEED);
        gSavedCount.store(0, std::memory_order_relaxed);
    }
}
#endif // __linux__

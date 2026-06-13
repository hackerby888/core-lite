// Standalone equivalence test: KangarooTwelvePaged (page-aware, src/extensions/k12_paged.h) must produce
// byte-identical output to the canonical KangarooTwelve (src/kangaroo_twelve.h) over the same LOGICAL bytes,
// where reserved (never-committed) pages count as zeros. This is the consensus-safety proof for the Windows
// lazy-commit contract-state digest.
//
// Build (from repo root, in a VS x64 dev shell or via vcvars64):
//   cl /nologo /O2 /EHsc /DNOMINMAX /DNO_UEFI /I src /I . tools\k12paged_test.cpp /Fe:tools\k12paged_test.exe
#define NO_UEFI
#define NOMINMAX
#include <windows.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>

// platform/memory.h (NO_UEFI branch) only DECLARES these; provide trivial defs for the standalone TU.
void setMem(void* b, unsigned long long n, unsigned char v) { memset(b, v, (size_t)n); }
void copyMem(void* d, const void* s, unsigned long long n) { memcpy(d, (const void*)s, (size_t)n); }
bool allocatePool(unsigned long long, void**) { return false; }
void freePool(void*) {}

#include "kangaroo_twelve.h"
#include "extensions/k12_paged.h"

static unsigned int PS;   // page size

// Deterministic pseudo-random byte (no Math.random / time — reproducible).
static unsigned char prb(unsigned long long i) { unsigned long long x = i * 6364136223846793005ULL + 1442695040888963407ULL; return (unsigned char)(x >> 33); }

// One case: region of `size` bytes; `pattern` decides which pages are committed+written.
//   0 = none committed (all zero), 1 = all committed+written, 2 = first page only, 3 = scattered (every 7th page).
static int run_case(unsigned long long size, int pattern) {
    unsigned char* reserved = (unsigned char*)VirtualAlloc(NULL, (SIZE_T)size, MEM_RESERVE, PAGE_READWRITE);
    unsigned char* refbuf   = (unsigned char*)VirtualAlloc(NULL, (SIZE_T)size, MEM_RESERVE | MEM_COMMIT, PAGE_READWRITE); // zero-filled
    if (!reserved || !refbuf) { printf("  ALLOC FAIL size=%llu\n", size); return 1; }

    unsigned long long nPages = (size + PS - 1) / PS;
    for (unsigned long long pg = 0; pg < nPages; pg++) {
        bool commit = (pattern == 1) || (pattern == 2 && pg == 0) || (pattern == 3 && (pg % 7) == 0);
        if (!commit) continue;
        unsigned long long poff = pg * PS;
        unsigned long long plen = (poff + PS <= size) ? PS : (size - poff);
        VirtualAlloc(reserved + poff, (SIZE_T)plen, MEM_COMMIT, PAGE_READWRITE);
        for (unsigned long long j = 0; j < plen; j++) { unsigned char b = prb(poff + j); reserved[poff + j] = b; refbuf[poff + j] = b; }
    }

    unsigned char dp[32], dr[32];
    KangarooTwelvePaged(reserved, (unsigned int)size, dp, 32);
    KangarooTwelve(refbuf, (unsigned int)size, dr, 32);
    int ok = (memcmp(dp, dr, 32) == 0);
    printf("  size=%-11llu pattern=%d  %s\n", size, pattern, ok ? "OK" : "*** MISMATCH ***");
    if (!ok) {
        printf("    paged="); for (int i=0;i<32;i++) printf("%02x", dp[i]); printf("\n");
        printf("    plain="); for (int i=0;i<32;i++) printf("%02x", dr[i]); printf("\n");
    }
    VirtualFree(reserved, 0, MEM_RELEASE);
    VirtualFree(refbuf, 0, MEM_RELEASE);
    return ok ? 0 : 1;
}

int main() {
    SYSTEM_INFO si; GetSystemInfo(&si); PS = si.dwPageSize;
    printf("page size = %u\n", PS);
    unsigned long long sizes[] = {
        8, 168, 4096, 8192, 8193, 16384, 100000, 1000000,
        100ULL*1024*1024, 593ULL*1024*1024 /* QX-ish */, 1ULL<<30 /* 1 GB dyn slot */
    };
    int fails = 0;
    for (unsigned long long s : sizes) {
        // pattern 1 (all committed) for huge sizes would commit GBs — skip it for >100MB; still test 0/2/3.
        int patterns[] = { 0, 2, 3, 1 };
        for (int p : patterns) {
            if (p == 1 && s > 100ULL*1024*1024) continue;  // avoid committing GBs for the all-written case
            fails += run_case(s, p);
        }
    }
    printf(fails ? "\nRESULT: %d MISMATCH(es)\n" : "\nRESULT: all equivalent (paged == plain)\n", fails);
    return fails ? 1 : 0;
}

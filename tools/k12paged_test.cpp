// Compare page-aware K12 with the canonical implementation on Windows.
#define NO_UEFI
#define NOMINMAX
#include <windows.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>

void setMem(void* buffer, unsigned long long size, unsigned char value)
{
    memset(buffer, value, (size_t)size);
}

void copyMem(
    void* destination,
    const void* source,
    unsigned long long size)
{
    memcpy(destination, source, (size_t)size);
}

bool allocatePool(unsigned long long, void**)
{
    return false;
}

void freePool(void*)
{
}

#include "kangaroo_twelve.h"
#include "extensions/k12_paged.h"

static unsigned int pageSize;

static unsigned char pseudoRandomByte(unsigned long long index)
{
    const unsigned long long value =
        index * 6364136223846793005ULL + 1442695040888963407ULL;
    return (unsigned char)(value >> 33);
}

static int runCase(unsigned long long size, int pattern)
{
    unsigned char* reserved = (unsigned char*)VirtualAlloc(
        NULL,
        (SIZE_T)size,
        MEM_RESERVE,
        PAGE_READWRITE);
    unsigned char* reference = (unsigned char*)VirtualAlloc(
        NULL,
        (SIZE_T)size,
        MEM_RESERVE | MEM_COMMIT,
        PAGE_READWRITE);
    if (!reserved || !reference)
    {
        printf("  ALLOC FAIL size=%llu\n", size);
        return 1;
    }

    const unsigned long long pageCount =
        (size + pageSize - 1) / pageSize;
    for (unsigned long long page = 0; page < pageCount; page++)
    {
        const bool shouldCommit = pattern == 1
            || (pattern == 2 && page == 0)
            || (pattern == 3 && page % 7 == 0);
        if (!shouldCommit)
        {
            continue;
        }

        const unsigned long long offset = page * pageSize;
        const unsigned long long length = offset + pageSize <= size
            ? pageSize
            : size - offset;
        VirtualAlloc(
            reserved + offset,
            (SIZE_T)length,
            MEM_COMMIT,
            PAGE_READWRITE);
        for (unsigned long long i = 0; i < length; i++)
        {
            const unsigned char value = pseudoRandomByte(offset + i);
            reserved[offset + i] = value;
            reference[offset + i] = value;
        }
    }

    unsigned char pagedDigest[32];
    unsigned char referenceDigest[32];
    KangarooTwelvePaged(reserved, (unsigned int)size, pagedDigest, 32);
    KangarooTwelve(reference, (unsigned int)size, referenceDigest, 32);
    const bool matches = memcmp(pagedDigest, referenceDigest, 32) == 0;
    printf(
        "  size=%-11llu pattern=%d  %s\n",
        size,
        pattern,
        matches ? "OK" : "*** MISMATCH ***");
    if (!matches)
    {
        printf("    paged=");
        for (int i = 0; i < 32; i++)
        {
            printf("%02x", pagedDigest[i]);
        }
        printf("\n");
        printf("    plain=");
        for (int i = 0; i < 32; i++)
        {
            printf("%02x", referenceDigest[i]);
        }
        printf("\n");
    }
    VirtualFree(reserved, 0, MEM_RELEASE);
    VirtualFree(reference, 0, MEM_RELEASE);
    return matches ? 0 : 1;
}

int main()
{
    SYSTEM_INFO systemInfo;
    GetSystemInfo(&systemInfo);
    pageSize = systemInfo.dwPageSize;
    printf("page size = %u\n", pageSize);
    unsigned long long sizes[] = {
        8, 168, 4096, 8192, 8193, 16384, 100000, 1000000,
        100ULL * 1024 * 1024,
        593ULL * 1024 * 1024,
        1ULL << 30,
    };
    int fails = 0;
    for (unsigned long long size : sizes)
    {
        int patterns[] = { 0, 2, 3, 1 };
        for (int pattern : patterns)
        {
            if (pattern == 1 && size > 100ULL * 1024 * 1024)
            {
                continue;
            }
            fails += runCase(size, pattern);
        }
    }
    printf(
        fails
            ? "\nRESULT: %d MISMATCH(es)\n"
            : "\nRESULT: all equivalent (paged == plain)\n",
        fails);
    return fails ? 1 : 0;
}

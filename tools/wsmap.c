// wsmap — per-region working-set map of a process. Diagnostic for the Windows port's RAM usage
// (WINDOWS_PORT.md "Known issues"): attributes resident pages to VirtualAlloc regions so the
// page-toucher can be identified. Usage: wsmap <pid> [minMB]
#include <windows.h>
#include <psapi.h>
#include <stdio.h>
#include <stdlib.h>

#pragma comment(lib, "psapi.lib")

int main(int argc, char** argv)
{
    if (argc < 2) { printf("usage: wsmap <pid> [minMB]\n"); return 1; }
    DWORD pid = (DWORD)atoi(argv[1]);
    SIZE_T minBytes = (argc > 2 ? (SIZE_T)atoll(argv[2]) : 50) * 1024 * 1024;

    HANDLE h = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, pid);
    if (!h) { printf("OpenProcess failed: %lu\n", GetLastError()); return 1; }

    SYSTEM_INFO si; GetSystemInfo(&si);
    const SIZE_T page = si.dwPageSize;
    const SIZE_T BATCH = 65536; // pages per QueryWorkingSetEx call (64K * 16B = 1MB scratch)
    PSAPI_WORKING_SET_EX_INFORMATION* wsx =
        (PSAPI_WORKING_SET_EX_INFORMATION*)malloc(BATCH * sizeof(*wsx));

    unsigned char* addr = 0;
    MEMORY_BASIC_INFORMATION mbi;
    unsigned long long totCommit = 0, totResident = 0;
    printf("%-16s %10s %10s %10s  %s\n", "base", "size MB", "commit MB", "ws MB", "type/protect");
    // group by allocation base: regions sharing an AllocationBase are one VirtualAlloc reserve
    unsigned char* curAllocBase = 0;
    unsigned long long grpSize = 0, grpCommit = 0, grpResident = 0;
    DWORD grpProtect = 0, grpType = 0;
    for (;;)
    {
        SIZE_T got = VirtualQueryEx(h, addr, &mbi, sizeof(mbi));
        int done = (got == 0);
        if (!done && mbi.State == MEM_FREE) { addr = (unsigned char*)mbi.BaseAddress + mbi.RegionSize; continue; }

        if (done || (unsigned char*)mbi.AllocationBase != curAllocBase)
        {
            if (curAllocBase && grpSize >= minBytes)
                printf("%-16p %10llu %10llu %10llu  type=%lx prot=%lx\n", curAllocBase,
                       grpSize >> 20, grpCommit >> 20, grpResident >> 20, grpType, grpProtect);
            if (done) break;
            curAllocBase = (unsigned char*)mbi.AllocationBase;
            grpSize = grpCommit = grpResident = 0;
            grpType = mbi.Type; grpProtect = mbi.AllocationProtect;
        }

        grpSize += mbi.RegionSize;
        if (mbi.State == MEM_COMMIT)
        {
            grpCommit += mbi.RegionSize; totCommit += mbi.RegionSize;
            // count resident pages in batches
            SIZE_T pages = mbi.RegionSize / page;
            unsigned char* p = (unsigned char*)mbi.BaseAddress;
            while (pages)
            {
                SIZE_T n = pages > BATCH ? BATCH : pages;
                for (SIZE_T i = 0; i < n; i++) wsx[i].VirtualAddress = p + i * page;
                if (QueryWorkingSetEx(h, wsx, (DWORD)(n * sizeof(*wsx))))
                    for (SIZE_T i = 0; i < n; i++)
                        if (wsx[i].VirtualAttributes.Valid) { grpResident += page; totResident += page; }
                p += n * page; pages -= n;
            }
        }
        addr = (unsigned char*)mbi.BaseAddress + mbi.RegionSize;
    }
    printf("TOTAL commit=%llu MB resident=%llu MB\n", totCommit >> 20, totResident >> 20);
    return 0;
}

// Print a process working-set map grouped by VirtualAlloc reservation.
#include <windows.h>
#include <psapi.h>
#include <stdio.h>
#include <stdlib.h>

#pragma comment(lib, "psapi.lib")

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        printf("usage: wsmap <pid> [minMB]\n");
        return 1;
    }
    const DWORD processId = (DWORD)atoi(argv[1]);
    const SIZE_T minimumBytes = (argc > 2 ? (SIZE_T)atoll(argv[2]) : 50) * 1024 * 1024;

    HANDLE process = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, processId);
    if (!process)
    {
        printf("OpenProcess failed: %lu\n", GetLastError());
        return 1;
    }

    SYSTEM_INFO systemInfo;
    GetSystemInfo(&systemInfo);
    const SIZE_T pageSize = systemInfo.dwPageSize;
    const SIZE_T batchSize = 65536;
    PSAPI_WORKING_SET_EX_INFORMATION* workingSet = (PSAPI_WORKING_SET_EX_INFORMATION*)malloc(batchSize * sizeof(*workingSet));

    unsigned char* address = 0;
    MEMORY_BASIC_INFORMATION memoryInfo;
    unsigned long long totalCommit = 0;
    unsigned long long totalResident = 0;
    printf(
        "%-16s %10s %10s %10s  %s\n",
        "base",
        "size MB",
        "commit MB",
        "ws MB",
        "type/protect");

    unsigned char* currentAllocationBase = 0;
    unsigned long long groupSize = 0;
    unsigned long long groupCommit = 0;
    unsigned long long groupResident = 0;
    DWORD groupProtect = 0;
    DWORD groupType = 0;
    for (;;)
    {
        const SIZE_T queried = VirtualQueryEx(process, address, &memoryInfo, sizeof(memoryInfo));
        const int done = queried == 0;
        if (!done && memoryInfo.State == MEM_FREE)
        {
            address = (unsigned char*)memoryInfo.BaseAddress + memoryInfo.RegionSize;
            continue;
        }

        if (done || (unsigned char*)memoryInfo.AllocationBase != currentAllocationBase)
        {
            if (currentAllocationBase && groupSize >= minimumBytes)
            {
                printf(
                    "%-16p %10llu %10llu %10llu  type=%lx prot=%lx\n",
                    currentAllocationBase,
                    groupSize >> 20,
                    groupCommit >> 20,
                    groupResident >> 20,
                    groupType,
                    groupProtect);
            }
            if (done)
            {
                break;
            }
            currentAllocationBase = (unsigned char*)memoryInfo.AllocationBase;
            groupSize = 0;
            groupCommit = 0;
            groupResident = 0;
            groupType = memoryInfo.Type;
            groupProtect = memoryInfo.AllocationProtect;
        }

        groupSize += memoryInfo.RegionSize;
        if (memoryInfo.State == MEM_COMMIT)
        {
            groupCommit += memoryInfo.RegionSize;
            totalCommit += memoryInfo.RegionSize;
            SIZE_T pages = memoryInfo.RegionSize / pageSize;
            unsigned char* pageAddress = (unsigned char*)memoryInfo.BaseAddress;
            while (pages)
            {
                const SIZE_T count = pages > batchSize ? batchSize : pages;
                for (SIZE_T i = 0; i < count; i++)
                {
                    workingSet[i].VirtualAddress = pageAddress + i * pageSize;
                }
                if (QueryWorkingSetEx(process, workingSet, (DWORD)(count * sizeof(*workingSet))))
                {
                    for (SIZE_T i = 0; i < count; i++)
                    {
                        if (workingSet[i].VirtualAttributes.Valid)
                        {
                            groupResident += pageSize;
                            totalResident += pageSize;
                        }
                    }
                }
                pageAddress += count * pageSize;
                pages -= count;
            }
        }
        address = (unsigned char*)memoryInfo.BaseAddress + memoryInfo.RegionSize;
    }
    printf("TOTAL commit=%llu MB resident=%llu MB\n", totalCommit >> 20, totalResident >> 20);
    return 0;
}

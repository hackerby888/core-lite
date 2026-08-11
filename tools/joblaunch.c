// Launch a child in a Windows Job Object with a fixed commit limit.
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char** argv)
{
    if (argc < 4)
    {
        fprintf(stderr, "usage: joblaunch <commitLimitMB> <logfile> <exe> [args...]\n");
        return 2;
    }
    const unsigned long long limitMB = _strtoui64(argv[1], NULL, 10);
    const char* logFile = argv[2];

    HANDLE job = CreateJobObjectW(NULL, NULL);
    if (!job)
    {
        fprintf(stderr, "CreateJobObject failed %lu\n", GetLastError());
        return 1;
    }

    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limitInfo;
    ZeroMemory(&limitInfo, sizeof(limitInfo));
    limitInfo.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_JOB_MEMORY;
    limitInfo.JobMemoryLimit = (SIZE_T)(limitMB * 1024ULL * 1024ULL);
    if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation, &limitInfo, sizeof(limitInfo)))
    {
        fprintf(stderr, "SetInformationJobObject failed %lu\n", GetLastError());
        return 1;
    }

    size_t commandLength = 1;
    for (int i = 3; i < argc; i++)
    {
        commandLength += strlen(argv[i]) + 3;
    }
    char* command = (char*)malloc(commandLength + 1);
    command[0] = 0;
    for (int i = 3; i < argc; i++)
    {
        strcat(command, "\"");
        strcat(command, argv[i]);
        strcat(command, "\" ");
    }

    SECURITY_ATTRIBUTES securityAttributes;
    ZeroMemory(&securityAttributes, sizeof(securityAttributes));
    securityAttributes.nLength = sizeof(securityAttributes);
    securityAttributes.bInheritHandle = TRUE;
    HANDLE logHandle = CreateFileA(
        logFile,
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        &securityAttributes,
        CREATE_ALWAYS,
        FILE_ATTRIBUTE_NORMAL,
        NULL);
    if (logHandle == INVALID_HANDLE_VALUE)
    {
        fprintf(stderr, "open log failed %lu\n", GetLastError());
        return 1;
    }

    STARTUPINFOA startupInfo;
    ZeroMemory(&startupInfo, sizeof(startupInfo));
    startupInfo.cb = sizeof(startupInfo);
    startupInfo.dwFlags = STARTF_USESTDHANDLES;
    startupInfo.hStdOutput = logHandle;
    startupInfo.hStdError = logHandle;
    startupInfo.hStdInput = NULL;

    PROCESS_INFORMATION processInfo;
    ZeroMemory(&processInfo, sizeof(processInfo));
    if (!CreateProcessA(
            NULL,
            command,
            NULL,
            NULL,
            TRUE,
            CREATE_SUSPENDED,
            NULL,
            NULL,
            &startupInfo,
            &processInfo))
    {
        fprintf(stderr, "CreateProcess failed %lu\n", GetLastError());
        return 1;
    }
    if (!AssignProcessToJobObject(job, processInfo.hProcess))
    {
        fprintf(stderr, "AssignProcessToJobObject failed %lu\n", GetLastError());
        TerminateProcess(processInfo.hProcess, 1);
        return 1;
    }
    ResumeThread(processInfo.hThread);
    printf("JOBLAUNCH pid=%lu limitMB=%llu\n", processInfo.dwProcessId, limitMB);
    fflush(stdout);
    WaitForSingleObject(processInfo.hProcess, INFINITE);
    DWORD exitCode = 0;
    GetExitCodeProcess(processInfo.hProcess, &exitCode);
    printf("JOBLAUNCH child exited code=%lu\n", exitCode);
    fflush(stdout);
    return 0;
}

// joblaunch.c — launch a child process inside a Job Object with a hard COMMIT (job-memory) cap.
// Simulates a low-commit-limit host (e.g. the 16 GB windows-latest CI VM) on a big-RAM dev box, so the
// Windows tick-stall (commit exhaustion when a deploy arms a WAMR contract) reproduces locally.
//
//   joblaunch <commitLimitMB> <logfile> <exe> [args...]
//
// The child is created SUSPENDED and assigned to the job BEFORE it runs, so the cap applies to every
// allocation including boot. stdout+stderr go to <logfile>. The launcher holds the job handle and waits
// for the child (run it in the background; kill the child to make it exit).
#include <windows.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char** argv) {
    if (argc < 4) { fprintf(stderr, "usage: joblaunch <commitLimitMB> <logfile> <exe> [args...]\n"); return 2; }
    unsigned long long limitMB = _strtoui64(argv[1], NULL, 10);
    const char* logfile = argv[2];

    HANDLE job = CreateJobObjectW(NULL, NULL);
    if (!job) { fprintf(stderr, "CreateJobObject failed %lu\n", GetLastError()); return 1; }

    JOBOBJECT_EXTENDED_LIMIT_INFORMATION eli;
    ZeroMemory(&eli, sizeof(eli));
    eli.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_JOB_MEMORY;
    eli.JobMemoryLimit = (SIZE_T)(limitMB * 1024ULL * 1024ULL);
    if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation, &eli, sizeof(eli))) {
        fprintf(stderr, "SetInformationJobObject failed %lu\n", GetLastError()); return 1;
    }

    // command line: "exe" "arg1" "arg2" ...
    size_t len = 1;
    for (int i = 3; i < argc; i++) len += strlen(argv[i]) + 3;
    char* cmd = (char*)malloc(len + 1); cmd[0] = 0;
    for (int i = 3; i < argc; i++) { strcat(cmd, "\""); strcat(cmd, argv[i]); strcat(cmd, "\" "); }

    SECURITY_ATTRIBUTES sa; ZeroMemory(&sa, sizeof(sa)); sa.nLength = sizeof(sa); sa.bInheritHandle = TRUE;
    HANDLE hlog = CreateFileA(logfile, GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, &sa,
                              CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
    if (hlog == INVALID_HANDLE_VALUE) { fprintf(stderr, "open log failed %lu\n", GetLastError()); return 1; }

    STARTUPINFOA si; ZeroMemory(&si, sizeof(si)); si.cb = sizeof(si);
    si.dwFlags = STARTF_USESTDHANDLES;
    si.hStdOutput = hlog; si.hStdError = hlog; si.hStdInput = NULL;
    PROCESS_INFORMATION pi; ZeroMemory(&pi, sizeof(pi));
    if (!CreateProcessA(NULL, cmd, NULL, NULL, TRUE, CREATE_SUSPENDED, NULL, NULL, &si, &pi)) {
        fprintf(stderr, "CreateProcess failed %lu\n", GetLastError()); return 1;
    }
    if (!AssignProcessToJobObject(job, pi.hProcess)) {
        fprintf(stderr, "AssignProcessToJobObject failed %lu\n", GetLastError());
        TerminateProcess(pi.hProcess, 1); return 1;
    }
    ResumeThread(pi.hThread);
    printf("JOBLAUNCH pid=%lu limitMB=%llu\n", pi.dwProcessId, limitMB); fflush(stdout);
    WaitForSingleObject(pi.hProcess, INFINITE);
    DWORD code = 0; GetExitCodeProcess(pi.hProcess, &code);
    printf("JOBLAUNCH child exited code=%lu\n", code); fflush(stdout);
    return 0;
}

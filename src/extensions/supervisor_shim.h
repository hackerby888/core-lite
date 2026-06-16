#pragma once

// Supervisor shim: a tiny subreaper parent that keeps a stable PID across fork-rollback promotes
// (the promoted child reparents to it, not init). By default it also forks the RPC proxy as a sibling
// that survives promotes + restarts it on death. Linux-only; opt out QUBIC_NO_SUPERVISOR=1 (no shim)
// or --rpc-inprocess (shim, but RPC served in-process).

#ifdef __linux__

#include <sys/prctl.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <string>

inline char gSidecarPort[16] = "41841";   // node http port -> sidecar listen + unix-socket key

// Forward a stop signal to the children so the container/service stops promptly.
static void shimForwardSignal(int sig)
{
    signal(sig, SIG_IGN);
    kill(0, sig);   // node + donors + sidecar share our process group
}

// Re-exec self as the stateless RPC proxy (a sibling of the node).
static pid_t shimForkSidecar()
{
    pid_t p = fork();
    if (p != 0) return p;                 // shim: child pid (or -1)
    char self[512];
    ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
    if (n <= 0) _exit(127);
    self[n] = 0;
    execl(self, "qubic-rpc-sidecar", "--rpc-proxy",
          "--rpc-listen", gSidecarPort, "--rpc-node", gSidecarPort, (char*)nullptr);
    _exit(127);                           // execl failed
}

// True while any child other than the sidecar exists (i.e. the node lineage is still alive).
static bool shimHasNodeChild(pid_t sidecar)
{
    char path[64];
    snprintf(path, sizeof(path), "/proc/self/task/%d/children", (int)getpid());
    FILE* f = fopen(path, "r");
    if (!f) return true;                  // can't tell -> assume yes (never exit prematurely)
    int c;
    bool any = false;
    while (fscanf(f, "%d", &c) == 1)
        if (c != (int)sidecar) { any = true; break; }
    fclose(f);
    return any;
}

// Returns ONLY in the node child. The supervisor parent loops here and _exit()s when the node drains.
static inline void runUnderSupervisor(int argc, const char** argv)
{
    // No shim -> no sidecar process; tell main() to serve RPC in-process (dev / screen).
    if (getenv("QUBIC_NO_SUPERVISOR")) { setenv("QUBIC_RPC_INPROCESS", "1", 1); return; }

    bool wantSidecar = true;   // sidecar is the default; --rpc-inprocess opts back to in-process drogon
    for (int i = 1; i < argc; i++)
    {
        if (std::string(argv[i]) == "--rpc-inprocess") wantSidecar = false;
        else if (std::string(argv[i]) == "--http-port" && i + 1 < argc)
        {
            std::strncpy(gSidecarPort, argv[i + 1], sizeof(gSidecarPort) - 1);
            gSidecarPort[sizeof(gSidecarPort) - 1] = 0;
        }
    }
#ifdef NO_RPC
    wantSidecar = false;
#endif
    // Every path that does not fork a sidecar must hand RPC back to the in-process server.
    if (!wantSidecar) setenv("QUBIC_RPC_INPROCESS", "1", 1);

    if (prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0)   // can't subreap: run the node inline
    {
        setenv("QUBIC_RPC_INPROCESS", "1", 1);
        return;
    }

    pid_t sidecar = wantSidecar ? shimForkSidecar() : -1;

    pid_t node = fork();
    if (node < 0)                                         // fork failed: run the node inline
    {
        setenv("QUBIC_RPC_INPROCESS", "1", 1);
        return;
    }
    if (node == 0) return;                                // CHILD: become the node

    // SUPERVISOR (stable PID). Reap everything (PID-1 duty under docker) + forward stop signals.
    signal(SIGTERM, shimForwardSignal);
    signal(SIGINT, shimForwardSignal);

    int lastSt = 0;
    bool sawSignal = false;
    for (;;)
    {
        int st = 0;
        pid_t dead = waitpid(-1, &st, 0);
        if (dead < 0)
        {
            if (errno == EINTR) continue;
            break;                                        // ECHILD: nothing left
        }
        if (wantSidecar && dead == sidecar)
        {
            sidecar = shimForkSidecar();                  // RPC must not stay down: restart it
            continue;
        }
        // a node-lineage generation ended; a promoted successor (if any) has reparented to us.
        lastSt = st;
        if (WIFSIGNALED(st)) sawSignal = true;
        if (!shimHasNodeChild(sidecar)) break;            // node lineage drained -> shim exits
    }
    if (wantSidecar && sidecar > 0) kill(sidecar, SIGTERM);
    _exit(sawSignal ? 1 : (WIFEXITED(lastSt) ? WEXITSTATUS(lastSt) : 1));
}

#else
static inline void runUnderSupervisor(int, const char**) {}   // non-Linux: no fork rollback, no shim
#endif

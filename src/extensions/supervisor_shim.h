#pragma once

// Supervisor shim: keep the supervisor-facing PID stable across fork-rollback promotes.
//
// The fork rollback promotes by replacing the process (the old node _exit()s, the forked child
// takes over), so the PID changes on every mismatch. Under docker (the node as PID 1) or systemd
// Restart=, that looks like the service died -> PID-namespace teardown kills the promoted child, or
// a duplicate node is restarted on the inherited port. This shim is a tiny non-consensus parent
// that owns the stable PID: it forks the node, becomes a child-subreaper so a promoted grandchild
// reparents to IT (not init), and keeps a node alive in its process tree until the tree drains,
// then exits with the last node's fate so the supervisor sees the real outcome.
//
// Linux-only (prctl subreaper). Opt out with QUBIC_NO_SUPERVISOR=1 (e.g. under screen / dev).

#ifdef __linux__

#include <sys/prctl.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#include <cstdlib>
#include <cerrno>

// Forward a stop signal to the node generation so the container/service stops promptly. We ignore
// the signal for ourselves first so kill(0, ...) (whole process group) does not re-enter the shim.
static void shimForwardSignal(int sig)
{
    signal(sig, SIG_IGN);
    kill(0, sig);   // the active node + any donor children share our process group
}

// Returns ONLY in the node child. The supervisor parent loops here and _exit()s when the tree drains.
static inline void runUnderSupervisor()
{
    if (getenv("QUBIC_NO_SUPERVISOR")) return;          // opt-out: run the node inline (screen / dev)
    if (prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0) return;  // can't subreap: run inline, don't risk it

    pid_t node = fork();
    if (node < 0) return;                               // fork failed: run the node inline
    if (node == 0) return;                              // CHILD: become the node

    // SUPERVISOR (stable PID). Reap everything (PID-1 duty under docker) + forward stop signals.
    signal(SIGTERM, shimForwardSignal);
    signal(SIGINT, shimForwardSignal);

    // A promote = the current node _exit()s and its child reparents to us (subreaper). Keep waiting
    // until the whole tree drains; remember the last fate (and whether anything crashed) to propagate.
    int lastSt = 0;
    bool sawSignal = false;
    for (;;)
    {
        int st = 0;
        pid_t dead = waitpid(-1, &st, 0);
        if (dead < 0)
        {
            if (errno == EINTR) continue;               // our signal handler interrupted: retry
            break;                                      // ECHILD: no node left in the tree
        }
        lastSt = st;
        if (WIFSIGNALED(st)) sawSignal = true;          // a generation crashed somewhere in the chain
    }
    _exit(sawSignal ? 1 : (WIFEXITED(lastSt) ? WEXITSTATUS(lastSt) : 1));
}

#else
static inline void runUnderSupervisor() {}              // non-Linux: no fork rollback, no shim
#endif

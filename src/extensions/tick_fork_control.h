#pragma once

#ifdef __linux__

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

namespace tickForkControl
{
    inline constexpr char promoteTag = 'P';
    inline constexpr char retireTag = 'R';

    enum class ChildAction
    {
        Promote,
        Retire,
    };

    struct ChildCommand
    {
        ChildAction action;
        unsigned int targetTick;
    };

    // Accepted node-side RPC sockets retain the listener path after fork.
    inline unsigned int closeInheritedRpcUnixSocketsForPromote(
        int listenerFd,
        const char* rpcPath)
    {
        unsigned int closedCount = 0;
        if (listenerFd >= 0 && close(listenerFd) == 0)
        {
            closedCount++;
        }

        DIR* directory = opendir("/proc/self/fd");
        if (!directory)
        {
            const int error = errno;
            fprintf(stderr,
                    "[RPC] promote opendir failed: errno=%d (%s)\n",
                    error,
                    strerror(error));
            fflush(stderr);
            return closedCount;
        }

        const int directoryFd = dirfd(directory);
        for (;;)
        {
            errno = 0;
            dirent* entry = readdir(directory);
            if (!entry)
            {
                const int error = errno;
                closedir(directory);
                if (error)
                {
                    fprintf(stderr,
                            "[RPC] promote readdir failed: errno=%d (%s)\n",
                            error,
                            strerror(error));
                }
                fprintf(stderr,
                        "[RPC] promote closed %u inherited RPC AF_UNIX fd(s)\n",
                        closedCount);
                fflush(stderr);
                return closedCount;
            }

            const int fd = atoi(entry->d_name);
            if (fd <= STDERR_FILENO || fd == directoryFd || fd == listenerFd)
            {
                continue;
            }

            int domain = 0;
            socklen_t domainSize = sizeof(domain);
            if (getsockopt(fd, SOL_SOCKET, SO_DOMAIN, &domain, &domainSize) != 0
                || domain != AF_UNIX)
            {
                continue;
            }

            sockaddr_un address{};
            socklen_t addressSize = sizeof(address);
            if (getsockname(fd, (sockaddr*)&address, &addressSize) != 0
                || strncmp(address.sun_path, rpcPath, sizeof(address.sun_path)) != 0)
            {
                continue;
            }

            if (close(fd) == 0)
            {
                closedCount++;
            }
        }
    }

    inline ssize_t readRetryOnInterrupt(int pipeFd, void* buffer, size_t size)
    {
        ssize_t readSize;
        do
        {
            readSize = read(pipeFd, buffer, size);
        }
        while (readSize < 0 && errno == EINTR);
        return readSize;
    }

    inline bool writeRetireCommand(int pipeFd)
    {
        ssize_t writeSize;
        do
        {
            writeSize = write(pipeFd, &retireTag, 1);
        }
        while (writeSize < 0 && errno == EINTR);
        return writeSize == 1;
    }

    inline ChildCommand readChildCommand(int pipeFd, unsigned int crashTargetTick)
    {
        char tag = 0;
        unsigned int targetTick = 0;
        const ssize_t readSize = readRetryOnInterrupt(pipeFd, &tag, 1);

        if (readSize == 1 && tag == retireTag)
            return { ChildAction::Retire, 0 };

        if (readSize == 1 && tag == promoteTag)
        {
            if (readRetryOnInterrupt(pipeFd, &targetTick, sizeof(targetTick)) != (ssize_t)sizeof(targetTick))
                targetTick = 0;
        }

        // EOF, a short frame, or an unknown tag means the parent failed unexpectedly.
        if (targetTick == 0)
            targetTick = crashTargetTick;
        return { ChildAction::Promote, targetTick };
    }
}

#endif

#pragma once

#ifdef __linux__

#include <cerrno>
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

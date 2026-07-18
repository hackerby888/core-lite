#pragma once

#ifdef NO_UEFI

#include <cerrno>
#include <cstdio>
#include <filesystem>

enum class HostFileMode : unsigned char
{
    ReadBinary,
    WriteBinary,
};

[[nodiscard]] inline int openHostFile(FILE** output, const std::filesystem::path& path, HostFileMode mode)
{
    if (!output)
    {
        return EINVAL;
    }
    *output = nullptr;

    if (path.empty())
    {
        return EINVAL;
    }

#ifdef _WIN32
    const wchar_t* modeString = nullptr;
#else
    const char* modeString = nullptr;
#endif
    switch (mode)
    {
    case HostFileMode::ReadBinary:
#ifdef _WIN32
        modeString = L"rb";
#else
        modeString = "rb";
#endif
        break;
    case HostFileMode::WriteBinary:
#ifdef _WIN32
        modeString = L"wb";
#else
        modeString = "wb";
#endif
        break;
    default:
        return EINVAL;
    }

#if defined(_WIN32) && defined(_MSC_VER)
    errno = 0;
    const int error = _wfopen_s(output, path.c_str(), modeString);
    if (error)
    {
        return error;
    }
#else
    errno = 0;
#ifdef _WIN32
    *output = _wfopen(path.c_str(), modeString);
#else
    *output = std::fopen(path.c_str(), modeString);
#endif
#endif

    if (!*output)
    {
        return errno ? errno : EIO;
    }
    return 0;
}

#endif

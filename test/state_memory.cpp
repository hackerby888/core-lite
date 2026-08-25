#include <cstring>

void setMem(void* buffer, unsigned long long size, unsigned char value)
{
    std::memset(buffer, value, size);
}

void copyMem(void* destination, const void* source, unsigned long long length)
{
    std::memcpy(destination, source, length);
}

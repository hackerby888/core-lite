#ifdef _WIN32

void* qVirtualAlloc(const unsigned long long size, bool commitMem);

void* qVirtualAllocLazy(const unsigned long long size)
{
    return qVirtualAlloc(size, true);
}

#endif

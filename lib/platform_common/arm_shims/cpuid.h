#pragma once

// ARM has no x86 CPUID, so callers must use their fallback path.
#if !defined(__x86_64__) && !defined(__i386__)
static inline int __get_cpuid(
    unsigned int leaf,
    unsigned int* a,
    unsigned int* b,
    unsigned int* c,
    unsigned int* d)
{
    (void)leaf;
    if (a)
    {
        *a = 0;
    }
    if (b)
    {
        *b = 0;
    }
    if (c)
    {
        *c = 0;
    }
    if (d)
    {
        *d = 0;
    }
    return 0;
}
#else
#include_next <cpuid.h>
#endif

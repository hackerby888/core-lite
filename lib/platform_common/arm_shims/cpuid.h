#pragma once
// arm shim for the x86-only <cpuid.h>. No CPUID on arm -> __get_cpuid fails (returns 0) so callers
// fall back to the non-CPUID path (TSC frequency via __rdtsc). __cpuid (MSVC int[4] form) is provided
// by qintrin.h. This header only exists to satisfy `#include <cpuid.h>` and __get_cpuid on arm.
#if !defined(__x86_64__) && !defined(__i386__)
static inline int __get_cpuid(unsigned int leaf, unsigned int* a, unsigned int* b, unsigned int* c, unsigned int* d)
{
    (void)leaf;
    if (a) *a = 0; if (b) *b = 0; if (c) *c = 0; if (d) *d = 0;
    return 0; // no CPUID on arm -> signal failure -> caller uses fallback
}
#else
#include_next <cpuid.h>
#endif

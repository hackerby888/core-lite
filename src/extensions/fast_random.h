#pragma once

#include <lib/platform_common/qintrin.h>

// Ordering/fairness only (response shuffles). Keys, nonces and anything consensus-visible stay on random()/RDRAND.
inline unsigned int randomFast(unsigned int range)
{
    static thread_local unsigned long long state = 0;
    if (state == 0)
    {
        _rdrand64_step(&state);
        state |= 1;
    }
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    return (unsigned int)((state * 0x2545F4914F6CDD1DULL) >> 32) % range;
}

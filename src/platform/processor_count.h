#pragma once

#include <thread>

// The main thread pins itself to a single CPU during startup (see Overload::initializeUefi), and on
// builds whose libc derives the processor count from the affinity mask rather than from sysfs, every
// later std::thread::hardware_concurrency() then answers 1 — which is enough to make the node refuse to
// start ("At least 4 healthy enabled processors are required"). Take the count once during static
// initialisation, before any affinity is set, and answer from it everywhere.
inline const unsigned int gTotalProcessorCount = std::thread::hardware_concurrency();

inline unsigned int totalProcessorCount()
{
    return gTotalProcessorCount;
}

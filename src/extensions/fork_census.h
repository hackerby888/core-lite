#pragma once

// Census-aware mutex wrappers (tick fork-rollback). A std::mutex held by a non-AP thread would be
// inherited locked by the forked child; declaring it SmartMutex/SmartSharedMutex funnels it through the
// same census as the ACQUIRE/RELEASE spin-locks so bspForkPoint's gate sees it too -> no hand lock list.

#include <mutex>
#include <shared_mutex>
#include "platform/concurrency.h"   // forkCensusEnter/forkCensusLeave

struct SmartMutex
{
    std::mutex m;   // SMARTMUTEX-EXEMPT: wrapper internal (this IS the census-aware wrapper)
    const char* nm;
    explicit SmartMutex(const char* name = "SmartMutex") : nm(name) {}

    void lock()     { m.lock(); forkCensusEnter(nm); }
    bool try_lock() { if (!m.try_lock()) return false; forkCensusEnter(nm); return true; }
    void unlock()   { forkCensusLeave(); m.unlock(); }
};

struct SmartSharedMutex
{
    std::shared_mutex m;   // SMARTMUTEX-EXEMPT: wrapper internal (this IS the census-aware wrapper)
    const char* nm;
    explicit SmartSharedMutex(const char* name = "SmartSharedMutex") : nm(name) {}

    void lock()            { m.lock(); forkCensusEnter(nm); }
    bool try_lock()        { if (!m.try_lock()) return false; forkCensusEnter(nm); return true; }
    void unlock()          { forkCensusLeave(); m.unlock(); }

    void lock_shared()     { m.lock_shared(); forkCensusEnter(nm); }
    bool try_lock_shared() { if (!m.try_lock_shared()) return false; forkCensusEnter(nm); return true; }
    void unlock_shared()   { forkCensusLeave(); m.unlock_shared(); }
};

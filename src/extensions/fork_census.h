#pragma once

// SmartMutex/SmartSharedMutex: funnel std::mutex through the fork census so bspForkPoint's gate
// sees them — no hand-maintained lock list. A held mutex inherited by the child would deadlock.

#include <mutex>
#include <shared_mutex>
#include "platform/concurrency.h"   // forkCensusEnter/forkCensusLeave

struct SmartMutex
{
    std::mutex m;   // SMARTMUTEX-EXEMPT: wrapper internal (this IS the census-aware wrapper)
    const char* nm;
    explicit SmartMutex(const char* name = "SmartMutex") : nm(name) {}

    void lock()
    {
        m.lock();
        forkCensusEnter(nm);
    }
    bool try_lock()
    {
        if (!m.try_lock())
            return false;
        forkCensusEnter(nm);
        return true;
    }
    void unlock()
    {
        forkCensusLeave();
        m.unlock();
    }
};

struct SmartSharedMutex
{
    std::shared_mutex m;   // SMARTMUTEX-EXEMPT: wrapper internal (this IS the census-aware wrapper)
    const char* nm;
    explicit SmartSharedMutex(const char* name = "SmartSharedMutex") : nm(name) {}

    void lock()
    {
        m.lock();
        forkCensusEnter(nm);
    }
    bool try_lock()
    {
        if (!m.try_lock())
            return false;
        forkCensusEnter(nm);
        return true;
    }
    void unlock()
    {
        forkCensusLeave();
        m.unlock();
    }

    void lock_shared()
    {
        m.lock_shared();
        forkCensusEnter(nm);
    }
    bool try_lock_shared()
    {
        if (!m.try_lock_shared())
            return false;
        forkCensusEnter(nm);
        return true;
    }
    void unlock_shared()
    {
        forkCensusLeave();
        m.unlock_shared();
    }
};

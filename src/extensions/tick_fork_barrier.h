#pragma once

#include <atomic>

#if defined(__linux__) && !defined(LITE_WASM_SC)

#include <thread>
#include "public_settings.h"

namespace tickFork
{
    inline std::atomic<unsigned long long> gRequestProcessorParkPhase{ 0 };
    inline std::atomic<unsigned long long> gRequestProcessorParkAcknowledgement[MAX_NUMBER_OF_PROCESSORS]{};

    class RequestProcessorBarrier
    {
    public:
        RequestProcessorBarrier() = default;
        RequestProcessorBarrier(const RequestProcessorBarrier&) = delete;
        RequestProcessorBarrier& operator=(const RequestProcessorBarrier&) = delete;

        bool request()
        {
            if (_phase)
                return false;

            unsigned long long idlePhase = gRequestProcessorParkPhase.load(std::memory_order_acquire);
            if ((idlePhase & 1) || !gRequestProcessorParkPhase.compare_exchange_strong(idlePhase, idlePhase + 1, std::memory_order_acq_rel))
            {
                return false;
            }
            _phase = idlePhase + 1;
            return true;
        }

        bool allAcknowledged(const unsigned long long* processorIDs, int processorCount) const
        {
            if (!_phase)
                return false;
            for (int i = 0; i < processorCount; i++)
            {
                if (gRequestProcessorParkAcknowledgement[processorIDs[i]].load(std::memory_order_acquire) != _phase)
                {
                    return false;
                }
            }
            return true;
        }

        unsigned long long phase() const
        {
            return _phase;
        }

        void release()
        {
            if (!_phase)
                return;

            unsigned long long expected = _phase;
            gRequestProcessorParkPhase.compare_exchange_strong(
                expected, _phase + 1, std::memory_order_acq_rel);
            _phase = 0;
        }

        ~RequestProcessorBarrier()
        {
            release();
        }

    private:
        unsigned long long _phase = 0;
    };

    inline void requestProcessorParkPoint(unsigned long long processorNumber)
    {
        for (;;)
        {
            const unsigned long long parkPhase =
                gRequestProcessorParkPhase.load(std::memory_order_acquire);
            if (!(parkPhase & 1))
                return;
            gRequestProcessorParkAcknowledgement[processorNumber].store(
                parkPhase, std::memory_order_release);

            // Recheck after release so a worker cannot miss the next park request.
            while (gRequestProcessorParkPhase.load(std::memory_order_acquire) == parkPhase)
                std::this_thread::yield();
        }
    }
}

#else

namespace tickFork
{
    inline void requestProcessorParkPoint(unsigned long long) {}
}

#endif

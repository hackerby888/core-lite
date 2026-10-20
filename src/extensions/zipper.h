#pragma once

#ifdef __linux__

// Blosc2 is unavailable on aarch64 and unnecessary in RAM-only builds.
#if !defined(CMAKE_NO_USE_SWAP) && !defined(__aarch64__)

#include <blosc2.h>
#include <pthread.h>
#include <sched.h>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

// Process-wide CPU mask captured before any thread pins itself. blosc workers inherit their creator's mask,
// so without it every worker of a pinned processor thread would share that thread's single core.
inline cpu_set_t gZipperWorkerCpuMask;
inline bool gZipperWorkerCpuMaskCaptured = false;

class Zipper
{
    // blosc2 spawns its worker threads on a context's first job and keeps them until the context is freed.
    struct ThreadContexts
    {
        blosc2_context* cctx = nullptr;
        blosc2_context* dctx = nullptr;

        ~ThreadContexts()
        {
            if (cctx)
            {
                blosc2_free_ctx(cctx);
            }
            if (dctx)
            {
                blosc2_free_ctx(dctx);
            }
        }
    };

    static ThreadContexts& threadContexts()
    {
        static thread_local ThreadContexts contexts;
        return contexts;
    }

    // Widens the calling thread for the duration of a job so workers blosc creates inherit the process mask.
    struct WorkerCpuMaskScope
    {
        cpu_set_t saved;
        bool active = false;

        WorkerCpuMaskScope()
        {
            if (!gZipperWorkerCpuMaskCaptured)
            {
                return;
            }
            if (pthread_getaffinity_np(pthread_self(), sizeof(saved), &saved) != 0)
            {
                return;
            }
            active = (pthread_setaffinity_np(pthread_self(), sizeof(gZipperWorkerCpuMask), &gZipperWorkerCpuMask) == 0);
        }

        ~WorkerCpuMaskScope()
        {
            if (active)
            {
                pthread_setaffinity_np(pthread_self(), sizeof(saved), &saved);
            }
        }
    };

    // A fork child inherits the context structs but none of the worker threads; using or freeing them would
    // block forever on the pool barrier, so the child leaks them and recreates on first use.
    static void dropContextsInForkChild()
    {
        ThreadContexts& contexts = threadContexts();
        contexts.cctx = nullptr;
        contexts.dctx = nullptr;
    }

    static void initOnce()
    {
        static std::once_flag once;
        std::call_once(once, []()
        {
            blosc2_init();
            pthread_atfork(nullptr, nullptr, dropContextsInForkChild);
        });
    }

public:
    // Call from the main thread before any thread is pinned.
    static void captureProcessCpuMask()
    {
        if (sched_getaffinity(0, sizeof(gZipperWorkerCpuMask), &gZipperWorkerCpuMask) == 0)
        {
            gZipperWorkerCpuMaskCaptured = true;
        }
    }

    static std::vector<unsigned char> zip(unsigned char* src, size_t srcSize, int nthreads = 0)
    {
        if (nthreads <= 0)
        {
            nthreads = std::thread::hardware_concurrency();
        }
        initOnce();

        ThreadContexts& contexts = threadContexts();
        if (!contexts.cctx)
        {
            blosc2_cparams cparams = BLOSC2_CPARAMS_DEFAULTS;
            cparams.typesize = 1;
            cparams.clevel = 1;
            cparams.nthreads = nthreads;
            cparams.compcode = BLOSC_BLOSCLZ;
            contexts.cctx = blosc2_create_cctx(cparams);
        }

        std::vector<unsigned char> dst(srcSize + BLOSC2_MAX_OVERHEAD);
        int cSize;
        {
            WorkerCpuMaskScope wideMask;
            cSize = blosc2_compress_ctx(contexts.cctx, src, srcSize, dst.data(), dst.size());
        }
        if (cSize <= 0)
        {
            throw std::runtime_error("Compression failed!");
        }

        dst.resize(cSize);
        return dst;
    }

    static std::vector<unsigned char> unzip(unsigned char* compressed_data, size_t compressed_data_size, int nthreads = 0)
    {
        if (nthreads <= 0)
        {
            nthreads = std::thread::hardware_concurrency();
        }
        initOnce();

        int32_t nbytes, cbytes, blocksize;
        blosc2_cbuffer_sizes(compressed_data, &nbytes, &cbytes, &blocksize);
        std::vector<unsigned char> decompressed_out(nbytes);

        ThreadContexts& contexts = threadContexts();
        if (!contexts.dctx)
        {
            blosc2_dparams dparams = BLOSC2_DPARAMS_DEFAULTS;
            dparams.nthreads = nthreads;
            contexts.dctx = blosc2_create_dctx(dparams);
        }

        int dSize;
        {
            WorkerCpuMaskScope wideMask;
            dSize = blosc2_decompress_ctx(contexts.dctx, compressed_data, compressed_data_size, decompressed_out.data(), decompressed_out.size());
        }
        if (dSize <= 0)
        {
            throw std::runtime_error("Decompression failed!");
        }

        return decompressed_out;
    }
};

#else
#include <vector>
#include <stdexcept>
#include <cstddef>

class Zipper
{
public:
    static void captureProcessCpuMask() {}

    static std::vector<unsigned char> zip(unsigned char*, size_t, int = 0)
    {
        throw std::runtime_error("Zipper: NO_USE_SWAP build");
    }

    static std::vector<unsigned char> unzip(unsigned char*, size_t, int = 0)
    {
        throw std::runtime_error("Zipper: NO_USE_SWAP build");
    }
};

#endif // CMAKE_NO_USE_SWAP

#endif // __linux__

#pragma once

// In-process HTTP front for Wasm-contract builds: a thin drogon adapter that forwards every
// request to the RPC_ROUTE router (rpc_core.h). Non-Wasm Linux builds serve the same routes
// through the unix-socket + sidecar stack instead and never start this server.

#if defined(LITE_WASM_SC) && !defined(NO_RPC)

#include <drogon/drogon.h>
#include <atomic>
#include <shared_mutex>
#include <string>
#include <thread>

#include "extensions/rpc/rpc_core.h"
#include "extensions/http/rate_limit.h"
#include "platform/virtual_memory.h"   // PinScope: drain swap-page pins taken by handlers

class QubicHttpServer
{
    static void serverThread(int port)
    {
        using namespace drogon;

        app().registerSyncAdvice(
            [](const HttpRequestPtr& req) -> HttpResponsePtr
            {
#ifndef NO_HTTP_RATE_LIMIT
                if (!rl::checkRateLimit(rl::extractClientIp(req)))
                {
                    auto r = HttpResponse::newHttpResponse();
                    r->setStatusCode(k429TooManyRequests);
                    r->addHeader("Retry-After", "1");
                    r->setBody("Too Many Requests");
                    return r;
                }
#endif
                RpcReq rpcReq;
                rpcReq.method = req->getMethodString();
                rpcReq.path = req->getPath();
                rpcReq.query = std::string(req->getQuery());
                rpcReq.body = std::string(req->getBody());

                RpcResp rpcResp;
                {
                    // Same discipline as the unix-socket handler: drain swap pins on this thread
                    // and answer 503 until node init completes.
                    PinScope pinScope;
                    std::shared_lock<SmartSharedMutex> dispatchGuard(gRpcDispatchLock);
                    if (!gRpcNodeReady.load(std::memory_order_acquire))
                        rpcResp = { 503, "application/json", "{\"error\":\"node not ready\"}", "", "" };
                    else
                        rpcResp = gRpc.dispatch(rpcReq);
                }

                HttpResponsePtr resp;
                if (!rpcResp.filePath.empty())
                {
                    resp = HttpResponse::newFileResponse(rpcResp.filePath);
                    if (!rpcResp.downloadName.empty())
                        resp->addHeader("Content-Disposition", "attachment; filename=\"" + rpcResp.downloadName + "\"");
                }
                else
                {
                    resp = HttpResponse::newHttpResponse();
                    resp->setBody(rpcResp.body);
                    resp->setContentTypeString(rpcResp.contentType);
                }
                for (const auto& header : rpcResp.headers)
                    resp->addHeader(header.first, header.second);
                resp->setStatusCode((drogon::HttpStatusCode)rpcResp.status);
                return resp;
            });

        app().addListener("0.0.0.0", port)
            .setThreadNum(std::thread::hardware_concurrency())
            .disableSigtermHandling()
            .run();
    }

public:
    static void start(int port)
    {
        std::thread(serverThread, port).detach();
    }

    static void stop()
    {
        drogon::app().quit();
    }
};

#else

// Stub keeps qubic.cpp call sites compiling in configurations without the in-process server.
class QubicHttpServer
{
public:
    static void start(int) {}
    static void stop() {}
};

#endif // LITE_WASM_SC && !NO_RPC

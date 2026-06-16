#pragma once

// RPC sidecar (drogon): a stateless HTTP -> unix-socket forwarder.
//
// Runs as a separate process (re-exec self with --rpc-proxy), forked once by the
// supervisor shim as a sibling of the node, so node fork-promotes never touch it.
// It holds no node state: every request is forwarded verbatim to the node's unix
// socket and the framed reply is turned back into HTTP. Adding an API never edits
// this file. drogon's fork-hostility is now isolated to a process that never forks.

#if defined(__linux__) && !defined(NO_RPC)

#include <drogon/drogon.h>
#include <string>
#include <thread>
#include <memory>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include "extensions/rpc/rpc_core.h"

inline int rpcProxyMain(int httpPort, std::string unixPath)
{
    using namespace drogon;

    // Forward EVERY request to the node unix socket as a SYNC ADVICE, not a
    // default handler. A sync advice runs BEFORE routing, so it pre-empts the
    // drogon HttpControllers that this same binary auto-registers at static init
    // (RpcLive/RpcQueryV2/RpcStats/Explorer). Were those left to route here, they
    // would run inside the proxy process — which has no node state — and read
    // zeroed globals or segfault (e.g. richList scanning an unmapped spectrum).
    // Returning a non-null response short-circuits routing entirely.
    app().registerSyncAdvice(
        [unixPath](const HttpRequestPtr& req) -> HttpResponsePtr
        {
            int s = socket(AF_UNIX, SOCK_STREAM, 0);
            sockaddr_un addr{};
            addr.sun_family = AF_UNIX;
            std::strncpy(addr.sun_path, unixPath.c_str(), sizeof(addr.sun_path) - 1);
            if (s < 0 || connect(s, (sockaddr*)&addr, sizeof(addr)) != 0)
            {
                if (s >= 0) close(s);
                auto r = HttpResponse::newHttpResponse();
                r->setStatusCode(k503ServiceUnavailable);
                r->setBody("node RPC unavailable (mid-promote?)");
                return r;
            }

            Json::Value m;
            m["method"] = req->getMethodString();
            m["path"]   = req->getPath();
            m["query"]  = std::string(req->getQuery());
            Json::StreamWriterBuilder wb; wb["indentation"] = "";
            rpcwire::writeFrame(s, Json::writeString(wb, m), std::string(req->getBody()));

            std::string meta, body;
            bool ok = rpcwire::readFrame(s, meta, body);
            close(s);
            if (!ok)
            {
                auto r = HttpResponse::newHttpResponse();
                r->setStatusCode(k502BadGateway);
                r->setBody("node RPC framing error");
                return r;
            }

            Json::Value rm;
            { Json::CharReaderBuilder rb; std::string err;
              const std::unique_ptr<Json::CharReader> rd(rb.newCharReader());
              rd->parse(meta.data(), meta.data() + meta.size(), &rm, &err); }

            HttpResponsePtr resp;
            std::string filePath = rm.get("filePath", "").asString();
            if (!filePath.empty())
            {
                resp = HttpResponse::newFileResponse(filePath);
                std::string dn = rm.get("downloadName", "").asString();
                if (!dn.empty())
                    resp->addHeader("Content-Disposition", "attachment; filename=\"" + dn + "\"");
            }
            else
            {
                resp = HttpResponse::newHttpResponse();
                resp->setBody(std::move(body));
                resp->setContentTypeString(rm.get("contentType", "application/json").asString());
            }
            const Json::Value& hdrs = rm["headers"];
            for (const auto& h : hdrs)
                resp->addHeader(h.get("k", "").asString(), h.get("v", "").asString());
            resp->setStatusCode((HttpStatusCode)rm.get("status", 200).asInt());
            return resp;
        });

    LOG_INFO << "RPC sidecar: HTTP :" << httpPort << " -> unix " << unixPath;
    app().addListener("0.0.0.0", httpPort)
         .setThreadNum(std::thread::hardware_concurrency())
         .run();
    return 0;
}

#endif // __linux__ && !NO_RPC

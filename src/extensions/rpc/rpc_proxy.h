#pragma once

// RPC sidecar: a stateless drogon HTTP -> node-unix-socket forwarder. A separate process
// (re-exec self --rpc-proxy), sibling of the node, so fork-promotes never touch it.

#if defined(__linux__) && !defined(NO_RPC)

#include <drogon/drogon.h>
#include <atomic>
#include <cerrno>
#include <string>
#include <thread>
#include <memory>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include "extensions/rpc/rpc_core.h"
#include "extensions/http/rate_limit.h"
#include <chrono>
#include <mutex>
#include <sstream>
#include <algorithm>
#include <unordered_map>

// ---------------- timing + per-client stats ----------------
struct RpcClientStats
{
    uint64_t count = 0;
    uint64_t slowCount = 0;
    uint64_t totalRespBytes = 0;
    uint64_t peakRespBytes = 0;
    uint64_t totalElapsedMs = 0;
    uint64_t maxElapsedMs = 0;
    std::unordered_map<std::string, uint64_t> pathCounts;
};

static constexpr size_t RPC_MAX_PATHS_PER_IP = 32;
static constexpr size_t RPC_TOP_PATHS_PER_IP_REPORTED = 3;
static constexpr int RPC_SLOW_REQUEST_MS = 500;
static constexpr double RPC_CLIENT_REPORT_INTERVAL_SEC = 60.0;
static constexpr size_t RPC_CLIENT_REPORT_TOP_N = 10;

inline std::mutex& rpcClientStatsMutex()
{
    static std::mutex m;  // SMARTMUTEX-EXEMPT: sidecar stats counters only, never node state
    return m;
}

inline std::unordered_map<std::string, RpcClientStats>& rpcClientStats()
{
    static std::unordered_map<std::string, RpcClientStats> s;
    return s;
}

inline int rpcProxyMain(int httpPort, std::string unixPath)
{
    using namespace drogon;

    // Forward every request via a SYNC ADVICE (runs before routing) so it pre-empts the
    // HttpControllers this binary auto-registers — they'd run here with no node state and crash.
    app().registerSyncAdvice(
        [unixPath](const HttpRequestPtr& req) -> HttpResponsePtr
        {
            // Per-IP rate limit at the edge: the sidecar sees the real client IP; the node behind
            // the unix socket only sees loopback, so it must throttle here, before the forward.
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
            int s = socket(AF_UNIX, SOCK_STREAM, 0);
            const char* failedOperation = nullptr;
            int connectionError = 0;
            if (s < 0)
            {
                failedOperation = "socket";
                connectionError = errno;
            }
            else
            {
                sockaddr_un addr{};
                addr.sun_family = AF_UNIX;
                std::strncpy(addr.sun_path, unixPath.c_str(), sizeof(addr.sun_path) - 1);
                if (connect(s, (sockaddr*)&addr, sizeof(addr)) != 0)
                {
                    failedOperation = "connect";
                    connectionError = errno;
                }
            }

            static std::atomic<bool> nodeUnavailable{ false };
            if (failedOperation)
            {
                if (s >= 0)
                {
                    close(s);
                }
                if (!nodeUnavailable.exchange(true))
                {
                    LOG_ERROR << "RPC sidecar: node " << failedOperation
                              << " failed for " << unixPath
                              << ": errno=" << connectionError
                              << " (" << strerror(connectionError) << ")";
                }
                auto r = HttpResponse::newHttpResponse();
                r->setStatusCode(k503ServiceUnavailable);
                r->setBody("node RPC unavailable (mid-promote?)");
                return r;
            }
            if (nodeUnavailable.exchange(false))
            {
                LOG_INFO << "RPC sidecar: node RPC connection recovered for " << unixPath;
            }

            Json::Value m;
            m["method"] = req->getMethodString();
            m["path"]   = req->getPath();
            m["query"]  = std::string(req->getQuery());
            Json::StreamWriterBuilder wb;
            wb["indentation"] = "";
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
            {
                Json::CharReaderBuilder rb;
                std::string err;
                const std::unique_ptr<Json::CharReader> rd(rb.newCharReader());
                rd->parse(meta.data(), meta.data() + meta.size(), &rm, &err);
            }

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

    app().registerPreHandlingAdvice(
        [](const HttpRequestPtr& req)
        {
            req->getAttributes()->insert(
                "qubic_start",
                std::chrono::steady_clock::now());
        });

    app().registerPostHandlingAdvice(
        [](const HttpRequestPtr& req, const HttpResponsePtr& resp)
        {
            auto end = std::chrono::steady_clock::now();
            std::chrono::steady_clock::time_point start;
            try
            {
                start = req->getAttributes()->get<std::chrono::steady_clock::time_point>(
                    "qubic_start");
            }
            catch (...)
            {
                return;
            }

            uint64_t ms = (uint64_t)std::chrono::duration_cast<
                std::chrono::milliseconds>(end - start).count();
            size_t bodyLen = resp ? resp->body().length() : 0;
            int status = resp ? (int)resp->statusCode() : 0;
            const auto& peer = req->peerAddr();
            std::string peerIp = peer.toIp();
            std::string clientIp = rl::extractClientIp(req);

            {
                std::lock_guard<std::mutex> lk(rpcClientStatsMutex());
                auto& s = rpcClientStats()[clientIp];
                s.count++;
                s.totalRespBytes += bodyLen;
                s.totalElapsedMs += ms;
                if (bodyLen > s.peakRespBytes) s.peakRespBytes = bodyLen;
                if (ms > s.maxElapsedMs) s.maxElapsedMs = ms;
                if (ms >= RPC_SLOW_REQUEST_MS) s.slowCount++;
                const std::string& path = req->path();
                auto pit = s.pathCounts.find(path);
                if (pit != s.pathCounts.end())
                    pit->second++;
                else if (s.pathCounts.size() < RPC_MAX_PATHS_PER_IP)
                    s.pathCounts.emplace(path, 1);
            }

            if (ms >= RPC_SLOW_REQUEST_MS)
            {
                std::ostringstream line;
                line << "HTTP slow " << ms << "ms "
                     << req->methodString() << " "
                     << req->path() << " from " << clientIp;
                if (clientIp != peerIp)
                    line << " via " << peer.toIpPort();
                else
                    line << ":" << peer.toPort();
                line << " status=" << status
                     << " body=" << bodyLen << "B";
                LOG_INFO << line.str();
            }
        });

    LOG_INFO << "RPC sidecar: HTTP :" << httpPort << " -> unix " << unixPath;
#ifndef NO_HTTP_RATE_LIMIT
    if (rl::rateLimitEnabled())
        LOG_INFO << "RPC sidecar rate limit: " << rl::rateLimitRefillPerSec()
                 << " req/s, burst " << rl::rateLimitBurst() << " (per IP, loopback/RFC1918 exempt)";
    app().getLoop()->runEvery(60.0, []() { rl::logRateLimitReport(60, 10); });
#endif

    app().getLoop()->runEvery(RPC_CLIENT_REPORT_INTERVAL_SEC, []()
    {
        std::vector<std::pair<std::string, RpcClientStats>> snapshot;
        {
            std::lock_guard<std::mutex> lk(rpcClientStatsMutex());
            if (rpcClientStats().empty()) return;
            snapshot.reserve(rpcClientStats().size());
            for (const auto& kv : rpcClientStats())
                snapshot.emplace_back(kv.first, kv.second);
            rpcClientStats().clear();
        }
        std::sort(snapshot.begin(), snapshot.end(),
                  [](const auto& a, const auto& b) { return a.second.count > b.second.count; });
        size_t n = std::min(snapshot.size(), RPC_CLIENT_REPORT_TOP_N);
        std::ostringstream oss;
        oss << "HTTP client report (last " << (int)RPC_CLIENT_REPORT_INTERVAL_SEC
            << "s, top " << n << " of " << snapshot.size() << " IPs):";
        for (size_t i = 0; i < n; ++i)
        {
            const auto& p = snapshot[i];
            uint64_t avgMs = p.second.count ? p.second.totalElapsedMs / p.second.count : 0;
            oss << " " << p.first
                << "[req=" << p.second.count
                << ",slow=" << p.second.slowCount
                << ",bytes=" << p.second.totalRespBytes
                << ",peak=" << p.second.peakRespBytes
                << "B,avgMs=" << avgMs
                << ",maxMs=" << p.second.maxElapsedMs;
            const auto& pc = p.second.pathCounts;
            if (!pc.empty())
            {
                std::vector<std::pair<std::string, uint64_t>> paths(pc.begin(), pc.end());
                std::sort(paths.begin(), paths.end(),
                          [](const auto& a, const auto& b) { return a.second > b.second; });
                size_t pn = std::min(paths.size(), RPC_TOP_PATHS_PER_IP_REPORTED);
                oss << ",paths={";
                for (size_t j = 0; j < pn; ++j)
                {
                    if (j) oss << ",";
                    oss << paths[j].first << "=" << paths[j].second;
                }
                if (paths.size() > pn) oss << ",+" << (paths.size() - pn) << "more";
                oss << "}";
            }
            oss << "]";
        }
        LOG_INFO << oss.str();
    });
    app().addListener("0.0.0.0", httpPort)
         .setThreadNum(std::thread::hardware_concurrency())
         .run();
    return 0;
}

#endif // __linux__ && !NO_RPC

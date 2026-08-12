#pragma once

// Node-side drogon-free RPC. The router core is portable (Wasm builds serve it through the
// in-process drogon adapter in http.h); the unix-socket transport below is Linux-only — the
// drogon sidecar forwards HTTP there and the node re-binds the socket after a fork-promote.
// The legacy Windows solution opts out entirely because it does not link jsoncpp.

#if defined(__linux__) || defined(LITE_WASM_SC)

#include <string>
#include <map>
#include <vector>
#include <functional>
#include <thread>
#include <atomic>
#include <shared_mutex>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <new>
#include <cstdint>
#include <cstring>
#include "json/json.h"
#include "extensions/fork_census.h"   // SmartSharedMutex (census-aware gRpcDispatchLock)

// ---------------- request / response ----------------
struct RpcReq
{
    std::string method, path, query, body;
    std::map<std::string, std::string> params;   // captured :path params

    static int hexValue(char c)
    {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        return -1;
    }

    static std::string decodeQueryComponent(const std::string& s)
    {
        std::string out;
        out.reserve(s.size());
        for (size_t i = 0; i < s.size(); i++)
        {
            if (s[i] == '%' && i + 2 < s.size())
            {
                int hi = hexValue(s[i + 1]);
                int lo = hexValue(s[i + 2]);
                if (hi >= 0 && lo >= 0)
                {
                    out.push_back((char)((hi << 4) | lo));
                    i += 2;
                    continue;
                }
            }
            out.push_back(s[i] == '+' ? ' ' : s[i]);
        }
        return out;
    }

    // Mirrors drogon's req->getParameter(): path param first, then query string.
    std::string getParameter(const std::string& key) const
    {
        auto it = params.find(key);
        if (it != params.end()) return it->second;
        size_t pos = 0;
        while (pos < query.size())
        {
            size_t amp = query.find('&', pos);
            std::string kv = query.substr(pos, amp == std::string::npos ? std::string::npos : amp - pos);
            size_t eq = kv.find('=');
            if (eq != std::string::npos && decodeQueryComponent(kv.substr(0, eq)) == key)
                return decodeQueryComponent(kv.substr(eq + 1));
            if (amp == std::string::npos) break;
            pos = amp + 1;
        }
        return "";
    }
};

struct RpcResp
{
    int status = 200;
    std::string contentType = "application/json";
    std::string body;          // served when filePath is empty
    std::string filePath;      // if set, the sidecar serves this file (same host)
    std::string downloadName;  // optional Content-Disposition filename
    std::vector<std::pair<std::string, std::string>> headers;  // extra response headers
};

inline RpcResp jsonResp(const Json::Value& j, int status = 200)
{
    Json::StreamWriterBuilder b;
    b["indentation"] = "";
    return { status, "application/json", Json::writeString(b, j), "", "" };
}
inline RpcResp fileResp(const std::string& absPath, const std::string& downloadName)
{
    RpcResp r;
    r.contentType = "application/octet-stream";
    r.filePath = absPath;
    r.downloadName = downloadName;
    return r;
}

// ---------------- router ----------------
using RpcHandler = std::function<RpcResp(const RpcReq&)>;

class RpcRouter
{
    struct Route
    {
        std::string method;
        std::vector<std::string> segments;
        RpcHandler handler;
    };
    std::vector<Route> routes_;

    static std::vector<std::string> split(const std::string& p)
    {
        std::vector<std::string> out;
        size_t i = 0;
        while (i < p.size())
        {
            if (p[i] == '/') { ++i; continue; }
            size_t j = p.find('/', i);
            out.push_back(p.substr(i, j == std::string::npos ? std::string::npos : j - i));
            if (j == std::string::npos) break;
            i = j + 1;
        }
        return out;
    }

public:
    void route(const std::string& method, const std::string& pattern, RpcHandler h)
    {
        routes_.push_back({ method, split(pattern), std::move(h) });
    }

    RpcResp dispatch(RpcReq req) const
    {
        auto segments = split(req.path);
        for (const auto& route : routes_)
        {
            if (route.method != req.method || route.segments.size() != segments.size())
                continue;
            std::map<std::string, std::string> params;
            bool ok = true;
            for (size_t i = 0; i < segments.size(); i++)
            {
                if (!route.segments[i].empty() && route.segments[i][0] == ':')
                {
                    params[route.segments[i].substr(1)] = segments[i];
                }
                else if (route.segments[i] != segments[i])
                {
                    ok = false;
                    break;
                }
            }
            if (!ok) continue;
            req.params = std::move(params);
            try
            {
                return route.handler(req);
            }
            catch (...)
            {
                return { 500, "application/json", "{\"error\":\"handler exception\"}", "", "" };
            }
        }
        return { 404, "application/json", "{\"error\":\"not found\"}", "", "" };
    }
};

inline RpcRouter gRpc;

// Set once the node finishes init and enters the main loop. Until then a handler would read
// uninitialized/zeroed consensus state and segfault, so dispatch answers 503 before it is set.
inline std::atomic<bool> gRpcNodeReady{ false };

// Fork-safety: dispatch takes a SHARED lock; bspForkPoint takes it EXCLUSIVE before fork() so no
// handler holds a node lock at fork. Reinit in the child.
inline SmartSharedMutex gRpcDispatchLock{ "gRpcDispatchLock" };

// One RPC_ROUTE block per API; __COUNTER__ captured once (RPC_ROUTE_I) for TU-unique names.
#define QRPC_CAT_(a, b) a##b
#define QRPC_CAT(a, b) QRPC_CAT_(a, b)
#define RPC_ROUTE(METHOD, PATTERN) RPC_ROUTE_I(METHOD, PATTERN, __COUNTER__)
#define RPC_ROUTE_I(METHOD, PATTERN, N)                              \
    static RpcResp QRPC_CAT(qrpc_h_, N)(const RpcReq&);              \
    static const bool QRPC_CAT(qrpc_r_, N) =                         \
        (gRpc.route(METHOD, PATTERN, QRPC_CAT(qrpc_h_, N)), true);   \
    static RpcResp QRPC_CAT(qrpc_h_, N)(const RpcReq& req)

#ifdef __linux__

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

// ---------------- wire framing: [u32 metaLen][meta json][u32 bodyLen][body] ----------------
namespace rpcwire
{
    inline bool writeAll(int fd, const char* data, size_t remainingBytes)
    {
        while (remainingBytes)
        {
            const ssize_t bytesWritten = ::write(fd, data, remainingBytes);
            if (bytesWritten <= 0)
                return false;

            data += bytesWritten;
            remainingBytes -= (size_t)bytesWritten;
        }
        return true;
    }
    inline bool readAll(int fd, char* data, size_t remainingBytes)
    {
        while (remainingBytes)
        {
            const ssize_t bytesRead = ::read(fd, data, remainingBytes);
            if (bytesRead <= 0)
                return false;

            data += bytesRead;
            remainingBytes -= (size_t)bytesRead;
        }
        return true;
    }
    inline bool writeFrame(int fd, const std::string& meta, const std::string& body)
    {
        uint32_t ml = (uint32_t)meta.size(), bl = (uint32_t)body.size();
        return writeAll(fd, (char*)&ml, 4) && writeAll(fd, meta.data(), ml)
            && writeAll(fd, (char*)&bl, 4) && writeAll(fd, body.data(), bl);
    }
    inline bool readFrame(int fd, std::string& meta, std::string& body)
    {
        uint32_t ml = 0, bl = 0;
        if (!readAll(fd, (char *)&ml, 4))
            return false;
        if (ml > 16u * 1024 * 1024)
            return false;
        meta.resize(ml);
        if (ml && !readAll(fd, &meta[0], ml))
            return false;
        if (!readAll(fd, (char *)&bl, 4))
            return false;
        if (bl > 256u * 1024 * 1024)
            return false;
        body.resize(bl);
        if (bl && !readAll(fd, &body[0], bl))
            return false;
        return true;
    }
} // namespace rpcwire

// ---------------- node-side unix-socket server ----------------
inline std::atomic<bool> gRpcUnixRunning{ false };

// The unix listen socket fd, so a promoted child can close the one it inherited from the forked
// parent before re-binding (else each promote leaks one stale LISTEN socket).
inline std::atomic<int> gRpcUnixListenFd{ -1 };

inline void rpcUnixHandleConn(int c)
{
    std::string meta, body;
    if (!rpcwire::readFrame(c, meta, body))
    {
        close(c);
        return;
    }

    Json::Value m;
    {
        Json::CharReaderBuilder rb;
        std::string err;
        const std::unique_ptr<Json::CharReader> rd(rb.newCharReader());
        rd->parse(meta.data(), meta.data() + meta.size(), &m, &err);
    }

    RpcReq req;
    req.method = m.get("method", "GET").asString();
    req.path   = m.get("path", "/").asString();
    req.query  = m.get("query", "").asString();
    req.body   = std::move(body);

    RpcResp resp;
    {
        // Release swap-page pins the handler takes via tickData/ticks/tx accessors. This is a one-shot
        // detached thread with no tickProcessor PinScope, so without this the pins leak permanently
        // (thread_local arena dies unreleased) until the swapVM hits "all cache pages pinned".
        PinScope _pinScope;
        std::shared_lock<SmartSharedMutex> g(gRpcDispatchLock);
        if (!gRpcNodeReady.load(std::memory_order_acquire))
            resp = { 503, "application/json", "{\"error\":\"node not ready\"}", "", "" };
        else
            resp = gRpc.dispatch(req);
    }

    Json::Value rm;
    rm["status"] = resp.status;
    rm["contentType"] = resp.contentType;
    rm["filePath"] = resp.filePath;
    rm["downloadName"] = resp.downloadName;
    Json::Value hdrs(Json::arrayValue);
    for (const auto& kv : resp.headers)
    {
        Json::Value h;
        h["k"] = kv.first;
        h["v"] = kv.second;
        hdrs.append(h);
    }
    rm["headers"] = hdrs;
    Json::StreamWriterBuilder wb;
    wb["indentation"] = "";
    rpcwire::writeFrame(c, Json::writeString(wb, rm), resp.body);
    close(c);
}

inline void rpcUnixServe(std::string path)
{
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

    std::string lastFailedOperation;
    int lastError = 0;
    auto logFailure = [&](const char* operation, int error)
    {
        if (lastFailedOperation == operation && lastError == error)
        {
            return;
        }
        fprintf(stderr,
                "[RPC] unix %s failed for %s: errno=%d (%s); retrying\n",
                operation,
                path.c_str(),
                error,
                strerror(error));
        fflush(stderr);
        lastFailedOperation = operation;
        lastError = error;
    };
    auto logRecovery = [&]()
    {
        if (lastFailedOperation.empty())
        {
            return;
        }
        fprintf(stderr, "[RPC] unix listener recovered for %s\n", path.c_str());
        fflush(stderr);
        lastFailedOperation.clear();
        lastError = 0;
    };

    for (;;)
    {
        int srv = socket(AF_UNIX, SOCK_STREAM, 0);
        if (srv < 0)
        {
            const int error = errno;
            logFailure("socket", error);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        if (unlink(path.c_str()) != 0 && errno != ENOENT)
        {
            const int error = errno;
            close(srv);
            logFailure("unlink", error);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        if (bind(srv, (sockaddr*)&addr, sizeof(addr)) != 0)
        {
            const int error = errno;
            close(srv);
            logFailure("bind", error);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        if (listen(srv, 64) != 0)
        {
            const int error = errno;
            close(srv);
            unlink(path.c_str());
            logFailure("listen", error);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        gRpcUnixListenFd.store(srv, std::memory_order_release);
        logRecovery();
        for (;;)
        {
            int connection = accept(srv, nullptr, nullptr);
            if (connection >= 0)
            {
                logRecovery();
                std::thread(rpcUnixHandleConn, connection).detach();
                continue;
            }

            const int error = errno;
            if (error == EINTR)
            {
                continue;
            }
            logFailure("accept", error);
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

// Spawn the unix server (idempotent). Child re-binds after a promote (resets gRpcUnixRunning).
inline void rpcUnixStart(const std::string& path)
{
    bool expected = false;
    if (!gRpcUnixRunning.compare_exchange_strong(expected, true))
    {
        return;
    }
    std::thread(rpcUnixServe, path).detach();
}

inline std::string rpcUnixPath(int httpPort)
{
    return "/tmp/qubic-rpc-" + std::to_string(httpPort) + ".sock";
}

#endif // __linux__

#endif // __linux__ || LITE_WASM_SC

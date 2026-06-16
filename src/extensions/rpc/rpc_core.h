#pragma once

// Node-side drogon-free RPC: a router + unix-socket server. The drogon sidecar forwards
// HTTP here; the node re-binds the socket after a fork-promote so RPC survives. Linux-only.

#ifdef __linux__

#include <string>
#include <map>
#include <vector>
#include <functional>
#include <thread>
#include <atomic>
#include <shared_mutex>
#include <new>
#include <cstdint>
#include <cstring>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include "json/json.h"

// ---------------- request / response ----------------
struct RpcReq
{
    std::string method, path, query, body;
    std::map<std::string, std::string> params;   // captured :path params

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
            if (eq != std::string::npos && kv.substr(0, eq) == key) return kv.substr(eq + 1);
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
    struct Route { std::string method; std::vector<std::string> segs; RpcHandler h; };
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
        auto segs = split(req.path);
        for (const auto& r : routes_)
        {
            if (r.method != req.method || r.segs.size() != segs.size()) continue;
            std::map<std::string, std::string> params;
            bool ok = true;
            for (size_t i = 0; i < segs.size(); i++)
            {
                if (!r.segs[i].empty() && r.segs[i][0] == ':') params[r.segs[i].substr(1)] = segs[i];
                else if (r.segs[i] != segs[i]) { ok = false; break; }
            }
            if (!ok) continue;
            req.params = std::move(params);
            try { return r.h(req); }
            catch (...) { return { 500, "application/json", "{\"error\":\"handler exception\"}", "", "" }; }
        }
        return { 404, "application/json", "{\"error\":\"not found\"}", "", "" };
    }
};

inline RpcRouter gRpc;

// One RPC_ROUTE block per API; __COUNTER__ captured once (RPC_ROUTE_I) for TU-unique names.
#define QRPC_CAT_(a, b) a##b
#define QRPC_CAT(a, b) QRPC_CAT_(a, b)
#define RPC_ROUTE(METHOD, PATTERN) RPC_ROUTE_I(METHOD, PATTERN, __COUNTER__)
#define RPC_ROUTE_I(METHOD, PATTERN, N)                              \
    static RpcResp QRPC_CAT(qrpc_h_, N)(const RpcReq&);              \
    static const bool QRPC_CAT(qrpc_r_, N) =                         \
        (gRpc.route(METHOD, PATTERN, QRPC_CAT(qrpc_h_, N)), true);   \
    static RpcResp QRPC_CAT(qrpc_h_, N)(const RpcReq& req)

// ---------------- wire framing: [u32 metaLen][meta json][u32 bodyLen][body] ----------------
namespace rpcwire
{
    inline bool writeAll(int fd, const char* p, size_t n)
    {
        while (n) { ssize_t w = ::write(fd, p, n); if (w <= 0) return false; p += w; n -= (size_t)w; }
        return true;
    }
    inline bool readAll(int fd, char* p, size_t n)
    {
        while (n) { ssize_t r = ::read(fd, p, n); if (r <= 0) return false; p += r; n -= (size_t)r; }
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
        if (!readAll(fd, (char*)&ml, 4)) return false;
        if (ml > 16u * 1024 * 1024) return false;
        meta.resize(ml); if (ml && !readAll(fd, &meta[0], ml)) return false;
        if (!readAll(fd, (char*)&bl, 4)) return false;
        if (bl > 256u * 1024 * 1024) return false;
        body.resize(bl); if (bl && !readAll(fd, &body[0], bl)) return false;
        return true;
    }
}

// ---------------- node-side unix-socket server ----------------
inline std::atomic<bool> gRpcUnixRunning{ false };

// Fork-safety: dispatch takes a SHARED lock; bspForkPoint takes it EXCLUSIVE before fork() so no
// handler holds a node lock at fork. Reinit in the child.
inline std::shared_mutex gRpcDispatchLock;

inline void rpcUnixHandleConn(int c)
{
    std::string meta, body;
    if (!rpcwire::readFrame(c, meta, body)) { close(c); return; }

    Json::Value m;
    { Json::CharReaderBuilder rb; std::string err;
      const std::unique_ptr<Json::CharReader> rd(rb.newCharReader());
      rd->parse(meta.data(), meta.data() + meta.size(), &m, &err); }

    RpcReq req;
    req.method = m.get("method", "GET").asString();
    req.path   = m.get("path", "/").asString();
    req.query  = m.get("query", "").asString();
    req.body   = std::move(body);

    RpcResp resp;
    { std::shared_lock<std::shared_mutex> g(gRpcDispatchLock); resp = gRpc.dispatch(req); }

    Json::Value rm;
    rm["status"] = resp.status;
    rm["contentType"] = resp.contentType;
    rm["filePath"] = resp.filePath;
    rm["downloadName"] = resp.downloadName;
    Json::Value hdrs(Json::arrayValue);
    for (const auto& kv : resp.headers)
    {
        Json::Value h; h["k"] = kv.first; h["v"] = kv.second;
        hdrs.append(h);
    }
    rm["headers"] = hdrs;
    Json::StreamWriterBuilder wb; wb["indentation"] = "";
    rpcwire::writeFrame(c, Json::writeString(wb, rm), resp.body);
    close(c);
}

inline void rpcUnixServe(std::string path)
{
    int srv = socket(AF_UNIX, SOCK_STREAM, 0);
    if (srv < 0) return;
    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);
    unlink(path.c_str());
    if (bind(srv, (sockaddr*)&addr, sizeof(addr)) != 0) { close(srv); return; }
    listen(srv, 64);
    for (;;)
    {
        int c = accept(srv, nullptr, nullptr);
        if (c < 0) continue;
        std::thread(rpcUnixHandleConn, c).detach();
    }
}

// Spawn the unix server (idempotent). Child re-binds after a promote (resets gRpcUnixRunning).
inline void rpcUnixStart(const std::string& path)
{
    bool expected = false;
    if (!gRpcUnixRunning.compare_exchange_strong(expected, true)) return;
    std::thread(rpcUnixServe, path).detach();
}

inline std::string rpcUnixPath(int httpPort)
{
    return "/tmp/qubic-rpc-" + std::to_string(httpPort) + ".sock";
}

#endif // __linux__

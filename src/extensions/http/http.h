#pragma once

#include "../tx_stats.h"
#include "../tx_slot_index.h"
#include "../tick_bench.h"

static unsigned long long httpPasscodes[4] = {};

#ifdef __linux__

#include <drogon/drogon.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#ifndef NO_RPC
#include "controller/rpc_queryv2_controller.h"
#include "controller/rpc_live_controller.h"
#include "controller/rpc_stats_controller.h"
#include "controller/explorer_controller.h"
#endif

#include "rate_limit.h"   // shared per-IP token bucket + extractClientIp (also used by the sidecar)

using namespace drogon;

namespace MiddleWare
{
class PasscodeVerifier : public HttpMiddleware<PasscodeVerifier>
{
public:
    PasscodeVerifier() {}

    void invoke(const HttpRequestPtr &req,
                MiddlewareNextCallback &&nextCb,
                MiddlewareCallback &&mcb) override
    {
        static std::string correctPasscode = std::to_string(httpPasscodes[0]) + "-" +
                                                std::to_string(httpPasscodes[1]) + "-" +
                                                    std::to_string(httpPasscodes[2]) + "-" +
                                                        std::to_string(httpPasscodes[3]);
        bool isPasscodeValid = req->getParameter("passcode") == correctPasscode;
        if (!isPasscodeValid)
        {
            auto resp = HttpResponse::newHttpResponse();
            resp->setStatusCode(k401Unauthorized);
            resp->setBody("Unauthorized: Invalid passcode");
            mcb(resp);
            return;
        }

        nextCb([mcb = std::move(mcb)](const HttpResponsePtr &resp) { mcb(resp); });
    }
};
}

class QubicHttpServer
{
private:
    static inline std::string hiddenFolder = ".qubic-tmp";

    struct ClientStats
    {
        uint64_t count = 0;
        uint64_t slowCount = 0;
        uint64_t totalRespBytes = 0;
        uint64_t peakRespBytes = 0;
        uint64_t totalElapsedMs = 0;
        uint64_t maxElapsedMs = 0;
        // Per-path counts; capped to keep memory bounded under abuse.
        std::unordered_map<std::string, uint64_t> pathCounts;
    };

    static constexpr size_t MAX_PATHS_PER_IP = 32;
    static constexpr size_t TOP_PATHS_PER_IP_REPORTED = 3;

    static std::mutex& clientStatsMutex()
    {
        static std::mutex m;
        return m;
    }

    static std::unordered_map<std::string, ClientStats>& clientStats()
    {
        static std::unordered_map<std::string, ClientStats> s;
        return s;
    }

    static constexpr int SLOW_REQUEST_MS = 500;
    static constexpr double CLIENT_REPORT_INTERVAL_SEC = 60.0;
    static constexpr size_t CLIENT_REPORT_TOP_N = 10;

    static void __http_thread(int port)
    {
        HttpAppFramework &app = drogon::app();
        drogon::app()
            .addListener("0.0.0.0", port)
            .disableSigtermHandling();

        // Per-IP rate limit (sync advice runs before routing, so 429s
        // bypass all handlers).  Loopback/RFC1918 are exempt.
#ifndef NO_HTTP_RATE_LIMIT
        if (rl::rateLimitEnabled())
        {
            LOG_INFO << "HTTP rate limit enabled: "
                     << rl::rateLimitRefillPerSec() << " req/s, burst "
                     << rl::rateLimitBurst() << " (per IP, "
                     << "loopback/RFC1918 exempt)";
            app.registerSyncAdvice(
                [](const HttpRequestPtr &req) -> HttpResponsePtr
                {
                    std::string ip = rl::extractClientIp(req);
                    if (rl::checkRateLimit(ip))
                        return HttpResponsePtr();
                    auto resp = HttpResponse::newHttpResponse();
                    resp->setStatusCode(k429TooManyRequests);
                    resp->addHeader("Retry-After", "1");
                    resp->setBody("Too Many Requests");
                    return resp;
                });
        }
        else
        {
            LOG_INFO << "HTTP rate limit disabled (set "
                        "QUBIC_HTTP_RATE_LIMIT_ENABLED=1 to enable)";
        }
#else
        LOG_INFO << "HTTP rate limit compiled out (NO_HTTP_RATE_LIMIT)";
#endif // NO_HTTP_RATE_LIMIT

        // Per-request timing + per-client stats.  Pre-advice stamps a
        // start time on the request; post-advice computes elapsed ms,
        // updates per-IP counters, and logs slow requests.
        app.registerPreHandlingAdvice(
            [](const HttpRequestPtr &req)
            {
                req->getAttributes()->insert(
                    "qubic_start",
                    std::chrono::steady_clock::now());
            });

        app.registerPostHandlingAdvice(
            [](const HttpRequestPtr &req,
               const HttpResponsePtr &resp)
            {
                // Release any swap-cache page pins taken while serving this request (the
                // handler ran on this same loop thread). See releaseThreadPins / PinScope.
                releaseThreadPins();

                auto end = std::chrono::steady_clock::now();
                std::chrono::steady_clock::time_point start;
                try
                {
                    start = req->getAttributes()
                        ->get<std::chrono::steady_clock::time_point>(
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
                const auto &peer = req->peerAddr();
                std::string peerIp = peer.toIp();
                std::string clientIp = rl::extractClientIp(req);

                {
                    std::lock_guard<std::mutex> lk(clientStatsMutex());
                    auto &s = clientStats()[clientIp];
                    s.count++;
                    s.totalRespBytes += bodyLen;
                    s.totalElapsedMs += ms;
                    if (bodyLen > s.peakRespBytes)
                        s.peakRespBytes = bodyLen;
                    if (ms > s.maxElapsedMs)
                        s.maxElapsedMs = ms;
                    if (ms >= SLOW_REQUEST_MS)
                        s.slowCount++;
                    const std::string &path = req->path();
                    auto pit = s.pathCounts.find(path);
                    if (pit != s.pathCounts.end())
                        pit->second++;
                    else if (s.pathCounts.size() < MAX_PATHS_PER_IP)
                        s.pathCounts.emplace(path, 1);
                    // else: drop (cap reached) — protects memory under abuse
                }

                if (ms >= SLOW_REQUEST_MS)
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

        // Periodic per-client request report.
        drogon::app().getLoop()->runEvery(
            CLIENT_REPORT_INTERVAL_SEC,
            []()
            {
                std::vector<std::pair<std::string, ClientStats>> snapshot;
                {
                    std::lock_guard<std::mutex> lk(clientStatsMutex());
                    if (clientStats().empty())
                        return;
                    snapshot.reserve(clientStats().size());
                    for (const auto &kv : clientStats())
                        snapshot.emplace_back(kv.first, kv.second);
                    clientStats().clear();
                }
                std::sort(snapshot.begin(), snapshot.end(),
                          [](const auto &a, const auto &b)
                          {
                              return a.second.count > b.second.count;
                          });
                size_t n = std::min(snapshot.size(),
                                    CLIENT_REPORT_TOP_N);
                std::ostringstream oss;
                oss << "HTTP client report (last "
                    << (int)CLIENT_REPORT_INTERVAL_SEC
                    << "s, top " << n << " of "
                    << snapshot.size() << " IPs):";
                for (size_t i = 0; i < n; ++i)
                {
                    const auto &p = snapshot[i];
                    uint64_t avgMs = p.second.count
                        ? p.second.totalElapsedMs / p.second.count
                        : 0;
                    oss << " " << p.first
                        << "[req=" << p.second.count
                        << ",slow=" << p.second.slowCount
                        << ",bytes=" << p.second.totalRespBytes
                        << ",peak=" << p.second.peakRespBytes
                        << "B,avgMs=" << avgMs
                        << ",maxMs=" << p.second.maxElapsedMs;
                    // Append top-N paths for this IP.
                    const auto &pc = p.second.pathCounts;
                    if (!pc.empty())
                    {
                        std::vector<std::pair<std::string, uint64_t>> paths(
                            pc.begin(), pc.end());
                        std::sort(paths.begin(), paths.end(),
                                  [](const auto &a, const auto &b)
                                  {
                                      return a.second > b.second;
                                  });
                        size_t pn = std::min(paths.size(),
                                             TOP_PATHS_PER_IP_REPORTED);
                        oss << ",paths={";
                        for (size_t j = 0; j < pn; ++j)
                        {
                            if (j) oss << ",";
                            oss << paths[j].first << "=" << paths[j].second;
                        }
                        if (paths.size() > pn)
                            oss << ",+" << (paths.size() - pn) << "more";
                        oss << "}";
                    }
                    oss << "]";
                }
                LOG_INFO << oss.str();

                // Rate-limit report + stale-bucket cleanup.
#ifndef NO_HTTP_RATE_LIMIT
                rl::logRateLimitReport((int)CLIENT_REPORT_INTERVAL_SEC, CLIENT_REPORT_TOP_N);
#endif // NO_HTTP_RATE_LIMIT
            });

        app.registerHandler(
            "/",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                auto resp = HttpResponse::newHttpResponse();
                resp->setBody("Hello, World!2");
                callback(resp);
            });

        app.registerHandler(
            "/tick-info",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                Json::Value json;
                json["epoch"] = system.epoch;
                json["tick"] = system.tick;
                json["initialTick"] = system.initialTick;
                json["alignedVotes"] = gTickNumberOfComputors;
                json["misalignedVotes"] = gTickTotalNumberOfComputors - gTickNumberOfComputors;
                json["mainAuxStatus"] = mainAuxStatus;
                json["duration"] = 0;
                json["isSavingSnapshot"] = (bool)persistingNodeStateTickProcWaiting;
                std::string challenge = req->getParameter("challenge");
                json["extraInfo"] = getCheckInData(challenge);
                auto resp = HttpResponse::newHttpJsonResponse(json);
                callback(resp);
            });

        app.registerHandler(
            "/running-ids",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                Json::Value json;
                Json::Value idsJson(Json::arrayValue);
                for (int i = 0; i < computorSeedsCount; i++)
                {
                    CHAR16 id[61] = {};
                    m256i publicKey = {};
                    m256i privateKey = {};
                    m256i subseed = {};
                    bool isOk = getSubseed(reinterpret_cast<const unsigned char *>(computorSeeds[i]), subseed.m256i_u8);
                    if (!isOk)
                        continue;
                    getPrivateKey(subseed.m256i_u8, privateKey.m256i_u8);
                    getPublicKey(privateKey.m256i_u8, publicKey.m256i_u8);
                    getIdentity(publicKey.m256i_u8, id, false);
                    if (publicKey != computorPublicKeys[i])
                        continue;

                    idsJson.append(wchar_to_string(id));
                }
                json["runningIds"] = idsJson;
                auto resp = HttpResponse::newHttpJsonResponse(json);
                callback(resp);
            });

        app.registerHandler(
            "/latest-created-tick-info",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                Json::Value json;
                CHAR16 id[61] = {};
                getIdentity((const unsigned char*)&latestCreatedTickInfo.id, id, false);
                json["tick"] = latestCreatedTickInfo.tick;
                json["epoch"] = latestCreatedTickInfo.epoch;
                json["numberOfTxs"] = latestCreatedTickInfo.numberOfTxs;
                json["id"] = wchar_to_string(id);
                auto resp = HttpResponse::newHttpJsonResponse(json);
                callback(resp);
            });

        app.registerHandler(
            "/solutions",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                Json::Value json(Json::arrayValue);
                for (unsigned int i = 0; i < system.numberOfSolutions; i++)
                {
                    Json::Value solutionJson;
                    solutionJson["computorPublicKey"] = byteToHex((unsigned char *)&system.solutions[i].computorPublicKey, sizeof(m256i));
                    solutionJson["miningSeed"] = byteToHex((unsigned char *)&system.solutions[i].miningSeed, sizeof(m256i));
                    solutionJson["nonce"] = byteToHex((unsigned char *)&system.solutions[i].nonce, sizeof(m256i));
                    json.append(solutionJson);
                }
                auto resp = HttpResponse::newHttpJsonResponse(json);
                callback(resp);
            });

        app.registerHandler(
            "/solution-publish-ticks",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                Json::Value json(Json::arrayValue);
                for (unsigned int i = 0; i < system.numberOfSolutions; i++)
                {
                    Json::Value jsonObject;
                    jsonObject["solutionIndex"] = i;
                    jsonObject["publishTick"] = solutionPublicationTicks[i];
                    json.append(jsonObject);
                }
                auto resp = HttpResponse::newHttpJsonResponse(json);
                callback(resp);
            });

        app.registerHandler(
            "/request-save-snapshot",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                requestPersistingNodeState = 1;
                Json::Value json;
                json["status"] = "ok";
                auto resp = HttpResponse::newHttpJsonResponse(json);
                callback(resp);
            });

        app.registerHandler(
            "/spectrum",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                bool isZip = req->getParameter("zip") == "true";
                std::string path;
                if (isZip)
                {
                    path = hiddenFolder + "/" + "spectrum.zip";
                }
                else
                {
                    path = "spectrum." + std::to_string(system.epoch);
                }

                // create zip if not exists in .qubic-tmp/
                if (isZip && !std::filesystem::exists(path))
                {
                    // check if hidden folder exists
                    if (!std::filesystem::exists(hiddenFolder + "/"))
                    {
                        std::filesystem::create_directory(hiddenFolder);
                    }

                    std::string inputFile = "spectrum." + std::to_string(system.epoch);
                    std::string command = "zip -j " + path + " " + inputFile;
                    if (exec(command.c_str()) != 0)
                    {
                        auto resp = HttpResponse::newHttpResponse();
                        resp->setStatusCode(k500InternalServerError);
                        resp->setBody("Failed to create zip file");
                        callback(resp);
                        return;
                    }
                }

                auto resp = HttpResponse::newFileResponse(path);
                std::string fileName = isZip ? "spectrum.zip" : ("spectrum." + std::to_string(system.epoch));
                resp->addHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
                callback(resp);
            }, {drogon::Get, "MiddleWare::PasscodeVerifier"});

        app.registerHandler(
            "/universe",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                bool isZip = req->getParameter("zip") == "true";
                std::string path;
                if (isZip)
                {
                    path = hiddenFolder + "/" + "universe.zip";
                }
                else
                {
                    path = "universe." + std::to_string(system.epoch);
                }

                // create zip if not exists in .qubic-tmp/
                if (isZip && !std::filesystem::exists(path))
                {
                    // check if hidden folder exists
                    if (!std::filesystem::exists(hiddenFolder + "/"))
                    {
                        std::filesystem::create_directory(hiddenFolder);
                    }
                    std::string inputFile = "universe." + std::to_string(system.epoch);
                    std::string command = "zip -j " + path + " " + inputFile;
                    if (exec(command.c_str()) != 0)
                    {
                        Json::Value json;
                        json["error"] = "Failed to create zip file";
                        auto resp = HttpResponse::newHttpJsonResponse(json);
                        callback(resp);
                        return;
                    }
                }

                auto resp = HttpResponse::newFileResponse(path);
                std::string fileName = isZip ? "universe.zip" : ("universe." + std::to_string(system.epoch));
                resp->addHeader("Content-Disposition", "attachment; filename=\"" + fileName + "\"");
                callback(resp);
            }, {drogon::Get, "MiddleWare::PasscodeVerifier"});

        app.registerHandler(
            "/shutdown",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                shutDownNode = 1;
                Json::Value json;
                json["status"] = "ok";
                auto resp = HttpResponse::newHttpJsonResponse(json);
                callback(resp);
            }, {drogon::Get, "MiddleWare::PasscodeVerifier"});

        app.registerHandler("/set-max-inbound",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                int n = std::stoi(req->getParameter("n"));
                if (n < 0) n = 0;
                if (n > NUMBER_OF_INCOMING_CONNECTIONS) n = NUMBER_OF_INCOMING_CONNECTIONS;
                maxInboundAccepts = n;
                Json::Value json;
                json["status"] = "ok";
                json["maxInboundAccepts"] = maxInboundAccepts;
                auto resp = HttpResponse::newHttpJsonResponse(json);
                callback(resp);
            }, {drogon::Get});

        app.registerHandler("/spam",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                // get query parameters
                char enable = 0; // can be 0,1,2
                std::string enableStr = req->getParameter("enable");
                bool withRpc = req->getParameter("withRpc") == "true" || req->getParameter("withRpc") == "1";
                enable = static_cast<char>(std::stoi(enableStr));
                if (enable > 2)
                {
                    enable = 2;
                }
                enableBadBoySpammer = enable;
                spammerWithRpc = withRpc;
                Json::Value json;
                json["status"] = "ok";
                json["spamEnabled"] = enableBadBoySpammer;
                json["withRpc"] = spammerWithRpc;
                auto resp = HttpResponse::newHttpJsonResponse(json);
                callback(resp);
            }, {drogon::Get});

        app.registerHandler("/set-operator-seed",
            [](const HttpRequestPtr &req,
               std::function<void(const HttpResponsePtr &)> &&callback)
            {
                std::string seed = req->getParameter("seed");
                if (seed.length() != 55)
                {
                    auto resp = HttpResponse::newHttpResponse();
                    resp->setStatusCode(k400BadRequest);
                    resp->setBody("Invalid seed length");
                    callback(resp);
                    return;
                }
                mySeed = seed;
                CHAR16 id[61] = {};
                m256i publicKey = {};
                m256i privateKey = {};
                m256i subseed = {};
                bool isOk = getSubseed(reinterpret_cast<const unsigned char *>(mySeed.c_str()), subseed.m256i_u8);
                if (!isOk)
                {
                    auto resp = HttpResponse::newHttpResponse();
                    resp->setStatusCode(k400BadRequest);
                    resp->setBody("Invalid seed format");
                    callback(resp);
                    return;
                }
                getPrivateKey(subseed.m256i_u8, privateKey.m256i_u8);
                getPublicKey(privateKey.m256i_u8, publicKey.m256i_u8);
                getIdentity(publicKey.m256i_u8, id, false);
                myOperatorId = wchar_to_string(id);
                mySubseed = subseed;
                myPublicKey = publicKey;
                Json::Value json;
                json["status"] = "ok";
                json["newId"] = myOperatorId;
                auto resp = HttpResponse::newHttpJsonResponse(json);
                callback(resp);
            }, {drogon::Get});

        app.setThreadNum(std::thread::hardware_concurrency()).run();
    }
public:
    static void start(int port = 41841)
    {
        std::thread server_thread(__http_thread, port);
        server_thread.detach();
    }

    static void stop()
    {
        drogon::app().quit();
    }
};
#else
class QubicHttpServer
{
public:
	static void start(int port = 41841)
	{
		// No-op on non-Linux platforms
	}
	static void stop()
	{
		// No-op on non-Linux platforms
	}
};
#endif
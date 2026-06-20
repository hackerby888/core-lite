#pragma once
#include "../utils.h"
#include "extensions/utils.h"

#include <cmath>
#include <drogon/HttpController.h>

using namespace drogon;

namespace RpcStats
{
class RpcStatsController : public HttpController<RpcStatsController>
{
public:
    METHOD_LIST_BEGIN
    ADD_METHOD_TO(RpcStatsController::queryAssetOwners, "/v1/issuers/{issuerIdentity}/assets/{assetName}/owners", Get);
    ADD_METHOD_TO(RpcStatsController::latestStats, "/v1/latest-stats", Get);
    ADD_METHOD_TO(RpcStatsController::richList, "/v1/rich-list", Get);
    ADD_METHOD_TO(RpcStatsController::txStats, "/v1/tx-stats", Get);
    ADD_METHOD_TO(RpcStatsController::tickBench, "/v1/tick-bench", Get);
    ADD_METHOD_TO(RpcStatsController::peerStats, "/v1/peer-stats", Get);
    METHOD_LIST_END

    // Peer connection state + disconnect-reason counters (why handshaked peers drop).
    inline void peerStats(const HttpRequestPtr &req,
                         std::function<void(const HttpResponsePtr &)> &&cb)
    {
        using namespace std;
        Json::Value result;

        Json::Value reasons;
        for (unsigned int r = 0; r < PeerDisc::REASON_COUNT; r++)
            reasons[PeerDisc::kName[r]] = Json::UInt64(PeerDisc::gReasonCount[r].load(memory_order_relaxed));
        result["disconnectReasons"] = reasons;
        result["disconnectTotal"] = Json::UInt64(PeerDisc::gTotal.load(memory_order_relaxed));
        unsigned int last = PeerDisc::gLastReason.load(memory_order_relaxed);
        result["lastReason"] = PeerDisc::kName[last < PeerDisc::REASON_COUNT ? last : 0];

        unsigned int connected = 0, handshaked = 0;
        Json::Value slots(Json::arrayValue);
        unsigned int n = NUMBER_OF_OUTGOING_CONNECTIONS + NUMBER_OF_INCOMING_CONNECTIONS;
        for (unsigned int i = 0; i < n; i++)
        {
            auto &p = peers[i];
            Json::Value e;
            e["slot"] = i;
            e["outgoing"] = (i < NUMBER_OF_OUTGOING_CONNECTIONS);
            e["hasConn"] = (((unsigned long long)p.tcp4Protocol) > 1);
            e["connected"] = (bool)p.isConnectedAccepted;
            e["handshaked"] = (bool)p.exchangedPublicPeers;
            e["closing"] = (bool)p.isClosing;
            e["incoming"] = (bool)p.isIncommingConnection;
            e["peerReportedTick"] = p.peerReportedTick;
            e["lastActiveTick"] = p.lastActiveTick;
            e["ip"] = std::to_string(p.address.u8[0]) + "." + std::to_string(p.address.u8[1]) + "." +
                      std::to_string(p.address.u8[2]) + "." + std::to_string(p.address.u8[3]);
            unsigned int sc = (i < PeerDisc::MAX_SLOTS) ? PeerDisc::gSlotCount[i].load(memory_order_relaxed) : 0;
            unsigned int sr = (i < PeerDisc::MAX_SLOTS) ? PeerDisc::gSlotLastReason[i].load(memory_order_relaxed) : 0;
            e["disconnects"] = Json::UInt(sc);
            e["lastReason"] = PeerDisc::kName[sr < PeerDisc::REASON_COUNT ? sr : 0];
            e["rxBytes"] = Json::UInt64((i < PeerDisc::MAX_SLOTS) ? PeerDisc::gSlotRxBytes[i].load(memory_order_relaxed) : 0);
            e["txBytes"] = Json::UInt64((i < PeerDisc::MAX_SLOTS) ? PeerDisc::gSlotTxBytes[i].load(memory_order_relaxed) : 0);
            if (p.isConnectedAccepted) connected++;
            if (p.exchangedPublicPeers) handshaked++;
            slots.append(e);
        }
        result["connectedCount"] = connected;
        result["handshakedCount"] = handshaked;
        result["peers"] = slots;
        result["currentTick"] = system.tick;
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    // Per-phase processTick() timings (TSC). Query: reset=1 clears counters after reading.
    inline void tickBench(const HttpRequestPtr &req,
                         std::function<void(const HttpResponsePtr &)> &&cb)
    {
        using namespace std;
        unsigned long long freq = frequency; // TSC Hz, set at boot
        auto toUs = [freq](uint64_t t) -> Json::UInt64
        { return Json::UInt64(freq ? (t * 1000000ull / freq) : 0); };

        Json::Value result;
        Json::Value phases(Json::arrayValue);
        for (unsigned int p = 0; p < TickBench::PHASE_COUNT; p++)
        {
            auto &s = TickBench::gStat[p];
            uint64_t c = s.count.load(memory_order_relaxed);
            uint64_t sum = s.sumTsc.load(memory_order_relaxed);
            Json::Value e;
            e["phase"] = TickBench::kPhaseName[p];
            e["count"] = Json::UInt64(c);
            e["sumUs"] = toUs(sum);
            e["avgUs"] = toUs(c ? sum / c : 0);
            e["maxUs"] = toUs(s.maxTsc.load(memory_order_relaxed));
            e["lastUs"] = toUs(s.lastTsc.load(memory_order_relaxed));
            phases.append(e);
        }
        result["frequencyHz"] = Json::UInt64(freq);
        result["currentTick"] = system.tick;
        result["phases"] = phases;

        if (req->getParameter("reset") == "1" || req->getParameter("reset") == "true")
            TickBench::reset();

        cb(HttpResponse::newHttpJsonResponse(result));
    }

    // Broadcast-tx throughput counters from processBroadcastTransaction().
    // Query: count = number of recent target-ticks to return (default 20, max RING).
    inline void txStats(const HttpRequestPtr &req,
                        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        using namespace std;
        Json::Value result;
        Json::Value data;
        data["totalReceived"] = Json::UInt64(TxStats::gTotalReceived.load(memory_order_relaxed));
        data["totalValid"] = Json::UInt64(TxStats::gTotalValid.load(memory_order_relaxed));
        data["totalStored"] = Json::UInt64(TxStats::gTotalStored.load(memory_order_relaxed));
        uint32_t last = TxStats::gLastTick.load(memory_order_relaxed);
        data["lastTick"] = Json::UInt(last);
        data["currentTick"] = system.tick;

        long long count = 20;
        if (req->getParameter("count") != "")
            count = std::stoll(req->getParameter("count"));
        if (count < 0) count = 0;
        if (count > (long long)TxStats::RING) count = TxStats::RING;

        Json::Value perTick(Json::arrayValue);
        for (long long i = count - 1; i >= 0; i--)
        {
            if ((long long)last - i < 0) continue;
            uint32_t t = (uint32_t)(last - i);
            TxStats::TickSlot &s = TxStats::gRing[t & TxStats::RING_MASK];
            if (s.tick.load(memory_order_relaxed) != t) continue; // evicted / never seen
            Json::Value e;
            e["tick"] = Json::UInt(t);
            e["received"] = Json::UInt(s.received.load(memory_order_relaxed));
            e["stored"] = Json::UInt(s.stored.load(memory_order_relaxed));
            perTick.append(e);
        }
        data["perTick"] = perTick;
        result["data"] = data;
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void queryAssetOwners(const HttpRequestPtr &req,
                                 std::function<void(const HttpResponsePtr &)> &&cb,
                                 const std::string &issuerIdentity,
                                 const std::string &assetName)
    {
        Json::Value result;
        Json::Value ownersArray(Json::arrayValue);
        m256i issuerPublicKey;
        getPublicKeyFromIdentity(reinterpret_cast<const unsigned char *>(issuerIdentity.c_str()), issuerPublicKey.m256i_u8);
        auto targetIssuanceIndex = issuanceIndex(issuerPublicKey, HttpUtils::assetNameFromString(assetName.c_str()));

        // extract page,pageSize from query parameters
        long long page = 0;
        long long pageSize = 10;
        long long currentIndex = 0;
        if (req->getParameter("page") != "")
        {
            page = std::stoll(req->getParameter("page"));
        }
        if (req->getParameter("pageSize") != "")
        {
            pageSize = std::stoll(req->getParameter("pageSize"));
        }

        for (unsigned long long i = 0; i < ASSETS_CAPACITY; i++)
        {
            if (assets[i].varStruct.ownership.type == OWNERSHIP)
            {
                auto &asset = assets[i].varStruct.ownership;
                unsigned short currentManagingContractIndex = asset.managingContractIndex;
                unsigned int currentIssuanceIndex = asset.issuanceIndex;
                if (targetIssuanceIndex >= 0 && currentIssuanceIndex != targetIssuanceIndex)
                {
                    continue;
                }
                // apply pagination
                if (currentIndex < (page + 1) * pageSize && currentIndex >= page * pageSize)
                {
                    CHAR16 identity[61] = {};
                    getIdentity((unsigned char *)&asset.publicKey, identity, false);
                    std::string identityStr = wchar_to_string(identity);
                    Json::Value ownerJson;
                    ownerJson["identity"] = identityStr;
                    ownerJson["numberOfShares"] = std::to_string(asset.numberOfShares);
                    ownersArray.append(ownerJson);
                }
                currentIndex++;
            }
        }

        Json::Value pagination;
        pagination["totalRecords"] = Json::UInt64(currentIndex);
        pagination["currentPage"] = Json::UInt64(page);
        pagination["totalPages"] = Json::UInt64(std::ceil((float)currentIndex / pageSize));
        pagination["pageSize"] = Json::UInt64(pageSize);

        result["pagination"] = pagination;
        result["owners"] = ownersArray;
        result["tick"] = system.tick;
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void latestStats(const HttpRequestPtr &req,
                            std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value result;
        Json::Value data;
        TickStorage::tickData.acquireLock();
        TickData *tickData = TickStorage::tickData.getByTickIfNotEmpty(system.tick - 1);
        if (tickData)
        {
            data["timestamp"] = HttpUtils::formatTimestamp(
                tickData->millisecond,
                tickData->second,
                tickData->minute,
                tickData->hour,
                tickData->day,
                tickData->month,
                tickData->year);
        } else
        {
            data["timestamp"] = "0";
        }
        TickStorage::tickData.releaseLock();

        data["circulatingSupply"] = Json::UInt64(spectrumInfo.totalAmount);
        data["activeAddresses"] = spectrumInfo.numberOfEntities;
        data["price"] = 0;
        data["marketCap"] = "0";
        data["epoch"] = system.epoch;
        data["currentTick"] = system.tick;
        data["ticksInCurrentEpoch"] = system.tick - system.initialTick;
        unsigned int emptyTicks = 0;
        // Per-iteration lock + PinScope: a whole-epoch scan must not hold tickDataLock across the loop
        // (starves the tick processor) nor accumulate a swap-page pin per tick (exhausts the cache -> exit(1)).
        for (unsigned int tick = system.initialTick; tick < system.tick; tick++)
        {
            PinScope _pinScope;
            TickStorage::tickData.acquireLock();
            const bool empty = (TickStorage::tickData.getByTickIfNotEmpty(tick) == nullptr);
            TickStorage::tickData.releaseLock();
            if (empty)
            {
                emptyTicks++;
            }
        }
        data["emptyTicksInCurrentEpoch"] = emptyTicks;
        data["epochTickQuality"] = system.tick - system.initialTick == 0 ? 0 : std::roundf((float)(system.tick - system.initialTick - emptyTicks) / (float)(system.tick - system.initialTick) * 100000.0f) / 100000.0f;
        data["burnedQus"] = 0;
        result["data"] = data;
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void richList(const HttpRequestPtr &req,
                         std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value result;
        Json::Value richListArray(Json::arrayValue);

        // extract page,pageSize from query parameters
        long long page = 0;
        long long pageSize = 10;
        if (req->getParameter("page") != "")
        {
            page = std::stoll(req->getParameter("page"));
        }
        if (req->getParameter("pageSize") != "")
        {
            pageSize = std::stoll(req->getParameter("pageSize"));
        }

        // create a vector of pairs to sort
        std::vector<std::pair<m256i, long long>> balances;
        for (unsigned int i = 0; i < SPECTRUM_CAPACITY; i++)
        {
            const long long balance = spectrum[i].incomingAmount - spectrum[i].outgoingAmount;
            if (balance > 0)
            {
                balances.emplace_back(spectrum[i].publicKey, balance);
            }
        }

        // sort the vector by balance in descending order
        std::sort(balances.begin(), balances.end(),
                  [](const std::pair<m256i, long long> &a, const std::pair<m256i, long long> &b)
                  {
                      return a.second > b.second;
                  });

        // apply pagination
        long long start = page * pageSize;
        long long end = std::min(start + pageSize, (long long)balances.size());
        for (long long i = start; i < end; i++)
        {
            Json::Value entry;
            CHAR16 identity[61] = {};
            getIdentity((unsigned char *)&balances[i].first, identity, false);
            std::string identityStr = wchar_to_string(identity);
            entry["identity"] = identityStr;
            entry["balance"] = std::to_string(balances[i].second);
            richListArray.append(entry);
        }

        Json::Value pagination;
        pagination["totalRecords"] = Json::UInt64(balances.size());
        pagination["currentPage"] = Json::UInt64(page);
        pagination["totalPages"] = Json::UInt64(std::ceil((float)balances.size() / pageSize));
        pagination["pageSize"] = Json::UInt64(pageSize);
        result["pagination"] = pagination;
        result["richList"]["entities"] = richListArray;
        result["epoch"] = system.epoch;
        cb(HttpResponse::newHttpJsonResponse(result));
    }
};
}


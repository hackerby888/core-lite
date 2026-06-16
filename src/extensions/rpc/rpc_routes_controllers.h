#pragma once

// Migrated controller endpoints (explorer, stats, live, query/v2): the SAME logic
// as the drogon HttpController classes, but as RpcReq->RpcResp handlers behind the
// router. Path params {x} -> :x (captured into req.params); JSON bodies parsed from
// req.body; cb(newHttpJsonResponse(j)) -> return jsonResp(j). Logic verbatim.

#ifdef __linux__

#include <string>
#include <vector>
#include <memory>
#include <cmath>
#include <algorithm>
#include "extensions/rpc/rpc_core.h"
#include "extensions/http/controller/explorer_assets.generated.h"

// Parse a JSON request body; null on failure (mirrors drogon req->getJsonObject()).
static std::shared_ptr<Json::Value> rpcJsonBody(const std::string& body)
{
    auto j = std::make_shared<Json::Value>();
    Json::CharReaderBuilder rb; std::string err;
    const std::unique_ptr<Json::CharReader> rd(rb.newCharReader());
    if (!rd->parse(body.data(), body.data() + body.size(), j.get(), &err)) return nullptr;
    return j;
}

static RpcResp rpcEmbedded(const char* body, const char* contentType)
{
    RpcResp r;
    r.contentType = contentType;
    r.body = body;
    r.headers.push_back({ "Cache-Control", "no-store, must-revalidate" });
    r.headers.push_back({ "Pragma", "no-cache" });
    return r;
}

// ============================ explorer ============================
RPC_ROUTE("GET", "/explorer")        { (void)req; return rpcEmbedded(EXPLORER_INDEX_HTML, "text/html; charset=utf-8"); }
RPC_ROUTE("GET", "/explorer/")       { (void)req; return rpcEmbedded(EXPLORER_INDEX_HTML, "text/html; charset=utf-8"); }
RPC_ROUTE("GET", "/explorer/style.css") { (void)req; return rpcEmbedded(EXPLORER_STYLE_CSS, "text/css"); }
RPC_ROUTE("GET", "/explorer/app.js") { (void)req; return rpcEmbedded(EXPLORER_APP_JS, "text/javascript; charset=utf-8"); }

RPC_ROUTE("GET", "/explorer/data")
{
    (void)req;
    Json::Value out;

    out["header"]["tick"]                = system.tick;
    out["header"]["epoch"]               = system.epoch;
    out["header"]["initialTick"]         = system.initialTick;
    out["header"]["alignedVotes"]        = gTickNumberOfComputors;
    out["header"]["misalignedVotes"]     = gTickTotalNumberOfComputors - gTickNumberOfComputors;
    out["header"]["mainAuxStatus"]       = mainAuxStatus;
    out["header"]["isSavingSnapshot"]    = (bool)persistingNodeStateTickProcWaiting;
    out["header"]["ticksInCurrentEpoch"] = system.tick - system.initialTick;
    out["header"]["latestCreatedTick"]   = latestCreatedTickInfo.tick;

    constexpr unsigned int N = 20;
    Json::Value recent(Json::arrayValue);
    TickStorage::tickData.acquireLock();
    unsigned int start = (system.tick >= N) ? (system.tick - N + 1) : 0;
    if (start < system.initialTick) start = system.initialTick;
    for (unsigned int t = start; t <= system.tick; t++)
    {
        TickData* td = TickStorage::tickData.getByTickIfNotEmpty(t);
        Json::Value row;
        row["tick"] = t;
        const m256i& leaderKey = broadcastedComputors.computors.publicKeys[t % NUMBER_OF_COMPUTORS];
        CHAR16 leaderId[61] = {};
        getIdentity((unsigned char*)&leaderKey, leaderId, false);
        row["leader"] = wchar_to_string(leaderId);
        if (td)
        {
            unsigned int txc = 0;
            for (unsigned int i = 0; i < NUMBER_OF_TRANSACTIONS_PER_TICK; i++)
                if (!isZero(td->transactionDigests[i])) txc++;
            row["empty"]     = false;
            row["txCount"]   = txc;
            row["timestamp"] = HttpUtils::formatTimestamp(
                td->millisecond, td->second, td->minute, td->hour,
                td->day, td->month, td->year);
        }
        else
        {
            row["empty"]     = true;
            row["txCount"]   = 0;
            row["timestamp"] = "";
        }
        recent.append(row);
    }
    TickStorage::tickData.releaseLock();
    out["recentTicks"] = recent;

    Json::Value perTick(Json::arrayValue);
    for (unsigned int t = system.tick; t <= system.tick + 10; t++)
    {
        Json::Value e;
        e["tick"]  = t;
        e["count"] = pendingTxsPool.getNumberOfPendingTickTxs(t);
        perTick.append(e);
    }
    out["mempool"]["totalPending"] = pendingTxsPool.getTotalNumberOfPendingTxs(system.tick);
    out["mempool"]["perTick"]      = perTick;

    Json::Value computors(Json::arrayValue);
    for (unsigned int i = 0; i < NUMBER_OF_COMPUTORS; i++)
    {
        Json::Value c;
        c["index"] = i;
        CHAR16 id[61] = {};
        getIdentity((unsigned char*)&broadcastedComputors.computors.publicKeys[i], id, false);
        c["publicKey"] = wchar_to_string(id);
        computors.append(c);
    }
    out["computors"] = computors;

    unsigned int connected = 0, outg = 0, inc = 0;
    constexpr unsigned int peersLen = sizeof(peers) / sizeof(peers[0]);
    for (unsigned int i = 0; i < peersLen; i++)
    {
        if (peers[i].isConnectedAccepted && !peers[i].isClosing)
        {
            connected++;
            if (peers[i].isIncommingConnection) inc++; else outg++;
        }
    }
    out["network"]["connectedPeers"] = connected;
    out["network"]["outgoing"]       = outg;
    out["network"]["incoming"]       = inc;

    Json::Value topMiners(Json::arrayValue);
    ACQUIRE(minerScoreArrayLock);
    const unsigned int topN = (numberOfMiners < 10) ? numberOfMiners : 10;
    for (unsigned int i = 0; i < topN; i++)
    {
        Json::Value m;
        CHAR16 id[61] = {};
        getIdentity((unsigned char*)&minerPublicKeys[i], id, false);
        m["publicKey"] = wchar_to_string(id);
        m["score"]     = minerScores[i];
        topMiners.append(m);
    }
    RELEASE(minerScoreArrayLock);
    out["mining"]["numberOfSolutions"] = system.numberOfSolutions;
    out["mining"]["topMiners"]         = topMiners;

    out["spectrum"]["circulatingSupply"] = std::to_string(spectrumInfo.totalAmount);
    out["spectrum"]["activeAddresses"]   = spectrumInfo.numberOfEntities;

    m256i sSpec = etalonTick.saltedSpectrumDigest;
    m256i sUniv = etalonTick.saltedUniverseDigest;
    m256i sComp = etalonTick.saltedComputerDigest;
    m256i pSpec = etalonTick.prevSpectrumDigest;
    m256i pUniv = etalonTick.prevUniverseDigest;
    m256i pComp = etalonTick.prevComputerDigest;
    out["state"]["saltedSpectrumDigest"]      = base64_encode(sSpec.m256i_u8, 32);
    out["state"]["saltedUniverseDigest"]      = base64_encode(sUniv.m256i_u8, 32);
    out["state"]["saltedComputerDigest"]      = base64_encode(sComp.m256i_u8, 32);
    out["state"]["prevSpectrumDigest"]        = base64_encode(pSpec.m256i_u8, 32);
    out["state"]["prevUniverseDigest"]        = base64_encode(pUniv.m256i_u8, 32);
    out["state"]["prevComputerDigest"]        = base64_encode(pComp.m256i_u8, 32);
    out["state"]["saltedResourceTestingDigest"] = etalonTick.saltedResourceTestingDigest;
    out["state"]["prevResourceTestingDigest"]   = etalonTick.prevResourceTestingDigest;
    out["state"]["saltedTransactionBodyDigest"] = etalonTick.saltedTransactionBodyDigest;
    out["state"]["prevTransactionBodyDigest"]   = etalonTick.prevTransactionBodyDigest;
    out["state"]["resourceTestingDigest"]       = (Json::UInt64)resourceTestingDigest;

    return jsonResp(out);
}

// ============================ stats (/v1/...) ============================
RPC_ROUTE("GET", "/v1/peer-stats")
{
    (void)req;
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
    return jsonResp(result);
}

RPC_ROUTE("GET", "/v1/tick-bench")
{
    using namespace std;
    unsigned long long freq = frequency;
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

    if (req.getParameter("reset") == "1" || req.getParameter("reset") == "true")
        TickBench::reset();

    return jsonResp(result);
}

RPC_ROUTE("GET", "/v1/tx-stats")
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
    if (req.getParameter("count") != "")
        count = std::stoll(req.getParameter("count"));
    if (count < 0) count = 0;
    if (count > (long long)TxStats::RING) count = TxStats::RING;

    Json::Value perTick(Json::arrayValue);
    for (long long i = count - 1; i >= 0; i--)
    {
        if ((long long)last - i < 0) continue;
        uint32_t t = (uint32_t)(last - i);
        TxStats::TickSlot &s = TxStats::gRing[t & TxStats::RING_MASK];
        if (s.tick.load(memory_order_relaxed) != t) continue;
        Json::Value e;
        e["tick"] = Json::UInt(t);
        e["received"] = Json::UInt(s.received.load(memory_order_relaxed));
        e["stored"] = Json::UInt(s.stored.load(memory_order_relaxed));
        perTick.append(e);
    }
    data["perTick"] = perTick;
    result["data"] = data;
    return jsonResp(result);
}

RPC_ROUTE("GET", "/v1/issuers/:issuerIdentity/assets/:assetName/owners")
{
    std::string issuerIdentity = req.getParameter("issuerIdentity");
    std::string assetName = req.getParameter("assetName");
    Json::Value result;
    Json::Value ownersArray(Json::arrayValue);
    m256i issuerPublicKey;
    getPublicKeyFromIdentity(reinterpret_cast<const unsigned char *>(issuerIdentity.c_str()), issuerPublicKey.m256i_u8);
    auto targetIssuanceIndex = issuanceIndex(issuerPublicKey, HttpUtils::assetNameFromString(assetName.c_str()));

    long long page = 0;
    long long pageSize = 10;
    long long currentIndex = 0;
    if (req.getParameter("page") != "")
        page = std::stoll(req.getParameter("page"));
    if (req.getParameter("pageSize") != "")
        pageSize = std::stoll(req.getParameter("pageSize"));

    for (unsigned long long i = 0; i < ASSETS_CAPACITY; i++)
    {
        if (assets[i].varStruct.ownership.type == OWNERSHIP)
        {
            auto &asset = assets[i].varStruct.ownership;
            unsigned int currentIssuanceIndex = asset.issuanceIndex;
            if (targetIssuanceIndex >= 0 && currentIssuanceIndex != targetIssuanceIndex)
                continue;
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
    return jsonResp(result);
}

RPC_ROUTE("GET", "/v1/latest-stats")
{
    (void)req;
    Json::Value result;
    Json::Value data;
    TickStorage::tickData.acquireLock();
    TickData *tickData = TickStorage::tickData.getByTickIfNotEmpty(system.tick - 1);
    if (tickData)
    {
        data["timestamp"] = HttpUtils::formatTimestamp(
            tickData->millisecond, tickData->second, tickData->minute,
            tickData->hour, tickData->day, tickData->month, tickData->year);
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
    {
        TickStorage::tickData.acquireLock();
       for (unsigned int tick = system.initialTick; tick < system.tick; tick++)
       {
           TickData *tickData = TickStorage::tickData.getByTickIfNotEmpty(tick);
           if (!tickData)
               emptyTicks++;
       }
        TickStorage::tickData.releaseLock();
    }
    data["emptyTicksInCurrentEpoch"] = emptyTicks;
    data["epochTickQuality"] = system.tick - system.initialTick == 0 ? 0 : std::roundf((float)(system.tick - system.initialTick - emptyTicks) / (float)(system.tick - system.initialTick) * 100000.0f) / 100000.0f;
    data["burnedQus"] = 0;
    result["data"] = data;
    return jsonResp(result);
}

RPC_ROUTE("GET", "/v1/rich-list")
{
    Json::Value result;
    Json::Value richListArray(Json::arrayValue);

    long long page = 0;
    long long pageSize = 10;
    if (req.getParameter("page") != "")
        page = std::stoll(req.getParameter("page"));
    if (req.getParameter("pageSize") != "")
        pageSize = std::stoll(req.getParameter("pageSize"));

    std::vector<std::pair<m256i, long long>> balances;
    for (unsigned int i = 0; i < SPECTRUM_CAPACITY; i++)
    {
        const long long balance = spectrum[i].incomingAmount - spectrum[i].outgoingAmount;
        if (balance > 0)
            balances.emplace_back(spectrum[i].publicKey, balance);
    }

    std::sort(balances.begin(), balances.end(),
              [](const std::pair<m256i, long long> &a, const std::pair<m256i, long long> &b)
              { return a.second > b.second; });

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
    return jsonResp(result);
}

// ============================ live (/live/v1/...) ============================
RPC_ROUTE("GET", "/live/v1/assets/issuances")
{
    std::string issuerIdentity = req.getParameter("issuerIdentity");
    std::string assetName = req.getParameter("assetName");
    Json::Value result;
    Json::Value assetsArray(Json::arrayValue);
    unsigned long long targetUniverseIndex = -1;
    for (unsigned long long i = 0; i < ASSETS_CAPACITY; i++)
    {
        if (assets[i].varStruct.issuance.type == ISSUANCE)
        {
            auto &asset = assets[i].varStruct.issuance;
            CHAR16 identity[61] = {};
            getIdentity((unsigned char *)&asset.publicKey, identity, false);
            std::string identityStr = wchar_to_string(identity);
            std::string assetNameStr = std::string(asset.name);

            if ((!issuerIdentity.empty() && identityStr != issuerIdentity) ||
                (!assetName.empty() && assetNameStr != assetName))
                continue;

            Json::Value root;
            Json::Value assetJson = HttpUtils::issuanceToJson((HttpUtils::AssetIssuanceType *)&asset);
            root["data"] = assetJson;
            assetsArray.append(root);
            targetUniverseIndex = i;
            break;
        }
    }
    result["assets"] = assetsArray;
    result["tick"] = system.tick;
    result["universeIndex"] = Json::UInt64(targetUniverseIndex);
    return jsonResp(result);
}

RPC_ROUTE("GET", "/live/v1/assets/issuances/:index")
{
    Json::Value result;
    unsigned long long index = std::stoull(req.getParameter("index"));
    if (index >= ASSETS_CAPACITY)
    {
        result["code"] = 3;
        result["message"] = "Index out of range";
        return jsonResp(result);
    }
    if (assets[index].varStruct.issuance.type != ISSUANCE)
    {
        result["code"] = 3;
        result["message"] = "No asset issuance at the given index";
        return jsonResp(result);
    }
    auto &asset = assets[index].varStruct.issuance;
    Json::Value assetJson = HttpUtils::issuanceToJson((HttpUtils::AssetIssuanceType *)&asset);
    result["data"] = assetJson;
    result["tick"] = system.tick;
    result["universeIndex"] = Json::UInt64(index);
    return jsonResp(result);
}

RPC_ROUTE("GET", "/live/v1/assets/ownerships")
{
    std::string issuerIdentity = req.getParameter("issuerIdentity");
    std::string assetName = req.getParameter("assetName");
    std::string ownerIdentity = req.getParameter("ownerIdentity");
    int64_t ownershipManagingContract = -1;
    if (req.getParameter("ownershipManagingContract") != "")
        ownershipManagingContract = stoll(req.getParameter("ownershipManagingContract"));
    Json::Value result;
    Json::Value assetsArray(Json::arrayValue);

    m256i issuerPublicKey;
    getPublicKeyFromIdentity(reinterpret_cast<const unsigned char *>(issuerIdentity.c_str()), issuerPublicKey.m256i_u8);
    auto targetIssuanceIndex = issuanceIndex(issuerPublicKey, HttpUtils::assetNameFromString(assetName.c_str()));
    for (unsigned long long i = 0; i < ASSETS_CAPACITY; i++)
    {
        if (assets[i].varStruct.ownership.type == OWNERSHIP)
        {
            auto &asset = assets[i].varStruct.ownership;
            CHAR16 identity[61] = {};
            getIdentity((unsigned char *)&asset.publicKey, identity, false);
            std::string identityStr = wchar_to_string(identity);

            if ((!ownerIdentity.empty() && identityStr != ownerIdentity) ||
                (ownershipManagingContract >= 0 && asset.managingContractIndex != ownershipManagingContract) ||
                (targetIssuanceIndex >= 0 && asset.issuanceIndex != (unsigned int)targetIssuanceIndex))
                continue;

            Json::Value root;
            Json::Value assetJson = HttpUtils::ownershipToJson((HttpUtils::AssetOwnershipType *)&asset);
            root["tick"] = system.tick;
            root["universeIndex"] = Json::UInt64(i);
            root["data"] = assetJson;
            assetsArray.append(root);
        }
    }
    result["assets"] = assetsArray;
    return jsonResp(result);
}

RPC_ROUTE("GET", "/live/v1/assets/ownerships/:index")
{
    Json::Value result;
    unsigned long long index = std::stoull(req.getParameter("index"));
    if (index >= ASSETS_CAPACITY)
    {
        result["code"] = 3;
        result["message"] = "Index out of range";
        return jsonResp(result);
    }
    if (assets[index].varStruct.ownership.type != OWNERSHIP)
    {
        result["code"] = 3;
        result["message"] = "No asset ownership at the given index";
        return jsonResp(result);
    }
    auto &asset = assets[index].varStruct.ownership;
    Json::Value assetJson = HttpUtils::ownershipToJson((HttpUtils::AssetOwnershipType *)&asset);
    result["data"] = assetJson;
    result["tick"] = system.tick;
    result["universeIndex"] = Json::UInt64(index);
    return jsonResp(result);
}

RPC_ROUTE("GET", "/live/v1/assets/possessions")
{
    std::string issuerIdentity = req.getParameter("issuerIdentity");
    std::string assetName = req.getParameter("assetName");
    std::string ownerIdentity = req.getParameter("ownerIdentity");
    std::string possessorIdentity = req.getParameter("possessorIdentity");
    int64_t ownershipManagingContract = -1;
    int64_t possessionManagingContract = -1;
    if (req.getParameter("ownershipManagingContract") != "")
        ownershipManagingContract = stoll(req.getParameter("ownershipManagingContract"));
    if (req.getParameter("possessionManagingContract") != "")
        possessionManagingContract = stoll(req.getParameter("possessionManagingContract"));
    Json::Value result;
    Json::Value assetsArray(Json::arrayValue);

    m256i issuerPublicKey;
    getPublicKeyFromIdentity(reinterpret_cast<const unsigned char *>(issuerIdentity.c_str()), issuerPublicKey.m256i_u8);
    auto targetIssuanceIndex = issuanceIndex(issuerPublicKey, HttpUtils::assetNameFromString(assetName.c_str()));

    for (unsigned long long i = 0; i < ASSETS_CAPACITY; i++)
    {
        if (assets[i].varStruct.possession.type == POSSESSION)
        {
            auto &asset = assets[i].varStruct.possession;
            CHAR16 identity[61] = {};
            getIdentity((unsigned char *)&asset.publicKey, identity, false);
            std::string identityStr = wchar_to_string(identity);
            unsigned short currentOwnershipManagingContractIndex = assets[asset.ownershipIndex].varStruct.ownership.managingContractIndex;
            unsigned int currentIssuanceIndex = assets[asset.ownershipIndex].varStruct.ownership.issuanceIndex;
            if ((!possessorIdentity.empty() && identityStr != possessorIdentity) ||
                (!ownerIdentity.empty() && identityStr != ownerIdentity) ||
                (ownershipManagingContract >= 0 && currentOwnershipManagingContractIndex != ownershipManagingContract) ||
                (possessionManagingContract >= 0 && asset.managingContractIndex != possessionManagingContract) ||
                (targetIssuanceIndex >= 0 && currentIssuanceIndex != targetIssuanceIndex))
                continue;

            Json::Value root;
            Json::Value assetJson = HttpUtils::possessionToJson((HttpUtils::AssetPossessionType *)&asset);
            root["data"] = assetJson;
            root["tick"] = system.tick;
            root["universeIndex"] = Json::UInt64(i);
            assetsArray.append(root);
        }
    }
    result["assets"] = assetsArray;
    return jsonResp(result);
}

RPC_ROUTE("GET", "/live/v1/assets/possessions/:index")
{
    Json::Value result;
    unsigned long long index = std::stoull(req.getParameter("index"));
    if (index >= ASSETS_CAPACITY)
    {
        result["code"] = 3;
        result["message"] = "Index out of range";
        return jsonResp(result);
    }
    if (assets[index].varStruct.possession.type != POSSESSION)
    {
        result["code"] = 3;
        result["message"] = "No asset possession at the given index";
        return jsonResp(result);
    }
    auto &asset = assets[index].varStruct.possession;
    Json::Value assetJson = HttpUtils::possessionToJson((HttpUtils::AssetPossessionType *)&asset);
    result["data"] = assetJson;
    result["tick"] = system.tick;
    result["universeIndex"] = Json::UInt64(index);
    return jsonResp(result);
}

RPC_ROUTE("GET", "/live/v1/assets/:identity/issued")
{
    std::string identityStr = req.getParameter("identity");
    Json::Value result;
    Json::Value assetsArray(Json::arrayValue);

    m256i identityPublicKey;
    getPublicKeyFromIdentity(reinterpret_cast<const unsigned char *>(identityStr.c_str()), identityPublicKey.m256i_u8);

    for (unsigned long long i = 0; i < ASSETS_CAPACITY; i++)
    {
        if (assets[i].varStruct.issuance.type == ISSUANCE)
        {
            auto &asset = assets[i].varStruct.issuance;
            if (asset.publicKey != identityPublicKey)
                continue;

            Json::Value root;
            Json::Value assetJson = HttpUtils::issuanceToJson((HttpUtils::AssetIssuanceType *)&asset);
            root["data"] = assetJson;
            Json::Value info(Json::objectValue);
            info["tick"] = system.tick;
            info["universeIndex"] = Json::UInt64(i);
            root["info"] = info;
            assetsArray.append(root);
        }
    }
    result["issuedAssets"] = assetsArray;
    return jsonResp(result);
}

RPC_ROUTE("GET", "/live/v1/assets/:identity/owned")
{
    std::string identityStr = req.getParameter("identity");
    Json::Value result;
    Json::Value assetsArray(Json::arrayValue);

    m256i identityPublicKey;
    getPublicKeyFromIdentity(reinterpret_cast<const unsigned char *>(identityStr.c_str()), identityPublicKey.m256i_u8);

    for (unsigned long long i = 0; i < ASSETS_CAPACITY; i++)
    {
        if (assets[i].varStruct.ownership.type == OWNERSHIP)
        {
            auto &asset = assets[i].varStruct.ownership;
            auto issuanceAsset = &assets[asset.issuanceIndex].varStruct.issuance;
            if (asset.publicKey != identityPublicKey)
                continue;

            Json::Value root;
            Json::Value assetJson = HttpUtils::ownershipToJson((HttpUtils::AssetOwnershipType *)&asset);
            assetJson["issuedAsset"] = HttpUtils::issuanceToJson((HttpUtils::AssetIssuanceType *)issuanceAsset);
            root["data"] = assetJson;
            Json::Value info(Json::objectValue);
            info["tick"] = system.tick;
            info["universeIndex"] = Json::UInt64(i);
            root["info"] = info;
            assetsArray.append(root);
        }
    }
    result["ownedAssets"] = assetsArray;
    return jsonResp(result);
}

RPC_ROUTE("GET", "/live/v1/assets/:identity/possessed")
{
    std::string identityStr = req.getParameter("identity");
    Json::Value result;
    Json::Value assetsArray(Json::arrayValue);

    m256i identityPublicKey;
    getPublicKeyFromIdentity(reinterpret_cast<const unsigned char *>(identityStr.c_str()), identityPublicKey.m256i_u8);

    for (unsigned long long i = 0; i < ASSETS_CAPACITY; i++)
    {
        if (assets[i].varStruct.possession.type == POSSESSION)
        {
            auto &asset = assets[i].varStruct.possession;
            auto ownershipAsset = &assets[asset.ownershipIndex].varStruct.ownership;
            auto issuanceAsset = &assets[ownershipAsset->issuanceIndex].varStruct.issuance;
            if (asset.publicKey != identityPublicKey)
                continue;

            Json::Value root;
            Json::Value assetJson = HttpUtils::possessionToJson((HttpUtils::AssetPossessionType *)&asset);
            assetJson["ownedAsset"] = HttpUtils::ownershipToJson((HttpUtils::AssetOwnershipType *)ownershipAsset);
            assetJson["ownedAsset"]["issuedAsset"] = HttpUtils::issuanceToJson((HttpUtils::AssetIssuanceType *)issuanceAsset);
            root["data"] = assetJson;
            Json::Value info(Json::objectValue);
            info["tick"] = system.tick;
            info["universeIndex"] = Json::UInt64(i);
            root["info"] = info;
            assetsArray.append(root);
        }
    }
    result["possessedAssets"] = assetsArray;
    return jsonResp(result);
}

RPC_ROUTE("GET", "/live/v1/balances/:id")
{
    std::string idStr = req.getParameter("id");
    Json::Value result;
    Json::Value balance;
    m256i identityPublicKey;
    getPublicKeyFromIdentity(reinterpret_cast<const unsigned char *>(idStr.c_str()), identityPublicKey.m256i_u8);
    auto spectrumInfo = spectrum[spectrumIndex(identityPublicKey)];
    balance["id"] = idStr;
    balance["balance"] = std::to_string(spectrumInfo.incomingAmount - spectrumInfo.outgoingAmount);
    balance["validForTick"] = system.tick;
    balance["latestIncomingTransferTick"] = spectrumInfo.latestIncomingTransferTick;
    balance["latestOutgoingTransferTick"] = spectrumInfo.latestOutgoingTransferTick;
    balance["incomingAmount"] = std::to_string(spectrumInfo.incomingAmount);
    balance["outgoingAmount"] = std::to_string(spectrumInfo.outgoingAmount);
    balance["numberOfIncomingTransfers"] = spectrumInfo.numberOfIncomingTransfers;
    balance["numberOfOutgoingTransfers"] = spectrumInfo.numberOfOutgoingTransfers;
    result["balance"] = balance;
    return jsonResp(result);
}

// block-height + tick-info share the same body in the drogon controller.
static RpcResp rpcLiveTickInfo(const RpcReq& req)
{
    (void)req;
    Json::Value json;
    json["epoch"] = system.epoch;
    json["tick"] = system.tick;
    json["initialTick"] = system.initialTick;
    json["alignedVotes"] = gTickNumberOfComputors;
    json["misalignedVotes"] = gTickTotalNumberOfComputors - gTickNumberOfComputors;
    json["mainAuxStatus"] = mainAuxStatus;
    json["duration"] = 0;
    json["tickInfo"]["tick"] = system.tick;
    return jsonResp(json);
}
RPC_ROUTE("GET", "/live/v1/block-height") { return rpcLiveTickInfo(req); }
RPC_ROUTE("GET", "/live/v1/tick-info")    { return rpcLiveTickInfo(req); }

RPC_ROUTE("POST", "/live/v1/broadcast-transaction")
{
    Json::Value result;
    try
    {
        auto json = rpcJsonBody(req.body);
        if (!json)
        {
            result["code"] = 3;
            result["message"] = "Invalid JSON";
            return jsonResp(result);
        }

        std::string txBase64 = (*json)["encodedTransaction"].asString();
        auto txData = base64_decode(txBase64);
        std::cout << "tx data size: " << txData.size() << std::endl;
        Transaction *tx = (Transaction*)txData.data();
        if (!tx->checkValidity())
        {
            result["code"] = 3;
            result["message"] = "Invalid validity";
            return jsonResp(result);
        }
        std::cout << "tx json" << HttpUtils::transactionToJson(tx, false) << std::endl;
        {
            unsigned char digest[32];
            KangarooTwelve(txData.data(), tx->totalSize() - SIGNATURE_SIZE, digest, sizeof(digest));
            if (!verify(tx->sourcePublicKey.m256i_u8, digest, tx->signaturePtr()))
            {
                result["code"] = 3;
                result["message"] = "Invalid signature";
                return jsonResp(result);
            }
        }

        std::vector<uint8_t> packet(sizeof(RequestResponseHeader) + tx->totalSize());
        RequestResponseHeader *header = (RequestResponseHeader *)packet.data();
        header->setSize2(packet.size());
        header->setDejavu(0);
        header->setType(BROADCAST_TRANSACTION);
        copyMem(packet.data() + sizeof(RequestResponseHeader), txData.data(), packet.size() - sizeof(RequestResponseHeader));
        enqueueResponse(NULL, header);

        uint8_t digest[32];
        KangarooTwelve(packet.data() + sizeof(RequestResponseHeader), tx->totalSize(), digest, 32);
        CHAR16 txHash[61] = {};
        getIdentity(digest, txHash, true);

        result["peersBroadcasted"] = 1;
        result["encodedTransaction"] = txBase64;
        result["transactionId"] = wchar_to_string(txHash);
        return jsonResp(result);
    }
    catch (const std::exception &e)
    {
        result["code"] = -1;
        result["message"] = "Exception: " + std::string(e.what());
        return jsonResp(result);
    }
}

RPC_ROUTE("GET", "/live/v1/ipos/active")
{
    (void)req;
    Json::Value result;
    Json::Value iposArray(Json::arrayValue);
    for (unsigned int contractIndex = 1; contractIndex < contractCount; ++contractIndex)
    {
        if (system.epoch == contractDescriptions[contractIndex].constructionEpoch - 1)
        {
            Json::Value ipoJson;
            ipoJson["contractIndex"] = contractIndex;
            ipoJson["assetName"] = std::string(contractDescriptions[contractIndex].assetName);
            iposArray.append(ipoJson);
        }
    }
    result["ipos"] = iposArray;
    return jsonResp(result);
}

RPC_ROUTE("POST", "/live/v1/querySmartContract")
{
    Json::Value result;
    try
    {
        auto json = rpcJsonBody(req.body);
        if (!json)
        {
            result["code"] = 3;
            result["message"] = "Invalid JSON";
            return jsonResp(result);
        }

        unsigned int contractIndex = (*json)["contractIndex"].asUInt();
        if (contractIndex < 1 || contractIndex >= contractCount)
        {
            result["code"] = 3;
            result["message"] = "contractIndex out of range";
            return jsonResp(result, 400);
        }
        unsigned short inputType = (*json)["inputType"].asUInt();
        unsigned short inputSize = (*json)["inputSize"].asUInt();
        std::string requestData = (*json)["requestData"].asString();
        std::vector<uint8_t> inputData = base64_decode(requestData);
        if (inputData.size() != inputSize)
        {
            result["code"] = 3;
            result["message"] = "Input size mismatch";
            return jsonResp(result, 400);
        }
        QpiContextUserFunctionCall qpiContext(contractIndex);
        auto errorCode = qpiContext.call(inputType, inputData.data(), inputSize);
        if (errorCode == NoContractError)
        {
            std::vector<uint8_t> responseData(qpiContext.outputSize);
            copyMem(responseData.data(), qpiContext.outputBuffer, qpiContext.outputSize);
            result["responseData"] = base64_encode(responseData);
            return jsonResp(result);
        }
        else
        {
            result["code"] = -1;
            result["message"] = "Error calling smart contract function: " + std::to_string(errorCode);
            return jsonResp(result, 500);
        }
    }
    catch (const std::exception &e)
    {
        result["code"] = -1;
        result["message"] = "Exception: " + std::string(e.what());
        return jsonResp(result, 500);
    }
}

#include "extensions/rpc/rpc_routes_queryv2.h"

#endif // __linux__

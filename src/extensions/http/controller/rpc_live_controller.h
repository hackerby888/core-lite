#pragma once
#include "extensions/utils.h"
#include "../utils.h"
#include <drogon/HttpController.h>

using namespace drogon;

namespace RpcLive
{

class RpcLiveController : public HttpController<RpcLiveController>
{
  public:
    METHOD_LIST_BEGIN
    ADD_METHOD_TO(RpcLiveController::assetsIssuances, "/live/v1/assets/issuances", Get);
    ADD_METHOD_TO(RpcLiveController::assetsIssuancesIndex, "/live/v1/assets/issuances/{index}", Get);
    ADD_METHOD_TO(RpcLiveController::assetsOwnerships, "/live/v1/assets/ownerships", Get);
    ADD_METHOD_TO(RpcLiveController::assetsOwnershipsIndex, "/live/v1/assets/ownerships/{index}", Get);
    ADD_METHOD_TO(RpcLiveController::assetsPossessions, "/live/v1/assets/possessions", Get);
    ADD_METHOD_TO(RpcLiveController::assetsPossessionsIndex, "/live/v1/assets/possessions/{index}", Get);
    ADD_METHOD_TO(RpcLiveController::assetsIdentityIssued, "/live/v1/assets/{identity}/issued", Get);
    ADD_METHOD_TO(RpcLiveController::assetsIdentityOwned, "/live/v1/assets/{identity}/owned", Get);
    ADD_METHOD_TO(RpcLiveController::assetsIdentityPossessed, "/live/v1/assets/{identity}/possessed", Get);
    ADD_METHOD_TO(RpcLiveController::balancesId, "/live/v1/balances/{id}", Get);
    ADD_METHOD_TO(RpcLiveController::tickInfo, "/live/v1/block-height", Get);
    ADD_METHOD_TO(RpcLiveController::tickInfo, "/live/v1/tick-info", Get);
    ADD_METHOD_TO(RpcLiveController::broadcastTransaction, "/live/v1/broadcast-transaction", Post);
    ADD_METHOD_TO(RpcLiveController::iposActive, "/live/v1/ipos/active", Get);
    ADD_METHOD_TO(RpcLiveController::querySmartContract, "/live/v1/querySmartContract", Post);
#ifdef LITE_DYNAMIC_CONTRACTS
    ADD_METHOD_TO(RpcLiveController::dynRegistry, "/live/v1/dyn-registry", Get);
    ADD_METHOD_TO(RpcLiveController::dynUpload, "/live/v1/dyn-upload", Get);
    ADD_METHOD_TO(RpcLiveController::logStats, "/live/v1/log-stats", Get);
#ifdef LITE_WASM_CONTRACTS
    ADD_METHOD_TO(RpcLiveController::debugTrace, "/live/v1/debug-trace", Get);
    ADD_METHOD_TO(RpcLiveController::devDebug, "/live/v1/dev/debug", Get);
    ADD_METHOD_TO(RpcLiveController::devDebugClear, "/live/v1/dev/debug-clear", Get);
#endif
#if ADDON_TX_STATUS_REQUEST
    ADD_METHOD_TO(RpcLiveController::txStatus, "/live/v1/tx-status/{tick}/{tx}", Get);
#endif
#if defined(TESTNET)
    ADD_METHOD_TO(RpcLiveController::devFundedSeed, "/live/v1/dev/funded-seed", Get);
    ADD_METHOD_TO(RpcLiveController::devPutContractSource, "/live/v1/dev/contract-source", Post);
#endif
#endif
    METHOD_LIST_END

    inline void assetsIssuances(const HttpRequestPtr &req,
                                std::function<void(const HttpResponsePtr &)> &&cb)
    {
        auto issuerIdentity = req->getParameter("issuerIdentity");
        auto assetName = req->getParameter("assetName");
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
                {
                    continue;
                }

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
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void assetsIssuancesIndex(const HttpRequestPtr &req,
                                     std::function<void(const HttpResponsePtr &)> &&cb,
                                     const std::string &indexStr)
    {
        Json::Value result;
        unsigned long long index = std::stoull(indexStr);
        if (index >= ASSETS_CAPACITY)
        {
            result["code"] = 3;
            result["message"] = "Index out of range";
            cb(HttpResponse::newHttpJsonResponse(result));
            return;
        }

        if (assets[index].varStruct.issuance.type != ISSUANCE)
        {
            result["code"] = 3;
            result["message"] = "No asset issuance at the given index";
            cb(HttpResponse::newHttpJsonResponse(result));
            return;
        }

        auto &asset = assets[index].varStruct.issuance;
        Json::Value assetJson = HttpUtils::issuanceToJson((HttpUtils::AssetIssuanceType *)&asset);
        result["data"] = assetJson;
        result["tick"] = system.tick;
        result["universeIndex"] = Json::UInt64(index);
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void assetsOwnerships(const HttpRequestPtr &req,
                                 std::function<void(const HttpResponsePtr &)> &&cb)
    {
        auto issuerIdentity = req->getParameter("issuerIdentity");
        auto assetName = req->getParameter("assetName");
        auto ownerIdentity = req->getParameter("ownerIdentity");
        int64_t ownershipManagingContract = -1;
        if (req->getParameter("ownershipManagingContract") != "")
        {
            ownershipManagingContract = stoll(req->getParameter("ownershipManagingContract"));
        }
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
                {
                    continue;
                }

                Json::Value root;
                Json::Value assetJson = HttpUtils::ownershipToJson((HttpUtils::AssetOwnershipType *)&asset);
                root["tick"] = system.tick;
                root["universeIndex"] = Json::UInt64(i);
                root["data"] = assetJson;
                assetsArray.append(root);
            }
        }
        result["assets"] = assetsArray;
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void assetsOwnershipsIndex(const HttpRequestPtr &req,
                                      std::function<void(const HttpResponsePtr &)> &&cb,
                                      const std::string &indexStr)
    {
        Json::Value result;
        unsigned long long index = std::stoull(indexStr);
        if (index >= ASSETS_CAPACITY)
        {
            result["code"] = 3;
            result["message"] = "Index out of range";
            cb(HttpResponse::newHttpJsonResponse(result));
            return;
        }

        if (assets[index].varStruct.ownership.type != OWNERSHIP)
        {
            result["code"] = 3;
            result["message"] = "No asset ownership at the given index";
            cb(HttpResponse::newHttpJsonResponse(result));
            return;
        }

        auto &asset = assets[index].varStruct.ownership;
        Json::Value assetJson = HttpUtils::ownershipToJson((HttpUtils::AssetOwnershipType *)&asset);
        result["data"] = assetJson;
        result["tick"] = system.tick;
        result["universeIndex"] = Json::UInt64(index);
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void assetsPossessions(const HttpRequestPtr &req,
                                  std::function<void(const HttpResponsePtr &)> &&cb)
    {
        auto issuerIdentity = req->getParameter("issuerIdentity");
        auto assetName = req->getParameter("assetName");
        auto ownerIdentity = req->getParameter("ownerIdentity");
        auto possessorIdentity = req->getParameter("possessorIdentity");
        int64_t ownershipManagingContract = -1;
        int64_t possessionManagingContract = -1;
        if (req->getParameter("ownershipManagingContract") != "")
        {
            ownershipManagingContract = stoll(req->getParameter("ownershipManagingContract"));
        }
        if (req->getParameter("possessionManagingContract") != "")
        {
            possessionManagingContract = stoll(req->getParameter("possessionManagingContract"));
        }
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
                {
                    continue;
                }

                Json::Value root;
                Json::Value assetJson = HttpUtils::possessionToJson((HttpUtils::AssetPossessionType *)&asset);
                root["data"] = assetJson;
                root["tick"] = system.tick;
                root["universeIndex"] = Json::UInt64(i);
                assetsArray.append(root);
            }
        }
        result["assets"] = assetsArray;
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void assetsPossessionsIndex(const HttpRequestPtr &req,
                                       std::function<void(const HttpResponsePtr &)> &&cb,
                                       const std::string &indexStr)
    {
        Json::Value result;
        unsigned long long index = std::stoull(indexStr);
        if (index >= ASSETS_CAPACITY)
        {
            result["code"] = 3;
            result["message"] = "Index out of range";
            cb(HttpResponse::newHttpJsonResponse(result));
            return;
        }

        if (assets[index].varStruct.possession.type != POSSESSION)
        {
            result["code"] = 3;
            result["message"] = "No asset possession at the given index";
            cb(HttpResponse::newHttpJsonResponse(result));
            return;
        }

        auto &asset = assets[index].varStruct.possession;
        Json::Value assetJson = HttpUtils::possessionToJson((HttpUtils::AssetPossessionType *)&asset);
        result["data"] = assetJson;
        result["tick"] = system.tick;
        result["universeIndex"] = Json::UInt64(index);
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void assetsIdentityIssued(const HttpRequestPtr &req,
                                     std::function<void(const HttpResponsePtr &)> &&cb,
                                     const std::string &identityStr)
    {
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
                {
                    continue;
                }

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
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void assetsIdentityOwned(const HttpRequestPtr &req,
                                    std::function<void(const HttpResponsePtr &)> &&cb,
                                    const std::string &identityStr)
    {
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
                {
                    continue;
                }

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
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void assetsIdentityPossessed(const HttpRequestPtr &req,
                                        std::function<void(const HttpResponsePtr &)> &&cb,
                                        const std::string &identityStr)
    {
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
                {
                    continue;
                }

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
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void balancesId(const HttpRequestPtr &req,
                           std::function<void(const HttpResponsePtr &)> &&cb,
                           const std::string &idStr)
    {
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
        cb(HttpResponse::newHttpJsonResponse(result));
    }

    inline void tickInfo(const HttpRequestPtr &req,
                         std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        json["epoch"] = system.epoch;
        json["tick"] = system.tick;
        json["initialTick"] = system.initialTick;
        json["alignedVotes"] = gTickNumberOfComputors;
        json["misalignedVotes"] = gTickTotalNumberOfComputors - gTickNumberOfComputors;
        json["mainAuxStatus"] = mainAuxStatus;
        json["duration"] = 0;
        json["tickInfo"]["tick"] = system.tick;
        auto resp = HttpResponse::newHttpJsonResponse(json);
        cb(resp);
    }

    inline void broadcastTransaction(const HttpRequestPtr &req,
                                     std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value result;
        try
        {
            auto json = req->getJsonObject();
            if (!json)
            {
                result["code"] = 3;
                result["message"] = "Invalid JSON";
                cb(HttpResponse::newHttpJsonResponse(result));
                return;
            }

            std::string txBase64 = (*json)["encodedTransaction"].asString();
            // decode base64
            auto txData = base64_decode(txBase64);
            if (txData.size() < sizeof(Transaction))
            {
                result["code"] = 3;
                result["message"] = "Transaction too small";
                cb(HttpResponse::newHttpJsonResponse(result));
                return;
            }
            Transaction *tx = (Transaction*)txData.data();
            if (tx->totalSize() != txData.size() || !tx->checkValidity())
            {
                result["code"] = 3;
                result["message"] = "Invalid transaction";
                cb(HttpResponse::newHttpJsonResponse(result));
                return;
            }
            // verify signature
            {
                unsigned char digest[32];
                KangarooTwelve(txData.data(), tx->totalSize() - SIGNATURE_SIZE, digest, sizeof(digest));
                if (!verify(tx->sourcePublicKey.m256i_u8, digest, tx->signaturePtr()))
                {
                    result["code"] = 3;
                    result["message"] = "Invalid signature";
                    cb(HttpResponse::newHttpJsonResponse(result));
                    return;
                }
            }

            std::vector<uint8_t> packet(sizeof(RequestResponseHeader) + tx->totalSize());
            // Broadcast
            RequestResponseHeader *header = (RequestResponseHeader *)packet.data();
            header->setSize2(packet.size());
            header->setDejavu(0);
            header->setType(BROADCAST_TRANSACTION);
            copyMem(packet.data() + sizeof(RequestResponseHeader), txData.data(), packet.size() - sizeof(RequestResponseHeader));
            enqueueResponse(NULL, header);

            uint8_t digest[32];
            KangarooTwelve(packet.data() + sizeof(RequestResponseHeader),
                           tx->totalSize(),
                           digest,
                           32);
            CHAR16 txHash[61] = {};
            getIdentity(digest, txHash, true);

            result["peersBroadcasted"] = 1;
            result["encodedTransaction"] = txBase64;
            result["transactionId"] = wchar_to_string(txHash);
            cb(HttpResponse::newHttpJsonResponse(result));
        }
        catch (const std::exception &e)
        {
            result["code"] = -1;
            result["message"] = "Exception: " + std::string(e.what());
            cb(HttpResponse::newHttpJsonResponse(result));
        }
    }

    inline void iposActive(const HttpRequestPtr &req,
                           std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value result;
        Json::Value iposArray(Json::arrayValue);
        for (unsigned int contractIndex = 1; contractIndex < contractCount; ++contractIndex)
        {
            if (system.epoch == contractDescriptions[contractIndex].constructionEpoch - 1) // IPO happens in the epoch before construction
            {
                Json::Value ipoJson;
                ipoJson["contractIndex"] = contractIndex;
                ipoJson["assetName"] = std::string(contractDescriptions[contractIndex].assetName);
                iposArray.append(ipoJson);
            }
        }
        result["ipos"] = iposArray;
        cb(HttpResponse::newHttpJsonResponse(result));
    }

#ifdef LITE_DYNAMIC_CONTRACTS
    // Dynamic-contract registry: deployed slots + their function/procedure inputTypes (tooling autocomplete).
    inline void dynRegistry(const HttpRequestPtr &req,
                            std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        Json::Value arr(Json::arrayValue);
        // All reserved slots (armed + free) so tooling can resolve name -> slot and auto-allocate.
        for (unsigned int i = 0; i < LITE_DYN_SLOT_COUNT; i++)
        {
            const LiteDynSlot &s = g_liteDynSlots[i];
            unsigned int idx = LITEDYN0_CONTRACT_INDEX + i;
            Json::Value c;
            c["index"] = idx;
            c["armed"] = s.armed;
            c["constructed"] = s.constructed;
            c["version"] = s.version;
            c["name"] = std::string(s.name);
            char hex[65];
            for (int b = 0; b < 32; b++) snprintf(hex + b * 2, 3, "%02x", s.codeHash[b]);
            c["codeHash"] = std::string(hex, 64);
            Json::Value fns(Json::arrayValue), procs(Json::arrayValue);
            if (s.armed)
                for (unsigned int t = 1; t <= 65535; t++)
                {
                    if (contractUserFunctions[idx][t])
                    {
                        Json::Value e; e["inputType"] = t;
                        e["inputSize"] = contractUserFunctionInputSizes[idx][t];
                        e["outputSize"] = contractUserFunctionOutputSizes[idx][t];
                        fns.append(e);
                    }
                    if (contractUserProcedures[idx][t])
                    {
                        Json::Value e; e["inputType"] = t;
                        e["inputSize"] = contractUserProcedureInputSizes[idx][t];
                        e["outputSize"] = contractUserProcedureOutputSizes[idx][t];
                        procs.append(e);
                    }
                }
            c["functions"] = fns;
            c["procedures"] = procs;
            c["source"] = s.sourceH;   // contract .h source (if submitted via /dev/contract-source) for callee resolution
#ifdef LITE_WASM_CONTRACTS
            c["lastError"] = liteWasmLastTrap(idx);   // most recent dispatch trap reason (empty if last call ok) — for tooling
#endif
            arr.append(c);
        }
        json["slotBase"] = (unsigned int)LITEDYN0_CONTRACT_INDEX;
        json["slotCount"] = (unsigned int)LITE_DYN_SLOT_COUNT;
        json["contracts"] = arr;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Dev-only: store a deployed contract's .h source (node-local, off-chain) keyed by slot, so tooling can
    // resolve inter-contract callees (types + slot via dyn-registry) without the caller passing --callee.
    inline void devPutContractSource(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        int idx = std::atoi(req->getParameter("slot").c_str());
        int local = idx - (int)LITEDYN0_CONTRACT_INDEX;
        if (local < 0 || local >= (int)LITE_DYN_SLOT_COUNT)
        {
            json["ok"] = false; json["error"] = "bad slot";
            cb(HttpResponse::newHttpJsonResponse(json));
            return;
        }
        g_liteDynSlots[local].sourceH = std::string(req->getBody());
        json["ok"] = true; json["slot"] = idx; json["len"] = (Json::UInt)g_liteDynSlots[local].sourceH.size();
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Active dynamic-contract upload session: which chunks landed, which are still missing. Lets tooling
    // confirm assembly before sending DEPLOY and resend only the missing seqs (idempotent, order-free).
    inline void dynUpload(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        const LiteDynUpload &u = g_liteDynUpload;
        char sid[32]; snprintf(sid, sizeof(sid), "%llu", (unsigned long long)u.sessionId);
        json["active"] = u.active;
        json["sessionId"] = std::string(sid);     // u64 as string (JSON loses precision past 2^53)
        json["totalSize"] = u.totalSize;
        json["chunkSize"] = 1008u;                 // mirrors liteDynOnUploadChunk's seq*1008 layout
        json["chunkCount"] = u.chunkCount;
        json["receivedCount"] = u.receivedCount;
        json["complete"] = (u.active && u.receivedCount == u.chunkCount);
        char hex[65];
        for (int b = 0; b < 32; b++) snprintf(hex + b * 2, 3, "%02x", u.finalHash[b]);
        json["finalHash"] = std::string(hex, 64);
        // Missing seqs (bit clear in g_liteDynSeqSeen), capped so a large upload can't bloat the response.
        Json::Value missing(Json::arrayValue);
        unsigned int missingCount = 0;
        const unsigned int CAP = 4096;
        if (u.active)
            for (unsigned int seq = 0; seq < u.chunkCount; seq++)
            {
                const unsigned int byteIdx = seq >> 3, bit = 1u << (seq & 7);
                if (!(g_liteDynSeqSeen[byteIdx] & bit))
                {
                    if (missingCount < CAP) missing.append(seq);
                    missingCount++;
                }
            }
        json["missing"] = missing;
        json["missingCount"] = missingCount;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Running logId + the last few stored log entries (type + payload) — for verifying contract logs.
    inline void logStats(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        unsigned long long cur = qLogger::logId;
        json["logId"] = (Json::UInt64)cur;
        Json::Value arr(Json::arrayValue);
        unsigned long long start = cur > 16 ? cur - 16 : 0;
        for (unsigned long long id = start; id < cur; id++)
        {
            auto it = qLogger::tmpLogBuffer.find(id);
            if (it == qLogger::tmpLogBuffer.end() || !it->second) continue;
            const unsigned char *b = (const unsigned char *)it->second;
            unsigned int szType = *((unsigned int *)(b + 6));
            unsigned int msgSize = szType & 0xFFFFFF;
            Json::Value e;
            e["logId"] = (Json::UInt64)id;
            e["type"] = (unsigned int)(szType >> 24);
            if (msgSize >= 4) e["contractIndex"] = *((unsigned int *)(b + 26));
            char hx[65];
            unsigned int n = msgSize < 32 ? msgSize : 32;
            for (unsigned int k = 0; k < n; k++) snprintf(hx + k * 2, 3, "%02x", b[26 + k]);
            e["payloadHex"] = std::string(hx, n * 2);
            arr.append(e);
        }
        json["recent"] = arr;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

#ifdef LITE_WASM_CONTRACTS
    // GET /live/v1/debug-trace?since=<seq>&limit=<n> — recent wasm contract-call traces (debug toggle).
    inline void debugTrace(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb)
    {
        unsigned long long since = 0; unsigned int limit = 64;
        try { auto s = req->getParameter("since"); if (!s.empty()) since = std::stoull(s); } catch (...) {}
        try { auto s = req->getParameter("limit"); if (!s.empty()) limit = (unsigned int)std::stoul(s); } catch (...) {}
        if (limit == 0 || limit > LITE_WASM_TRACE_RING) limit = LITE_WASM_TRACE_RING;
        Json::Value json; json["enabled"] = liteWasmDebugEnabled();
        Json::Value arr(Json::arrayValue);
        for (const auto &t : liteWasmTraceSnapshot(since, limit))
        {
            Json::Value e;
            e["seq"] = (Json::UInt64)t.seq; e["tick"] = t.tick; e["index"] = t.idx;
            e["entry"] = (unsigned int)t.it; e["kind"] = (unsigned int)t.kind; e["ok"] = t.ok;
            e["execNs"] = (Json::UInt64)t.execNs;
            e["inSize"] = t.inSize; e["outSize"] = t.outSize; e["stateSize"] = t.stateSize; e["stateTruncated"] = t.stateTruncated;
            e["invocator"] = liteWasmHex(&t.invocator, 32);
            e["invocationReward"] = (Json::Int64)t.invocationReward;
            unsigned int ih = t.inSize  < LITE_WASM_TRACE_HEAD ? t.inSize  : LITE_WASM_TRACE_HEAD;
            unsigned int oh = t.outSize < LITE_WASM_TRACE_HEAD ? t.outSize : LITE_WASM_TRACE_HEAD;
            unsigned int sc = t.stateSize < LITE_WASM_TRACE_STATE ? t.stateSize : LITE_WASM_TRACE_STATE;
            e["inHex"] = liteWasmHex(t.inHead, ih);
            e["outHex"] = liteWasmHex(t.outHead, oh);
            e["stateBeforeHex"] = liteWasmHex(t.stateBefore, sc);
            e["stateAfterHex"] = liteWasmHex(t.stateAfter, sc);
            if (!t.trap.empty()) e["trap"] = t.trap;
            Json::Value hc(Json::arrayValue);
            for (const auto &h : t.hostCalls) { Json::Value x; x["name"] = h.name; x["detail"] = h.detail; hc.append(x); }
            e["hostCalls"] = hc;
            arr.append(e);
        }
        json["entries"] = arr;
        cb(HttpResponse::newHttpJsonResponse(json));
    }
    // GET /live/v1/dev/debug?on=0|1 — toggle trace capture (off by default; on adds per-call overhead).
    inline void devDebug(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb)
    {
        auto on = req->getParameter("on");
        if (!on.empty()) g_liteWasmDebug.store(on == "1" || on == "true", std::memory_order_relaxed);
        Json::Value json; json["enabled"] = liteWasmDebugEnabled();
        cb(HttpResponse::newHttpJsonResponse(json));
    }
    // GET /live/v1/dev/debug-clear — drop all captured traces.
    inline void devDebugClear(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb)
    {
        (void)req; liteWasmTraceClear();
        Json::Value json; json["cleared"] = true;
        cb(HttpResponse::newHttpJsonResponse(json));
    }
#endif

#if ADDON_TX_STATUS_REQUEST
    // Exact tx confirmation: is transaction <tx> (60-char id) included+processed in tick <tick>?
    // Reads the qli tx-status store (confirmedTx, keyed per tick). Lets tooling wait for a specific
    // tx instead of guessing a tick margin. found => included; processed => node ticked past <tick>
    // (so a false `found` with processed=true means the tx was dropped/not accepted).
    inline void txStatus(const HttpRequestPtr &req,
                         std::function<void(const HttpResponsePtr &)> &&cb,
                         const std::string &tickStr, const std::string &txId)
    {
        Json::Value result;
        unsigned int tick = (unsigned int)strtoul(tickStr.c_str(), nullptr, 10);
        result["tick"] = tick;
        result["currentTick"] = system.tick;
        result["txId"] = txId;

        // tx id is the digest in the identity alphabet, lowercased — uppercase, then decode to m256i.
        std::string up = txId;
        for (auto &ch : up) if (ch >= 'a' && ch <= 'z') ch -= 32;
        m256i target;
        getPublicKeyFromIdentity(reinterpret_cast<const unsigned char *>(up.c_str()), target.m256i_u8);

        // Locate the tick in the confirmed-tx store (current epoch, or kept ticks of the previous one).
        bool inRange = false;
        int tickIndex = 0;
        if (tick >= txStatusData.confirmedTxCurrentEpochBeginTick && tick < txStatusData.confirmedTxCurrentEpochBeginTick + MAX_NUMBER_OF_TICKS_PER_EPOCH)
        {
            tickIndex = tick - txStatusData.confirmedTxCurrentEpochBeginTick;
            inRange = true;
        }
        else if (txStatusData.confirmedTxPreviousEpochBeginTick != 0 && tick >= txStatusData.confirmedTxPreviousEpochBeginTick && tick < txStatusData.confirmedTxCurrentEpochBeginTick)
        {
            tickIndex = tick - txStatusData.confirmedTxPreviousEpochBeginTick + MAX_NUMBER_OF_TICKS_PER_EPOCH;
            inRange = true;
        }

        bool found = false, moneyFlew = false;
        if (inRange)
        {
            ACQUIRE(confirmedTxLock);
            unsigned int start = txStatusData.tickTxIndexStart[tickIndex];
            unsigned int count = txStatusData.tickTxCounter[tickIndex];
            for (unsigned int i = 0; i < count; i++)
            {
                ConfirmedTx &c = confirmedTx[start + i];
                if (c.digest == target) { found = true; moneyFlew = (c.moneyFlew != 0); break; }
            }
            RELEASE(confirmedTxLock);
        }
        result["found"] = found;
        result["moneyFlew"] = moneyFlew;
        result["processed"] = (system.tick > tick); // verdict is final once the node ticked past <tick>
        cb(HttpResponse::newHttpJsonResponse(result));
    }
#endif

#if defined(TESTNET)
    // Testnet dev only: a prefilled (funded) seed so tooling can sign deploy txs with no seed set.
    inline void devFundedSeed(const HttpRequestPtr &req, std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        if (std::size(broadcastedComputorSeeds) > 0)
            json["seed"] = std::string((const char *)broadcastedComputorSeeds[0]);
        cb(HttpResponse::newHttpJsonResponse(json));
    }
#endif
#endif

    inline void querySmartContract(const HttpRequestPtr &req,
                                   std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value result;
        try
        {
            auto json = req->getJsonObject();
            if (!json)
            {
                result["code"] = 3;
                result["message"] = "Invalid JSON";
                cb(HttpResponse::newHttpJsonResponse(result));
                return;
            }

            unsigned int contractIndex = (*json)["contractIndex"].asUInt();
            // contract indices: 1..contractCount-1. idx 0 is the null/burn sentinel.
            if (contractIndex < 1 || contractIndex >= contractCount)
            {
                result["code"] = 3;
                result["message"] = "contractIndex out of range";
                auto res = HttpResponse::newHttpJsonResponse(result);
                res->setStatusCode(k400BadRequest);
                cb(res);
                return;
            }
            unsigned short inputType = (*json)["inputType"].asUInt();
            unsigned short inputSize = (*json)["inputSize"].asUInt();
            std::string requestData = (*json)["requestData"].asString();
            std::vector<uint8_t> inputData = base64_decode(requestData);
            if (inputData.size() != inputSize)
            {
                result["code"] = 3;
                result["message"] = "Input size mismatch";
                auto res = HttpResponse::newHttpJsonResponse(result);
                res->setStatusCode(k400BadRequest);
                cb(res);
                return;
            }
            // Guard: unregistered function (e.g. a dynamic slot whose .so failed to load) would
            // otherwise call a null fn ptr (contract_exec.h has no null check) and crash the node.
            if (!contractUserFunctions[contractIndex][inputType])
            {
                result["code"] = 3;
                result["message"] = "No function registered at the given inputType";
                auto res = HttpResponse::newHttpJsonResponse(result);
                res->setStatusCode(k400BadRequest);
                cb(res);
                return;
            }
            QpiContextUserFunctionCall qpiContext(contractIndex);
            auto errorCode = qpiContext.call(inputType, inputData.data(), inputSize);
            if (errorCode == NoContractError)
            {
                // success: respond with function output
                std::vector<uint8_t> responseData(qpiContext.outputSize);
                copyMem(responseData.data(), qpiContext.outputBuffer, qpiContext.outputSize);
                result["responseData"] = base64_encode(responseData);
                cb(HttpResponse::newHttpJsonResponse(result));
            }
            else
            {
                result["code"] = -1;
                result["message"] = "Error calling smart contract function: " + std::to_string(errorCode);
                auto res = HttpResponse::newHttpJsonResponse(result);
                res->setStatusCode(k500InternalServerError);
                cb(res);
            }
        }
        catch (const std::exception &e)
        {
            result["code"] = -1;
            result["message"] = "Exception: " + std::string(e.what());
            auto res = HttpResponse::newHttpJsonResponse(result);
            res->setStatusCode(k500InternalServerError);
            cb(res);
        }
    }
};
} // namespace RpcLive
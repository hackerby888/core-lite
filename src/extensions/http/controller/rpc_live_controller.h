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
#ifdef LITE_WASM_SC
    ADD_METHOD_TO(RpcLiveController::dynRegistry, "/live/v1/dyn-registry", Get);
    ADD_METHOD_TO(RpcLiveController::dynUpload, "/live/v1/dyn-upload", Get);
    ADD_METHOD_TO(RpcLiveController::logStats, "/live/v1/log-stats", Get);
    ADD_METHOD_TO(RpcLiveController::debugTrace, "/live/v1/debug-trace", Get);
    ADD_METHOD_TO(RpcLiveController::devDebug, "/live/v1/dev/debug", Get);
    ADD_METHOD_TO(RpcLiveController::devDebugClear, "/live/v1/dev/debug-clear", Get);
    ADD_METHOD_TO(RpcLiveController::devStateRead, "/live/v1/dev/state-read", Get);
    ADD_METHOD_TO(RpcLiveController::devContractDigest, "/live/v1/dev/contract-digest", Get);
#if ADDON_TX_STATUS_REQUEST
    ADD_METHOD_TO(RpcLiveController::txStatus, "/live/v1/tx-status/{tick}/{tx}", Get);
#endif
#if defined(TESTNET)
    ADD_METHOD_TO(RpcLiveController::devFundedSeed, "/live/v1/dev/funded-seed", Get);
    ADD_METHOD_TO(RpcLiveController::devFundedSeeds, "/live/v1/dev/funded-seeds", Get);
    ADD_METHOD_TO(RpcLiveController::devPutContractSource, "/live/v1/dev/contract-source", Post);
    ADD_METHOD_TO(RpcLiveController::devEpochInfo, "/live/v1/dev/epoch-info", Get);
    ADD_METHOD_TO(RpcLiveController::devAdvanceTick, "/live/v1/dev/advance-tick", Get);
    ADD_METHOD_TO(RpcLiveController::devAdvanceToLast, "/live/v1/dev/advance-to-last", Get);
    ADD_METHOD_TO(RpcLiveController::devAdvanceEpoch, "/live/v1/dev/advance-epoch", Get);
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

#ifdef LITE_WASM_SC
    // Return reserved slots and their registered entry points.
    inline void dynRegistry(const HttpRequestPtr &req,
                            std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        Json::Value contractsJson(Json::arrayValue);
        for (unsigned int i = 0; i < WASM_RESERVED_SLOT_COUNT; i++)
        {
            const Wasm::Runtime::ContractSlot &slot = Wasm::Runtime::contractSlots[i];
            const unsigned int slotIndex = WASM_RESERVED_SLOT_BASE + i;
            Json::Value contractJson;
            contractJson["index"] = slotIndex;
            contractJson["armed"] = slot.armed;
            contractJson["constructed"] = slot.constructed;
            contractJson["version"] = slot.version;
            contractJson["name"] = std::string(slot.name);

            char hashHex[65];
            for (int byteIndex = 0; byteIndex < 32; byteIndex++)
            {
                snprintf(
                    hashHex + byteIndex * 2,
                    3,
                    "%02x",
                    slot.codeHash[byteIndex]);
            }
            contractJson["codeHash"] = std::string(hashHex, 64);

            Json::Value functionsJson(Json::arrayValue);
            Json::Value proceduresJson(Json::arrayValue);
            if (slot.armed)
            {
                for (unsigned int inputType = 1; inputType <= 65535; inputType++)
                {
                    if (contractUserFunctions[slotIndex][inputType])
                    {
                        Json::Value entry;
                        entry["inputType"] = inputType;
                        entry["inputSize"] =
                            contractUserFunctionInputSizes[slotIndex][inputType];
                        entry["outputSize"] =
                            contractUserFunctionOutputSizes[slotIndex][inputType];
                        functionsJson.append(entry);
                    }
                    if (contractUserProcedures[slotIndex][inputType])
                    {
                        Json::Value entry;
                        entry["inputType"] = inputType;
                        entry["inputSize"] =
                            contractUserProcedureInputSizes[slotIndex][inputType];
                        entry["outputSize"] =
                            contractUserProcedureOutputSizes[slotIndex][inputType];
                        proceduresJson.append(entry);
                    }
                }
            }
            contractJson["functions"] = functionsJson;
            contractJson["procedures"] = proceduresJson;
            contractJson["source"] = slot.sourceH;
            contractJson["lastError"] = Wasm::Runtime::lastTrap(slotIndex);
            contractsJson.append(contractJson);
        }
        json["slotBase"] = (unsigned int)WASM_RESERVED_SLOT_BASE;
        json["slotCount"] = (unsigned int)WASM_RESERVED_SLOT_COUNT;
        json["contracts"] = contractsJson;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Store node-local source used for inter-contract type resolution.
    inline void devPutContractSource(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        const int slotIndex = std::atoi(req->getParameter("slot").c_str());
        const int localIndex = slotIndex - (int)WASM_RESERVED_SLOT_BASE;
        if (localIndex < 0 || localIndex >= (int)WASM_RESERVED_SLOT_COUNT)
        {
            json["ok"] = false;
            json["error"] = "bad slot";
            cb(HttpResponse::newHttpJsonResponse(json));
            return;
        }
        Wasm::Runtime::contractSlots[localIndex].sourceH = std::string(req->getBody());
        json["ok"] = true;
        json["slot"] = slotIndex;
        json["len"] =
            (Json::UInt)Wasm::Runtime::contractSlots[localIndex].sourceH.size();
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Return progress for the active dynamic-contract upload.
    inline void dynUpload(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        const Wasm::Runtime::ModuleUpload &upload = Wasm::Runtime::moduleUpload;
        char sessionId[32];
        snprintf(
            sessionId,
            sizeof(sessionId),
            "%llu",
            (unsigned long long)upload.sessionId);
        json["active"] = upload.active;
        // JSON cannot represent every 64-bit session ID exactly.
        json["sessionId"] = std::string(sessionId);
        json["totalSize"] = upload.totalSize;
        json["chunkSize"] = 1008u;
        json["chunkCount"] = upload.chunkCount;
        json["receivedCount"] = upload.receivedCount;
        json["complete"] = upload.active && upload.receivedCount == upload.chunkCount;

        char hashHex[65];
        for (int byteIndex = 0; byteIndex < 32; byteIndex++)
        {
            snprintf(
                hashHex + byteIndex * 2,
                3,
                "%02x",
                upload.finalHash[byteIndex]);
        }
        json["finalHash"] = std::string(hashHex, 64);

        // Cap the missing-sequence response for large uploads.
        Json::Value missing(Json::arrayValue);
        unsigned int missingCount = 0;
        const unsigned int CAP = 4096;
        if (upload.active)
        {
            for (unsigned int sequence = 0; sequence < upload.chunkCount; sequence++)
            {
                const unsigned int byteIndex = sequence >> 3;
                const unsigned int bit = 1u << (sequence & 7);
                if (!(Wasm::Runtime::receivedChunkBits[byteIndex] & bit))
                {
                    if (missingCount < CAP)
                    {
                        missing.append(sequence);
                    }
                    missingCount++;
                }
            }
        }
        json["missing"] = missing;
        json["missingCount"] = missingCount;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Return the current log ID and a small recent sample.
    inline void logStats(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        const unsigned long long currentLogId = qLogger::logId;
        json["logId"] = (Json::UInt64)currentLogId;
        Json::Value recentEntries(Json::arrayValue);
        const unsigned long long firstLogId =
            currentLogId > 16 ? currentLogId - 16 : 0;
        for (unsigned long long logId = firstLogId; logId < currentLogId; logId++)
        {
            auto logEntry = qLogger::tmpLogBuffer.find(logId);
            if (logEntry == qLogger::tmpLogBuffer.end() || !logEntry->second)
            {
                continue;
            }

            const unsigned char *bytes = (const unsigned char *)logEntry->second;
            const unsigned int sizeAndType = *((unsigned int *)(bytes + 6));
            const unsigned int messageSize = sizeAndType & 0xFFFFFF;
            Json::Value entry;
            entry["logId"] = (Json::UInt64)logId;
            entry["type"] = (unsigned int)(sizeAndType >> 24);
            if (messageSize >= 4)
            {
                entry["contractIndex"] = *((unsigned int *)(bytes + 26));
            }

            char payloadHex[65];
            const unsigned int capturedSize = messageSize < 32 ? messageSize : 32;
            for (unsigned int i = 0; i < capturedSize; i++)
            {
                snprintf(payloadHex + i * 2, 3, "%02x", bytes[26 + i]);
            }
            entry["payloadHex"] = std::string(payloadHex, capturedSize * 2);
            recentEntries.append(entry);
        }
        json["recent"] = recentEntries;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Return recent Wasm call traces after the requested sequence.
    inline void debugTrace(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        unsigned long long since = 0;
        unsigned int limit = 64;
        try
        {
            const auto value = req->getParameter("since");
            if (!value.empty())
            {
                since = std::stoull(value);
            }
        }
        catch (...)
        {
        }
        try
        {
            const auto value = req->getParameter("limit");
            if (!value.empty())
            {
                limit = (unsigned int)std::stoul(value);
            }
        }
        catch (...)
        {
        }
        if (limit == 0 || limit > WASM_TRACE_RING_CAPACITY)
        {
            limit = WASM_TRACE_RING_CAPACITY;
        }

        Json::Value json;
        json["enabled"] = Wasm::Runtime::traceEnabled();
        Json::Value entries(Json::arrayValue);
        for (const auto &trace : Wasm::Runtime::traceSnapshot(since, limit))
        {
            Json::Value entry;
            entry["seq"] = (Json::UInt64)trace.sequence;
            entry["tick"] = trace.tick;
            entry["index"] = trace.contractIndex;
            entry["entry"] = (unsigned int)trace.inputType;
            entry["kind"] = (unsigned int)trace.kind;
            entry["ok"] = trace.ok;
            entry["execNs"] = (Json::UInt64)trace.executionNanoseconds;
            entry["inSize"] = trace.inputSize;
            entry["outSize"] = trace.outputSize;
            entry["stateSize"] = trace.stateSize;
            entry["stateTruncated"] = trace.stateTruncated;
            entry["invocator"] = Wasm::Runtime::hex(&trace.invocator, 32);
            entry["invocationReward"] = (Json::Int64)trace.invocationReward;

            const unsigned int inputHeadSize = trace.inputSize < WASM_TRACE_CAPTURE_SIZE
                ? trace.inputSize
                : WASM_TRACE_CAPTURE_SIZE;
            const unsigned int outputHeadSize = trace.outputSize < WASM_TRACE_CAPTURE_SIZE
                ? trace.outputSize
                : WASM_TRACE_CAPTURE_SIZE;
            entry["inHex"] = Wasm::Runtime::hex(trace.inputHead, inputHeadSize);
            entry["outHex"] = Wasm::Runtime::hex(trace.outputHead, outputHeadSize);

            Json::Value stateDiff(Json::arrayValue);
            for (const auto &run : trace.stateDiff)
            {
                Json::Value diff;
                diff["off"] = run.offset;
                diff["before"] = run.before;
                diff["after"] = run.after;
                stateDiff.append(diff);
            }
            entry["stateDiff"] = stateDiff;
            if (!trace.trap.empty())
            {
                entry["trap"] = trace.trap;
            }

            Json::Value hostCalls(Json::arrayValue);
            for (const auto &hostCall : trace.hostCalls)
            {
                Json::Value call;
                call["name"] = hostCall.name;
                call["detail"] = hostCall.detail;
                hostCalls.append(call);
            }
            entry["hostCalls"] = hostCalls;

            Json::Value logs(Json::arrayValue);
            for (const auto &log : trace.logs)
            {
                Json::Value logEntry;
                logEntry["type"] = (unsigned int)log.type;
                logEntry["size"] = log.size;
                logEntry["hex"] = log.hex;
                logs.append(logEntry);
            }
            entry["logs"] = logs;
            entries.append(entry);
        }
        json["entries"] = entries;
        cb(HttpResponse::newHttpJsonResponse(json));
    }
    // Toggle Wasm trace capture.
    inline void devDebug(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        auto on = req->getParameter("on");
        if (!on.empty())
        {
            Wasm::Runtime::setTraceEnabled(on == "1" || on == "true");
        }
        Json::Value json;
        json["enabled"] = Wasm::Runtime::traceEnabled();
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Drop all captured traces.
    inline void devDebugClear(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        (void)req;
        Wasm::Runtime::clearTrace();
        Json::Value json;
        json["cleared"] = true;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Return a bounded best-effort snapshot of contract state bytes.
    inline void devStateRead(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        const int slotIndex = std::atoi(req->getParameter("slot").c_str());
        unsigned long long offset =
            strtoull(req->getParameter("off").c_str(), nullptr, 10);
        unsigned long long length =
            strtoull(req->getParameter("len").c_str(), nullptr, 10);
        const int localIndex = slotIndex - (int)WASM_RESERVED_SLOT_BASE;
        bool validSlot;
        unsigned long long stateSize;
        if (slotIndex >= (int)WASM_RESERVED_SLOT_BASE)
        {
            validSlot = localIndex >= 0
                && localIndex < (int)WASM_RESERVED_SLOT_COUNT
                && Wasm::Runtime::isContractLoaded(slotIndex)
                && contractStates[slotIndex];
            stateSize = validSlot
                ? Wasm::Runtime::effectiveStateSize(
                    slotIndex,
                    contractDescriptions[slotIndex].stateSize)
                : 0;
        }
        else
        {
            validSlot = slotIndex >= 1
                && slotIndex < (int)contractCount
                && contractStates[slotIndex];
            stateSize = validSlot ? contractDescriptions[slotIndex].stateSize : 0;
        }
        if (!validSlot)
        {
            json["error"] = "bad slot";
            cb(HttpResponse::newHttpJsonResponse(json));
            return;
        }

        if (offset > stateSize)
        {
            offset = stateSize;
        }
        if (length > 262144ull)
        {
            length = 262144ull;
        }
        if (offset + length > stateSize)
        {
            length = stateSize - offset;
        }

        const unsigned char *state = contractStates[slotIndex];
        static const char *hexDigits = "0123456789abcdef";
        std::string hex;
        hex.reserve((size_t)length * 2);
        for (unsigned long long i = 0; i < length; i++)
        {
            hex += hexDigits[state[offset + i] >> 4];
            hex += hexDigits[state[offset + i] & 15];
        }
        json["off"] = (Json::UInt64)offset;
        json["len"] = (Json::UInt64)length;
        json["stateSize"] = (Json::UInt64)stateSize;
        json["hex"] = hex;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Return the canonical K12 digest of a contract's effective state.
    inline void devContractDigest(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        const int slotIndex = std::atoi(req->getParameter("slot").c_str());
        const int localIndex = slotIndex - (int)WASM_RESERVED_SLOT_BASE;
        bool validSlot;
        unsigned long long stateSize;
        if (slotIndex >= (int)WASM_RESERVED_SLOT_BASE)
        {
            validSlot = localIndex >= 0
                && localIndex < (int)WASM_RESERVED_SLOT_COUNT
                && Wasm::Runtime::isContractLoaded(slotIndex)
                && contractStates[slotIndex];
            stateSize = validSlot
                ? Wasm::Runtime::effectiveStateSize(
                    slotIndex,
                    contractDescriptions[slotIndex].stateSize)
                : 0;
        }
        else
        {
            validSlot = slotIndex >= 1
                && slotIndex < (int)contractCount
                && contractStates[slotIndex];
            stateSize = validSlot ? contractDescriptions[slotIndex].stateSize : 0;
        }
        if (!validSlot)
        {
            json["error"] = "bad slot";
            cb(HttpResponse::newHttpJsonResponse(json));
            return;
        }

        unsigned char digest[32];
        KangarooTwelve(
            contractStates[slotIndex],
            (unsigned int)stateSize,
            digest,
            32);
        static const char *hexDigits = "0123456789abcdef";
        std::string hex;
        hex.reserve(64);
        for (int i = 0; i < 32; i++)
        {
            hex += hexDigits[digest[i] >> 4];
            hex += hexDigits[digest[i] & 15];
        }
        json["slot"] = slotIndex;
        json["stateSize"] = (Json::UInt64)stateSize;
        json["digest"] = hex;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

#if ADDON_TX_STATUS_REQUEST
    // Return exact inclusion and processing status for one transaction.
    inline void txStatus(const HttpRequestPtr &,
                         std::function<void(const HttpResponsePtr &)> &&cb,
                         const std::string &tickString,
                         const std::string &transactionId)
    {
        Json::Value result;
        const unsigned int tick =
            (unsigned int)strtoul(tickString.c_str(), nullptr, 10);
        result["tick"] = tick;
        result["currentTick"] = system.tick;
        result["txId"] = transactionId;

        std::string uppercaseId = transactionId;
        for (auto &character : uppercaseId)
        {
            if (character >= 'a' && character <= 'z')
            {
                character -= 32;
            }
        }
        m256i targetDigest;
        getPublicKeyFromIdentity(
            reinterpret_cast<const unsigned char *>(uppercaseId.c_str()),
            targetDigest.m256i_u8);

        // Search the current or retained previous epoch.
        bool inRange = false;
        int tickIndex = 0;
        if (tick >= txStatusData.confirmedTxCurrentEpochBeginTick
            && tick < txStatusData.confirmedTxCurrentEpochBeginTick
                + MAX_NUMBER_OF_TICKS_PER_EPOCH)
        {
            tickIndex = tick - txStatusData.confirmedTxCurrentEpochBeginTick;
            inRange = true;
        }
        else if (txStatusData.confirmedTxPreviousEpochBeginTick != 0
                 && tick >= txStatusData.confirmedTxPreviousEpochBeginTick
                 && tick < txStatusData.confirmedTxCurrentEpochBeginTick)
        {
            tickIndex = tick - txStatusData.confirmedTxPreviousEpochBeginTick
                + MAX_NUMBER_OF_TICKS_PER_EPOCH;
            inRange = true;
        }

        bool found = false;
        bool moneyFlew = false;
        if (inRange)
        {
            ACQUIRE(confirmedTxLock);
            const unsigned int firstIndex =
                txStatusData.tickTxIndexStart[tickIndex];
            const unsigned int transactionCount =
                txStatusData.tickTxCounter[tickIndex];
            for (unsigned int i = 0; i < transactionCount; i++)
            {
                const ConfirmedTx &transaction = confirmedTx[firstIndex + i];
                if (transaction.digest == targetDigest)
                {
                    found = true;
                    moneyFlew = transaction.moneyFlew != 0;
                    break;
                }
            }
            RELEASE(confirmedTxLock);
        }
        result["found"] = found;
        result["moneyFlew"] = moneyFlew;
        result["processed"] = system.tick > tick;
        cb(HttpResponse::newHttpJsonResponse(result));
    }
#endif

#if defined(TESTNET)
    // Return a pre-funded testnet seed.
    inline void devFundedSeed(
        const HttpRequestPtr &,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        Json::Value json;
        if (std::size(broadcastedComputorSeeds) > 0)
        {
            json["seed"] = std::string((const char *)broadcastedComputorSeeds[0]);
        }
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Return the requested number of pre-funded testnet seeds.
    inline void devFundedSeeds(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        const unsigned int total = (unsigned int)std::size(broadcastedComputorSeeds);
        unsigned int limit = 32;
        try
        {
            const auto value = req->getParameter("limit");
            if (!value.empty())
            {
                limit = (unsigned int)std::stoul(value);
            }
        }
        catch (...)
        {
        }
        if (limit == 0 || limit > total)
        {
            limit = total;
        }

        Json::Value json;
        Json::Value seeds(Json::arrayValue);
        for (unsigned int i = 0; i < limit; i++)
        {
            seeds.append(std::string((const char *)broadcastedComputorSeeds[i]));
        }
        json["seeds"] = seeds;
        json["count"] = total;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    inline unsigned int liteDevEpochLastTick() const
    {
        return system.initialTick + (unsigned int)TESTNET_EPOCH_DURATION - 1;
    }

    // Fast-forward with a timeout, then restore the configured tick delay.
    inline unsigned int liteDevFastForwardTo(unsigned int target, unsigned int timeoutMs)
    {
        if (system.tick >= target)
        {
            return system.tick;
        }

        const unsigned long long savedTickDelay = tickDelay;
        tickDelay = 0;
        const auto startTime = std::chrono::steady_clock::now();
        while (system.tick < target)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - startTime);
            if (elapsed.count() > (long long)timeoutMs)
            {
                break;
            }
        }
        tickDelay = savedTickDelay;
        return system.tick;
    }

    // Return the current testnet epoch window.
    inline void devEpochInfo(
        const HttpRequestPtr &,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        const unsigned int lastTick = liteDevEpochLastTick();
        Json::Value json;
        json["epoch"] = (unsigned int)system.epoch;
        json["tick"] = system.tick;
        json["initialTick"] = system.initialTick;
        json["epochLastTick"] = lastTick;
        json["ticksLeft"] = system.tick <= lastTick
            ? lastTick - system.tick
            : 0u;
        json["duration"] = (unsigned int)TESTNET_EPOCH_DURATION;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Advance without crossing the current epoch boundary.
    inline void devAdvanceTick(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        unsigned int requestedTicks = 1;
        try
        {
            const auto value = req->getParameter("n");
            if (!value.empty())
            {
                requestedTicks = (unsigned int)std::stoul(value);
            }
        }
        catch (...)
        {
        }
        if (requestedTicks == 0)
        {
            requestedTicks = 1;
        }

        const unsigned int startTick = system.tick;
        const unsigned int lastTick = liteDevEpochLastTick();
        unsigned int targetTick = startTick + requestedTicks;
        const bool capped = targetTick > lastTick;
        if (capped)
        {
            targetTick = lastTick;
        }
        const unsigned int reachedTick = liteDevFastForwardTo(targetTick, 12000);

        Json::Value json;
        json["from"] = startTick;
        json["requested"] = requestedTicks;
        json["target"] = targetTick;
        json["reached"] = reachedTick;
        json["epochLastTick"] = lastTick;
        json["cappedAtEpochEnd"] = capped;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Advance to a safe gap before the current epoch boundary.
    inline void devAdvanceToLast(
        const HttpRequestPtr &req,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        unsigned int gap = 3;
        try
        {
            const auto value = req->getParameter("gap");
            if (!value.empty())
            {
                gap = (unsigned int)std::stoul(value);
            }
        }
        catch (...)
        {
        }

        const unsigned int startTick = system.tick;
        const unsigned int lastTick = liteDevEpochLastTick();
        const unsigned int targetTick = lastTick > gap
            ? lastTick - gap
            : lastTick;
        const unsigned int reachedTick = liteDevFastForwardTo(targetTick, 12000);

        Json::Value json;
        json["from"] = startTick;
        json["target"] = targetTick;
        json["reached"] = reachedTick;
        json["epochLastTick"] = lastTick;
        json["epoch"] = (unsigned int)system.epoch;
        cb(HttpResponse::newHttpJsonResponse(json));
    }

    // Advance through the node's normal epoch transition.
    inline void devAdvanceEpoch(
        const HttpRequestPtr &,
        std::function<void(const HttpResponsePtr &)> &&cb)
    {
        const unsigned int startEpoch = (unsigned int)system.epoch;
        const unsigned int startTick = system.tick;
        const unsigned long long savedTickDelay = tickDelay;
        tickDelay = 0;
        forceSwitchEpoch = true;
        const auto startTime = std::chrono::steady_clock::now();
        while ((unsigned int)system.epoch == startEpoch)
        {
            // Keep the transition moving through its clean-memory wait.
            epochTransitionCleanMemoryFlag = 1;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - startTime);
            if (elapsed.count() > 25000)
            {
                break;
            }
        }
        tickDelay = savedTickDelay;
        if ((unsigned int)system.epoch == startEpoch)
        {
            forceSwitchEpoch = false;
        }

        Json::Value json;
        json["fromEpoch"] = startEpoch;
        json["toEpoch"] = (unsigned int)system.epoch;
        json["fromTick"] = startTick;
        json["tick"] = system.tick;
        json["initialTick"] = system.initialTick;
        json["switched"] = (unsigned int)system.epoch != startEpoch;
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
            // Reject unregistered functions before dispatching through a null pointer.
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

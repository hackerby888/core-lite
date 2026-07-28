#pragma once

// Live endpoints as RpcReq->RpcResp router handlers.

#ifdef __linux__

#include <string>
#include <vector>
#include <memory>
#include <cmath>
#include <algorithm>
#include "extensions/rpc/rpc_core.h"

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

#endif // __linux__

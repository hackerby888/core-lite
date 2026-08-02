#pragma once

// Static helpers for the query-v2 RPC handlers. No Drogon types; pure data manipulation.

#include "extensions/http/utils.h"
#include "extensions/utils.h"
#include <fmt/format.h>
#include <optional>
#include <deque>

namespace RpcQueryV2
{
#if ENABLED_LOGGING
// Encode a 32-byte pubkey as a 60-char identity string (uppercase or lowercase per the spec).
static inline std::string identityFromBytes(const unsigned char *bytes, bool lowercase)
{
    CHAR16 id[61] = {};
    getIdentity(const_cast<unsigned char *>(bytes), id, lowercase);
    return wchar_to_string(id);
}

// Read N bytes from an unaligned offset into the payload.
template <typename T>
static inline T readUnaligned(const unsigned char *p, size_t off)
{
    T v{};
    memcpy(&v, p + off, sizeof(T));
    return v;
}

// Return the value of a filterable field, as the string representation the spec uses
// ("123" for numbers, 60-char identity for pubkeys, 7-char trimmed for asset name).
// Returns std::nullopt if the field is not present on this payload type.
static inline std::optional<std::string> payloadFieldValue(
    unsigned int logType,
    const unsigned char *p,
    unsigned int payloadSize,
    const std::string &field)
{
    auto fitId = [&](size_t off, bool lowercase = false) -> std::optional<std::string>
    {
        if (off + 32 > payloadSize) return std::nullopt;
        return identityFromBytes(p + off, lowercase);
    };
    auto fitU64 = [&](size_t off) -> std::optional<std::string>
    {
        if (off + 8 > payloadSize) return std::nullopt;
        return std::to_string(readUnaligned<unsigned long long>(p, off));
    };
    auto fitI64 = [&](size_t off) -> std::optional<std::string>
    {
        if (off + 8 > payloadSize) return std::nullopt;
        return std::to_string(readUnaligned<long long>(p, off));
    };
    auto fitU32 = [&](size_t off) -> std::optional<std::string>
    {
        if (off + 4 > payloadSize) return std::nullopt;
        return std::to_string(readUnaligned<unsigned int>(p, off));
    };
    auto fitI32 = [&](size_t off) -> std::optional<std::string>
    {
        if (off + 4 > payloadSize) return std::nullopt;
        return std::to_string(readUnaligned<int>(p, off));
    };
    auto fitU8 = [&](size_t off) -> std::optional<std::string>
    {
        if (off + 1 > payloadSize) return std::nullopt;
        return std::to_string((unsigned int)p[off]);
    };
    auto fitName = [&](size_t off) -> std::optional<std::string>
    {
        if (off + 7 > payloadSize) return std::nullopt;
        char name[8] = {0};
        memcpy(name, p + off, 7);
        return std::string(name);
    };

    switch (logType)
    {
    case QU_TRANSFER:
        if (field == "source") return fitId(0);
        if (field == "destination") return fitId(32);
        if (field == "amount") return fitI64(64);
        break;
    case ASSET_ISSUANCE:
        if (field == "assetIssuer") return fitId(0);
        if (field == "numberOfShares") return fitI64(32);
        if (field == "managingContractIndex") return fitI64(40);
        if (field == "assetName") return fitName(48);
        break;
    case ASSET_OWNERSHIP_CHANGE:
    case ASSET_POSSESSION_CHANGE:
        if (field == "source") return fitId(0);
        if (field == "destination") return fitId(32);
        if (field == "assetIssuer") return fitId(64);
        if (field == "numberOfShares") return fitI64(96);
        if (field == "managingContractIndex") return fitI64(104);
        if (field == "assetName") return fitName(112);
        break;
    case ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE:
        if (field == "assetIssuer") return fitId(32);
        if (field == "numberOfShares") return fitI64(72);
        if (field == "assetName") return fitName(80);
        break;
    case ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE:
        if (field == "assetIssuer") return fitId(64);
        if (field == "numberOfShares") return fitI64(104);
        if (field == "assetName") return fitName(112);
        break;
    case BURNING:
        if (field == "source") return fitId(0);
        if (field == "amount") return fitI64(32);
        if (field == "contractIndex") return fitU32(40);
        break;
    case CONTRACT_ERROR_MESSAGE:
    case CONTRACT_WARNING_MESSAGE:
    case CONTRACT_INFORMATION_MESSAGE:
    case CONTRACT_DEBUG_MESSAGE:
        if (field == "contractIndex") return fitU32(0);
        if (field == "contractMessageType") return fitU32(4);
        break;
    case CONTRACT_RESERVE_DEDUCTION:
        if (field == "deductedAmount") return fitU64(0);
        if (field == "remainingAmount") return fitI64(8);
        if (field == "contractIndex") return fitU32(16);
        break;
    case ORACLE_QUERY_STATUS_CHANGE:
        if (field == "queryingEntity") return fitId(0);
        if (field == "queryId") return fitI64(32);
        if (field == "interfaceIndex") return fitU32(40);
        if (field == "queryType") return fitU8(44);
        if (field == "queryStatus") return fitU8(45);
        break;
    case ORACLE_SUBSCRIBER_MESSAGE:
        if (field == "subscriptionId") return fitI32(0);
        if (field == "interfaceIndex") return fitU32(4);
        if (field == "contractIndex") return fitU32(8);
        break;
    case CUSTOM_MESSAGE:
        if (field == "customMessage") return fitU64(0);
        break;
    default:
        break;
    }
    return std::nullopt;
}

// Compare value against a Range (gt/gte/lt/lte). All comparisons are unsigned-decimal.
// Returns true if value satisfies the range, false otherwise.
static inline bool rangeMatches(const std::string &value, const Json::Value &range)
{
    if (!range.isObject()) return true;
    unsigned long long v;
    try { v = std::stoull(value); } catch (...) { return false; }
    if (range.isMember("gt"))
    {
        try { if (!(v > std::stoull(range["gt"].asString()))) return false; }
        catch (...) { return false; }
    }
    if (range.isMember("gte"))
    {
        try { if (!(v >= std::stoull(range["gte"].asString()))) return false; }
        catch (...) { return false; }
    }
    if (range.isMember("lt"))
    {
        try { if (!(v < std::stoull(range["lt"].asString()))) return false; }
        catch (...) { return false; }
    }
    if (range.isMember("lte"))
    {
        try { if (!(v <= std::stoull(range["lte"].asString()))) return false; }
        catch (...) { return false; }
    }
    return true;
}

// Build the typed payload sub-object for an event, based on logType.
static inline void buildTypedPayload(
    unsigned int logType,
    const unsigned char *p,
    unsigned int payloadSize,
    Json::Value &out)
{
    switch (logType)
    {
    case QU_TRANSFER:
    {
        if (payloadSize < 72) break;
        Json::Value sub;
        sub["source"] = identityFromBytes(p, false);
        sub["destination"] = identityFromBytes(p + 32, false);
        sub["amount"] = std::to_string(readUnaligned<long long>(p, 64));
        out["quTransfer"] = sub;
        break;
    }
    case ASSET_ISSUANCE:
    {
        if (payloadSize < 63) break;
        Json::Value sub;
        sub["assetIssuer"] = identityFromBytes(p, false);
        sub["numberOfShares"] = std::to_string(readUnaligned<long long>(p, 32));
        sub["managingContractIndex"] = std::to_string(readUnaligned<long long>(p, 40));
        char name[8] = {0};
        memcpy(name, p + 48, 7);
        sub["assetName"] = std::string(name);
        sub["numberOfDecimalPlaces"] = (unsigned int)(unsigned char)p[55];
        char uom[8] = {0};
        memcpy(uom, p + 56, 7);
        sub["unitOfMeasurement"] = std::string(uom);
        out["assetIssuance"] = sub;
        break;
    }
    case ASSET_OWNERSHIP_CHANGE:
    case ASSET_POSSESSION_CHANGE:
    {
        if (payloadSize < 127) break;
        Json::Value sub;
        sub["source"] = identityFromBytes(p, false);
        sub["destination"] = identityFromBytes(p + 32, false);
        sub["assetIssuer"] = identityFromBytes(p + 64, false);
        sub["numberOfShares"] = std::to_string(readUnaligned<long long>(p, 96));
        char name[8] = {0};
        memcpy(name, p + 112, 7);
        sub["assetName"] = std::string(name);
        out[logType == ASSET_OWNERSHIP_CHANGE ? "assetOwnershipChange" : "assetPossessionChange"] = sub;
        break;
    }
    case ASSET_OWNERSHIP_MANAGING_CONTRACT_CHANGE:
    {
        if (payloadSize < 87) break;
        Json::Value sub;
        sub["owner"] = identityFromBytes(p, false);
        sub["assetIssuer"] = identityFromBytes(p + 32, false);
        sub["sourceContractIndex"] = std::to_string(readUnaligned<unsigned int>(p, 64));
        sub["destinationContractIndex"] = std::to_string(readUnaligned<unsigned int>(p, 68));
        sub["numberOfShares"] = std::to_string(readUnaligned<long long>(p, 72));
        char name[8] = {0};
        memcpy(name, p + 80, 7);
        sub["assetName"] = std::string(name);
        out["assetOwnershipManagingContractChange"] = sub;
        break;
    }
    case ASSET_POSSESSION_MANAGING_CONTRACT_CHANGE:
    {
        if (payloadSize < 119) break;
        Json::Value sub;
        sub["possessor"] = identityFromBytes(p, false);
        sub["owner"] = identityFromBytes(p + 32, false);
        sub["assetIssuer"] = identityFromBytes(p + 64, false);
        sub["sourceContractIndex"] = std::to_string(readUnaligned<unsigned int>(p, 96));
        sub["destinationContractIndex"] = std::to_string(readUnaligned<unsigned int>(p, 100));
        sub["numberOfShares"] = std::to_string(readUnaligned<long long>(p, 104));
        char name[8] = {0};
        memcpy(name, p + 112, 7);
        sub["assetName"] = std::string(name);
        out["assetPossessionManagingContractChange"] = sub;
        break;
    }
    case BURNING:
    {
        if (payloadSize < 44) break;
        Json::Value sub;
        sub["source"] = identityFromBytes(p, false);
        sub["amount"] = std::to_string(readUnaligned<long long>(p, 32));
        sub["contractIndex"] = std::to_string(readUnaligned<unsigned int>(p, 40));
        out["burning"] = sub;
        break;
    }
    case CONTRACT_ERROR_MESSAGE:
    case CONTRACT_WARNING_MESSAGE:
    case CONTRACT_INFORMATION_MESSAGE:
    case CONTRACT_DEBUG_MESSAGE:
    {
        if (payloadSize < 8) break;
        Json::Value sub;
        sub["contractIndex"] = std::to_string(readUnaligned<unsigned int>(p, 0));
        sub["contractMessageType"] = std::to_string(readUnaligned<unsigned int>(p, 4));
        out["smartContractMessage"] = sub;
        break;
    }
    case CONTRACT_RESERVE_DEDUCTION:
    {
        if (payloadSize < 20) break;
        Json::Value sub;
        sub["deductedAmount"] = std::to_string(readUnaligned<unsigned long long>(p, 0));
        sub["remainingAmount"] = std::to_string(readUnaligned<long long>(p, 8));
        sub["contractIndex"] = std::to_string(readUnaligned<unsigned int>(p, 16));
        out["contractReserveDeduction"] = sub;
        break;
    }
    case ORACLE_QUERY_STATUS_CHANGE:
    {
        if (payloadSize < 46) break;
        Json::Value sub;
        sub["queryingEntity"] = identityFromBytes(p, false);
        sub["queryId"] = std::to_string(readUnaligned<long long>(p, 32));
        sub["interfaceIndex"] = std::to_string(readUnaligned<unsigned int>(p, 40));
        sub["queryType"] = std::to_string((unsigned int)(unsigned char)p[44]);
        sub["queryStatus"] = std::to_string((unsigned int)(unsigned char)p[45]);
        out["oracleQueryStatusChange"] = sub;
        break;
    }
    case ORACLE_SUBSCRIBER_MESSAGE:
    {
        if (payloadSize < 24) break;
        Json::Value sub;
        sub["subscriptionId"] = std::to_string(readUnaligned<int>(p, 0));
        sub["interfaceIndex"] = std::to_string(readUnaligned<unsigned int>(p, 4));
        sub["contractIndex"] = std::to_string(readUnaligned<unsigned int>(p, 8));
        sub["periodMillis"] = std::to_string(readUnaligned<unsigned int>(p, 12));
        sub["firstQueryTimestamp"] = std::to_string(readUnaligned<unsigned long long>(p, 16));
        out["oracleSubscriberLogMessage"] = sub;
        break;
    }
    case CUSTOM_MESSAGE:
    {
        if (payloadSize < 8) break;
        Json::Value sub;
        sub["value"] = std::to_string(readUnaligned<unsigned long long>(p, 0));
        out["customMessage"] = sub;
        break;
    }
    default:
        break;
    }
}

// Build the full Event JSON object from one raw [header || payload] log blob.
static inline Json::Value eventLogToJson(
    const char *blob,
    unsigned int blobLen,
    const TickData &tickDataForTimestamp,
    const std::string &transactionHashOrEmpty,
    const Json::Value &categories)
{
    Json::Value out;
    if (blobLen < LOG_HEADER_SIZE) return out;
    const unsigned char *p = reinterpret_cast<const unsigned char *>(blob);
    unsigned short epoch = readUnaligned<unsigned short>(p, 0);
    unsigned int tick = readUnaligned<unsigned int>(p, 2);
    unsigned int sizeAndType = readUnaligned<unsigned int>(p, 6);
    unsigned long long logId = readUnaligned<unsigned long long>(p, 10);
    unsigned long long logDigest = readUnaligned<unsigned long long>(p, 18);
    unsigned int payloadSize = sizeAndType & 0xFFFFFF;
    unsigned char logType = (unsigned char)(sizeAndType >> 24);
    if (LOG_HEADER_SIZE + (unsigned long long)payloadSize > blobLen)
    {
        payloadSize = (blobLen >= LOG_HEADER_SIZE) ? (blobLen - LOG_HEADER_SIZE) : 0;
    }
    const unsigned char *payload = p + LOG_HEADER_SIZE;

    out["epoch"] = epoch;
    out["tickNumber"] = tick;
    out["timestamp"] = HttpUtils::formatTimestamp(
        tickDataForTimestamp.millisecond,
        tickDataForTimestamp.second,
        tickDataForTimestamp.minute,
        tickDataForTimestamp.hour,
        tickDataForTimestamp.day,
        tickDataForTimestamp.month,
        tickDataForTimestamp.year);
    out["transactionHash"] = transactionHashOrEmpty;
    out["logType"] = (unsigned int)logType;
    out["logId"] = std::to_string(logId);
    out["logDigest"] = std::to_string(logDigest);
    out["categories"] = categories;
    out["rawPayload"] = base64_encode(const_cast<unsigned char *>(payload), payloadSize);

    buildTypedPayload(logType, payload, payloadSize, out);
    return out;
}

// Evaluate one matched event against the request's filters.
static inline bool eventMatchesFilters(
    unsigned int logType,
    unsigned int epoch,
    unsigned int tickNumber,
    unsigned long long logId,
    const std::string &transactionHash,
    const Json::Value &categories,
    const unsigned char *payload,
    unsigned int payloadSize,
    const TickData &tickDataForTimestamp,
    const Json::Value &filters,
    const Json::Value &exclude,
    const Json::Value &should,
    const Json::Value &ranges)
{
    auto valueOf = [&](const std::string &field) -> std::optional<std::string>
    {
        if (field == "logType") return std::to_string(logType);
        if (field == "epoch") return std::to_string(epoch);
        if (field == "tickNumber") return std::to_string(tickNumber);
        if (field == "logId") return std::to_string(logId);
        if (field == "transactionHash") return transactionHash;
        if (field == "timestamp")
        {
            return HttpUtils::formatTimestamp(
                tickDataForTimestamp.millisecond,
                tickDataForTimestamp.second,
                tickDataForTimestamp.minute,
                tickDataForTimestamp.hour,
                tickDataForTimestamp.day,
                tickDataForTimestamp.month,
                tickDataForTimestamp.year);
        }
        if (field == "categories")
        {
            if (categories.isArray() && categories.size() > 0)
                return std::to_string(categories[0].asInt());
            return std::optional<std::string>();
        }
        return payloadFieldValue(logType, payload, payloadSize, field);
    };

    // Include filters: every key must match (value == filters[key]).
    if (filters.isObject())
    {
        for (const auto &key : filters.getMemberNames())
        {
            auto v = valueOf(key);
            if (!v.has_value()) return false;
            if (*v != filters[key].asString()) return false;
        }
    }

    // Exclude filters: any matching key drops the event.
    if (exclude.isObject())
    {
        for (const auto &key : exclude.getMemberNames())
        {
            auto v = valueOf(key);
            if (v.has_value() && *v == exclude[key].asString()) return false;
        }
    }

    // Top-level ranges.
    if (ranges.isObject())
    {
        for (const auto &key : ranges.getMemberNames())
        {
            auto v = valueOf(key);
            if (!v.has_value()) return false;
            if (!rangeMatches(*v, ranges[key])) return false;
        }
    }

    // Should: at least one clause must match (each clause is AND of terms+ranges).
    if (should.isArray() && should.size() > 0)
    {
        bool anyClauseMatches = false;
        for (Json::ArrayIndex i = 0; i < should.size(); i++)
        {
            const Json::Value &clause = should[i];
            bool clauseOk = true;
            const Json::Value &cTerms = clause["terms"];
            if (cTerms.isObject())
            {
                for (const auto &k : cTerms.getMemberNames())
                {
                    auto v = valueOf(k);
                    if (!v.has_value() || *v != cTerms[k].asString())
                    {
                        clauseOk = false;
                        break;
                    }
                }
            }
            if (clauseOk)
            {
                const Json::Value &cRanges = clause["ranges"];
                if (cRanges.isObject())
                {
                    for (const auto &k : cRanges.getMemberNames())
                    {
                        auto v = valueOf(k);
                        if (!v.has_value() || !rangeMatches(*v, cRanges[k]))
                        {
                            clauseOk = false;
                            break;
                        }
                    }
                }
            }
            if (clauseOk)
            {
                anyClauseMatches = true;
                break;
            }
        }
        if (!anyClauseMatches) return false;
    }

    return true;
}
#endif // ENABLED_LOGGING
} // namespace RpcQueryV2

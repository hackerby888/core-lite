#pragma once
// Resolve Wasm32 offsets for the canonical lhost import table.
#ifdef LITE_WASM_SC
#include <cstdint>
#include <type_traits>
#include <array>
#include "wasm_export.h"
#include "extensions/wasm/shared/abi_metadata.h"
#include "extensions/wasm/shared/abi_types.h"
#include "extensions/wasm/runtime/trace.h"

namespace Wasm::Runtime
{

struct CallContext
{
    const void* ctx;
    uint32_t arenaBase;
    uint32_t arenaBump;
    uint32_t arenaEnd;
    void* trace = nullptr;
};

static inline CallContext* activeCallContext(wasm_exec_env_t execEnv)
{
    return (CallContext*)wasm_runtime_get_user_data(execEnv);
}

static inline void* nativeAddress(wasm_exec_env_t execEnv, uint32_t offset)
{
    if (!offset)
    {
        return nullptr;
    }

    return wasm_runtime_addr_app_to_native(wasm_runtime_get_module_inst(execEnv), offset);
}

static inline void traceHostCall(
    CallContext* callContext,
    const char* name,
    const std::string& detail)
{
    if (callContext && callContext->trace)
    {
        recordHostCall(
            (TraceEntry*)callContext->trace,
            name,
            detail);
    }
}

// WAMR signatures use i/I/f/F for i32/i64/f32/f64. Pointers and narrow integers use i32.
template<class T>
constexpr size_t safeSizeOf()
{
    if constexpr (std::is_void_v<T>)
    {
        return 8;
    }
    else
    {
        return sizeof(T);
    }
}

template<class T>
constexpr char signatureCharacter()
{
    if constexpr (std::is_void_v<T>)
    {
        return '\0';
    }
    else if constexpr (std::is_pointer_v<T>)
    {
        return 'i';
    }
    else if constexpr (std::is_floating_point_v<T>)
    {
        return sizeof(T) == 4 ? 'f' : 'F';
    }
    else
    {
        return safeSizeOf<T>() <= 4 ? 'i' : 'I';
    }
}

template<class T>
using AbiType =
    std::conditional_t<std::is_void_v<T>, void,
      std::conditional_t<std::is_pointer_v<T>, uint32_t,
        std::conditional_t<(std::is_integral_v<T> && safeSizeOf<T>() <= 4), uint32_t, T>>>;

template<class Parameter>
static inline Parameter convertArgument(
    wasm_exec_env_t execEnv,
    AbiType<Parameter> argument)
{
    if constexpr (std::is_pointer_v<Parameter>)
    {
        return (Parameter)nativeAddress(execEnv, (uint32_t)argument);
    }
    else
    {
        return (Parameter)argument;
    }
}

template<class Ret, class... Args>
constexpr std::array<char, 4 + sizeof...(Args)> makeSignature()
{
    std::array<char, 4 + sizeof...(Args)> signature{};
    int offset = 0;
    signature[offset++] = '(';

    const char parameterTypes[] = { signatureCharacter<Args>()..., '\0' };
    for (size_t index = 0; index < sizeof...(Args); index++)
    {
        signature[offset++] = parameterTypes[index];
    }

    signature[offset++] = ')';
    const char returnType = signatureCharacter<Ret>();
    if (returnType)
    {
        signature[offset++] = returnType;
    }

    signature[offset] = '\0';
    return signature;
}

constexpr bool equalStrings(const char* left, const char* right)
{
    while (*left && *left == *right)
    {
        ++left;
        ++right;
    }

    return *left == *right;
}

template<auto Member>
struct QpiImport;

template<class R, class... A, R(*HostServices::*Member)(const void*, A...)>
struct QpiImport<Member>
{
    static AbiType<R> call(wasm_exec_env_t execEnv, AbiType<A>... arguments)
    {
        CallContext* callContext = activeCallContext(execEnv);

        if constexpr (std::is_void_v<R>)
        {
            (hostServices.*Member)(
                callContext->ctx,
                convertArgument<A>(execEnv, arguments)...);
        }
        else
        {
            return (AbiType<R>)(hostServices.*Member)(
                callContext->ctx,
                convertArgument<A>(execEnv, arguments)...);
        }
    }

    static constexpr auto sig = makeSignature<R, A...>();
};

template<auto Member>
struct InfrastructureImport;

template<class R, class... A, R(*HostServices::*Member)(A...)>
struct InfrastructureImport<Member>
{
    static AbiType<R> call(wasm_exec_env_t execEnv, AbiType<A>... arguments)
    {
        if constexpr (std::is_void_v<R>)
        {
            (hostServices.*Member)(
                convertArgument<A>(execEnv, arguments)...);
        }
        else
        {
            return (AbiType<R>)(hostServices.*Member)(
                convertArgument<A>(execEnv, arguments)...);
        }
    }

    static constexpr auto sig = makeSignature<R, A...>();
};

// Bespoke wrappers retain derived signatures through the ABI rows below.
static uint32_t w_acquireScratch(
    wasm_exec_env_t execEnv,
    uint64_t size,
    uint32_t initializeToZero)
{
    CallContext* callContext = activeCallContext(execEnv);
    const uint32_t alignedSize = (uint32_t)((size + 7) & ~7ull);

    if (!callContext || callContext->arenaBump + alignedSize > callContext->arenaEnd)
    {
        wasm_runtime_set_exception(
            wasm_runtime_get_module_inst(execEnv),
            "lhost: scratch arena exhausted");
        return 0;
    }

    const uint32_t allocationOffset = callContext->arenaBump;
    callContext->arenaBump += alignedSize;

    if (initializeToZero)
    {
        setMem(nativeAddress(execEnv, allocationOffset), alignedSize, 0);
    }

    return allocationOffset;
}

static void w_releaseScratch(wasm_exec_env_t execEnv, uint32_t offset)
{
    (void)execEnv;
    (void)offset;
}

static void w_logBytes(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t type,
    uint32_t messageOffset,
    uint32_t size)
{
    CallContext* callContext = activeCallContext(execEnv);
    void* message = nativeAddress(execEnv, messageOffset);

    if (callContext && callContext->trace)
    {
        recordLog(
            (TraceEntry*)callContext->trace,
            (unsigned char)type,
            message,
            size);
    }

    hostServices.logBytes(contractIndex, (unsigned char)type, message, size);
}

static int64_t w_transfer(wasm_exec_env_t execEnv, uint32_t destinationOffset, int64_t amount)
{
    CallContext* callContext = activeCallContext(execEnv);
    void* destination = nativeAddress(execEnv, destinationOffset);

    traceHostCall(
        callContext,
        "transfer",
        hex(destination, 8) + ".. " + std::to_string(amount));
    return hostServices.transfer(callContext->ctx, destination, amount);
}

static int64_t w_transferTyped(
    wasm_exec_env_t execEnv,
    uint32_t destinationOffset,
    int64_t amount,
    uint32_t transferType)
{
    CallContext* callContext = activeCallContext(execEnv);
    void* destination = nativeAddress(execEnv, destinationOffset);

    traceHostCall(
        callContext,
        "transferTyped",
        hex(destination, 8) + ".. " + std::to_string(amount)
            + " t=" + std::to_string(transferType));
    return hostServices.transferTyped(
        callContext->ctx,
        destination,
        amount,
        (unsigned char)transferType);
}

static void w_abort(wasm_exec_env_t execEnv, uint32_t errorCode)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(callContext, "abort", std::to_string(errorCode));
    hostServices.abort(callContext->ctx, errorCode);
}

static int64_t w_burn(wasm_exec_env_t execEnv, int64_t amount, uint32_t contractIndex)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "burn",
        std::to_string(amount) + " for " + std::to_string(contractIndex));
    return hostServices.burn(callContext->ctx, amount, contractIndex);
}

static int64_t w_issueAsset(
    wasm_exec_env_t execEnv,
    uint64_t name,
    uint32_t issuerOffset,
    uint32_t decimals,
    int64_t shares,
    uint64_t unit)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "issueAsset",
        "name=" + std::to_string(name) + " shares=" + std::to_string(shares));
    return hostServices.issueAsset(
        callContext->ctx,
        name,
        nativeAddress(execEnv, issuerOffset),
        (signed char)decimals,
        shares,
        unit);
}

static int64_t w_transferShares(
    wasm_exec_env_t execEnv,
    uint64_t name,
    uint32_t issuerOffset,
    uint32_t ownerOffset,
    uint32_t possessorOffset,
    int64_t shares,
    uint32_t newOwnerOffset)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "transferShares",
        "name=" + std::to_string(name) + " shares=" + std::to_string(shares));
    return hostServices.transferShareOwnershipAndPossession(
        callContext->ctx,
        name,
        nativeAddress(execEnv, issuerOffset),
        nativeAddress(execEnv, ownerOffset),
        nativeAddress(execEnv, possessorOffset),
        shares,
        nativeAddress(execEnv, newOwnerOffset));
}

static int64_t w_acquireShares(
    wasm_exec_env_t execEnv,
    uint64_t name,
    uint32_t issuerOffset,
    uint32_t ownerOffset,
    uint32_t possessorOffset,
    int64_t shares,
    uint32_t sourceOwnershipManagement,
    uint32_t sourcePossessionManagement,
    int64_t fee)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "acquireShares",
        "name=" + std::to_string(name) + " shares=" + std::to_string(shares));
    return hostServices.acquireShares(
        callContext->ctx,
        name,
        nativeAddress(execEnv, issuerOffset),
        nativeAddress(execEnv, ownerOffset),
        nativeAddress(execEnv, possessorOffset),
        shares,
        (unsigned short)sourceOwnershipManagement,
        (unsigned short)sourcePossessionManagement,
        fee);
}

static int64_t w_releaseShares(
    wasm_exec_env_t execEnv,
    uint64_t name,
    uint32_t issuerOffset,
    uint32_t ownerOffset,
    uint32_t possessorOffset,
    int64_t shares,
    uint32_t destinationOwnershipManagement,
    uint32_t destinationPossessionManagement,
    int64_t fee)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "releaseShares",
        "name=" + std::to_string(name) + " shares=" + std::to_string(shares));
    return hostServices.releaseShares(
        callContext->ctx,
        name,
        nativeAddress(execEnv, issuerOffset),
        nativeAddress(execEnv, ownerOffset),
        nativeAddress(execEnv, possessorOffset),
        shares,
        (unsigned short)destinationOwnershipManagement,
        (unsigned short)destinationPossessionManagement,
        fee);
}

static uint32_t w_assetEnumerate(
    wasm_exec_env_t execEnv,
    uint32_t kind,
    uint32_t issuanceOffset,
    uint32_t ownershipOffset,
    uint32_t possessionOffset,
    uint32_t outputOffset,
    uint32_t capacity)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(callContext, "assetEnumerate", "kind=" + std::to_string(kind));
    return hostServices.assetEnumerate(
        callContext->ctx,
        kind,
        nativeAddress(execEnv, issuanceOffset),
        nativeAddress(execEnv, ownershipOffset),
        nativeAddress(execEnv, possessionOffset),
        nativeAddress(execEnv, outputOffset),
        capacity);
}

static uint32_t w_dayOfWeek(
    wasm_exec_env_t execEnv,
    uint32_t year,
    uint32_t month,
    uint32_t day)
{
    CallContext* callContext = activeCallContext(execEnv);
    return hostServices.dayOfWeek(
        callContext->ctx,
        (unsigned char)year,
        (unsigned char)month,
        (unsigned char)day);
}

static uint32_t w_signatureValidity(
    wasm_exec_env_t execEnv,
    uint32_t entityOffset,
    uint32_t digestOffset,
    uint32_t signatureOffset)
{
    CallContext* callContext = activeCallContext(execEnv);
    return hostServices.signatureValidity(
        callContext->ctx,
        nativeAddress(execEnv, entityOffset),
        nativeAddress(execEnv, digestOffset),
        nativeAddress(execEnv, signatureOffset));
}

static int64_t w_bidInIPO(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    int64_t price,
    uint32_t quantity)
{
    CallContext* callContext = activeCallContext(execEnv);
    return hostServices.bidInIPO(callContext->ctx, contractIndex, price, quantity);
}

static void w_ipoBidId(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t bidIndex,
    uint32_t outputOffset)
{
    CallContext* callContext = activeCallContext(execEnv);
    hostServices.ipoBidId(
        callContext->ctx,
        contractIndex,
        bidIndex,
        nativeAddress(execEnv, outputOffset));
}

static int64_t w_ipoBidPrice(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t bidIndex)
{
    CallContext* callContext = activeCallContext(execEnv);
    return hostServices.ipoBidPrice(callContext->ctx, contractIndex, bidIndex);
}

static void w_computeMiningFunction(
    wasm_exec_env_t execEnv,
    uint32_t seedOffset,
    uint32_t publicKeyOffset,
    uint32_t nonceOffset,
    uint32_t outputOffset)
{
    CallContext* callContext = activeCallContext(execEnv);
    hostServices.computeMiningFunction(
        callContext->ctx,
        nativeAddress(execEnv, seedOffset),
        nativeAddress(execEnv, publicKeyOffset),
        nativeAddress(execEnv, nonceOffset),
        nativeAddress(execEnv, outputOffset));
}

static void w_initMiningSeed(wasm_exec_env_t execEnv, uint32_t seedOffset)
{
    CallContext* callContext = activeCallContext(execEnv);
    hostServices.initMiningSeed(
        callContext->ctx,
        nativeAddress(execEnv, seedOffset));
}

static uint32_t w_getOracleQueryStatus(wasm_exec_env_t execEnv, int64_t queryId)
{
    CallContext* callContext = activeCallContext(execEnv);
    return hostServices.getOracleQueryStatus(callContext->ctx, queryId);
}

static uint32_t w_unsubscribeOracle(wasm_exec_env_t execEnv, int32_t subscriptionId)
{
    CallContext* callContext = activeCallContext(execEnv);
    return hostServices.unsubscribeOracle(callContext->ctx, subscriptionId);
}

static int64_t w_queryOracle(
    wasm_exec_env_t execEnv,
    uint32_t interfaceIndex,
    uint32_t queryOffset,
    uint32_t querySize,
    uint32_t notificationProcedureId,
    uint32_t timeoutMilliseconds,
    int64_t fee)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "queryOracle",
        "iface=" + std::to_string(interfaceIndex));
    return hostServices.queryOracle(
        callContext->ctx,
        interfaceIndex,
        nativeAddress(execEnv, queryOffset),
        querySize,
        notificationProcedureId,
        timeoutMilliseconds,
        fee);
}

static int32_t w_subscribeOracle(
    wasm_exec_env_t execEnv,
    uint32_t interfaceIndex,
    uint32_t queryOffset,
    uint32_t querySize,
    uint32_t notificationProcedureId,
    uint32_t periodMilliseconds,
    uint32_t notifyWithPreviousReply,
    int64_t fee)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "subscribeOracle",
        "iface=" + std::to_string(interfaceIndex));
    return hostServices.subscribeOracle(
        callContext->ctx,
        interfaceIndex,
        nativeAddress(execEnv, queryOffset),
        querySize,
        notificationProcedureId,
        periodMilliseconds,
        notifyWithPreviousReply,
        fee);
}

static uint32_t w_getOracleQuery(
    wasm_exec_env_t execEnv,
    int64_t queryId,
    uint32_t outputOffset,
    uint32_t size)
{
    CallContext* callContext = activeCallContext(execEnv);
    return hostServices.getOracleQuery(
        callContext->ctx,
        queryId,
        nativeAddress(execEnv, outputOffset),
        size);
}

static uint32_t w_getOracleReply(
    wasm_exec_env_t execEnv,
    int64_t queryId,
    uint32_t outputOffset,
    uint32_t size)
{
    CallContext* callContext = activeCallContext(execEnv);
    return hostServices.getOracleReply(
        callContext->ctx,
        queryId,
        nativeAddress(execEnv, outputOffset),
        size);
}

static uint32_t w_distributeDividends(wasm_exec_env_t execEnv, int64_t amountPerShare)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(callContext, "distributeDividends", std::to_string(amountPerShare));
    return hostServices.distributeDividends(callContext->ctx, amountPerShare);
}

static int32_t w_liteCallFunction(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t inputType,
    uint32_t inputOffset,
    uint32_t inputSize,
    uint32_t outputOffset,
    uint32_t outputSize)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "callFunction",
        "-> " + std::to_string(contractIndex) + "/" + std::to_string(inputType));
    return hostServices.liteCallFunction(
        callContext->ctx,
        contractIndex,
        (unsigned short)inputType,
        nativeAddress(execEnv, inputOffset),
        inputSize,
        nativeAddress(execEnv, outputOffset),
        outputSize);
}

static int32_t w_liteInvokeProcedure(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t inputType,
    uint32_t inputOffset,
    uint32_t inputSize,
    uint32_t outputOffset,
    uint32_t outputSize,
    int64_t invocationReward)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "invokeProcedure",
        "-> " + std::to_string(contractIndex) + "/" + std::to_string(inputType)
            + " reward " + std::to_string(invocationReward));
    return hostServices.liteInvokeProcedure(
        callContext->ctx,
        contractIndex,
        (unsigned short)inputType,
        nativeAddress(execEnv, inputOffset),
        inputSize,
        nativeAddress(execEnv, outputOffset),
        outputSize,
        invocationReward);
}

static int32_t w_liteSetShareholderProposal(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t proposalOffset,
    int64_t invocationReward)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "setShareholderProposal",
        "-> " + std::to_string(contractIndex));
    return hostServices.setShareholderProposal(
        callContext->ctx,
        contractIndex,
        nativeAddress(execEnv, proposalOffset),
        invocationReward);
}

static int32_t w_liteSetShareholderVotes(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t voteOffset,
    uint32_t voteSize,
    int64_t invocationReward)
{
    CallContext* callContext = activeCallContext(execEnv);

    traceHostCall(
        callContext,
        "setShareholderVotes",
        "-> " + std::to_string(contractIndex));
    return hostServices.setShareholderVotes(
        callContext->ctx,
        contractIndex,
        nativeAddress(execEnv, voteOffset),
        voteSize,
        invocationReward);
}

// Every declared signature is checked against the type-derived WAMR signature.
#define LHOST_AS_GQ(importName, member, signatureLiteral) \
    static_assert( \
        equalStrings( \
            QpiImport<&HostServices::member>::sig.data(), \
            signatureLiteral), \
        "wasm sig drift: " importName);
#define LHOST_AS_GI(importName, member, signatureLiteral) \
    static_assert( \
        equalStrings( \
            InfrastructureImport<&HostServices::member>::sig.data(), \
            signatureLiteral), \
        "wasm sig drift: " importName);
#define LHOST_AS_HQ(importName, member, adapter, signatureLiteral) \
    LHOST_AS_GQ(importName, member, signatureLiteral)
#define LHOST_AS_HI(importName, member, adapter, signatureLiteral) \
    LHOST_AS_GI(importName, member, signatureLiteral)
WASM_LHOST_ABI_ROWS(LHOST_AS_GQ, LHOST_AS_GI, LHOST_AS_HQ, LHOST_AS_HI)

// Generated rows use templates; handwritten rows use their named adapters.
#define LHOST_ROW_GQ(importName, member, signatureLiteral) \
    { \
        importName, \
        (void*)&QpiImport<&HostServices::member>::call, \
        QpiImport<&HostServices::member>::sig.data(), \
        NULL, \
    },
#define LHOST_ROW_GI(importName, member, signatureLiteral) \
    { \
        importName, \
        (void*)&InfrastructureImport<&HostServices::member>::call, \
        InfrastructureImport<&HostServices::member>::sig.data(), \
        NULL, \
    },
#define LHOST_ROW_HQ(importName, member, adapter, signatureLiteral) \
    { \
        importName, \
        (void*)adapter, \
        QpiImport<&HostServices::member>::sig.data(), \
        NULL, \
    },
#define LHOST_ROW_HI(importName, member, adapter, signatureLiteral) \
    { \
        importName, \
        (void*)adapter, \
        InfrastructureImport<&HostServices::member>::sig.data(), \
        NULL, \
    },
static NativeSymbol nativeSymbols[] =
{
    WASM_LHOST_ABI_ROWS(LHOST_ROW_GQ, LHOST_ROW_GI, LHOST_ROW_HQ, LHOST_ROW_HI)
};
static const uint32_t nativeSymbolCount =
    (uint32_t)(sizeof(nativeSymbols) / sizeof(nativeSymbols[0]));

// The extra vtable slot is abiVersion.
static_assert(
    sizeof(HostServices) == sizeof(void*) * (nativeSymbolCount + 1),
    "wasm import table (nativeSymbols) out of sync with the host vtable (HostServices)");

} // namespace Wasm::Runtime

#endif // LITE_WASM_SC

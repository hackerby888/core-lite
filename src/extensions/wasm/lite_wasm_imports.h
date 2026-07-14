#pragma once
// WAMR adapters for the canonical "lhost" import table. Pointers cross the ABI as Wasm32 offsets and are
// resolved against the active module instance. Keep the ABI row order and signatures unchanged.
#ifdef LITE_WASM_SC
#include <cstdint>
#include <type_traits>
#include <array>
#include "wasm_export.h"
#include "extensions/wasm/lite_abi_metadata.h"
#include "extensions/wasm/lite_wasm_debug.h"

struct LiteWasmCallCtx
{
    const void* ctx;
    uint32_t arenaBase;
    uint32_t arenaBump;
    uint32_t arenaEnd;
    void* trace = nullptr;
};

static inline LiteWasmCallCtx* liteWasmCallContext(wasm_exec_env_t execEnv)
{
    return (LiteWasmCallCtx*)wasm_runtime_get_user_data(execEnv);
}

static inline void* liteWasmNativeAddress(wasm_exec_env_t execEnv, uint32_t offset)
{
    if (!offset)
    {
        return nullptr;
    }

    return wasm_runtime_addr_app_to_native(wasm_runtime_get_module_inst(execEnv), offset);
}

static inline void liteWasmTraceCall(
    LiteWasmCallCtx* callContext,
    const char* name,
    const std::string& detail)
{
    if (callContext && callContext->trace)
    {
        liteWasmTraceHostCall(
            (LiteWasmTraceEntry*)callContext->trace,
            name,
            detail);
    }
}

// WAMR signatures use i/I/f/F for i32/i64/f32/f64. Pointers and narrow integers use i32.
template<class T>
constexpr size_t liteSafeSizeof()
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
constexpr char liteWasmSigChar()
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
        return liteSafeSizeof<T>() <= 4 ? 'i' : 'I';
    }
}

template<class T>
using liteWasmAbi =
    std::conditional_t<std::is_void_v<T>, void,
      std::conditional_t<std::is_pointer_v<T>, uint32_t,
        std::conditional_t<(std::is_integral_v<T> && liteSafeSizeof<T>() <= 4), uint32_t, T>>>;

template<class Parameter>
static inline Parameter liteWasmConvertArgument(
    wasm_exec_env_t execEnv,
    liteWasmAbi<Parameter> argument)
{
    if constexpr (std::is_pointer_v<Parameter>)
    {
        return (Parameter)liteWasmNativeAddress(execEnv, (uint32_t)argument);
    }
    else
    {
        return (Parameter)argument;
    }
}

template<class Ret, class... Args>
constexpr std::array<char, 4 + sizeof...(Args)> liteWasmSig()
{
    std::array<char, 4 + sizeof...(Args)> signature{};
    int offset = 0;
    signature[offset++] = '(';

    const char parameterTypes[] = { liteWasmSigChar<Args>()..., '\0' };
    for (size_t index = 0; index < sizeof...(Args); index++)
    {
        signature[offset++] = parameterTypes[index];
    }

    signature[offset++] = ')';
    const char returnType = liteWasmSigChar<Ret>();
    if (returnType)
    {
        signature[offset++] = returnType;
    }

    signature[offset] = '\0';
    return signature;
}

constexpr bool liteCstrEq(const char* left, const char* right)
{
    while (*left && *left == *right)
    {
        ++left;
        ++right;
    }

    return *left == *right;
}

template<auto Member>
struct LiteQpiImport;

template<class R, class... A, R(*LiteHostServices::*Member)(const void*, A...)>
struct LiteQpiImport<Member>
{
    static liteWasmAbi<R> call(wasm_exec_env_t execEnv, liteWasmAbi<A>... arguments)
    {
        LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

        if constexpr (std::is_void_v<R>)
        {
            (g_liteHostServices.*Member)(
                callContext->ctx,
                liteWasmConvertArgument<A>(execEnv, arguments)...);
        }
        else
        {
            return (liteWasmAbi<R>)(g_liteHostServices.*Member)(
                callContext->ctx,
                liteWasmConvertArgument<A>(execEnv, arguments)...);
        }
    }

    static constexpr auto sig = liteWasmSig<R, A...>();
};

template<auto Member>
struct LiteInfraImport;

template<class R, class... A, R(*LiteHostServices::*Member)(A...)>
struct LiteInfraImport<Member>
{
    static liteWasmAbi<R> call(wasm_exec_env_t execEnv, liteWasmAbi<A>... arguments)
    {
        if constexpr (std::is_void_v<R>)
        {
            (g_liteHostServices.*Member)(
                liteWasmConvertArgument<A>(execEnv, arguments)...);
        }
        else
        {
            return (liteWasmAbi<R>)(g_liteHostServices.*Member)(
                liteWasmConvertArgument<A>(execEnv, arguments)...);
        }
    }

    static constexpr auto sig = liteWasmSig<R, A...>();
};

// Bespoke wrappers retain derived signatures through the ABI rows below.
static uint32_t w_acquireScratch(
    wasm_exec_env_t execEnv,
    uint64_t size,
    uint32_t initializeToZero)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
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
        setMem(liteWasmNativeAddress(execEnv, allocationOffset), alignedSize, 0);
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    void* message = liteWasmNativeAddress(execEnv, messageOffset);

    if (callContext && callContext->trace)
    {
        liteWasmTraceLog(
            (LiteWasmTraceEntry*)callContext->trace,
            (unsigned char)type,
            message,
            size);
    }

    g_liteHostServices.logBytes(contractIndex, (unsigned char)type, message, size);
}

static int64_t w_transfer(wasm_exec_env_t execEnv, uint32_t destinationOffset, int64_t amount)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    void* destination = liteWasmNativeAddress(execEnv, destinationOffset);

    liteWasmTraceCall(
        callContext,
        "transfer",
        liteWasmHex(destination, 8) + ".. " + std::to_string(amount));
    return g_liteHostServices.transfer(callContext->ctx, destination, amount);
}

static int64_t w_transferTyped(
    wasm_exec_env_t execEnv,
    uint32_t destinationOffset,
    int64_t amount,
    uint32_t transferType)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    void* destination = liteWasmNativeAddress(execEnv, destinationOffset);

    liteWasmTraceCall(
        callContext,
        "transferTyped",
        liteWasmHex(destination, 8) + ".. " + std::to_string(amount)
            + " t=" + std::to_string(transferType));
    return g_liteHostServices.transferTyped(
        callContext->ctx,
        destination,
        amount,
        (unsigned char)transferType);
}

static void w_abort(wasm_exec_env_t execEnv, uint32_t errorCode)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(callContext, "abort", std::to_string(errorCode));
    g_liteHostServices.abort(callContext->ctx, errorCode);
}

static int64_t w_burn(wasm_exec_env_t execEnv, int64_t amount, uint32_t contractIndex)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "burn",
        std::to_string(amount) + " for " + std::to_string(contractIndex));
    return g_liteHostServices.burn(callContext->ctx, amount, contractIndex);
}

static int64_t w_issueAsset(
    wasm_exec_env_t execEnv,
    uint64_t name,
    uint32_t issuerOffset,
    uint32_t decimals,
    int64_t shares,
    uint64_t unit)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "issueAsset",
        "name=" + std::to_string(name) + " shares=" + std::to_string(shares));
    return g_liteHostServices.issueAsset(
        callContext->ctx,
        name,
        liteWasmNativeAddress(execEnv, issuerOffset),
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "transferShares",
        "name=" + std::to_string(name) + " shares=" + std::to_string(shares));
    return g_liteHostServices.transferShareOwnershipAndPossession(
        callContext->ctx,
        name,
        liteWasmNativeAddress(execEnv, issuerOffset),
        liteWasmNativeAddress(execEnv, ownerOffset),
        liteWasmNativeAddress(execEnv, possessorOffset),
        shares,
        liteWasmNativeAddress(execEnv, newOwnerOffset));
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "acquireShares",
        "name=" + std::to_string(name) + " shares=" + std::to_string(shares));
    return g_liteHostServices.acquireShares(
        callContext->ctx,
        name,
        liteWasmNativeAddress(execEnv, issuerOffset),
        liteWasmNativeAddress(execEnv, ownerOffset),
        liteWasmNativeAddress(execEnv, possessorOffset),
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "releaseShares",
        "name=" + std::to_string(name) + " shares=" + std::to_string(shares));
    return g_liteHostServices.releaseShares(
        callContext->ctx,
        name,
        liteWasmNativeAddress(execEnv, issuerOffset),
        liteWasmNativeAddress(execEnv, ownerOffset),
        liteWasmNativeAddress(execEnv, possessorOffset),
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(callContext, "assetEnumerate", "kind=" + std::to_string(kind));
    return g_liteHostServices.assetEnumerate(
        callContext->ctx,
        kind,
        liteWasmNativeAddress(execEnv, issuanceOffset),
        liteWasmNativeAddress(execEnv, ownershipOffset),
        liteWasmNativeAddress(execEnv, possessionOffset),
        liteWasmNativeAddress(execEnv, outputOffset),
        capacity);
}

static uint32_t w_dayOfWeek(
    wasm_exec_env_t execEnv,
    uint32_t year,
    uint32_t month,
    uint32_t day)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    return g_liteHostServices.dayOfWeek(
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    return g_liteHostServices.signatureValidity(
        callContext->ctx,
        liteWasmNativeAddress(execEnv, entityOffset),
        liteWasmNativeAddress(execEnv, digestOffset),
        liteWasmNativeAddress(execEnv, signatureOffset));
}

static int64_t w_bidInIPO(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    int64_t price,
    uint32_t quantity)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    return g_liteHostServices.bidInIPO(callContext->ctx, contractIndex, price, quantity);
}

static void w_ipoBidId(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t bidIndex,
    uint32_t outputOffset)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    g_liteHostServices.ipoBidId(
        callContext->ctx,
        contractIndex,
        bidIndex,
        liteWasmNativeAddress(execEnv, outputOffset));
}

static int64_t w_ipoBidPrice(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t bidIndex)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    return g_liteHostServices.ipoBidPrice(callContext->ctx, contractIndex, bidIndex);
}

static void w_computeMiningFunction(
    wasm_exec_env_t execEnv,
    uint32_t seedOffset,
    uint32_t publicKeyOffset,
    uint32_t nonceOffset,
    uint32_t outputOffset)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    g_liteHostServices.computeMiningFunction(
        callContext->ctx,
        liteWasmNativeAddress(execEnv, seedOffset),
        liteWasmNativeAddress(execEnv, publicKeyOffset),
        liteWasmNativeAddress(execEnv, nonceOffset),
        liteWasmNativeAddress(execEnv, outputOffset));
}

static void w_initMiningSeed(wasm_exec_env_t execEnv, uint32_t seedOffset)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    g_liteHostServices.initMiningSeed(
        callContext->ctx,
        liteWasmNativeAddress(execEnv, seedOffset));
}

static uint32_t w_getOracleQueryStatus(wasm_exec_env_t execEnv, int64_t queryId)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    return g_liteHostServices.getOracleQueryStatus(callContext->ctx, queryId);
}

static uint32_t w_unsubscribeOracle(wasm_exec_env_t execEnv, int32_t subscriptionId)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    return g_liteHostServices.unsubscribeOracle(callContext->ctx, subscriptionId);
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "queryOracle",
        "iface=" + std::to_string(interfaceIndex));
    return g_liteHostServices.queryOracle(
        callContext->ctx,
        interfaceIndex,
        liteWasmNativeAddress(execEnv, queryOffset),
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "subscribeOracle",
        "iface=" + std::to_string(interfaceIndex));
    return g_liteHostServices.subscribeOracle(
        callContext->ctx,
        interfaceIndex,
        liteWasmNativeAddress(execEnv, queryOffset),
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    return g_liteHostServices.getOracleQuery(
        callContext->ctx,
        queryId,
        liteWasmNativeAddress(execEnv, outputOffset),
        size);
}

static uint32_t w_getOracleReply(
    wasm_exec_env_t execEnv,
    int64_t queryId,
    uint32_t outputOffset,
    uint32_t size)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);
    return g_liteHostServices.getOracleReply(
        callContext->ctx,
        queryId,
        liteWasmNativeAddress(execEnv, outputOffset),
        size);
}

static uint32_t w_distributeDividends(wasm_exec_env_t execEnv, int64_t amountPerShare)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(callContext, "distributeDividends", std::to_string(amountPerShare));
    return g_liteHostServices.distributeDividends(callContext->ctx, amountPerShare);
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "callFunction",
        "-> " + std::to_string(contractIndex) + "/" + std::to_string(inputType));
    return g_liteHostServices.liteCallFunction(
        callContext->ctx,
        contractIndex,
        (unsigned short)inputType,
        liteWasmNativeAddress(execEnv, inputOffset),
        inputSize,
        liteWasmNativeAddress(execEnv, outputOffset),
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
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "invokeProcedure",
        "-> " + std::to_string(contractIndex) + "/" + std::to_string(inputType)
            + " reward " + std::to_string(invocationReward));
    return g_liteHostServices.liteInvokeProcedure(
        callContext->ctx,
        contractIndex,
        (unsigned short)inputType,
        liteWasmNativeAddress(execEnv, inputOffset),
        inputSize,
        liteWasmNativeAddress(execEnv, outputOffset),
        outputSize,
        invocationReward);
}

static int32_t w_liteSetShareholderProposal(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t proposalOffset,
    int64_t invocationReward)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "setShareholderProposal",
        "-> " + std::to_string(contractIndex));
    return g_liteHostServices.setShareholderProposal(
        callContext->ctx,
        contractIndex,
        liteWasmNativeAddress(execEnv, proposalOffset),
        invocationReward);
}

static int32_t w_liteSetShareholderVotes(
    wasm_exec_env_t execEnv,
    uint32_t contractIndex,
    uint32_t voteOffset,
    uint32_t voteSize,
    int64_t invocationReward)
{
    LiteWasmCallCtx* callContext = liteWasmCallContext(execEnv);

    liteWasmTraceCall(
        callContext,
        "setShareholderVotes",
        "-> " + std::to_string(contractIndex));
    return g_liteHostServices.setShareholderVotes(
        callContext->ctx,
        contractIndex,
        liteWasmNativeAddress(execEnv, voteOffset),
        voteSize,
        invocationReward);
}

// Every declared signature is checked against the type-derived WAMR signature.
#define LHOST_AS_GQ(nm, m, lit)     static_assert(liteCstrEq(LiteQpiImport<&LiteHostServices::m>::sig.data(),   lit), "wasm sig drift: " nm);
#define LHOST_AS_GI(nm, m, lit)     static_assert(liteCstrEq(LiteInfraImport<&LiteHostServices::m>::sig.data(), lit), "wasm sig drift: " nm);
#define LHOST_AS_HQ(nm, m, wfn, lit) static_assert(liteCstrEq(LiteQpiImport<&LiteHostServices::m>::sig.data(),   lit), "wasm sig drift: " nm);
#define LHOST_AS_HI(nm, m, wfn, lit) static_assert(liteCstrEq(LiteInfraImport<&LiteHostServices::m>::sig.data(), lit), "wasm sig drift: " nm);
LITE_LHOST_ABI_ROWS(LHOST_AS_GQ, LHOST_AS_GI, LHOST_AS_HQ, LHOST_AS_HI)

// Generated rows use templates; handwritten rows use their named adapters.
#define LHOST_ROW_GQ(nm, m, lit)      { nm, (void*)&LiteQpiImport<&LiteHostServices::m>::call,   LiteQpiImport<&LiteHostServices::m>::sig.data(),   NULL },
#define LHOST_ROW_GI(nm, m, lit)      { nm, (void*)&LiteInfraImport<&LiteHostServices::m>::call, LiteInfraImport<&LiteHostServices::m>::sig.data(), NULL },
#define LHOST_ROW_HQ(nm, m, wfn, lit) { nm, (void*)wfn,                                          LiteQpiImport<&LiteHostServices::m>::sig.data(),   NULL },
#define LHOST_ROW_HI(nm, m, wfn, lit) { nm, (void*)wfn,                                          LiteInfraImport<&LiteHostServices::m>::sig.data(), NULL },
static NativeSymbol g_liteWasmNatives[] =
{
    LITE_LHOST_ABI_ROWS(LHOST_ROW_GQ, LHOST_ROW_GI, LHOST_ROW_HQ, LHOST_ROW_HI)
};
static const uint32_t g_liteWasmNativesCount =
    (uint32_t)(sizeof(g_liteWasmNatives) / sizeof(g_liteWasmNatives[0]));

// The extra vtable slot is abiVersion.
static_assert(
    sizeof(LiteHostServices) == sizeof(void*) * (g_liteWasmNativesCount + 1),
    "wasm import table (g_liteWasmNatives) out of sync with the host vtable (LiteHostServices)");

#endif // LITE_WASM_SC

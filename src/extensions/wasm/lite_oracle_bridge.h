#pragma once
// Host adapters for the Wasm oracle imports.
// Fees and caller identity are always derived from host-owned state.

static inline unsigned int liteOracleContractIndex(const void* context)
{
    return ((const QPI::QpiContextProcedureCall*)context)
        ->__qpiCurrentContractIndex();
}

static long long liteWasmQueryOracle(
    const void* context,
    unsigned int interfaceIndex,
    const void* query,
    unsigned int querySize,
    unsigned int notificationProcedureId,
    unsigned int timeoutMilliseconds,
    long long /*untrustedWasmFee*/)
{
    if (interfaceIndex >= OI::oracleInterfacesCount)
    {
        return -1;
    }

    if (!OI::getOracleQueryFeeFunc[interfaceIndex])
    {
        return -1;
    }

    if (querySize != OI::oracleInterfaces[interfaceIndex].querySize)
    {
        return -1;
    }

    const unsigned int contractIndex = liteOracleContractIndex(context);
    const m256i contractId = m256i(contractIndex, 0, 0, 0);
    const long long fee = OI::getOracleQueryFeeFunc[interfaceIndex](query);
    const int contractSpectrumIndex = ::spectrumIndex(contractId);

    if (fee < MIN_ORACLE_QUERY_FEE
        || contractSpectrumIndex < 0
        || !decreaseEnergy(contractSpectrumIndex, fee))
    {
        return -1;
    }

    const QuTransfer quTransfer = { contractId, m256i::zero(), fee };

    logger.logQuTransfer(quTransfer);

    const long long queryId = oracleEngine.startContractQuery(
        (uint16_t)contractIndex,
        interfaceIndex,
        query,
        (uint16_t)querySize,
        timeoutMilliseconds,
        notificationProcedureId);
    if (queryId < 0 && fee > 0)
    {
        oracleEngine.refundFees(contractId, fee);
    }

    return queryId;
}

// Subscription support is unavailable; the import remains stable and fails cleanly.
static int liteWasmSubscribeOracle(
    const void* /*context*/,
    unsigned int /*interfaceIndex*/,
    const void* /*query*/,
    unsigned int /*querySize*/,
    unsigned int /*notificationProcedureId*/,
    unsigned int /*periodMilliseconds*/,
    unsigned int /*notifyPrevious*/,
    long long /*fee*/)
{
    return -1;
}

static unsigned int liteWasmGetOracleQuery(
    const void* /*context*/,
    long long queryId,
    void* output,
    unsigned int size)
{
    return oracleEngine.getOracleQuery(queryId, output, (uint16_t)size)
        ? 1u
        : 0u;
}

static unsigned int liteWasmGetOracleReply(
    const void* /*context*/,
    long long queryId,
    void* output,
    unsigned int size)
{
    return oracleEngine.getOracleReply(queryId, output, (uint16_t)size)
        ? 1u
        : 0u;
}

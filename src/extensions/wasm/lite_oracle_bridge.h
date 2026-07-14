#pragma once
// Host-side implementation of the wasm oracle imports (queryOracle / subscribeOracle / getOracleQuery /
// getOracleReply). Included from lite_dynamic_contracts.h, which is compiled AFTER oracle_engine.h /
// oracle_interfaces_def.h / qpi_oracle_impl.h, so oracleEngine, OI::, logger, spectrumIndex, decreaseEnergy and
// the oracle constants are all in scope.
//
// These mirror the non-templated core of qpi_oracle_impl.h's __qpiQueryOracle: recompute the fee AUTHORITATIVELY
// from the interface (never trust the wasm-passed fee — a contract could underpay), burn it, then start the
// query on the shared oracleEngine. The running contract's index comes from the QpiContext (the host's
// authoritative identity), not from anything the wasm passes. getOracleQuery/getOracleReply need no identity.
//
// Notification delivery (the async reply callback) is wired separately at contract arm — see
// liteWasmRegisterProceduresForNotification() in lite_wasm_contracts.h.

// The running contract's index, straight from the host-owned QpiContext (see qpi.h __qpiCurrentContractIndex).
static inline unsigned int liteOracleContractIndex(const void* ctx) {
    return ((const QPI::QpiContextProcedureCall*)ctx)->__qpiCurrentContractIndex();
}

// QUERY_ORACLE: spend the (recomputed) query fee, start a one-shot contract query. Returns queryId, or -1 on any
// failure (bad interface/size, fee too low, insufficient energy, engine refusal). The v1 error path returns -1
// without firing the synchronous error-notification the native path does (see plan Tiers).
static long long liteWasmQueryOracle(const void* ctx, unsigned int interfaceIndex, const void* query,
                                     unsigned int querySize, unsigned int notificationProcId,
                                     unsigned int timeoutMillisec, long long /*wasmFee ignored*/) {
    if (interfaceIndex >= OI::oracleInterfacesCount) return -1;
    if (!OI::getOracleQueryFeeFunc[interfaceIndex]) return -1;
    if (querySize != OI::oracleInterfaces[interfaceIndex].querySize) return -1;

    const unsigned int contractIndex = liteOracleContractIndex(ctx);
    const m256i contractId = m256i(contractIndex, 0, 0, 0);

    // authoritative fee from the interface table — NOT the wasm-passed value
    const long long fee = OI::getOracleQueryFeeFunc[interfaceIndex](query);
    const int contractSpectrumIdx = ::spectrumIndex(contractId);
    if (fee < MIN_ORACLE_QUERY_FEE || contractSpectrumIdx < 0 || !decreaseEnergy(contractSpectrumIdx, fee)) {
        return -1;
    }
    const QuTransfer quTransfer = { contractId, m256i::zero(), fee };
    logger.logQuTransfer(quTransfer);

    const long long queryId = oracleEngine.startContractQuery(
        (uint16_t)contractIndex, interfaceIndex, query, (uint16_t)querySize, timeoutMillisec, notificationProcId);
    if (queryId < 0 && fee > 0) {
        oracleEngine.refundFees(contractId, fee);
    }
    return queryId;
}

// SUBSCRIBE_ORACLE: not yet supported for wasm contracts (v1). Real support needs a per-interface subscription
// fee table + subscription timestamp offset in oracle_interfaces_def.h; only Price is subscribable. Fail cleanly
// so the import binds (module instantiates) and the contract sees a subscription-failed (-1).
static int liteWasmSubscribeOracle(const void* /*ctx*/, unsigned int /*interfaceIndex*/, const void* /*query*/,
                                   unsigned int /*querySize*/, unsigned int /*notificationProcId*/,
                                   unsigned int /*periodMillisec*/, unsigned int /*notifyPrev*/, long long /*fee*/) {
    return -1;
}

// Read the stored query / reply bytes for a queryId into contract memory. No identity needed — the engine keys
// purely on queryId. Returns 1 on success, 0 if not available.
static unsigned int liteWasmGetOracleQuery(const void* /*ctx*/, long long queryId, void* out, unsigned int size) {
    return oracleEngine.getOracleQuery(queryId, out, (uint16_t)size) ? 1u : 0u;
}
static unsigned int liteWasmGetOracleReply(const void* /*ctx*/, long long queryId, void* out, unsigned int size) {
    return oracleEngine.getOracleReply(queryId, out, (uint16_t)size) ? 1u : 0u;
}

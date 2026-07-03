# Plan: complete the oracle wasm-bridge (dynamic contracts)

**Goal:** provide the 4 oracle host-imports the wasm dynamic-contract ABI declares but the node does not yet
bind — `queryOracle`, `subscribeOracle`, `getOracleQuery`, `getOracleReply` — so wasm contracts get full oracle
parity with native ones (and with the qinit engine, which already exposes all 60 imports).

**Branch:** core-lite `feat/dynamic-contracts`. Push ONLY to the hackerby888 remote, never qubic/origin.

**Design choice:** Option A — one read-only accessor in `qpi.h`; all logic in a new `extensions/` file. No
change to core contract semantics, no new QPI public API, no `qpi_oracle_impl.h` edits.

---

## Established facts (verified)

- **Contract side is complete.** `lite_wasm_tu.h:70-73` declares all 4 `lh_*` imports + the `QpiContext`
  method bodies (`__qpiQueryOracle`, `__qpiSubscribeOracle`, `getOracleQuery`, `getOracleReply`). No change.
- **Oracle engine is complete.** `oracle_engine.h` exposes `startContractQuery(contractIndex, ifaceIdx, query*,
  size, timeout, procId)` → int64 queryId (707); `startContractSubscription(..., period, procId, tsOffset)` →
  int32 subId (776); `getOracleQuery(id,out,size)` (2435); `getOracleReply(id,out,size)` (2443);
  `getOracleQueryStatus` (2471); `stopContractSubscription` (987); `refundFees` (1221). No change.
- **Gap = host bridge only.** Today `lite_dyn_abi.h`/`lite_dynamic_contracts.h`/`lite_wasm_imports.h` bind just
  `getOracleQueryStatus` + `unsubscribeOracle`. Add the other 4.
- **Identity:** the running contract's index lives in `QpiContext::_currentContractIndex` (protected, qpi.h:2495).
  `contractId = m256i(index,0,0,0)` (qpi.h:2487). No public getter exists → Option A adds one.
- **Fee authority:** `getOracleQueryFeeFunc[ifaceIdx]` (and the subscription-fee equivalent) is a global
  fn-ptr table keyed by interface index (oracle_interfaces_def.h:43). The host recomputes the fee from
  `ifaceIdx` and IGNORES the wasm-supplied fee → a malicious contract cannot underpay.
- **Slot alignment (verified):** dyn-registry reports GLOBAL indices `LITEDYN0 + i` (rpc_live_controller.h:537);
  qinit builds + deploys to that exact index; node runtime index = same. `__contract_index` (baked) ==
  `_currentContractIndex` (runtime). So reading `_currentContractIndex` is correct.
- **Async notification delivery exists:** on oracle resolution the engine emits `{contractIndex, procedureId}`
  (oracle_engine.h:2270/2291); `qubic.cpp:3796-3811` looks the proc up in `userProcedureRegistry` and
  `qubic.cpp:2613-2625` invokes it via `QpiContextUserProcedureNotificationCall::call()`. Wasm procs are
  native-callable via libffi closures → `liteWasmDispatch`, installed in `contractUserProcedures[idx][it]`
  (lite_wasm_contracts.h:486).

---

## Task 0 (GATE) — RESOLVED: Outcome B (wasm procs are NOT in `userProcedureRegistry`)

Traced. Async oracle notifications do NOT reach wasm contracts today:
- procId scheme: `notificationProcId = (CONTRACT_INDEX << 22) | __LINE__` (qpi.h:3144), globally unique;
  QUERY_ORACLE/SUBSCRIBE_ORACLE store this full id (qpi.h:3286/3323).
- `userProcedureRegistry` = `procId -> {procedure, contractIndex, sizes}` hash map (contract_def.h:652-690),
  keyed by full procId.
- Native contracts populate it via `__registerUserProcedureNotification` -> `add()` (contract_exec.h:964).
- Wasm contracts register into `g_wasmTuEntries[]` ONLY (lite_wasm_tu.h:183), storing procId truncated to the
  LOW 16 bits (= `__LINE__`). `userProcedureRegistry` is referenced NOWHERE in `extensions/`.
- => `userProcedureRegistry->get(fullProcId)` (qubic.cpp:3801) returns null for a wasm query -> ASSERT / dropped.

The Outcome-B fix (Task 6) is host-only, no ABI change. See Task 6. `query`/`subscribe` return values +
`getOracleQuery`/`getOracleReply`/`getOracleQueryStatus` (poll model) work WITHOUT Task 6; only the push
notification needs it.

---

## Task 1 — `qpi.h`: one read-only accessor

In the base `QpiContext` public section:
```cpp
// Wasm host-bridge only: expose the running contract's index so extensions/lite_oracle_bridge.h can drive
// oracleEngine on the contract's behalf (identity is otherwise protected). Read-only; no behavior change.
unsigned int __qpiCurrentContractIndex() const { return _currentContractIndex; }
```
Single line, pure getter. Only edit to core. **Files:** `contracts/qpi.h`.

---

## Task 2 — `lite_dyn_abi.h`: 4 fn pointers

Add to `LiteHostServices` (after line 101):
```c
long long    (*queryOracle)(const void* ctx, unsigned int ifaceIdx, const void* query, unsigned int querySize, unsigned int notifProcId, unsigned int timeoutMs, long long fee);
int          (*subscribeOracle)(const void* ctx, unsigned int ifaceIdx, const void* query, unsigned int querySize, unsigned int notifProcId, unsigned int periodMs, unsigned int notifyPrev, long long fee);
unsigned int (*getOracleQuery)(const void* ctx, long long queryId, void* out, unsigned int size);
unsigned int (*getOracleReply)(const void* ctx, long long queryId, void* out, unsigned int size);
```

---

## v1 scope (confirmed by pre-code checks)

- **queryOracle, getOracleQuery, getOracleReply — REAL.** All deps present: `OI::getOracleQueryFeeFunc[i]`
  (`sint64(*)(const void*)`), `oracleEngine.startContractQuery(uint16,uint32,const void*,uint16,uint32,uint)`
  →int64, `getOracleQuery/getOracleReply(int64,void*,uint16)`→bool, `MIN_ORACLE_QUERY_FEE=10`,
  `oracleInterfaces[i].querySize/replySize`. Extensions-only + the 1-line qpi.h getter.
- **subscribeOracle — STUB returning -1 in v1.** Satisfies import parity (binds → module instantiates),
  degrades cleanly. Real support deferred to v2 because it needs, in `oracle_interfaces_def.h`:
  (1) a `getOracleSubscriptionFeeFunc[]` table (only the query fee table exists), and (2) a per-interface
  subscription timestamp offset — `offsetof(OracleQuery, timestamp)` — which can't be a shared table since only
  `Price` has a `timestamp` member (`Mock`/`DogeShareValidation` are query-only). Only `Price` is subscribable.
- **Task 6 push notifications — REAL.** `MAX_CONTRACT_PROCEDURES_REGISTERED=16384`, ample.
- Scope confirmed: oracle globals (`oracleEngine`, `OI`, `logger`, spectrum) are in scope where
  `lite_dynamic_contracts.h` is included (qubic.cpp:193, after oracle headers 158-162).

## Task 3 — `extensions/lite_oracle_bridge.h` (NEW): the actual logic

All fee/log/engine work, using globals + the Task-1 accessor. No core touch.

```c
#pragma once
// Host-side implementation of the wasm oracle imports. Mirrors qpi_oracle_impl.h's non-templated subset:
// recompute the fee authoritatively from the interface (never trust the wasm-passed value), burn it, start
// the query/subscription on the shared oracleEngine. getQuery/getReply need no identity.

static inline unsigned int liteOracleContractIndex(const void* ctx) {
    return ((const QPI::QpiContextProcedureCall*)ctx)->__qpiCurrentContractIndex();
}

static long long liteWasmQueryOracle(const void* ctx, unsigned int ifaceIdx, const void* query,
                                     unsigned int querySize, unsigned int notifProcId,
                                     unsigned int timeoutMs, long long /*passedFee ignored*/) {
    if (ifaceIdx >= OI::oracleInterfacesCount) return -1;
    if (querySize != OI::oracleInterfaces[ifaceIdx].querySize) return -1;
    const unsigned int ci = liteOracleContractIndex(ctx);
    const m256i contractId = m256i(ci, 0, 0, 0);

    // authoritative fee from the interface table — NOT the wasm-passed value
    const long long fee = getOracleQueryFeeFunc[ifaceIdx](query);
    const int spectrumIdx = ::spectrumIndex(contractId);
    if (fee < MIN_ORACLE_QUERY_FEE || spectrumIdx < 0 || !decreaseEnergy(spectrumIdx, fee)) return -1;
    const QuTransfer t = { contractId, m256i::zero(), fee };
    logger.logQuTransfer(t);

    const long long queryId = oracleEngine.startContractQuery(
        (unsigned short)ci, ifaceIdx, query, (unsigned short)querySize, timeoutMs, notifProcId);
    if (queryId < 0 && fee > 0) oracleEngine.refundFees(contractId, fee);
    return queryId;
}

static int liteWasmSubscribeOracle(const void* ctx, unsigned int ifaceIdx, const void* query,
                                   unsigned int querySize, unsigned int notifProcId,
                                   unsigned int periodMs, unsigned int notifyPrev, long long /*ignored*/) {
    if (ifaceIdx >= OI::oracleInterfacesCount) return -1;
    if (querySize != OI::oracleInterfaces[ifaceIdx].querySize) return -1;
    const unsigned int ci = liteOracleContractIndex(ctx);
    const m256i contractId = m256i(ci, 0, 0, 0);

    const long long fee = getOracleSubscriptionFeeFunc[ifaceIdx](query, periodMs);   // confirm exact name/arity
    const int spectrumIdx = ::spectrumIndex(contractId);
    if (fee < MIN_ORACLE_SUBSCRIPTION_FEE || spectrumIdx < 0 || !decreaseEnergy(spectrumIdx, fee)) return -1;
    const QuTransfer t = { contractId, m256i::zero(), fee };
    logger.logQuTransfer(t);

    const unsigned short tsOffset = OI::oracleInterfaces[ifaceIdx].timestampOffset;   // confirm field
    const int subId = oracleEngine.startContractSubscription(
        (unsigned short)ci, ifaceIdx, query, (unsigned short)querySize, periodMs, notifProcId, tsOffset);
    if (subId < 0 && fee > 0) oracleEngine.refundFees(contractId, fee);
    // v1: notifyPrev immediate callback deferred (see Tiers). subId still returned.
    (void)notifyPrev;
    return subId;
}

static unsigned int liteWasmGetOracleQuery(const void*, long long id, void* out, unsigned int size) {
    return oracleEngine.getOracleQuery(id, out, (unsigned short)size) ? 1u : 0u;
}
static unsigned int liteWasmGetOracleReply(const void*, long long id, void* out, unsigned int size) {
    return oracleEngine.getOracleReply(id, out, (unsigned short)size) ? 1u : 0u;
}
```

Wire the pointers where `g_liteHostServices` is assigned (`lite_dynamic_contracts.h`, after the existing
`.getOracleQueryStatus`/`.unsubscribeOracle` lines):
```c
.queryOracle     = &liteWasmQueryOracle,
.subscribeOracle = &liteWasmSubscribeOracle,
.getOracleQuery  = &liteWasmGetOracleQuery,
.getOracleReply  = &liteWasmGetOracleReply,
```
`#include "lite_oracle_bridge.h"` in `lite_dynamic_contracts.h` (after the oracle headers are visible).

**Confirm-before-code:** exact name/arity of `getOracleSubscriptionFeeFunc`, the `oracleInterfaces[]` field
names (`querySize`, `timestampOffset`), and that `getOracleQueryFeeFunc`/`decreaseEnergy`/`logger`/`oracleEngine`
are all in scope from `lite_dynamic_contracts.h`'s include point.

---

## Task 4 — `lite_wasm_imports.h`: 4 wrappers + 4 table rows

Wrappers (after line 133):
```c
static int64_t  w_queryOracle(wasm_exec_env_t e, uint32_t i, uint32_t q, uint32_t qs, uint32_t p, uint32_t t, int64_t f) { LWC; return g_liteHostServices.queryOracle(cc->ctx, i, A2N(q), qs, p, t, f); }
static int32_t  w_subscribeOracle(wasm_exec_env_t e, uint32_t i, uint32_t q, uint32_t qs, uint32_t p, uint32_t per, uint32_t np, int64_t f) { LWC; return g_liteHostServices.subscribeOracle(cc->ctx, i, A2N(q), qs, p, per, np, f); }
static uint32_t w_getOracleQuery(wasm_exec_env_t e, int64_t id, uint32_t o, uint32_t s) { LWC; return g_liteHostServices.getOracleQuery(cc->ctx, id, A2N(o), s); }
static uint32_t w_getOracleReply(wasm_exec_env_t e, int64_t id, uint32_t o, uint32_t s) { LWC; return g_liteHostServices.getOracleReply(cc->ctx, id, A2N(o), s); }
```
Table rows (after line 200), sigs verified vs `lite_wasm_tu.h:70-73` (`i`=i32, `I`=i64):
```c
HQ("queryOracle",     queryOracle,     w_queryOracle,     "(iiiiiI)I")
HQ("subscribeOracle", subscribeOracle, w_subscribeOracle, "(iiiiiiI)i")
HQ("getOracleQuery",  getOracleQuery,  w_getOracleQuery,  "(Iii)i")
HQ("getOracleReply",  getOracleReply,  w_getOracleReply,  "(Iii)i")
```

| import | params | ret |
|---|---|---|
| queryOracle | iface, query*, size, procId, timeout, **fee:I** | queryId:**I** |
| subscribeOracle | iface, query*, size, procId, period, notifyPrev, **fee:I** | subId:i |
| getOracleQuery | **queryId:I**, out*, size | i (0/1) |
| getOracleReply | **queryId:I**, out*, size | i (0/1) |

Guard the copy in `getOracleQuery/getOracleReply` — the engine already checks `replySize ==
oracleInterfaces[..].replySize` internally; the wasm-passed `size` bounds the copy into contract memory.

---

## Task 5 — build + import-parity check

- Build the node (feat/dynamic-contracts) → confirm the `LHOST_TABLE`/`NativeSymbol` static_asserts pass (sig
  strings match the templated ABI).
- Re-run the qinit import-parity probe → expect **0 TS-only imports** (node now provides all 60).

---

## Task 6 — async notification: register wasm procs in `userProcedureRegistry` (host-only, no ABI change)

Required for the push-notification model (Task 0 = Outcome B). At wasm arm (lite_wasm_contracts.h, where the ffi
closures for `contractUserProcedures[idx][it]` are already built), loop the PROCEDURE entries:
```c
// contractIndex = the arm slot = baked CONTRACT_INDEX (verified equal); entry.inputType = low-16 = __LINE__.
unsigned int fullProcId = (contractIndex << 22) | entry.inputType;
userProcedureRegistry->add(fullProcId, { ffiClosure /*same one in contractUserProcedures[idx][it]*/,
                                         contractIndex, localsSize, inSize, outSize });
```
- `procedure` = the ffi closure → the notification re-enters wasm via `liteWasmDispatch`, like a normal call.
- **Register-all procedures** — notification procs are stored as plain `PROCEDURE` (indistinguishable), extras
  are harmless (never looked up unless a notification target). Avoids changing the wasm entry format / `reg_info`
  / the engine reader. No contract-side or ABI change.

Complications (all minor):
1. `MAX_CONTRACT_PROCEDURES_REGISTERED` capacity — bounded (4 dynamic slots × few procs); confirm headroom.
2. Re-arm/redeploy — clear the contract's stale entries first (unregister-by-contractIndex, or overwrite).
3. Reconstruction assumes `__LINE__ < 65536` (low-16 == full line) — true for real contracts.

If deferred: v1 ships POLL-ONLY oracle (query + status + getReply); document that push notifications are pending.

---

## Task 7 — tests (parity against the qinit engine, the behavioral spec)

- `TestExampleC.h` / `QUtil.h` already carry oracle procedures (`UnsubscribeOracle`, `getOracleQueryStatus`
  asserts). Build them as **dynamic wasm**, drive via `qinit gtest --corpus`.
- End-to-end: a wasm contract issues `QUERY_ORACLE`, the oracle resolves, its notification procedure fires and
  writes state; assert the reply landed. Compare native core-lite vs the qinit engine (`sim.ts` oracle
  collaborator) → identical queryId / status / reply bytes.
- Fee: assert a contract passing an under-value fee is charged the authoritative interface fee (host recompute),
  not the passed one.

---

## Scope tiers

- **v1** — return-code + async delivery: query/subscribe return queryId/subId; async oracle replies notify the
  contract by procId (given Task 0 Outcome A or B). Skip the immediate synchronous callbacks (error-notify;
  `notifyWithPreviousReply`), which native fires via ptr and wasm would need mid-procedure re-entry for.
- **v2** — full parity: immediate error / notifyPrev notification via synchronous procId re-entry into the wasm
  contract. Only if a contract depends on it.

---

## Files touched

| file | change | core? |
|---|---|---|
| `contracts/qpi.h` | 1 read-only getter | yes (1 line, no behavior) |
| `extensions/lite_dyn_abi.h` | 4 fn ptrs | no (feature) |
| `extensions/lite_dynamic_contracts.h` | 4 ptr bindings + include | no |
| `extensions/lite_oracle_bridge.h` | NEW — all logic | no |
| `extensions/lite_wasm_imports.h` | 4 wrappers + 4 rows | no |
| (Task 0-B, maybe) wasm arm path | register procs in userProcedureRegistry | no |

## Risks

1. **Task 0 Outcome B** — if wasm procs aren't in `userProcedureRegistry`, async delivery needs wiring. Size
   after the trace; this is the only place scope can grow.
2. **Fee-function names/arity** — `getOracleSubscriptionFeeFunc` + `oracleInterfaces[]` field names must be
   confirmed before coding Task 3.
3. **v2 synchronous re-entry** — deferred; document the semantic gap vs native so contract authors know v1
   reports errors via return value, not an immediate notification.

## Relationship to the compiler tree-shake

Independent. This makes oracle-using wasm contracts run on a real node (superset parity). The qinit compiler
tree-shake (emit only referenced imports) is still worth doing separately — smaller wasm — and would let
non-oracle contracts deploy real-node even before this lands.

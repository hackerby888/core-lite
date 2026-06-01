# Runtime Smart-Contract Deployment (testnet dev feature)

**Status:** design / not yet implemented
**Scope:** testnet + dev only. Hard-gated behind `TESTNET` and `LITE_DYNAMIC_CONTRACTS`.
**Goal:** deploy a smart contract into a running node without recompiling the core — compile a
contract to a shared object (`.so`), distribute it, and register + construct it on-chain. First
step toward an Anchor-like contract framework for Qubic (build / deploy / upgrade / IDL / client
codegen loop).

> ⚠️ **Not a mainnet mechanism.** Loading a `.so` runs arbitrary native code in the node process
> (RCE by design). Mainnet contracts stay compiled-in and IPO/quorum-governed. This feature must
> never be compiled into a shippable mainnet binary, and the framework must never wire `deploy` to
> a mainnet target. See [Security](#security).

---

## 1. Why this is non-trivial

Contracts are bound into the core at **compile time** by two constants:

- `MAX_NUMBER_OF_CONTRACTS = 1024` (`network_messages/common_def.h:9`) — the computer-state digest
  is a fixed 1024-leaf Merkle tree (`qubic.cpp:289`, `getComputerDigest()` at `:631`). Leaves
  `[contractCount .. 1023]` hash zero. **Spare leaves already exist.**
- `contractCount = sizeof(contractDescriptions)/…` (`contract_def.h:414`) — `constexpr`, ~28. This
  is the real knob. Every per-contract dispatch table is sized to it:
  `contractUserFunctions[contractCount][65536]`, `contractUserProcedures`, the size tables,
  `contractSystemProcedures[contractCount][12]`, `contractExpandProcedures[contractCount]`,
  `contractStateLock[contractCount]` (`contract_exec.h:62`). They are sized to `contractCount`,
  **not** 1024 (1024×65536×8 ≈ 512 MB each would be far too large).

Consequence: a contract index `>= contractCount` is **out of bounds** on every dispatch table, and
the routing/bounds checks reject it (`qubic.cpp:2655`, `:1715/:1716`). A new contract index must be
**counted in `contractCount`** to work — and `contractCount` is computed inside `contract_def.h`,
which has no extension hook. A new *counted* contract therefore cannot be added purely from an
extension; a minimal, guarded edit to `contract_def.h` is unavoidable. Everything else lives in
`src/extensions/`.

---

## 2. Architecture

Three concerns, cleanly separated — mapping onto Qubic's native proposal → construction lifecycle:

```
CONSENSUS (in a tick)          NODE-LOCAL (off-chain)        CONSTRUCTION (core-framed)
deploy tx → loader registry    .so bytes matched by hash     INITIALIZE at activation tick
framed by its own tx index     patch slot's fn-ptr tables    framed by SC_INITIALIZE_TX
all nodes agree {slot,H,tick}  (pointers are NOT consensus)  → correct event stream
```

### 2.1 Slot model (Anchor analogy)

| Anchor | Qubic dynamic deploy |
| --- | --- |
| program id / address | reserved **slot index** |
| program binary | staged **`.so`** |
| `anchor deploy` / `upgrade` | (re-)stage `.so` into a slot |
| IDL | descriptor the `.so` exports (§5.3) |
| program registry / upgradeable loader | **loader contract** (§2.2) |
| account model / PDAs | *no analog* — state is one `StateData` blob + QPI collections |
| local validator + tests | testnet node + deploy tx + RPC |

Compiled in **only** under `LITE_DYNAMIC_CONTRACTS` (testnet):

- **Deployable slots `D0..Dn`** — reserved contract slots. Their `StateData` holds **only the
  deployed contract's own business state** (digested, consensus). Register nothing at boot; user
  `.so`s land here.
- **Control entry point** — a guarded `processTickTransaction` hook (no contract): upload/deploy txs
  target the system address with lite-range `inputType`s and dispatch to the extension (§2.2).

> **Framework state is extension-owned, never in any contract `StateData`.** The deploy **registry**
> and the uploaded **`.so` bytes** are framework plumbing, not contract business data — they live in
> extension globals, persisted to the extension's own files (§2.2). Putting them in a `StateData`
> would conflate infrastructure with consensus business state.

Deployable slots are counted in `contractCount`, so *all* core machinery works unmodified for them:
dispatch-table sizing, `contractStateLock`, state-buffer allocation (`qubic.cpp:7587`), the digest
tree, transaction routing, and the function-call RPC.

### 2.2 Deploy subsystem & on-chain upload

The registry and the raw `.so` bytes are framework plumbing, held in **extension-owned** state
(globals + the extension's own persisted files), content-addressed by K12 hash — never in a contract
`StateData`. Determinism holds because this state is a pure function of consensus txs (below), and
the *constructed contract's* `StateData` (which IS digested) catches any execution divergence.

**Entry mechanism — tx-dispatch hook (chosen, no contract).** Upload/deploy txs reuse the existing
*"destination is system"* protocol-tx path: `destinationPublicKey == 0` with new `inputType`s in the
lite range (mirroring the `LiteCheckin` 230+ convention, to avoid colliding with upstream protocol tx
types). A guarded `#ifdef` block adds cases to the `isZero(destination)` switch in
`processTickTransaction` (`qubic.cpp:2930`), beside the existing `MiningSolution` / `Oracle*` /
`FileFragment` cases:

```cpp
#ifdef LITE_DYNAMIC_CONTRACTS
case LiteUploadBeginTx::transactionType(): liteDynOnUploadBegin(transaction); break;
case LiteUploadChunkTx::transactionType(): liteDynOnUploadChunk(transaction); break;
case LiteDeployTx::transactionType():      liteDynOnDeploy(transaction);      break;
#endif
```

The handlers mutate only extension-owned state (blob store + registry), run in the same
tick-processor context as the existing system-tx handlers, and the txs still pay fees via the normal
`decreaseEnergy` path above the switch. No contract, no loader slot.

**Transport = on-chain chunked txs** (chosen). `MAX_INPUT_SIZE = 1024`,
`NUMBER_OF_TRANSACTIONS_PER_TICK = 4096` → ~1008 B payload/chunk × 4096 ≈ **4 MB/tick capacity**
(capacity, not a guarantee — the leader controls inclusion, so a large `.so` may span several ticks).

```
tx UploadBegin { sessionId, totalSize, chunkCount, finalHash }
tx UploadChunk { sessionId, seq, len, bytes[<=1008] }
tx Deploy      { sessionId, targetSlot, finalHash, abiVersion, stateLayoutVersion }
auth (all): source / invocator() == configured deployer pubkey
```

**Tx order is leader-chosen but consensus-fixed** — it is recorded in `tickData` and agreed by
quorum, so every node replays the identical order; "random" order never causes divergence.
Reassembly is made order-independent regardless:

- **Self-addressing chunks → scatter-write.** `buf[seq*CHUNK ..] = bytes`. Any order, any number of
  ticks → the same buffer. No append, no ordering assumption.
- **Completion = seq bitmap full** (all `chunkCount` seqs present), not "last chunk seen." The tick
  stream is consensus, so every node flips *complete* on the same tick.
- **Hash gate.** Blob is usable only when `K12(buf[0:totalSize]) == finalHash` — catches any
  missing / duplicate / corrupt chunk independent of order.
- **Anti-grief.** A session is scoped to its deployer pubkey; only that key may write its
  `sessionId`. Stops a third party poisoning a `seq` to break the hash.
- **ARQ for dropped chunks.** The leader may omit chunks; the uploader polls a read endpoint /
  function returning the missing-seq bitmap and resends only the gaps.

**Activation is derived, never an uploader-chosen tick.** The uploader cannot know which tick the
leader finishes inclusion on, so timing is computed:

```
construct slot D at the first tick where:
    (a Deploy intent for {D, finalHash} exists)
    AND (blob[finalHash] is complete AND K12 == finalHash)
```

Both conditions are pure functions of consensus txs → every node derives the same trigger tick
(whichever becomes true last). Leader ordering of `Deploy` vs the chunk txs is irrelevant: whichever
lands first waits for the other. (This replaces the earlier, wrong "uploader picks `activationTick`"
idea.)

A read endpoint (lite HTTP GET / request message, served by the extension) exposes
`{ slot, finalHash, status, missing-seq bitmap, version }` for explorers, clients, and the uploader's
ARQ loop — the seed of an IDL/program registry.

---

## 3. Deploy lifecycle (design "B'": arbitrary-tick, correctly framed)

### 3.1 Logging-framing invariant (the hard constraint)

Every system-procedure phase registers a pseudo-tx **before** running, so emitted log events pair
with the correct frame (`qubic.cpp`):

```
:3561  logger.registerNewTx(tick, SC_INITIALIZE_TX);   then INITIALIZE phase
:3572  logger.registerNewTx(tick, SC_BEGIN_EPOCH_TX);  then BEGIN_EPOCH phase
:3581  logger.registerNewTx(tick, SC_BEGIN_TICK_TX);   then BEGIN_TICK phase
:3726  logger.registerNewTx(tick, transactionIndex);   per real transaction
```

**INITIALIZE events must appear under `SC_INITIALIZE_TX`.** Third-party indexers key off the
pseudo-tx reason; INITIALIZE events tagged as anything else (e.g. `SC_BEGIN_TICK_TX`) corrupt their
view. So construction must run under genuine `SC_INITIALIZE_TX` framing — not merely "paired with
some tx."

### 3.2 Threading

`processTick` (tick-processor thread) drives phases via
`contractProcessorPhase = X; contractProcessorState = 1; WAIT_WHILE(contractProcessorState)`.
`contractProcessor()` (`qubic.cpp:2510`) runs the work on a **separate thread** and clears the flag
when done (`:2726`). Contract code only ever runs on the contractProcessor thread (it `enableAVX()`s
at entry, `:2512`). Construction must reuse this handshake — not call INITIALIZE directly from the
processTick thread.

### 3.3 Flow

1. **Deploy tx (consensus).** A `Deploy` procedure call to the loader. Processed in-tick, framed by
   its own tx index (`:3726`) — correct. Writes the registry entry. All nodes see it at the same
   tick.

2. **Stage (node-local, any time before activation).** Each node `dlopen`s the local `.so` whose
   K12 hash matches `registry[D].codeHash`, validates the descriptor (§5.3), and patches slot `D`'s
   dispatch tables. Safe to do off-tick: a pre-construction slot has **no readers** (execution
   phases are gated `epoch >= constructionEpoch`, RPC rejects `epoch < constructionEpoch` at
   `:1716`), and function-pointer values are per-process, **not** consensus. If the `.so` is
   missing, the node logs loudly and must halt rather than diverge (§4).

3. **Framed construction (consensus, at `activationTick`).** Two tiny guarded insertion points:

   - In `processTick`, single-threaded section, **before** the BEGIN_TICK phase (`:3580`):
     ```cpp
     #ifdef LITE_DYNAMIC_CONTRACTS
     if (liteDynPendingForTick(system.tick)) {                  // reads loader registry
         logger.registerNewTx(system.tick, logger.SC_INITIALIZE_TX);  // correct frame
         contractProcessorPhase = LITE_DYN_INITIALIZE;
         contractProcessorState = 1;
         WAIT_WHILE(contractProcessorState);
         // optional: repeat with SC_BEGIN_EPOCH_TX + LITE_DYN_BEGIN_EPOCH to mirror epoch start
     }
     #endif
     ```
   - In `contractProcessor()`'s `switch` (runs on the safe thread):
     ```cpp
     #ifdef LITE_DYNAMIC_CONTRACTS
     case LITE_DYN_INITIALIZE:
         liteDynConstructPending(system.tick);   // extension
         break;
     #endif
     ```
   - Extension body — identical context to a real construction (AVX on, contractProcessor thread):
     ```cpp
     for (each slot S with registry[S].activationTick == tick && not already constructed) {
         patchSlotTables(S, localSo[registry[S].codeHash]);   // idempotent; non-consensus
         setMem(contractStates[S], slotStateSize, 0);
         QpiContextSystemProcedureCall(S, INITIALIZE).call();  // framed by SC_INITIALIZE_TX
         stampConstructedVersion(S, registry[S].version);      // into slot StateData header
     }
     ```

   Placing it before BEGIN_TICK means the freshly constructed slot also gets its BEGIN_TICK and is
   live for this tick's transactions.

### 3.4 Idempotency / restart

- Stamp `constructedVersion` in the slot's `StateData` header. On restart the core reloads the slot
  state from disk; the hook compares `header.version == registry.version` and **skips re-INITIALIZE**
  (so it never wipes live state). Survives restarts.
- Re-deploy = registry `version++` → the hook re-constructs (a reinit upgrade, §6).

### 3.5 Boot-time deploy (special case)

If the chosen slot's compile-time `constructionEpoch` equals the node's start epoch, the core's own
INITIALIZE phase (`:3556-3568`) constructs it with native `SC_INITIALIZE_TX` framing and a real
BEGIN_EPOCH — no hook needed. Boot deploy just patches tables after `initializeContracts()`
(`:7861`); the rest is the normal contract lifecycle. Use it when you want a contract present from
genesis with a full, unmodified lifecycle.

### 3.6 Bootstrap gates (must hold for a slot to run)

- **Fee reserve > 0** — BEGIN_TICK is fee-gated (`:2578`). Fresh testnet boot auto-seeds every slot
  with 10B (`:7849-7857`), so this is free on a clean start; it persists across restarts.
- **`contractError == NoContractError`** — `initializeContractErrors()` (`:7860`, gate at
  `contract_exec.h:225/:385`) stamps `ContractErrorIPOFailed` on slots without
  `NUMBER_OF_COMPUTORS` shares. Dev slots skip IPO, so clear `contractError[slot]` for all dev slots
  once, **after** `:7860`.

---

## 4. Determinism & multi-node

- **Timing & order** are consensus: upload/deploy txs live in `tickData` (quorum-agreed set *and*
  order). The extension blob store + registry are a deterministic function of that tx stream, so
  every node assembles identical bytes and derives the same activation tick (§2.2). Framework state
  itself need not be in the computer digest.
- **Code identity** is hash-verified (`K12(blob) == finalHash`) before use. Function-pointer *values*
  are per-process and never hashed; only the constructed contract's `StateData` and its execution
  effects are consensus — and that `StateData` IS digested, so any divergence in deployed behavior
  surfaces as a computer-digest mismatch.
- **Restart:** the extension persists assembled blobs (content-addressed) + the registry to its own
  files; on boot it re-`dlopen`s and re-patches each armed slot. No re-upload.
- **Late joiner from a state snapshot** does not get the bytes via contract-state sync (they are in
  no `StateData`). It obtains them by replaying the relevant ticks, or — preferred — a
  content-addressed peer fetch: the registry tells it which `finalHash`es it needs; it requests them
  from peers (a lite 230+ message, §9 phase 2) and hash-verifies. Missing + unfetchable → halt
  loudly, never guess.

---

## 5. The `.so` ABI

### 5.1 Empirical finding — `-rdynamic` alone does not work

The core is a single translation unit (`SINGLE_COMPILE_UNIT`) and inlines aggressively. On the
current build (`nm` of `cmake-build-relwithdebinfo/src/Qubic`):

```
nm -D  QpiContext exports      : 0
full symtab QpiContext symbols : 824 W (weak/comdat), 0 T (strong global)
  present (weak): issueAsset, __qpiAcquireStateForWriting, burn, …
  ABSENT entirely: transfer, numberOfShares, __registerUserProcedure, …
  ABSENT (inline-declared): getOracleQueryStatus, setShareholderProposal,
                            setShareholderVotes, unsubscribeOracle
```

Methods whose every host call site is inlined emit **no standalone symbol**. `-rdynamic` only
promotes already-emitted symbols into the dynamic table; it cannot export what does not exist. So
name-binding a `.so` against the host would leave core methods (`transfer`, `numberOfShares`, …)
unresolved → `dlopen` fails. Release inlines even more. **Conclusion: do not rely on `-rdynamic`.**

### 5.2 Resolution — explicit host-services vtable (bind by pointer, not name)

The host hands the `.so` a table of thin free-function wrappers, one per needed `QpiContext`
method. Each wrapper references the method, which **forces emission and absorbs the inlining**; the
wrapper is the stable entry. The `.so` calls through the table — immune to inlining, comdat GC, and
name mangling.

```cpp
// host side: building the vtable forces the methods to be emitted
svc.transfer       = +[](QPI::QpiContextProcedureCall* c, const m256i& d, long long a){ return c->transfer(d, a); };
svc.numberOfShares = +[](QPI::QpiContextFunctionCall*  c, const QPI::Asset& a, /*…*/)  { return c->numberOfShares(a /*…*/); };
// … ~58 entries …
// plus the static/template backends (§Appendix C/D):
svc.k12      = +[](const void* in, unsigned len, void* out32){ /* host KT128 */ };
svc.logBytes = +[](unsigned lvl, const void* m, unsigned n){ /* host logger */ };
svc.beginFn  = …; svc.endFn = …; svc.markDirty = …; svc.acquireScratch = …; svc.releaseScratch = …;
svc.pauseLog = …; svc.resumeLog = …;
```

```cpp
// .so side (lite_dyn_abi.h, included after qpi.h): member forwarders + static defs + template defs
inline LiteHostServices* g_host = nullptr;
extern "C" void liteSetHostServices(LiteHostServices* s) { g_host = s; }

long long QPI::QpiContextProcedureCall::transfer(const m256i& d, long long a) const
{ return g_host->transfer(const_cast<QpiContextProcedureCall*>(this), d, a); }

// the 11 pre_qpi_def.h statics, defined locally as forwarders:
static void __beginFunctionOrProcedure(unsigned int id){ g_host->beginFn(id); }
// … etc …

// host-TU template methods the .so cannot otherwise instantiate (forward to non-template backends):
template <typename T> m256i QPI::QpiContextFunctionCall::K12(const T& d) const
{ m256i o; g_host->k12(&d, sizeof(T), &o); return o; }
```

Handshake: the `.so` exports a single `extern "C" liteSetHostServices(...)`; the host calls it at
`dlopen`. **No `-rdynamic`, no exported host symbols, no data symbols.** (No host *data* symbols are
needed: a scan of `qpi.h` found no inline QPI code that reads `system`/`spectrum`/`universe`/
`assets` directly — all host state is reached through methods, now all in the vtable.)

Cost: ~58 host wrappers + ~58 `.so` forwarders + ~9 static/template backends. All mechanical and
**codegen-able from the `qpi.h` declarations** — a small generator is itself a future framework
tool (the "ABI generator"). Phase-1 can restrict deployed contracts to the non-oracle surface and
omit the 6 oracle/cross-contract template entries (Appendix C); add them in phase 2.

ABI risks, all acceptable for a same-compiler dev tool: template definitions differing across TUs
are technically ODR-UB but benign (disjoint instantiation sets, no cross-TU inlining); pin
**clang-18** both sides.

### 5.3 Descriptor (seed of the IDL)

Beyond code, the `.so` exports a machine-readable descriptor so the framework can generate clients
and track versions:

```cpp
struct LiteEntrypoint { uint16 inputType; uint8 kind; char name[32];
                        uint16 inputSize, outputSize, localsSize; };
struct LiteContractDescriptor {
    uint32 abiVersion;          // ABI compatibility gate
    char   name[16];
    uint64 stateSize;
    uint32 stateLayoutVersion;  // for upgrade / migration (§6)
    m256i  codeHash;            // multi-node verification
    uint16 entrypointCount;
    LiteEntrypoint entrypoints[];
};
extern "C" const LiteContractDescriptor* liteContractDescriptor();
extern "C" void liteContractRegister(/* fills sysproc fn ptrs + runs __registerUserFunctionsAndProcedures */);
```

Runtime truth (sizes, input-type ids, hash) comes from the `.so`. Rich type info (field names,
nested layouts) is emitted as a separate IDL JSON by the build tool, kept consistent with the
descriptor. Do not try to extract full C++ type layouts at runtime.

### 5.4 Build recipe ("anchor build")

Deterministic and reproducible (so `codeHash` agrees across nodes):

- clang-18, fixed flags, `-fPIC -shared`, `-O2` (pin everything).
- include `qpi.h` + the contract source + `lite_dyn_abi.h`; **never** include `contract_exec.h`
  (its method bodies reference host globals the `.so` does not have).
- compile with `CONTRACT_INDEX = <target slot>` and the contract's `CONTRACT_STATE_TYPE` macros.
- `static_assert(sizeof(StateData) <= slotStateSize)`.

---

## 6. Upgrade (designed-for, not yet built)

Upgrade = re-stage a new `.so` into the same slot (new registry `version`). `stateLayoutVersion`
lets the framework choose:

- **Reinit** — wipe state + INITIALIZE (framed). What the dev loop uses; B' already does it.
- **Migrate** — keep state, run a contract-defined migration procedure after the swap. Needs a
  frame; reuse the B' `SC_INITIALIZE_TX` hook (or a dedicated reason). Not built in phase 1; the
  version field + "INITIALIZE does not always wipe" assumption keep the door open.

---

## 7. Upstream footprint (all `#ifdef LITE_DYNAMIC_CONTRACTS`)

Minimized; everything else is in `src/extensions/`.

| File | Change | ~lines |
| --- | --- | --- |
| `contract_core/contract_def.h` | deployable slots `D0..Dn`: includes, `contractDescriptions[]` rows, `REGISTER_…` calls | ~12 |
| `contract_core/contract_def.h` (or near) | `LITE_DYN_INITIALIZE` (+ optional `LITE_DYN_BEGIN_EPOCH`) phase enum value | ~2 |
| `qubic.cpp` `processTick` | framed trigger block before BEGIN_TICK (`:3580`) | ~7 |
| `qubic.cpp` `contractProcessor` | one `case LITE_DYN_INITIALIZE` | ~3 |
| `qubic.cpp` `processTickTransaction` | 3 upload/deploy `inputType` cases in the `isZero(destination)` switch (`:2930`) | ~6 |
| `qubic.cpp` boot | `#include "extensions/lite_dynamic_contracts.h"` + `liteDynBootDeploy()` after `:7861` | ~2 |

No `-rdynamic`. Mainnet build (flag off): zero reserved slots, `contractCount` unchanged, no hooks,
binary and all digests byte-identical to upstream.

### Extension files (new, in `src/extensions/`)

- `lite_dynamic_contracts.h` — **extension-owned framework state** (registry + content-addressed
  blob store, persisted to own files); chunk reassembly (scatter-write / seq bitmap / hash gate /
  ARQ read endpoint); `.so` `dlopen`/validate; slot table patching; `liteDynPendingForTick` /
  `liteDynConstructPending` / `liteDynBootDeploy`; host-services vtable construction; upload/deploy
  tx handlers.
- `lite_dyn_stub_contract.h` — the deployable stub registered for slots `Dn` (registers nothing
  live; patched at deploy); included from `contract_def.h`. No loader contract — entry is the
  tx-dispatch hook.
- `lite_dyn_abi.h` — shipped to **compile** the `.so`: vtable type, `liteSetHostServices`, member
  forwarders, the 11 static forwarders, template definitions (`K12`, logs, optionally oracle).

---

## 8. Mainnet safety

```cpp
#if defined(LITE_DYNAMIC_CONTRACTS) && !defined(TESTNET)
#error "LITE_DYNAMIC_CONTRACTS is testnet-only"
#endif
```

CI builds with `-DONLY_LOGGING=ON` and without the flag, so it never compiles the feature. Flag off
⇒ the codebase is byte-for-byte upstream.

## Security

- Loading a `.so` = arbitrary native code in the node process (RCE by design). Testnet + dev only.
- Authorization is the **deploy tx signature**, checked against a configured deployer pubkey —
  on-chain and auditable. (If any HTTP/CLI control surface is added, it is GET-only per project
  convention, bound to localhost, and is a convenience over the tx, not a substitute.)
- The framework must present `deploy`/`upgrade` as a **dev-loop accelerator**, never a mainnet
  deployment path. State this boundary in the framework README before anyone wires `deploy` to a
  real target.
- Treat `.so` paths as trusted-operator input only; never accept a peer-supplied path.

---

## 9. Phasing

1. **Smoke test** — trivial `.so` + minimal vtable (force-emit the few needed methods via
   `__attribute__((used))` + `-rdynamic` as a throwaway), prove `dlopen` → patch tables → INITIALIZE
   → a user procedure call end-to-end on a single node.
2. **Phase 1** — full vtable (non-oracle surface), reserved slots, the chosen entry mechanism +
   on-chain chunked upload (scatter-write / seq bitmap / hash gate / ARQ), derived activation, B'
   framed construction hook, restart-safe idempotency. Single + multi-node determinism.
3. **Phase 2** — content-addressed peer fetch-by-hash for late joiners (lite 230+ msg);
   oracle/cross-contract template entries; descriptor → IDL JSON generator; client codegen;
   upgrade-with-migration.

---

## Appendix — QPI symbol surface (`.so` imports)

All resolved via the §5.2 vtable (bind by pointer). Counts from the current tree.

**A. External, non-inline, non-template `QpiContext` methods (~54)** — host wrappers force emission:
`transfer __transfer burn issueAsset numberOfShares numberOfPossessedShares
transferShareOwnershipAndPossession acquireShares releaseShares distributeDividends bidInIPO
ipoBidId ipoBidPrice queryFeeReserve arbitrator computor epoch tick year month day hour minute
second millisecond now dayOfWeek getEntity isContractId isAssetIssued signatureValidity nextId
prevId computeMiningFunction initMiningSeed numberOfTickTransactions getPrevSpectrumDigest
getPrevUniverseDigest getPrevComputerDigest __qpiAcquireStateForReading __qpiAcquireStateForWriting
__qpiReleaseStateForReading __qpiReleaseStateForWriting __qpiAllocLocals __qpiFreeLocals
__qpiFreeContext __qpiConstructContextOtherContractFunctionCall __qpiConstructProcedureCallContext
__qpiNotifyPostIncomingTransfer __qpiAbort __registerUserFunction __registerUserProcedure
__registerUserProcedureNotification`

**B. `inline`-declared methods (4)** — confirmed ABSENT from the symtab; the vtable wrappers force
their emission too: `getOracleQueryStatus setShareholderProposal setShareholderVotes
unsubscribeOracle`

**C. Host-TU template methods (6)** — `.so` shim defines, forwarding to non-template backends:
`K12` (required — contracts hash structs), `__qpiCallSystemProc` (internal; reached via non-template
host wrappers, so usually not needed directly), `__qpiQueryOracle __qpiSubscribeOracle
getOracleQuery getOracleReply` (oracle — phase 2 only).

**D. `static` free helpers (11, `pre_qpi_def.h:39-53`)** — `.so` defines as forwarders to vtable:
`__markContractStateDirty __beginFunctionOrProcedure __endFunctionOrProcedure __pauseLogMessage
__resumeLogMessage __acquireScratchpad __releaseScratchpad` + 4 templated
`__logContract{Debug,Error,Info,Warning}Message<T>` (funnel to one `logBytes`).

**Phase-1 minimal vtable ≈ 9 backends** (`beginFn endFn markDirty pauseLog resumeLog acquireScratch
releaseScratch logBytes k12`) **+ the ~58 method wrappers**, omitting all oracle entries.

---

## Decision log

- **Deploy timing via tx, not out-of-band.** Anything consensus-relevant goes through a tick. The
  deploy tx makes activation timing a consensus fact; the loader registry is the on-chain record.
- **Construction stays under `SC_INITIALIZE_TX`.** Hard requirement for third-party indexers. Ruled
  out running INITIALIZE under any other frame.
- **Chose B' (arbitrary-tick, framed hook) over A' (epoch-boundary only).** Framework DX needs
  instant, repeatable deploy without epoch waits or restarts. B' adds one framed `processTick` hook
  + one `contractProcessor` case, both guarded.
- **Reuse the contractProcessor thread for construction**, not a direct call from the processTick
  thread (AVX + the never-run-contract-code-off-that-thread invariant).
- **`.so` ABI is an explicit vtable, not `-rdynamic`.** `nm` proved most `QpiContext` methods are
  inlined away (weak/absent), so name-binding fails. Vtable binds by pointer — robust against the
  inliner, comdat GC, and mangling.
- **Framework state is extension-owned, never in contract `StateData`.** The `.so` bytes + deploy
  registry are infrastructure; only the deployed contract's own business state lives in its
  `StateData`.
- **Upload = on-chain chunked txs** (4096 txs/tick × ~1 KB ≈ 4 MB/tick). Bytes ride the consensus tx
  stream, assembled into extension-owned storage.
- **Order-independent reassembly + derived activation.** Tick order is leader-chosen but
  consensus-fixed; chunks self-address (scatter-write) and are hash-gated, so order/multi-tick spread
  is moot. Activation is derived from `(deploy intent ∧ blob complete-and-verified)`, never an
  uploader-chosen tick.
- **Entry mechanism = tx-dispatch hook (chosen).** Upload/deploy txs target the system address
  (`destination == 0`) with lite-range `inputType`s, dispatched in the `processTickTransaction`
  `isZero` switch (`:2930`) to extension handlers — beside the existing `MiningSolution`/`Oracle*`
  cases. No contract/loader slot. Chosen over a thin loader contract to avoid a contract calling host
  code and the rollback-idempotency concern.

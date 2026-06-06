# Runtime Smart-Contract Deployment (testnet dev feature)

**Status:** shipped (testnet). **Backend:** wasm-only — the native `.so` backend was removed.
**Scope:** testnet + dev only. Hard-gated behind `TESTNET` and `LITE_DYNAMIC_CONTRACTS`.
**Goal:** deploy a smart contract into a running node without recompiling the core — compile a
contract to a **wasm module**, upload it on-chain, and register + construct it on a running node.
First step toward an Anchor-like contract framework for Qubic (build / deploy / upgrade / IDL /
client codegen loop), driven by the `qinit` CLI.

This document is the **deploy framework**: reserved slots, on-chain chunked upload, the framed
construction lifecycle, determinism, and safety — all backend-agnostic. The **execution engine +
contract ABI** (WAMR, the `lhost` import surface, state, dispatch) live in
[`WASM_CONTRACTS.md`](WASM_CONTRACTS.md); this doc points there for engine detail.

> ⚠️ **Not a mainnet mechanism.** wasm is a memory-safe sandbox (no RCE-by-design like the old
> `.so` path), but dynamic deploy still **bypasses mainnet's IPO / quorum contract governance**.
> Mainnet contracts stay compiled-in and IPO-governed. This feature must never be compiled into a
> shippable mainnet binary, nor `deploy` wired to a mainnet target. See [Security](#security).

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
deploy tx → loader registry    wasm bytes matched by hash    INITIALIZE at activation tick
framed by its own tx index     load module + patch tables    framed by SC_INITIALIZE_TX
all nodes agree {slot,H,tick}  (closures are NOT consensus)  → correct event stream
```

### 2.1 Slot model (Anchor analogy)

| Anchor | Qubic dynamic deploy |
| --- | --- |
| program id / address | reserved **slot index** |
| program binary | staged **wasm module** |
| `anchor deploy` / `upgrade` | (re-)stage a wasm module into a slot |
| IDL | the IDL `qinit` emits from the contract source |
| program registry / upgradeable loader | extension-owned **deploy registry** (§2.2) |
| account model / PDAs | *no analog* — state is one `StateData` blob + QPI collections |
| local validator + tests | testnet node + deploy tx + RPC |

Compiled in **only** under `LITE_DYNAMIC_CONTRACTS` (testnet):

- **Deployable slots `D0..Dn`** — reserved contract slots. Their `StateData` holds **only the
  deployed contract's own business state** (digested, consensus). Register nothing at boot; uploaded
  wasm modules land here.
- **Control entry point** — a guarded `processTickTransaction` hook (no contract): upload/deploy txs
  target the system address with lite-range `inputType`s and dispatch to the extension (§2.2).

> **Framework state is extension-owned, never in any contract `StateData`.** The deploy **registry**
> and the uploaded **wasm bytes** are framework plumbing, not contract business data — they live in
> extension globals. Putting them in a `StateData` would conflate infrastructure with consensus
> business state.

Deployable slots are counted in `contractCount`, so *all* core machinery works unmodified for them:
dispatch-table sizing, `contractStateLock`, state-buffer allocation, the digest tree, transaction
routing, and the function-call RPC.

### 2.2 Deploy subsystem & on-chain upload

The registry and the raw wasm bytes are framework plumbing, held in **extension-owned** state
(globals), content-addressed by K12 hash — never in a contract `StateData`. Determinism holds
because this state is a pure function of consensus txs (below), and the *constructed contract's*
`StateData` (which IS digested) catches any execution divergence.

**Entry mechanism — tx-dispatch hook (no contract).** Upload/deploy txs reuse the existing
*"destination is system"* protocol-tx path with new `inputType`s in the lite range (mirroring the
`LiteCheckin` 230+ convention). A guarded `#ifdef` block dispatches them from
`processTickTransaction` to `liteDynDispatchTx` (`lite_dynamic_contracts.h`), beside the existing
`MiningSolution` / `Oracle*` cases. The handlers mutate only extension-owned state (blob buffer +
registry), run in the same tick-processor context, and pay fees via the normal path. The lite tx
inputTypes:

```
240 UploadBegin { sessionId, totalSize, chunkCount, finalHash }
241 UploadChunk { sessionId, seq, len, bytes[<=1008] }
242 Deploy      { sessionId, targetSlot, finalHash, abiVersion, stateLayoutVersion, name }
auth (all): tx source == configured deployer pubkey (deploy address id(99999,0,0,0))
```

**Transport = on-chain chunked txs.** `MAX_INPUT_SIZE = 1024`, `NUMBER_OF_TRANSACTIONS_PER_TICK =
4096` → ~1008 B payload/chunk × 4096 ≈ **4 MB/tick capacity** (capacity, not a guarantee — the
leader controls inclusion, so a large module may span several ticks). A ~160 KB contract.wasm is
~165 chunks, landing in one tick once the source seed is funded.

**Tx order is leader-chosen but consensus-fixed** — recorded in `tickData`, agreed by quorum, so
every node replays the identical order. Reassembly is order-independent regardless:

- **Self-addressing chunks → scatter-write.** `buf[seq*CHUNK ..] = bytes`. Any order, any number of
  ticks → the same buffer.
- **Completion = seq bitmap full** (all `chunkCount` seqs present), not "last chunk seen."
- **Hash gate.** Blob is usable only when `K12(buf[0:totalSize]) == finalHash`.
- **Anti-grief.** A session is scoped to its deployer pubkey; only that key may write its `sessionId`.
- **ARQ for dropped chunks.** The uploader polls a read endpoint for the missing-seq bitmap and
  resends only the gaps.

**Activation is derived, never an uploader-chosen tick.**

```
construct slot D at the first tick where:
    (a Deploy intent for {D, finalHash} exists)
    AND (blob[finalHash] is complete AND K12 == finalHash)
```

Both conditions are pure functions of consensus txs → every node derives the same trigger tick.
Leader ordering of `Deploy` vs the chunk txs is irrelevant: whichever lands first waits for the
other. A read endpoint (lite HTTP GET, served by the extension's dyn-registry) exposes
`{ slot, finalHash, status, name, version }` for explorers, clients, and the uploader's ARQ loop —
the seed of an IDL / program registry.

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
pseudo-tx reason; INITIALIZE events tagged as anything else corrupt their view. So construction must
run under genuine `SC_INITIALIZE_TX` framing.

### 3.2 Flow

1. **Deploy tx (consensus).** Processed in-tick, framed by its own tx index — correct. Arms the
   registry entry for the target slot. All nodes see it at the same tick.

2. **Load (node-local).** Each node hands the assembled, hash-verified wasm bytes to the engine —
   `liteWasmLoadFromBytes` (`lite_wasm_contracts.h`) instantiates the module in WAMR, reads its
   `reg_count` / `reg_info` / `state_addr` / `state_size` exports, and patches slot `D`'s
   `contractUserFunctions/Procedures/SystemProcedures[D][·]` with libffi closures that forward into
   the engine (engine detail: `WASM_CONTRACTS.md`). Safe off-tick: a pre-construction slot has **no
   readers** (execution gated `epoch >= constructionEpoch`, RPC rejects `epoch < constructionEpoch`),
   and the patched closures are per-process, **not** consensus. If the bytes are not a wasm module
   (`'\0asm'` magic), the slot stays armed-but-unrunnable and the node logs loudly.

3. **Framed construction (consensus).** `liteDynPendingForTick()` is checked in `processTick`;
   `liteDynConstructPending()` runs INITIALIZE on armed-but-unconstructed slots via
   `QpiContextSystemProcedureCall(D, INITIALIZE)` — under genuine `SC_INITIALIZE_TX` framing, the
   same path native contracts use. wasm slots patch `contractSystemProcedures[D][INITIALIZE]` with a
   closure at load, so this path runs the wasm INITIALIZE identically.

### 3.3 Idempotency / restart

- A slot is marked `constructed` after INITIALIZE; the pending check skips it thereafter (never wipes
  live state mid-run). Re-deploy = registry `version++` → re-load + re-construct (a reinit upgrade,
  §6).
- Restart-from-snapshot reload is a separate concern (see `WASM_CONTRACTS.md`); a plain restart boots
  from genesis (the documented norm).

### 3.4 Boot-time deploy (special case)

`liteDynBootDeploy()` clears the IPO-failed error stamp + seeds a fee reserve for the dev slots so
they can run, and prints the `LITE_DYNAMIC_CONTRACTS ENABLED` banner.

### 3.5 Bootstrap gates (must hold for a slot to run)

- **Fee reserve > 0** — BEGIN_TICK is fee-gated. `liteDynBootDeploy` auto-seeds every dev slot.
- **`contractError == NoContractError`** — `initializeContractErrors()` stamps `ContractErrorIPOFailed`
  on slots without `NUMBER_OF_COMPUTORS` shares. Dev slots skip IPO, so the boot hook clears
  `contractError[slot]` for all dev slots.

---

## 4. Determinism & multi-node

- **Timing & order** are consensus: upload/deploy txs live in `tickData` (quorum-agreed set *and*
  order). The extension blob buffer + registry are a deterministic function of that tx stream, so
  every node assembles identical bytes and derives the same activation tick (§2.2).
- **Code identity** is hash-verified (`K12(blob) == finalHash`) before use. The patched closure
  values are per-process and never hashed; only the constructed contract's `StateData` and its
  execution effects are consensus — and that `StateData` IS digested, so any divergence in deployed
  behavior surfaces as a computer-digest mismatch.
- **Cross-arch determinism** is a property of the wasm engine (fixed integer semantics, no float in
  QPI, bounded memory) — one `contract.wasm` runs bit-identically on every node platform, which is a
  key reason for the wasm-only backend.
- **Late joiner from a state snapshot** does not get the bytes via contract-state sync (they are in
  no `StateData`). It obtains them by replaying the relevant ticks, or — preferred — a
  content-addressed peer fetch (a lite 230+ message, §8 phase 2) and hash-verifies. Missing +
  unfetchable → halt loudly, never guess.

---

## 5. Contract ABI (wasm)

The contract compiles to a wasm module whose `qpi.X()` calls resolve to **imports** from module
`"lhost"`, and whose entry points are **exports** (`dispatch` / `reg_count` / `reg_info` /
`state_addr` / `state_size` / system-proc mask). The node fills a `LiteHostServices` vtable
(`lite_dyn_abi.h`) — the QPI surface — and exposes it to the module as those imports
(`lite_wasm_imports.h`); the contract-side binding is `lite_wasm_tu.h` (compiled INTO the wasm by
`qinit`, not into the node). Pointers cross the boundary as i32 linear-memory offsets; the host binds
the per-call `QpiContext` out-of-band.

Full ABI — the import table, marshalling shapes, state model, dispatch via libffi closures, system
procedures, big-state handling — is documented in [`WASM_CONTRACTS.md`](WASM_CONTRACTS.md). The
**IDL** (field names, nested layouts, entry input/output types) is emitted as JSON by `qinit` from
the contract source.

---

## 6. Upgrade (designed-for)

Upgrade = re-stage a new wasm module into the same slot (registry `version++`). `stateLayoutVersion`
lets the framework choose:

- **Reinit** — wipe state + INITIALIZE (framed). What the dev loop uses.
- **Migrate** — keep state, run a contract-defined migration after the swap. Reuses the
  `SC_INITIALIZE_TX` framing. The version field + "INITIALIZE does not always wipe" keep the door open.

---

## 7. Upstream footprint (all `#ifdef LITE_DYNAMIC_CONTRACTS` / `LITE_WASM_CONTRACTS`)

Minimized; everything else is in `src/extensions/`.

| File | Change |
| --- | --- |
| `contract_core/contract_def.h` | deployable slots `LITEDYN0..N`: includes, `contractDescriptions[]` rows, `REGISTER_…` calls |
| `qubic.cpp` `processTick` | framed construction trigger (`liteDynPendingForTick` → `SC_INITIALIZE_TX` → `liteDynConstructPending`) |
| `qubic.cpp` `processTickTransaction` | upload/deploy `inputType` dispatch to `liteDynDispatchTx` |
| `qubic.cpp` boot | include `lite_dynamic_contracts.h` + `lite_wasm_contracts.h`; `liteDynBootDeploy()` + `liteWasmRuntimeInit()` |
| `qubic.cpp` digest/save | `#ifdef LITE_WASM_CONTRACTS` hooks for the wasm slot's effective state size |

Mainnet build (flags off): zero reserved slots, `contractCount` unchanged, no hooks — binary and all
digests byte-identical to upstream.

### Extension files (`src/extensions/`)

- `lite_dynamic_contracts.h` — **deploy framework**: extension-owned registry + content-addressed
  blob buffer; chunk reassembly (scatter-write / seq bitmap / hash gate / ARQ); the
  `LiteHostServices` vtable (`g_liteHostServices`); `liteDynOnUpload*` / `liteDynOnDeploy` (magic-sniff
  `'\0asm'` → wasm engine); `liteDynPendingForTick` / `liteDynConstructPending` / `liteDynBootDeploy`.
- `lite_dyn_abi.h` — **shared ABI structs**: the `LiteHostServices` vtable type, the system-procedure
  id enum, small descriptors. (Host + engine; no `.so` forwarders.)
- `lite_dyn_stub_contract.h` — the deployable stub registered for slots `LITEDYN0..N` (registers
  nothing live; the engine patches its dispatch tables at deploy); included from `contract_def.h`.
- `lite_wasm_contracts.h` / `lite_wasm_imports.h` / `lite_wasm_tu.h` — the wasm engine, the `lhost`
  import table, and the contract-side binding. See `WASM_CONTRACTS.md`.

---

## Building & enabling the node (read this first)

Enable with the CMake **options** — never via `CMAKE_CXX_FLAGS` (the project resets it, silently
dropping the define):

```bash
cmake -S . -B build-wasm -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 \
  -DTESTNET=ON -DTESTNET_LITE_RAM=ON -DTESTNET_PREFILL_QUS=ON \
  -DLITE_WASM_CONTRACTS=ON          # pulls in the framework (LITE_DYNAMIC_CONTRACTS is set in qubic.cpp)
cmake --build build-wasm --target Qubic -j$(nproc)
```

- **Verify it compiled in:** `strings build-wasm/src/Qubic | grep -aE 'LITEDYN|LITEWASM'` is
  non-empty, and the node prints `LITE_DYNAMIC_CONTRACTS ENABLED` at startup.
- A *runnable* testnet build needs the **real** `src/private_settings.h` (do NOT use `-DONLY_LOGGING=ON`,
  whose empty `broadcastedComputorSeeds` breaks `std::size(...)`). `TESTNET_PREFILL_QUS` funds the
  computors so a deploy tx has a funded source.
- `LITE_WASM_CONTRACTS` needs `libffi` (`apt install libffi-dev`).
- Run with `--node-mode 3` (MAIN, ticks headless) and `--ticking-delay 1000`. **Wait for ticks to
  advance** before broadcasting deploy txs — RPC-up ≠ network-ready.
- Deploy txs target the dedicated address **`id(99999,0,0,0)`**. Use `qinit deploy --contract <h>`
  (wasm is the only/implicit target).

---

## 8. Mainnet safety

```cpp
#if defined(LITE_DYNAMIC_CONTRACTS) && !defined(TESTNET)
#error "LITE_DYNAMIC_CONTRACTS is testnet-only"
#endif
```

CI builds without the flag, so it never compiles the feature. Flag off ⇒ the codebase is
byte-for-byte upstream.

## Security

- wasm is a memory-safe sandbox — no arbitrary-native-code RCE (the reason the `.so` backend was
  retired). But dynamic deploy still bypasses mainnet's IPO/quorum contract governance, so it stays
  **testnet + dev only**.
- Authorization is the **deploy tx signature**, checked against a configured deployer pubkey —
  on-chain and auditable. Any HTTP/CLI control surface is GET-only per project convention, bound to
  localhost, a convenience over the tx, not a substitute.
- The framework presents `deploy`/`upgrade` as a **dev-loop accelerator**, never a mainnet path.

---

## 9. Phasing

1. **Engine** — WAMR + libffi, the `lhost` ABI, state, dispatch, system procedures, big state. Done
   (`WASM_CONTRACTS.md`).
2. **Framework** — reserved slots, the tx-dispatch entry + on-chain chunked upload (scatter-write /
   seq bitmap / hash gate / ARQ), derived activation, B' framed construction, restart-safe
   idempotency. Done.
3. **Next** — content-addressed peer fetch-by-hash for late joiners (lite 230+ msg); restart-from-
   snapshot reload; descriptor → richer IDL; upgrade-with-migration.

---

## Decision log

- **wasm-only backend.** The native `.so` path (dlopen of host-compiled shared objects, bound via an
  explicit host-services vtable) was removed once the wasm engine reached full QPI parity. wasm gives
  a memory-safe sandbox, cross-arch-deterministic execution, and **one** platform-independent
  artifact — retiring the per-platform `.so`/sysroot build matrix. The host-services vtable survives
  (it is the QPI surface), now exposed to the module as `lhost` wasm imports instead of bound into a
  `.so`.
- **Deploy timing via tx, not out-of-band.** Anything consensus-relevant goes through a tick. The
  deploy tx makes activation timing a consensus fact; the registry is the on-chain record.
- **Construction stays under `SC_INITIALIZE_TX`.** Hard requirement for third-party indexers.
- **Chose B' (arbitrary-tick, framed hook) over epoch-boundary-only.** Framework DX needs instant,
  repeatable deploy without epoch waits or restarts.
- **Framework state is extension-owned, never in contract `StateData`.** The wasm bytes + deploy
  registry are infrastructure; only the deployed contract's own business state lives in its `StateData`.
- **Upload = on-chain chunked txs** (4096 txs/tick × ~1 KB ≈ 4 MB/tick), assembled into
  extension-owned storage; order-independent reassembly + derived activation.
- **Entry mechanism = tx-dispatch hook.** Upload/deploy txs target the system address with lite-range
  `inputType`s, dispatched in `processTickTransaction` to extension handlers — no contract/loader slot.

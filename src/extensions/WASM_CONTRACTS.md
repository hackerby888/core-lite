# Feasibility: WASM-executed smart contracts (vs native .so/.dylib)

**Status:** feasibility sketch / not started. Sibling of `DYNAMIC_CONTRACTS.md` (the current native-`.so`
dynamic-contract engine). This explores replacing/augmenting native dlopen execution with an embedded WASM
runtime, where a contract ships as one `contract.wasm` instead of per-platform `.so`/`.dylib`.

## 1. Why consider it

Today (`lite_dynamic_contracts.h`): a contract is a native `.so`/`.dylib`, `dlopen`ed into the node, called
through `contractUserFunctions[]`/`contractUserProcedures[]`, and it calls the host through the `g_liteHost`
**vtable of raw function pointers**. Three problems wasm would fix:

- **No sandbox (the big one).** `dlopen` runs *arbitrary native code in the node process* — a buggy or
  malicious contract can corrupt node memory, crash it, or take it over. wasm is memory-safe + sandboxed:
  the node can run **untrusted** contracts safely. If contracts are ever third-party/permissionless, this is
  mandatory.
- **Per-platform artifacts.** Native needs x86-64 ELF / aarch64 ELF / arm64 Mach-O + the whole header-bundle/
  sysroot apparatus (see the wasm-*toolchain* work). A `contract.wasm` is **one platform-independent artifact**
  — retires that entire effort on the contract side.
- **Determinism.** wasm has tightly-specified semantics (defined overflow, NaN canonicalization, no UB) —
  friendlier to consensus than native UB.

Cost: a **wasm runtime embedded in the node** + a **new contract↔host ABI** (vtable pointers can't cross the
wasm boundary) + a marshalling layer + execution-speed and concurrency rework. **Extension-confinable** — the
execution seam is already abstracted (see §12); the earlier "core/consensus rewrite" framing was pessimistic.

## 2. Runtime choice

| runtime | lang | mode | notes |
|---|---|---|---|
| **WAMR** (WebAssembly Micro Runtime) | C | AOT + interp | small, embeddable in C++, **AOT → near-native**, deterministic, used in embedded/blockchain. **Recommended.** |
| wasmtime | Rust (C API) | JIT/Cranelift | fast, mature, but Rust dep + bigger; JIT codegen per-node |
| wasm3 | C | interpreter | tiny but ~10×+ slower — too slow for the tick budget |

**Recommend WAMR in AOT mode:** at *deploy*, compile `contract.wasm` → AOT native module (per-node, one-time);
at *run*, execute the AOT module — near-native speed, still fully sandboxed. (AOT is just a faster execution of
the same wasm semantics, so consensus determinism is preserved — the observable behavior is the wasm spec.)

## 3. ABI redesign (the core work)

### Exports (node → contract)
Replace the `contractUserFunctions[slot][inputType]` native pointers with wasm **exports**. Either:
- one dispatcher export: `__call(kind:i32, inputType:i32, inputPtr:i32, inputLen:i32, outPtr:i32, outCap:i32) -> i32`
  (kind = function|procedure|construct|begin_tick|end_tick), or
- per-entry exports named by inputType.
A dispatcher is simpler for the node + matches the existing per-inputType dispatch.

### Imports (contract → host = QPI)
The `g_liteHost` vtable (~50 fns: `numberOfShares`, `transferShareOwnershipAndPossession`, `issueAsset`,
entity/spectrum reads, logging, `__qpi` collection helpers, …) becomes **wasm imports** the node provides at
instantiation. Each takes/returns wasm scalars; complex args go via linear-memory offsets (next).

### Marshalling (linear memory)
wasm boundary is scalars only (`i32/i64/f32/f64/v128`). So:
- contract inputs/outputs (tx input structs, `Asset`, `m256i`) cross as `(i32 offset, i32 len)` into the
  **contract's exported linear memory**; the node reads/writes that memory directly.
- a small marshalling shim on each side (the qpi.h side already centralizes copies via `copyMemory`).
- the node needs a tiny allocator contract-side (the contract's wasm owns its heap in linear memory) or a
  fixed scratch region the node writes inputs into.

### `m256i` / SIMD
256-bit AVX `m256i` (`common_def.h`/`m256.h`) → wasm has only **`v128` (128-bit)**. Options: pass `m256i` as a
32-byte memory blob (no SIMD at the boundary — simplest), and inside the contract use `v128`×2 or scalar (SIMDe
already gives a portable path). The pass-by-value `m256i` ABI that native relies on goes away — everything is a
memory blob. Net: *simpler* boundary, but the contract's internal SIMD is 128-bit.

## 4. State + persistence

Native: `StateData` lives in node memory. wasm: the contract's `StateData` lives in its **linear memory**.
- **Persistence**: the node serializes the contract's linear memory (or a designated state region) to disk —
  fits the existing snapshot path (`DYNAMIC_CONTRACTS.md` snapshot/restore).
- **Rollback** (`reprocessSolutionTransaction`/`spectrumDataRollback`): snapshot the linear memory before the
  tick, restore on disagreement. Linear memory snapshot is a clean `memcpy` — *easier* than the current
  spectrum-index rollback dance.

## 5. The hard concurrency problem (must solve)

`tickProcessor()` runs `processTick()` on **MAX_NUMBER_OF_PROCESSORS** threads (6/32); a contract call can land
on **any** thread. A wasm instance's linear memory is **not** thread-safe, and **N instances = N divergent
states** (each its own memory) → wrong consensus. Two viable models:
- **Single instance + serialized access**: one wasm instance per contract; the dispatch already guarantees "one
  processor handles a given tick at a time" (the `spectrumDataRollback` invariant) — extend that to serialize
  contract calls. Simplest; may bottleneck if many contracts run hot.
- **Per-thread instances sharing a state region**: N instances, but the state lives in a **shared** linear
  memory / shared imported memory. Harder (wasm shared memory + atomics) and risks data races.
Recommend **single-instance-per-contract + serialized** first (matches the existing invariant).

## 6. Determinism checklist (consensus)
- wasm core semantics deterministic ✓ (defined int overflow, NaN canonicalization, no UB).
- no threads/atomics *inside* the contract (single-call execution) ✓.
- imports (QPI) must be deterministic — they already are (pure consensus state).
- AOT must preserve semantics across nodes ✓ (AOT = faster same-semantics).
- gas/fuel metering: wasm runtimes support instruction metering → a clean way to bound contract execution
  (replaces ad-hoc native time limits) — a *bonus* over native.

## 7. Performance budget
- AOT ~1.2–2× native; per-call marshalling overhead (struct copies) is the main add.
- **Instance reuse**: instantiate once per slot at deploy, keep warm, reset state per call as needed — never
  per-call instantiate (expensive).
- Risk: the consensus tick budget. Measure a hot contract (e.g. QX-like) AOT vs native before committing.

## 8. Migration / coexistence
- Keep the native `.so` engine (`LITE_DYNAMIC_CONTRACTS`); add the wasm engine behind a new flag
  (`LITE_WASM_CONTRACTS`). A node runs one or the other (or both, slot-tagged by artifact magic: `\0asm` →
  wasm, ELF/Mach-O → native).
- qinit: `qinit build --target wasm` → `clang --target=wasm32-wasi contract → contract.wasm` (one artifact,
  **no per-platform bundle, no sysroot** — much simpler than the native path). Deploy/upload unchanged (just
  different bytes); the dyn-upload/assembly-confirm pipeline already handles opaque bytes.

## 9. Risks / open questions
- **Concurrency** (§5) — the make-or-break design point.
- **QPI surface size** — ~50 imports to define + marshal; mechanical but large.
- **Perf on the tick budget** — must benchmark.
- **AOT cross-node determinism** — validate identical observable results on x86/arm nodes.
- **wasm32 4 GB linear-memory cap** — fine for contract state; confirm no contract needs >4 GB.
- It's **consensus-critical** (the execution engine) and needs merge-safe gating (`LITE_WASM_CONTRACTS`,
  node-default off) — but **extension-confined**, not a core/qpi rewrite (§12).

## 10. Phased plan
1. **Spike (no consensus) — DONE, PASSED (2026-06-06).** Embedded WAMR (fast-interp, static `vmlib`) in a C
   harness; trivial contract (`clang --target=wasm32 -nostdlib`, export/import attrs) → 612-byte `contract.wasm`.
   Proven: `run(7)=48` (host import call + return), `host_log` (pointer marshalling, app-addr→native via WAMR's
   `(*~)` signature), `sum_pair({100,23})=123` (host writes a struct into the contract's linear memory via
   `wasm_runtime_addr_app_to_native`, contract reads it). **Runtime + the import/marshalling ABI work — Stage-1
   gate cleared.** Spike lives at `~/wasm-spike/` (contract.c, harness.c, CMakeLists.txt). Next: full QPI import
   surface, then state, then the §5 concurrency model, then consensus.
2. **QPI imports — DONE, PASSED (2026-06-06).** Mapped the real `LiteHostServices` vtable (`lite_dyn_abi.h`,
   45 fns) onto six marshalling shapes and proved each in the spike (866-byte `contract.wasm`): scalar↔scalar
   (`host_add` `(ii)i`), ptr+len (`logBytes`/`host_log` `(*~)`), struct-in via linear memory (`numberOfShares`
   selectors / `sum_pair`), `m256i`(32B)-in + i64-return (`transfer`/`qpi_balance` `(i)I`), struct-out via
   linear memory (`getEntity`/`nextId`/digests / `qpi_get_entity` `(ii)`), all with `validate_app_addr`
   bounds-checks + `addr_app_to_native` (the general offset→native-ptr pattern the engine uses). **Two ABI
   mismatches between the native vtable and a wasm boundary — found and solved:**
   - **ctx threading.** Every QPI backend takes `const void* ctx` (native `QpiContext*`); a wasm contract can't
     hold a native ptr. The wasm import **drops ctx**; the host binds the current ctx per-call out-of-band via
     `wasm_runtime_set_user_data(exec_env, ctx)` and each native reads it with `get_user_data`. Proven: `qpi_tick`
     (no ctx arg) returned the bound `ctx.tick`.
   - **`acquireScratch` returns `void*`.** The host can't return a native ptr into wasm; it returns a
     **linear-memory offset** (in wasm32, memory base 0 ⇒ offset == pointer). Proven: `host_alloc` bump-allocs in
     a contract-reserved arena, returns the offset, contract uses it as a ptr.
   Remaining for a real contract run: the inter-contract calls (`liteCallFunction`/`liteInvokeProcedure`) are
   just ptr+len-in + ptr+len-out (covered shape) but need the host-side late-bound dispatch; state + entry
   dispatch are Stage-3.
3. **State + rollback — DONE, PASSED (2026-06-06).** Counter contract with `StateData` as a linear-memory
   global (1085-byte `contract.wasm`). Proven: **(a) no per-invoke copy** — 3 calls mutate the resident state
   in place (`count=3 sum=35`), instance reused; **(b) rollback** — snapshot = `memcpy` of **only the state
   region** (16 bytes, via the contract's `state_addr`/`state_size` exports = `LiteRegistration.stateSize`),
   not the whole linear memory; mutate past it, `memcpy` back → restored; **(c) persistence across instances**
   (restart / snapshot-reload path) — flush state to disk, `deinstantiate`, fresh instance boots zeroed,
   inject the saved bytes into the new instance's state region → `count=3 sum=35` reloaded.
   **State-cost answer:** 1 resident copy always; a transient 2nd copy of *only* the state region during a
   rollback window (same as the native `spectrumDataRollback` cost), NOT per-invoke, NOT the whole instance.
   The real per-invoke cost is **serialization** (single instance + mutex), not memory — a wasm instance isn't
   thread-safe and contract fns run on any thread, so N instances would diverge (§5). Snapshot assumes stack /
   `host_alloc` scratch are per-call transient (don't cross calls) — true for Qubic's state+locals model.
3b. **Entry dispatch + inter-contract calls — DONE, PASSED (2026-06-06).** 1452-byte `contract.wasm`, two
   instances (A caller, B math) from one module, host-indexed.
   - **Entry dispatch (table-driven, real `LiteUserEntry`):** the contract can't expose native fn ptrs, so it
     exports ONE `dispatch(inputType, in_off, out_off)` switching on `inputType` (the codegen'd switch) + a
     registration (`reg_count`/`reg_info`) the host reads to size in/out marshalling (= `LiteRegistration
     .userEntries[]`). Proven: host dispatched `ADD`→42 and `INCR`(procedure, mutates state)→count=1 **by
     inputType, not export name**.
   - **Inter-contract (`liteCallFunction`/`liteInvokeProcedure`):** contract A calls the `lite_call` host
     import; the host bridges the two linear memories via a host buffer (caller in → host buf → callee scratch
     → run callee `dispatch` → callee out → host buf → caller out — no ptr crosses). Proven: `A.dispatch
     (DELEGATE)` → host → `B.ADD` → 142; B's state stayed isolated from A's (`B.count=0`).
   - **WAMR integration finding (the real plumbing trap):** a nested cross-instance call must **reuse the
     current `exec_env`** and swap its module_inst (`wasm_runtime_set_module_inst`, backup+restore) — a
     separate/fresh `exec_env` traps `"invalid exec env"` because WAMR binds one current env per thread via TLS
     (`exec_env_tls`). The single-instance-per-contract + serialized model (§5) means one exec_env per thread,
     module_inst switched per callee.
4. **Consensus integration:** behind `LITE_WASM_CONTRACTS`; AOT-at-deploy; gas metering; cross-arch determinism
   tests; perf vs native. (ABI + dispatch + inter-contract + state/rollback all de-risked by Stages 1–3b — this
   step is the consensus wiring + AOT/gas, not new ABI work.)
5. **qinit:** `--target wasm` (drops the per-platform toolchain bundles for wasm contracts).

## 11. Verdict
**Build side: simpler** (one `contract.wasm`, no native toolchain/sysroots). **Node side: a new extension
backend** (embed WAMR + new ABI + marshalling + concurrency + state model) — consensus-critical and multi-week,
but **extension-confined** (§12), NOT a core/qpi rewrite. Worth it **iff** contracts must be untrusted/safe or platform-independence + determinism + gas
metering are priorities. If contracts stay trusted/first-party, the native `.so` engine is far less work.
Recommended next step is the **Stage-1 WAMR spike** — bounded, proves/kills the runtime + marshalling before
committing to the consensus rewrite.

## 12. Blast radius — code-checked (2026-06-06)

Verified against the live native engine (`lite_dyn_abi.h`, `lite_dynamic_contracts.h`, `qubic.cpp`,
`contract_def.h`). Verdict: **a wasm backend can be confined to `src/extensions/` + build config — no
`qpi.h`/`contract_core` logic rewrite.** Why:

- **QPI host binding is already extension-owned.** `qpi.h` (upstream, shared) is a pure interface — `grep
  g_liteHost src/contracts/qpi.h` = empty. The lite extension `lite_dyn_abi.h` *defines* the QPI context
  methods (`QPI::QpiContextFunctionCall::epoch()` → `g_liteHost->epoch(this)`, ~50 fns via `LiteHostServices`).
  → wasm just needs a **sibling binding** (qpi calls → wasm imports) compiled into the contract; the import
  table on the node calls the **same `g_liteHost`** impls. `qpi.h` untouched.
- **Execution reuses core's dispatch slots.** Native fills `contractUserProcedures[idx][it]`/`…Functions`
  (lite_dynamic_contracts.h:158/163) with dlopen'd fn-ptrs; core calls the slot. → register a **native shim**
  in the slot that drives WAMR. Core dispatch (`contract_exec.h`) unchanged.
- **State stays canonical in `contractStates[idx]`** (node memory). The shim copies it into the wasm instance's
  linear memory before the call and back after. → `getComputerDigest` (K12 over `contractStates[i]`),
  snapshot, and rollback are **untouched**.
- **Core hooks already exist + gated** under `LITE_DYNAMIC_CONTRACTS` (qubic.cpp:2958 deploy-tx / 3362
  construct / 7434 boot + contract_def.h). wasm reuses them — **no new core hook**. New code sits behind a new
  `LITE_WASM_CONTRACTS` gate, node-default off.

**Residual non-extension touches (small / acceptable):**
- a wasm runtime lib (WAMR) + libffi (for the dispatch closures, §13.3) in the build: `src/CMakeLists.txt` +
  `lib/` — build config, not core logic.
- dyn-contract `stateSize` registration in `contractDescriptions[]` — the native dyn engine already does this
  (contract_def.h gated); wasm reuses it.

**The one genuine consensus item (not a code-location issue):** a given contract must run as the SAME engine
on ALL nodes (wasm-only per contract, magic-tagged `\0asm` per §8) so the **wasm32 `StateData` layout**
(32-bit ptrs/alignment) is identical network-wide. QPI already bans raw pointers / mandates fixed-width types,
so layout is portable — but **validate** a contract's `StateData` bytes wasm-vs-native before trusting it.

**Spike to prove it (do NOT need consensus / core changes):** Stage-1 (§10) — embed WAMR in a standalone
harness, compile a trivial contract with `clang --target=wasm32-wasi` (the wasm toolchain already exists),
instantiate, call one `__call` export, round-trip one struct via linear memory, call ~3 imports back into a
fake `g_liteHost`. Proves runtime + marshalling + the import binding in days, entirely outside the node.

## 13. Stage-4 integration plan (2026-06-06)

Stages 1–3b (§10) are all PASSED in `~/wasm-spike/` (16/16 checks): runtime embed + the 6 marshalling shapes,
the full QPI surface mapped (incl. the two ABI mismatches — ctx-binding via exec_env user_data, and
`acquireScratch` returning a linear-memory offset), state (no per-invoke copy + region-only rollback +
restart-reload), and entry dispatch + inter-contract calls (incl. the WAMR `set_module_inst` re-entry trick).
Nothing ABI-shaped remains unknown. This section is the plan to wire that into the node behind
`LITE_WASM_CONTRACTS`, sharing the existing `.so` engine's deploy/upload/slot machinery
(`lite_dynamic_contracts.h`). Scope is testnet dynamic contracts (`[[reference_dynamic_contracts_design]]`).

**Both backends coexist, per slot — wasm is the default.** The node compiles in *both* engines and routes each
slot by the uploaded artifact's magic (§13.9): `\0asm` → wasm, ELF/Mach-O → native `.so`. wasm is the default
deploy target (sandboxed, deterministic, one platform-independent artifact). Native `.so` stays a
**first-class, supported opt-in** — the escape hatch for contracts that need a **native library**, full native
perf, or anything the wasm sandbox can't express. A node may run wasm and native slots side by side; the engine
is a property of the contract, chosen at build/deploy time, not a node mode.

### 13.1 Files

| file | change |
|---|---|
| `src/extensions/lite_wasm_contracts.h` | **new** — the WAMR engine: instance registry, load/AOT, dispatch + marshalling, QPI import table, gas. Mirrors `lite_dynamic_contracts.h`'s structure. |
| `src/extensions/lite_wasm_imports.h` | **new** — node-side: the `NativeSymbol[]` import table, one wasm import per `LiteHostServices` member, each forwarding to the same `g_liteHostServices` impl. |
| `src/extensions/lite_wasm_tu.h` | **new** — contract-side (compiled *into* the `.wasm` by qinit). The wasm analog of `lite_dyn_abi.h`'s `LITE_DYN_SO_BUILD` block: declares the QPI host fns as wasm **imports**, defines the `QPI::QpiContext` methods to call them, and emits the `dispatch`/`reg_count`/`reg_info`/`state_addr` **exports**. Shipped in the qinit header bundle. |
| `src/extensions/lite_dyn_abi.h` | reuse `LiteHostServices`/`LiteRegistration`/`LiteUserEntry` as-is. Add a `LiteSlotEngine { NATIVE_SO, WASM }` tag + a `LiteWasmEntry { idx, it, kind }` for the closure user_data. |
| `lib/wamr/` | **new** — vendored WAMR (interp + AOT). |
| `lib/libffi/` | **new** — vendored libffi, for the per-`(idx,it)` dispatch closures (§13.3). |
| `src/CMakeLists.txt`, `test/CMakeLists.txt` | link `vmlib` + `ffi`; `LITE_WASM_CONTRACTS` define; no new `-m` flags (both are plain C). |
| `src/contract_core/contract_exec.h` | **UNTOUCHED** — libffi closures sit in the existing fn-ptr table, so core dispatch stays byte-identical to upstream. |
| `src/qubic.cpp` | reuse the existing `liteDyn*` hooks (2958/3362/7435); branch on the slot's `LiteSlotEngine` to route a wasm-tagged deploy/construct to the wasm engine. Gated, additive — no new hook site. |

### 13.2 Engine — instance lifecycle

Per slot, one persistent `wasm_module_t` + one `wasm_module_inst_t` + the AOT-compiled code, held in a
`LiteWasmSlot[]` registry parallel to `g_liteDynSlots[]`. Built once at deploy/boot, reused for every call
(Stage-3: no per-invoke instance churn). `wasm_runtime_full_init` once at node start with the §13.4 import
table. Teardown on redeploy (new version) mirrors the `.so` `dlclose`/version-suffix flow.

### 13.3 The dispatch seam — libffi closures (chosen)

Native dyn contracts drop a C fn-ptr into `contractUserFunctions[idx][it]` / `…Procedures` /
`contractSystemProcedures[idx][sp]` and the core calls the slot transparently (5 call sites:
`contract_exec.h:770/1025/1038/1147/1285`, sig `fn(const QpiContext&, void* state, void* input, void* output,
void* locals)`). A wasm export is **not** a native fn-ptr, and the slot fn-ptr carries no `(idx, it)` for a
generic shim to recover — verified: `_currentContractIndex` + `_entryPoint` are `protected` (qpi.h:2459/2489)
and `_entryPoint` is `unsigned char` ≠ the `unsigned short` `inputType`, so the ctx alone can't yield the
function selector.

**Solution: a per-`(idx, it)` JIT thunk built with libffi closures, stored in the existing dispatch table —
zero core-dispatch edits.** At deploy/register, for each registered entry allocate a libffi closure bound to
`{idx, it, kind}`; libffi returns a real native fn-ptr that the core calls exactly like a native contract fn.
The closure invokes one C handler with the 5 args + the bound `user_data`, which forwards to
`liteWasmDispatch(idx, it, …)`. `contract_exec.h`, `qpi.h`, and the entire core dispatch path stay
**byte-identical to upstream** — §12's "native shim in the slot / core dispatch unchanged" claim holds
literally. (Testnet-scoped, so the RX-page / untrusted-code concern that would otherwise weigh against JIT is
out of scope.)

```c
// one ffi_cif per slot-fn shape, prepared once at init:
//   USER_FUNCTION/USER_PROCEDURE/SYSTEM_PROCEDURE all = void(const QpiContext&, void*, void*, void*, void*)
static ffi_type* argTypes[5] = { &ffi_type_pointer, &ffi_type_pointer, &ffi_type_pointer,
                                 &ffi_type_pointer, &ffi_type_pointer };
ffi_prep_cif(&g_liteWasmCif, FFI_DEFAULT_ABI, 5, &ffi_type_void, argTypes);

// deploy/register: one closure per registered (idx, it):
void* code;
ffi_closure* cl = ffi_closure_alloc(sizeof(ffi_closure), &code);
ffi_prep_closure_loc(cl, &g_liteWasmCif, liteWasmThunkHandler, &slot.entries[k] /*{idx,it,kind}*/, code);
contractUserProcedures[idx][it] = (USER_PROCEDURE)code;   // core calls it transparently
slot.closures[k] = cl;                                    // keep for ffi_closure_free on redeploy

// the single handler — args[] are the 5 ptrs the core passed; user = the bound entry:
static void liteWasmThunkHandler(ffi_cif*, void* /*ret=void*/, void** args, void* user) {
    LiteWasmEntry* e = (LiteWasmEntry*)user;
    liteWasmDispatch(e->idx, e->it, e->kind,
                     *(const QPI::QpiContext**)args[0],   // ctx
                     *(void**)args[1],                    // state
                     *(void**)args[2],                    // input
                     *(void**)args[3],                    // output
                     *(void**)args[4]);                   // locals
}
```

**Why libffi over hand-rolled asm:** libffi handles the x86-64/arm64/macOS calling conventions itself — no
per-arch assembly, no manual arg-shuffle to maintain. Battle-tested (CPython/Ruby/.NET use it). A hand-asm
thunk would save the dependency but reintroduce per-arch ABI code we'd own. **Cost of libffi:** one small
vendored lib + per-platform build (same platforms as the `.so` sysroots already done), plus closure
alloc/free bookkeeping per deploy. Per-call overhead is the libffi arg-marshal — negligible vs the wasm call +
the v1 state copy.

**Rejected alternatives:** (a) hand-rolled asm thunks — same idea, but per-arch asm we maintain;
(b) gated `if (liteWasmIsWasm(idx))` branches at the 5 `contract_exec.h` sites — dep-free and simple, but
diverges core dispatch from upstream forever; (c) compile-time `shim<it>` template table — needs a public
contract-index getter added to the shared `qpi.h` (worse merge surface) or an instantiation explosion.

### 13.4 Per-call marshalling (`liteWasmDispatch`)

1. **ctx-bind:** `wasm_runtime_set_user_data(execEnv, &qpiContext)` — the QPI imports read it back (Stage-2
   ABI #1); ctx is **not** an argument to any import.
2. **state in:** copy `contractStates[idx]` (size = `LiteRegistration.stateSize`) into the instance's state
   region at a fixed export offset. (v1; see §13.6 for the no-copy optimization.)
3. **input in:** copy `inputBuffer` (size from the entry table) into the instance's input scratch offset.
4. **call:** `wasm_runtime_call_wasm(execEnv, dispatchFn, {kind, it, stateOff, inOff, outOff, localsOff})`.
   `dispatch` is the single export switching on `(kind, it)` (Stage-3b). Gas budget set before the call.
5. **output out:** copy the instance's output scratch → `outputBuffer`.
6. **state out:** if procedure/system-proc (a write), copy the state region back → `contractStates[idx]` and
   set `contractStateChangeFlags[idx]`. Functions are read-only → skip.

### 13.5 QPI imports (`lite_wasm_imports.h`)

One `NativeSymbol` per `LiteHostServices` member. Each native fn: reads the bound ctx from
`get_user_data`, converts wasm offsets → native ptrs with `validate_app_addr` + `addr_app_to_native`, then
calls **the same `g_liteHostServices.*`** the `.so` engine uses. Signatures follow the Stage-2 shapes (scalar,
`(*~)`, `(i)I`, struct-in/out via offsets). `acquireScratch` returns a **linear-memory offset**, not a native
ptr (Stage-2 ABI #2) — back it with a per-instance bump arena reset each call.

### 13.6 State / digest / rollback / persistence

**v1 (recommended first):** `contractStates[idx]` stays canonical. §13.4 copies state in/out per call. This
reuses the existing machinery **unchanged** — `getComputerDigest` (K12 over `contractStates[i]`), the contract
snapshot/save, and rollback all operate on `contractStates[idx]` exactly as for native contracts. Cost = one
`stateSize` copy in (+ out for writes) per call; fine for the small testnet-contract states. **This is the
honest answer to "2x per invoke": v1 does copy `stateSize` per call by choice, to reuse rollback/digest/persist
untouched — not because wasm requires it.**

**v2 (optimization, later):** make the instance's linear memory canonical (Stage-3: zero per-call copy). Sync
`contractStates[idx]` ← linear memory lazily, only at the points that read it: before digest, before
snapshot/persist, and into the rollback backup. Keyed off `contractStateChangeFlags[idx]`. Removes the
per-call copy at the cost of sync hooks at those 3 read points.

### 13.7 Concurrency — reuse the existing lock

The core **already serializes per contract** via `contractStateLock[idx]` (write-lock held across the whole
dispatch: `contract_exec.h:1143–1153`). One instance per slot is therefore safe with **no new mutex** — the
core's per-contract lock already guarantees no concurrent entry into a given slot's instance, which is exactly
the §5 single-instance-serialized requirement. (Confirm the write lock spans `liteWasmDispatch` at each site;
it does for user proc/func today.)

### 13.8 Inter-contract calls

`g_liteHostServices.liteCallFunction/liteInvokeProcedure` already exist and do native table dispatch
(`lite_dynamic_contracts.h:79–107`). The wasm import forwards to them. When the callee is itself a wasm slot,
the host re-enters via Stage-3b's trick: **reuse the current `exec_env`, swap `module_inst`
(`wasm_runtime_set_module_inst`, backup+restore)** — a fresh exec_env traps `"invalid exec env"` (WAMR's
per-thread TLS env). Marshal caller↔callee through a host buffer (no ptr crosses). Nesting depth + reward
transfer reuse the native path.

### 13.9 Deploy / registration

Reuse `liteDynUpload` chunked upload + hash verify wholesale — the payload is `contract.wasm` instead of a
`.so`. On deploy: validate the wasm (magic `\0asm`, imports ⊆ the known host set, no disallowed instructions),
**AOT-compile** (`wasm_runtime_load` of AOT, or compile-at-deploy), instantiate, then read the contract's
`reg_count`/`reg_info` exports → fill `LiteRegistration` (Stage-3b). Mark the slot `WASM` + store `stateSize`
and the entry table; **do not** patch the native fn-ptr tables. Construct (`__initialize`) runs as a system-proc
dispatch on the framed tick step, like the native engine.

### 13.10 Determinism + gas

Consensus needs bit-identical execution on every node. WAMR AOT is deterministic if: no float NaN-payload
nondeterminism (ban float in contracts — QPI already does), no nondeterministic imports (the host set is all
deterministic chain reads), bounded memory (fixed linear-memory cap per slot), and **gas metering** (WAMR
`WAMR_BUILD_GAS`/bounded-fuel) so a contract can't hang a tick — over-budget = trap = the call aborts like a
native `qpi.abort`. Gas schedule must be fixed network-wide (part of the consensus params).

### 13.11 qinit changes (`--target wasm`)

qinit is the other half: it builds, verifies, and deploys contracts. **`--target wasm` becomes the default;
`--target native` (the current `.so`/`.dylib` path) stays supported as the opt-in escape hatch** (native libs,
full perf). Both build paths live; only the default flips. The wasm path reuses most of qinit; the deltas, by
subsystem:

- **build (`packages/build`).** The wasm compiler already exists — `compileWasm` (`wasm-toolchain.ts`) runs the
  `clang.wasm` multitool + per-platform header bundle (`[[reference_wasm_toolchain_plan]]`). New work for the
  **default wasm target**: wrap the user contract with the new **`lite_wasm_tu.h`** binding (the contract-side
  QPI import decls + `dispatch`/`reg_count`/`reg_info` exports) and add it to the bundled headers. Output is
  **one `contract.wasm`** — the per-platform `.so`/`.dylib`/sysroot bundle matrix is **skipped entirely** (the
  big simplification wasm buys on the build side). **`--target native`** keeps the current path: the
  `LITE_DYN_SO_BUILD` (`lite_dyn_abi.h`) wrap + per-platform `.so`/`.dylib` compile via the sysroot bundles.
- **verify.** `contractverify` (`[[project_qinit_contract_verify]]`) runs unchanged — the QPI source rules are
  engine-independent. Gate still blocks deploy on violation.
- **deploy (`packages/cli` deploy cmd).** Ship the `.wasm` bytes down the **same chunked upload + B′ framed
  deploy** path (`[[reference_dynamic_contracts_design]]`) — the proto is just `{bytes, totalSize, chunks,
  finalHash}`, format-agnostic, **no proto change**. `codeHash = K12(wasm bytes)`; the existing deploy
  arm-verify (poll dyn-registry `codeHash`, `[[project_qinit_pending_reliability]]`) + chunk retry work as-is.
  Funded-seed auto-fetch (`GET /live/v1/dev/funded-seed`, `[[project_qinit_deploy_gaps]]`) unchanged.
- **engine routing — no proto field needed.** The node **sniffs the uploaded bytes' magic**: `\0asm` →
  `LiteSlotEngine::WASM`, ELF/Mach-O magic → `NATIVE_SO`. So qinit signals the engine purely by *what it
  uploads*; `--target wasm` only changes the build artifact, not the deploy wire format.
- **IDL / call tooling.** qinit's local IDL (for `qinit call`/`query` arg encoding) is parsed from the contract
  source at build — unchanged. On-node, the wasm slot reports the same entry table via `reg_count`/`reg_info`
  (Stage-3b), so explorer/tooling that reads the node's registration sees an identical shape to a `.so` slot.
- **test (`qinit test`, `[[project_qinit_test]]`).** Add a wasm variant: spin the ephemeral node (must be the
  **wasm-enabled build**, see below), `deploy --target wasm`, run the same `bun test` deploy→call→assert flow.
  Procedures stay tick-bound → keep `await settle()` + `--timeout`.
- **node build target (`qinit node get`, `[[project_prebuilt_dyn_node_config]]`).** The prebuilt dyn-node
  config must add a **`LITE_WASM_CONTRACTS=ON`** build (alongside logging-event + ADDON_TX_STATUS) so
  `qinit test --target wasm` and real wasm deploys have a node that runs them.
- **distribution.** The `qinit-cli` binary itself is unaffected (same `bun --compile`); it just gains the
  `--target wasm` flag + bundles `lite_wasm_tu.h`. `curl|sh` install (`[[reference_qinit_cli_distribution]]`)
  unchanged.

Net qinit story: **build gets simpler** (one artifact, no sysroot matrix), **deploy/verify/test reuse the
existing paths** with a target flag, and the only genuinely new qinit code is the `lite_wasm_tu.h` wrap in the
build step + the test/node-build-target wiring.

### 13.12 Risks

- **`StateData` layout must be wasm32 network-wide** (§12): a contract runs as ONE engine on all nodes
  (magic-tagged). Validate a contract's state bytes wasm-vs-native before trusting layout portability.
- **AOT cross-arch determinism** — the same `.wasm` AOT-compiled on x86-64 vs arm64 must produce identical
  observable state. Test with a determinism harness (run a tx stream on both, compare digests) before mainnet.
- **Gas schedule = consensus param** — getting it wrong forks the chain; pin it.
- **Perf** — AOT is near-native but the per-call state copy (v1) + marshalling add overhead vs a raw `.so`
  call. Measure; move to v2 if hot.

### 13.13 Integration rollout (the build order)

1. Vendor WAMR + libffi, build `vmlib` + `ffi` into the node (WAMR interp first), `LITE_WASM_CONTRACTS` off by
   default.
2. `lite_wasm_imports.h` — port the spike's import table to the real `g_liteHostServices` (all ~45 fns).
3. `lite_wasm_contracts.h` — instance registry + `liteWasmDispatch` (v1 copy-in/out) + libffi closure
   registration into `contractUserFunctions/Procedures/SystemProcedures[idx][·]` (§13.3, no core edit). Boot-load
   a hand-built `contract.wasm` from disk (skip deploy), run a Counter via a tx. Compare its
   `contractStates[idx]` digest to the equivalent native `.so` contract.
4. Wire the deploy path: node sniffs `\0asm` magic → wasm engine → validate → AOT → register. qinit side —
   `lite_wasm_tu.h` build wrap + `--target wasm` (reuses chunked upload + arm-verify), and a
   `LITE_WASM_CONTRACTS=ON` node-build target for `qinit test --target wasm`.
5. Switch interp → AOT; add gas. Determinism harness across x86-64/arm64.
6. (v2) linear-mem-canonical state + lazy sync, if perf needs it.

Tests: a `test/wasm_contracts.cpp` gtest that loads a fixture `.wasm`, dispatches each entry kind, and asserts
the resulting `contractStates[idx]` byte-matches a native build of the same contract (the determinism contract).

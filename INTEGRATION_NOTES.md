# Integration: fork-rollback + dynamic-contracts

Branch `integrate/dynamic-contracts-fork-rollback`. Local only — not pushed.

## Inputs

| Ref | SHA | Role |
|---|---|---|
| `main` | `df31a9b0` | base |
| `feat/tick-fork-rollback` | `fdaef17a` | fast-forwarded (main was an ancestor) |
| `feat/dynamic-contracts` | `2186b89a` → `fdbec865` | merged after syncing main into it |

`main` was merged into `feat/dynamic-contracts` first (clean, zero conflicts). That lifted the
rollback×dynamic merge-base from `96ab388c` to `df31a9b0`, so the 13 main-only consensus commits
(BPP9000 canonical-nonce, `VERSION_C`, start tick) became base content and survived automatically.

## Commit sequence

1. `b28043d7` Restrict fork rollback to non-Wasm builds
2. `cd9f4e27` Reset peer reaper after fork promotion
3. `ffb8a991` Merge feat/dynamic-contracts (six conflicts resolved)
4. `31e78c2b` Reformat branch code to project style

## Product boundary

Rollback = `defined(__linux__) && !defined(LITE_WASM_SC)`; Wasm = `TESTNET && TESTNET_LITE_RAM &&
LITE_WASM_SC`. Rollback stays enabled on plain testnet (it is the rollback-testing config); only
Wasm builds exclude it. Verified in the binaries:

| Build | fork CLI strings | promote path |
|---|---|---|
| mainnet | present | present |
| plain testnet | present | present |
| wasm testnet | absent | absent |

`TESTNET=OFF` + `LITE_WASM_SC=ON` still fails at CMake configure.

## Conflict resolutions

- **explorer_controller.h** — rollback's RPC_ROUTE structure, dynamic's 200-tick history window; all
  eight response groups match dynamic's output.
- **rpc_live_controller.h** — rollback's router is the base; every Wasm/Qinit route re-implemented as
  `RPC_ROUTE` handlers (registry, upload, log-stats, traces, state-read, contract-digest, tx-status,
  funded seeds, contract-source, epoch info, tick/epoch advance). 70 routes total across all
  controllers.
- **http.h** — rewritten as a thin in-process drogon adapter that dispatches into the same `gRpc`
  router, compiled only for `LITE_WASM_SC && !NO_RPC`. Keeps the per-request `PinScope` drain and the
  shared rate limiter; no duplicated route logic. Non-Wasm Linux keeps the unix-socket + sidecar path.
- **overload.h** — rollback's networking lifecycle (SmartMutex, per-socket workers, bounded connect,
  child-promote reset); dynamic's portability reapplied (`processor_count.h`, `preciseSleepMicros`,
  pthread stack sizing, Windows `TCP_NODELAY`, lazy-commit, `__APPLE__` guards).
- **qubic.cpp** — rollback structure; dynamic's edits replayed at the include block, inside
  `spawnAPs()` (lazy-commit processor buffers), the CLI list, the signal handler (pager fault first
  under `LITE_SC_PAGER`), and `main()` platform guards.
- **test/CMakeLists.txt** — both `fork_rollback.cpp` and `fourq.cpp` (rollback had the latter
  commented out).

`rpc_core.h` was split: the router core is portable (Wasm builds reach it through the drogon
adapter), the unix-socket transport stays Linux-only.

## Fixes made during integration

- `src/CMakeLists.txt` guards `vmlib` with `if(NOT TARGET vmlib)`. `test/CMakeLists.txt` builds the
  same WAMR runtime, so configuring with `BUILD_TESTS=ON` previously failed with a duplicate-target
  error. This is the root-cause fix for a problem an earlier merge only worked around.
- `/live/v1/log-stats` re-implemented on `qLogger::logBuf.getBlobInfo/getMany`; the `tmpLogBuffer`
  map it used on the dynamic branch does not exist on the rollback side.

## Verification

Builds (clang-18, `ENABLE_AVX512=OFF`): mainnet, wasm testnet, plain testnet, plus the negative
config which must fail — all as expected.

Tests: 42 mainnet (fork suites + FourQ), 29 plain-testnet fork, 22 wasm-contract, 11 FourQ. All pass.

**Two-machine rollback soak** — remote `157.180.10.49` as MAIN (testnet build in
`/tmp/qubic-testnet-main`, `--node-mode 1 --http-port 41842`), local AUX dialing it, both plain
testnet. The remote also runs production mainnet from `/root/qlite`; it was never touched, and the
testnet RPC was moved to 41842 so it could not collide with the mainnet sidecar on 41841.

- AUX handshaked to the remote MAIN (82 MB received) and followed it.
- 4 forced fork+rollback cycles, all promoting the child: `forksRequested 4 / forksOk 4 / skipped 0`.
- fork() 270–417 ms, COW delta 0 MB.
- **Zero peer reaps across every promotion** (`disconnectReasons: {}`), `handshakedCount` 3
  throughout — the peer-reaper reset does its job; without it the promoted child judges the new
  sockets against the parent's timestamps and reaps them immediately.
- RPC rebound after each promote; supervisor PID stayed stable; no orphan checkpoint children.
- Tick gap to MAIN shrank (171 → 158) while promoting every 40 ticks.
- `--force-verify-solutions` suppressed forking completely: 97 ticks across several forced-fork
  points, all counters zero. The mainnet kill switch works.

**Wasm testnet smoke** — single node, `--node-mode 3`:

- Zero fork/checkpoint/supervisor evidence in the log; exactly one process (shim and sidecar are
  compiled out), confirming the boundary at runtime.
- The in-process adapter served every route 200: generic (`/tick-info`, `/live/v1/tick-info`,
  `/live/v1/whoami`, `/v1/peer-stats`) and Wasm-only (`dyn-registry`, `dyn-upload`,
  `dev/funded-seed`, `debug-trace`, `dev/epoch-info`).
- Payloads match the dynamic branch: `slotBase 29`, `slotCount 4`, `chunkSize 1008`, testnet seed
  gating live. `/live/v1/log-stats` — the one handler rebuilt on a different API rather than
  transcribed — returns populated entries (`logId 41`, 16 recent, each with `contractIndex`,
  `payloadHex`, `type`), so the `logBuf` read path is exercised with real data.
- The real Qinit CLI (`qinit ls --rpc`) drove the node and listed all 28 system contracts.
- Full contract round trip against the merged node: scaffold → build to wasm (3272 B) → chunked
  upload (5/5 assembled) → deploy tx → arm-confirm with matching codeHash at slot 29 → `Get` = 0 →
  `Inc` procedure processed at tick 72065324 → `Get` = 1, with `dev/state-read` returning
  `0100000000000000` and `dev/contract-digest` a valid K12 digest over the 8-byte state.

**Qinit** — `check:idl:native` passes (28 contracts, 5026 types, 3 oracle interfaces, 7 mutation log
messages; no ABI drift), `sources:check` current, `typecheck` clean.

## Known gaps

- `PeerReaper::resetForChildPromote()` has no unit test. It clears `sActive[]`, and the header needs
  the whole peer core, so a mock harness would dwarf the three-line loop. The soak's zero-reap result
  is the regression evidence.
- Live-checkpoint retirement on graceful shutdown was not exercised at runtime: it is triggered by
  the ESC key or an operator special command, neither reachable from a headless run. The
  `ForkRollbackControl` gtests cover the retire handoff.
- Windows and macOS Wasm builds are unverified — they need CI, which needs a push.
- `config/repositories.json` in Qinit still points at `hackerby888/core-lite@feat/dynamic-contracts`;
  the correct ref depends on the push target, which is undecided.

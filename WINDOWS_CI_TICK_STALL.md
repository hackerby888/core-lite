# Windows CI tick-stall — handoff (continue on a Windows box)

**One line:** on the **headless `windows-latest` CI runner** the MSVC node ticks ~14 times then **freezes
at `tx=?`** the moment the deploy's first transactions enter a tick — and the vote-count prefix reads
**`000:000(000)` (ZERO votes), a hard stop, not a slow crawl.** The deploy smoke (`qinit test`) then fails.
Linux-x64, linux-arm64, macOS all pass the same smoke. Last thing blocking the 4-way `qinit-release` deploy
smoke / digest-equivalence on `feat/dynamic-contracts`.

**Read `## Root cause` before coding** — earlier work (incl. two commits this session) chased a Windows
**timer-resolution / throughput** theory; the *zero* vote count disproves it. Current best explanation: a
**tickProcessor worker thread wedges in `processTick()`** on the first tx-bearing tick (most likely the
**WAMR contract-arm / linear-memory alloc**, which is *not* Linux-gated), so the node produces no further
votes. **Diagnostic 1 (a thread-state dump) should name the exact stuck call.**

This continues `WINDOWS_PORT.md` → *Phase 3b*. The local *interactive* port works (see that doc); the open
gap is the **headless** case (CI, or any detached/service launch).

---

## Symptom (from CI run 27435740466, `smoke (windows-latest)`)

```
✓ node      ephemeral · ticking at 55900000
✗ deploy    failed
...
260612191001 ... [+541 -0 *6 /14] 3|2|2 1/1/1 Dynamic behind=1 depth=2 tx=?   <- frozen
260612191001 ... 2 | Tick = 54.0 s                                            <- last tick pending 54s
... Latest created tick = 55'900'014  (never advances)
```

Decode of the status line (builder at `src/qubic.cpp:7778-7864`):

| field | meaning | value at stall |
|---|---|---|
| `[+541 -0 *6 /14]` | requests **processed / discarded / duplicate / disseminated**, per second | **541 processed/s** — the node is *not* network-dead |
| `3\|2\|2` | peer slots: connecting / connected / handshaked | has peers |
| `1/1/1` | public peers: handshaked / fullnode / total | — |
| `Dynamic` | peer-list mode (`listOfPeersIsStatic ? "Static" : "Dynamic"`) — **NOT** dynamic-contracts | — |
| `behind=1 depth=2` | `USE_FUTURE_TICK_PREFETCH` catch-up: ticks behind network tip / prefetch depth | 1 behind |
| **`tx=?`** | **votes have not converged on the next-tick data digest** (`!targetNextTickDataDigestIsKnown`) | **the stall** |

The node's own comment documents `tx=?` at `src/qubic.cpp:7847`. `tx=?` is the whole story: consensus
cannot pick a next-tick transaction digest.

---

## Root cause — corrected (read this; it supersedes the timer theory)

The node ticks **14 times**, then freezes the instant the deploy's transactions enter a tick. The frozen
status line's **prefix** is the decisive evidence (formatter at `src/qubic.cpp:498-512`):

```
AAA:BBB(CCC).TICK.EPOCH    ->    000:000(000).55900014.216
 |   |   |
 |   |   gFutureTickTotalNumberOfComputors  (votes for the NEXT tick)             = 0
 |   gTickTotalNumberOfComputors - gTickNumberOfComputors  (misaligned votes)     = 0
 gTickNumberOfComputors  (computors whose vote AGREES on this tick)               = 0
```

The node registers **ZERO votes** — current tick *and* next tick. This is **not** "votes crawling at
~25/s" (the historical timer-throttle signature from `WINDOWS_PORT.md` Phase 2); it is a **hard stop at
zero**. That distinction is everything:

- A throughput / timer-resolution problem makes the count **climb slowly** toward 451. Here it is **0**.
- **=> The 15.6 ms-timer / 50 µs-sleep starvation theory is the WRONG target.** That is also why this
  session's EcoQoS + high-res-sleep commits changed nothing — they fought a throughput problem that isn't
  the one happening. (The `qubic.cpp:2037` `sleep_for(50us)` pacing sleep is real, and the local Phase-2
  `timeBeginPeriod(1)` + `IGNORE_TIMER_RESOLUTION` opt-out is a legit fix for *that* — but a starved-but-
  progressing vote count would not sit at exactly 0.)

`tx=?` (`!targetNextTickDataDigestIsKnown`) then follows trivially: with 0 future votes,
`findNextTickDataDigestFromNextTickVotes()` (`src/qubic.cpp:5248`) isn't even entered (gated at `:6349` on
`gFutureTickTotalNumberOfComputors > 225`), and the MAIN-mode current-tick path
(`findNextTickDataDigestFromCurrentTickVotes`, `:6359`) has nothing to tally. Quorum **451**
(`= 676*2/3+1`, `common_def.h:11`) is unreachable from 0.

**So the real question: why does a single-node MAIN node — which produces all 676 of its own votes —
register ZERO votes the moment a tick carries transactions?** Framing facts (all verified):

- Node **is** MAIN: `--node-mode 3` → `mainAuxStatus=3` → `isMainMode()` true (MAIN&MAIN, `qubic.cpp:9593`).
  It voted fine for 14 ticks, so vote production worked **until transactions appeared**.
- `TARGET_TICK_DURATION = 1000 ms` (testnet, `public_settings.h:56`) — the tick is **54× past** target, yet
  the `AUTO_FORCE_NEXT_TICK` (`qubic.cpp:5775`) and `autoResendTickVotes` (`:5742`) safety paths are **not**
  rescuing it. A self-healing throughput dip would have force-ticked long ago. **Nothing recovers it.**
- The per-second status keeps printing (`Main loop duration`, `Ticker loop duration`) → the **main/logging
  thread is alive**; it is a **tickProcessor worker thread that is wedged**. (`tickProcessor` = 32 threads
  in TESTNET; `processTick()` per-thread at `qubic.cpp:6341`.)

### Leading hypotheses (a Windows thread-state dump decides between them in minutes)

1. **A tickProcessor thread is wedged inside `processTick()` on the first tx-bearing tick** — the per-tx
   path hangs on Windows, the tick never completes, next-tick votes are never produced, the counters stay
   at their last (reset → 0) value. The DEPLOY tx **arms a WAMR contract**: instantiate + the ~1 GB linear-
   memory arena via the Windows mmap-shim / `VirtualAlloc`. That path is **NOT** Linux-gated (unlike
   `k12_engine.h`) and is the most Windows-divergent code any transaction can reach. **Most likely** — and,
   importantly, it lives in `src/extensions/lite_wasm_contracts.h` / the WAMR glue, which **is** editable.
2. **Vote broadcast/counting halted** — the node still produces votes but they aren't stored/counted in
   `ts.ticks[]` (loopback self-vote dropped under the deploy burst, or a lock left held). Less likely (the
   `ACQUIRE` locks are `_mm_pause` spins, `concurrency.h:31`), but a thread dump rules it in or out.

### Ruled out (don't re-chase)
- **Timer-resolution / 50 µs-sleep throughput** — downgraded: it would show a *climbing* vote count, not 0.
- **Console / `_kbhit` hang** — no; the main loop prints every iteration.
- **k12 demand-zero contract-state engine** (`728aab3e`) — no; `k12_engine.h` is `#if defined(__linux__)`
  (`qubic.cpp:189`), compiled out on Windows. (NB: this is the *digest/eviction* engine — **distinct** from
  the WAMR contract-execution path in hypothesis 1, which is *not* gated and is still suspect.)
- **Network transmit/receive stall** — no; 541 req/s, `transmitProcessor` (`overload.h:1142`) is bounded
  (1 s/item, always drains). Node is busy, not network-dead.
- **A consensus *logic* bug** — no; identical code converges on Linux/macOS/arm. The divergence is in a
  Windows-specific primitive a tickProcessor thread touches.

### Tried this session, did **not** close CI (be honest / clean up)
Two commits on `feat/dynamic-contracts` claim "(fixes tick stall)" but do **not** fix CI — reword or drop:
- `acb8595a` — opt out of EcoQoS `EXECUTION_SPEED` throttling. (Targeted throughput; votes were at 0.)
- `6cebb3f2` — `preciseSleepMicros()` (`CREATE_WAITABLE_TIMER_HIGH_RESOLUTION`) into 3 sleep sites. Harmless
  Windows hardening; not the cause. Helper at `overload.h:52` is fine — just not relevant to a vote=0 halt.

(`winmm` **is** linked — `overload.h:13` `#pragma comment(lib,"winmm.lib")` — so a missing-winmm theory is
also out. `timeBeginPeriod`/`SetProcessInformation` return values are still unchecked, but with votes at 0
that is no longer the prime suspect.)

---

## Reproduce on your Windows machine

Build (the CI windows recipe, `.github/workflows/qinit-release.yml` `node-windows`):
```powershell
cmake -S . -B build-win -A x64 `
  -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_INSTALLATION_ROOT/scripts/buildsystems/vcpkg.cmake" `
  -DVCPKG_OVERLAY_TRIPLETS="$PWD/triplets-overlay" -DVCPKG_TARGET_TRIPLET=x64-windows-rel `
  -DBUILD_BINARY=ON -DBUILD_TESTS=OFF -DENABLE_AVX512=OFF -DUSE_SANITIZER=OFF `
  -DTESTNET=ON -DTESTNET_LITE_RAM=ON -DTESTNET_PREFILL_QUS=ON `
  -DLITE_DYNAMIC_CONTRACTS=ON -DLITE_WASM_CONTRACTS=ON -DCMAKE_NO_USE_SWAP=ON `
  -DADDON_TX_STATUS_REQUEST=ON -DONLY_LOGGING=OFF `
  -DFFI_LIB_SHARED="<vcpkg_installed>/lib/ffi.lib" -DFFI_INCLUDE_DIR="<vcpkg_installed>/include"
cmake --build build-win --config Release --target Qubic --parallel 4
```

**Repro the CI condition (this is the crux — a normal foreground run hides the bug):** launch the node as
a *background / headless* process so Windows throttles it the way CI does — detached, no attached
interactive console, stdout to a file. e.g. from a non-interactive service/scheduled-task context, or:
```powershell
Start-Process -FilePath build-win\src\Release\Qubic.exe `
  -ArgumentList '--peers 127.0.0.1 --node-mode 3 --ticking-delay 1000' `
  -WindowStyle Hidden -RedirectStandardOutput node.log -RedirectStandardError node.err
```
(qinit launches it the same way: `--peers 127.0.0.1 --node-mode 3 --ticking-delay 1000`, detached, stdio→
file — `Qinit/packages/cli/src/node-ops.ts:94`.) Then drive a deploy that puts **transactions** in a tick:
```bash
bun qinit/packages/cli/src/index.tsx test --bin build-win/src/Release/Qubic.exe \
  --core <core-lite> --contract contracts/DigestProbe.h --name DigestProbe \
  --skip-verify --wait 150 --timeout 90000 --keep
```
Watch `node.log`: empty ticks advance, then it sticks at `... tx=?` and `Tick = N s` climbs. **If a hidden/
background launch reproduces it and a normal terminal launch does not, the diagnosis is confirmed.**

---

## Diagnostics (do these first — in order; each is cheap and splits the tree)

1. **Thread-state dump — THE decisive one.** Repro the freeze, then attach the VS debugger (or `procdump
   -ma Qubic.exe`, or WinDbg `~*k`) and look at the **32 tickProcessor threads**. The wedged worker's call
   stack names the culprit directly. Expect to find one parked inside `processTick()` (`qubic.cpp:6341`) →
   the per-tx path → the WAMR arm/instantiate or the linear-memory `VirtualAlloc`/mmap-shim. (If instead
   every worker is idle and the stall is in the *counting* path, that's hypothesis 2.) **Do this before
   touching any code** — it likely ends the investigation.
2. **Contract vs any-tx split.** Submit a **plain transfer** tx (no contract) and watch the prefix vote
   count. If it **also** freezes at `000:000(000)` → the halt is generic to *any* tx in a tick (look at the
   tx/tickData assembly + vote production, not the wasm engine). If a plain tx sails through and **only the
   deploy** freezes → it's the **dynamic-contract path** (`src/extensions/lite_wasm_contracts.h` /
   `lite_dynamic_contracts.h`, WAMR arm) — hypothesis 1. This one test eliminates half the search space.
3. **Watch the vote counters live.** The prefix already prints them (`AAA:BBB(CCC)` = aligned : misaligned
   : future, `qubic.cpp:498-512`). Confirm they sit at **0** (halt) and never climb. If they climb slowly
   toward 451 instead, re-open the timer-throughput theory — but the captured CI log shows a flat 0.
4. **Only if hypothesis 1 is implicated:** add a log at `processTick()` entry/exit and around the WAMR
   instantiate / linear-memory alloc in the wasm glue — find the exact call that never returns on Windows.
   The arena is ~1 GB lazy-mapped (`io_base = [in 64K | out 64K | locals 32K | arena 1GB]`); a Windows
   commit/`VirtualAlloc` or mmap-shim that **eager-commits or faults** is the prime suspect.

---

## Candidate fixes (ranked — pick after the thread dump points somewhere)

> The thread dump in Diagnostic 1 should pick the fix for you. Most-likely target is the WAMR arm path:

1. **Fix the wedged call the thread dump names.** If it's WAMR instantiate / linear-memory alloc
   (`src/extensions/lite_wasm_contracts.h` + the mmap-shim used on Windows): make the arena allocation lazy/
   non-faulting on Windows (`MEM_RESERVE` then commit-on-touch, or a VEH commit handler), or bound/relocate
   whatever blocks. **In-scope (extensions-only).** This is where hypothesis 1 lands.
2. **If it's the counting/broadcast path (hypothesis 2):** a lock left held or a loopback self-vote dropped
   under the deploy burst — fix the specific drop/lock the dump shows. Verify votes appear in `ts.ticks[]`.
3. **Timer-resolution hardening (do regardless, but it is NOT this bug):** `timeBeginPeriod` is advisory and
   gets throttled on background processes; the undocumented **`NtSetTimerResolution`** (ntdll) forces it.
   Worth adding in `initializeUefi()` for general Windows-port health and to close the *original* Phase-2
   crawl on hostile hosts — but it will not move a vote=0 halt. Keep separate from the real fix; don't
   relabel it "fixes tick stall."

Validate any fix by re-running `qinit-release` and watching `smoke (windows-latest)` reach the deploy +
the `digest-check` job (the Windows `GET /live/v1/dev/contract-digest?slot=N` must byte-match the other
legs — that's the cross-platform consensus proof Phase 3b is for).

---

## Anchors / constants (verified)

- **Vote-count prefix** `AAA:BBB(CCC).tick.epoch` formatter: `src/qubic.cpp:498-512` — `AAA`=
  `gTickNumberOfComputors` (aligned current-tick votes), `CCC`=`gFutureTickTotalNumberOfComputors`
  (next-tick votes). Frozen value in CI: `000:000(000)` = **0 votes** (the key evidence).
- `tx=?` printer + status builder: `src/qubic.cpp:7778-7864`; flag decl `:313`.
- Vote convergence: `findNextTickDataDigestFromNextTickVotes()` `:5248` (entered only when future votes
  `>225`, gate `:6349`); MAIN current-tick path `findNextTickDataDigestFromCurrentTickVotes` `:6359`.
- Counter set/reset: `gTickNumberOfComputors` set `:6700`, reset `:6584`/`:6895`; future-vote count
  `:5116-5130`. Self-heal that is NOT firing: `AUTO_FORCE_NEXT_TICK` `:5775`, `autoResendTickVotes` `:5742`.
- `processTick()` per-thread call site: `src/qubic.cpp:6341`; `tickProcessor` = 32 threads in TESTNET (~6020).
- `QUORUM = 451`: `src/network_messages/common_def.h:11` (`676*2/3+1`). `TARGET_TICK_DURATION = 1000`:
  `src/public_settings.h:56`.
- MAIN mode: `--node-mode 3` → `mainAuxStatus=3` → `isMainMode()` (`qubic.cpp:9593`, def `:535`).
- WAMR contract-exec / arm path (hypothesis 1, NOT Linux-gated): `src/extensions/lite_wasm_contracts.h`,
  `lite_dynamic_contracts.h`. Contrast `k12_engine.h` which **is** `#if __linux__` (`qubic.cpp:189`).
- `winmm` linked: `src/extensions/overload.h:13`. Timer opt-out block: `overload.h:1266-1289`
  (`timeBeginPeriod` `:1271`, `IGNORE_TIMER_RESOLUTION` `:1288`). `preciseSleepMicros()` helper `:52`.
  Pacing sleep (real but not this bug): `src/qubic.cpp:2037`.
- Node launch (qinit smoke): `--peers 127.0.0.1 --node-mode 3 --ticking-delay 1000` (`node-ops.ts:94`).
- Failing CI run: `27435740466` (hackerby888/core-lite, `qinit-release`). Windows leg added by `5bf70556`;
  it has **never** passed (not a regression).
- Branch: `feat/dynamic-contracts`, remote **hackerby888** (not qubic upstream).

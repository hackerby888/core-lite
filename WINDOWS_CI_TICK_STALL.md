# Windows CI tick-stall — handoff (continue on a Windows box)

**One line:** the MSVC node deploys + ticks fine on a *local interactive* Windows session, but on the
**headless `windows-latest` CI runner** it ticks ~14 empty ticks then **freezes at `tx=?`** (vote
convergence stalls) the moment the deploy's first transactions arrive. The deploy smoke (`qinit test`)
then fails. Linux-x64, linux-arm64, and macOS all pass the same smoke. This is the **last** thing blocking
the 4-way `qinit-release` deploy smoke / digest-equivalence on `feat/dynamic-contracts`.

This continues `WINDOWS_PORT.md` → *Phase 3b*. The local port already works (see that doc); the open gap
is **CI-only**.

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

## Root cause (high confidence)

It ticks **14 empty ticks**, then stalls on **the first tick that carries transactions** (the upload
chunks + DEPLOY tx). That is the timing fingerprint of a **vote-throughput** problem, not a logic bug:

- Single-node testnet (`TESTNET_PREFILL_QUS`, 676-seat fixture) — the node produces **all** votes itself.
- Empty ticks vote a trivial (zero) transaction digest → all seats agree instantly → quorum trivially →
  ticks advance. (14 of them.)
- The first **non-empty** tick needs the node's seats to vote the *same* transaction digest and reach
  **QUORUM = 451** (`= 676*2/3+1`, `src/network_messages/common_def.h:11`). Convergence is decided in
  `findNextTickDataDigestFromNextTickVotes()` (`src/qubic.cpp:5248`): tally unique vote digests; if the
  top one has `>= QUORUM` → `targetNextTickDataDigestIsKnown = true` (line 5292-5295); else if quorum is
  *provably impossible* → fall back to an empty tick (line 5299-5304). **Permanent `tx=?` means neither
  fires** — the votes are still trickling in too slowly for the top digest to reach 451, but it isn't yet
  "impossible," so the node waits… forever.

**Why the votes trickle:** Windows' default multimedia timer resolution is **~15.6 ms**. The hot loops
pace themselves with sub-millisecond sleeps that round **up to 15.6 ms** when the resolution isn't raised.
The dominant one:

- **`src/qubic.cpp:2037`** — the request-processor loop: `std::this_thread::sleep_for(microseconds(50))`
  **every iteration**. At 15.6 ms/iter that's ~64 iters/s per processor instead of ~20 000 — request/vote
  dissemination collapses. This is exactly the `WINDOWS_PORT.md` Phase-2 signature: *"ticks 2-3×, then
  stalls at `tx=?` with the vote counter crawling (~25 votes/s; quorum needs 451)."*

`WINDOWS_PORT.md` already fixed this **locally** with, in `overload.h` `initializeUefi()` (`_MSC_VER`):
`timeBeginPeriod(1)` **+** `SetProcessInformation(ProcessPowerThrottling, IGNORE_TIMER_RESOLUTION)`. That
makes the 50 µs sleep ≈ 1 ms and the node hits Linux-parity ~1 tick/s.

**The open question — why CI still stalls:** the fix relies on Windows *honoring* the timer-resolution
request. The same doc notes the tell: *"interactive-console runs eventually recovered, qinit-spawned
(detached, stdio→file) nodes never did"* — Windows 11 **power-throttles background/occluded processes** and
silently ignores `timeBeginPeriod`. The CI `windows-latest` runner is the most extreme background case
(headless VM, no interactive desktop session). The hypothesis: **on the CI runner the timer-resolution
opt-out is not being honored, so the 50 µs sleeps are back at 15.6 ms and votes crawl.**

### Ruled out (don't re-chase)
- **Console / `_kbhit` hang** — no; the main loop runs every iteration (`Main loop duration` keeps printing).
- **WASM engine / k12 demand-zero** (`728aab3e`) — no; `k12_engine.h` is `#if defined(__linux__)`
  (`src/qubic.cpp:189`), compiled out on Windows.
- **Network transmit/receive stall** — no; 541 req/s, and `transmitProcessor` (`overload.h:1142`) is
  bounded (1 s/ item timeout, always drains). The node is busy, not blocked.
- **A consensus logic bug** — no; identical code converges on Linux/macOS/arm. It's throughput/timing.

### Tried this session, did **not** close CI (be honest / clean up)
Two commits on `feat/dynamic-contracts` claim "(fixes tick stall)" but do **not** fix CI — reword or drop:
- `acb8595a` — opt out of EcoQoS `EXECUTION_SPEED` throttling (a second `SetProcessInformation` call).
- `6cebb3f2` — `preciseSleepMicros()` (a `CREATE_WAITABLE_TIMER_HIGH_RESOLUTION` waitable timer) wired into
  **only 3 sites** (`Stall`, transmit-WOULDBLOCK, the timeout-wait) — **not** the `qubic.cpp:2037` pacing
  sleep, which is the actual bottleneck. The helper itself (`overload.h:52`) is the right primitive; it's
  just not applied where it matters. Both are harmless hardening; only the commit *messages* overclaim.

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

## Diagnostics (do these first — cheap, decisive)

1. **Is it throughput or logic?** Submit a *plain transfer* tx (no contract) instead of a deploy. If the
   node *also* stalls at that tick → pure vote-throughput/timer issue (expected). If a plain tx is fine and
   only the deploy stalls → the dynamic-contract tx handler is implicated instead (then look at
   `src/extensions/lite_wasm_contracts.h` / `lite_dynamic_contracts.h` on Windows). This single test splits
   the remaining hypotheses.
2. **Confirm the timer is throttled on the runner.** In the hidden/background process, log the actual
   granularity right after the opt-out: call `timeGetDevCaps`, then time a `sleep_for(microseconds(50))` in
   a loop and print the measured ms. Expect ~15.6 ms when throttled, ~1 ms when honored. Also **check the
   return value of `SetProcessInformation(ProcessPowerThrottling, …)`** and `timeBeginPeriod` — they may be
   failing silently on the CI VM.
3. **Confirm it's the votes.** Temporarily log, in `findNextTickDataDigestFromNextTickVotes()`
   (`src/qubic.cpp:5248`), `numberOfUniqueNextTickTransactionDigests`, the top counter, and how many of 676
   seats have voted (`epoch == system.epoch`). Stuck = top counter < 451 and seats-voted < 676 and climbing
   slowly → starved by the sleep. (split digests, by contrast, would show many uniques — that'd point at a
   *non-determinism* bug, not timing.)

---

## Candidate fixes (ranked)

1. **Force the timer resolution in a way background-throttling can't ignore.** `timeBeginPeriod` is
   advisory and gets throttled; the undocumented **`NtSetTimerResolution`** (ntdll) sets it directly and is
   commonly used to win exactly this fight. Try calling it in `initializeUefi()` (`overload.h`, `_MSC_VER`)
   alongside the existing opt-out. **In-scope (extensions-only), smallest blast radius — try first.**
2. **Make the hot pacing sleeps resolution-independent** rather than relying on the global timer. The
   bottleneck is `src/qubic.cpp:2037` (and any other per-iteration `sleep_for(microseconds(...))` in the
   request/tick loops — grep `sleep_for` in `qubic.cpp`). Route them through the existing
   `preciseSleepMicros()` (`overload.h:52`, high-res waitable timer — per-object, **not** subject to the
   global resolution/QoS). Caveat: `qubic.cpp` is normally off-limits in this fork; this is the one change
   that may warrant touching it, or hoist the loop's pacing into an extension helper.
3. **Drop the pacing sleep to a busy `_mm_pause()` spin** in the request loop (like `ACQUIRE`,
   `concurrency.h:31`) — no timer dependency at all. Costs idle CPU; fine for a CI smoke, maybe not for a
   real node. Could be gated to `_MSC_VER` + a "headless" flag.
4. **Verify `winmm` is actually linked** for `timeBeginPeriod`/`timeGetDevCaps` in the CMake/MSVC build
   (`src/CMakeLists.txt`) — if the opt-out silently no-ops because the lib isn't linked, that alone explains
   CI vs local.

Validate any fix by re-running `qinit-release` and watching `smoke (windows-latest)` reach the deploy +
the `digest-check` job (the Windows `GET /live/v1/dev/contract-digest?slot=N` must byte-match the other
legs — that's the cross-platform consensus proof Phase 3b is for).

---

## Anchors / constants (verified)

- `tx=?` printer + status builder: `src/qubic.cpp:7778-7864`; flag decl `:313`.
- Vote convergence: `findNextTickDataDigestFromNextTickVotes()` `src/qubic.cpp:5248` (quorum `:5292`,
  empty-fallback `:5299`).
- `QUORUM = 451`: `src/network_messages/common_def.h:11` (`676*2/3+1`).
- Bottleneck pacing sleep: `src/qubic.cpp:2037` (`sleep_for(microseconds(50))`).
- Timer opt-out + `preciseSleepMicros()`: `src/extensions/overload.h` (`initializeUefi()`, helper `:52`).
- `tickProcessor` is multi-threaded (32 in TESTNET) — votes produced across threads; `src/qubic.cpp:~6020`.
- Node launch (qinit smoke): `--peers 127.0.0.1 --node-mode 3 --ticking-delay 1000`.
- Failing CI run: `27435740466` (hackerby888/core-lite, `qinit-release`). Windows leg added by `5bf70556`;
  it has **never** passed (not a regression).
- Branch: `feat/dynamic-contracts`, remote **hackerby888** (not qubic upstream).

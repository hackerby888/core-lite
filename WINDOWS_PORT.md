# Windows (MSVC) port of the dynamic-contract node — continuation guide

Goal: build + run the **testnet dynamic/wasm node** on Windows with MSVC. This is the qubic-core-lite
fork (OS-process port). The dynamic-contract feature lives on branch **`feat/dynamic-contracts`** and is
wired through **CMake** (the mainnet `Qubic.sln` does NOT have the WAMR/libffi/dynamic glue). So Windows
builds via **cmake + the Visual Studio generator**, not the .sln.

**STATUS: the port works end-to-end.** `Qubic.exe` builds, boots, ticks, and runs deployed wasm
contracts: `qinit test` (Counter fixture) passes against the Windows node — build → upload txs →
deploy → WAMR exec → Get/Inc/Get green. Remaining work is §Phase 3b (CI matrix + digest equivalence)
and the §Known issues below.

All fixes are committed + pushed to `feat/dynamic-contracts` (remote **hackerby888**, NOT qubic
upstream). A fresh clone already has them.

## How to build locally (fast loop — beats the CI by ~100×)

Prereqs: Visual Studio 2022 (Desktop C++ workload → MSVC `cl`, `ml64`, CMake), Git, and vcpkg
(`git clone https://github.com/microsoft/vcpkg C:\vcpkg && C:\vcpkg\bootstrap-vcpkg.bat`). nasm is
optional (the `.asm` are pinned to ml64; blosc2's ASM_NASM probe tolerates no nasm — CI runs without it).

```bat
cmake -S . -B build-win -A x64 ^
  -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake ^
  -DBUILD_BINARY=ON -DBUILD_TESTS=OFF -DENABLE_AVX512=OFF -DUSE_SANITIZER=OFF ^
  -DTESTNET=ON -DTESTNET_LITE_RAM=ON -DTESTNET_PREFILL_QUS=ON ^
  -DLITE_WASM_SC=ON -DCMAKE_NO_USE_SWAP=ON ^
  -DADDON_TX_STATUS_REQUEST=ON -DONLY_LOGGING=OFF ^
  -DFFI_LIB_SHARED=%CD%/build-win/vcpkg_installed/x64-windows/lib/ffi.lib ^
  -DFFI_INCLUDE_DIR=%CD%/build-win/vcpkg_installed/x64-windows/include
cmake --build build-win --config Release --target Qubic
```
- vcpkg is **manifest mode**; packages land in `build-win/vcpkg_installed/<triplet>/` (NOT
  `C:/vcpkg/installed/` — the FFI paths above point at the manifest dir; the first configure runs
  before they exist, which is fine: they're only read at link).
- `vcpkg.json` lists ONLY the binary deps actually consumed from vcpkg: **libffi, openssl, c-ares,
  zlib, brotli** (the C libs drogon/trantor link). The C++ frameworks (fmt/jsoncpp/drogon) are
  **FetchContent only** — having them in the manifest too put their *dynamic* (dllimport) headers on
  the include path via FFI_INCLUDE_DIR, shadowing the FetchContent static ones → LNK2019 `__imp_*`
  on every trantor/drogon symbol. Don't re-add them.
- First configure builds openssl etc. from source (~45 min, once, cached). Tip: the CI's release-only
  overlay triplet (`VCPKG_BUILD_TYPE release`, see windows-dyn-build.yml) skips every port's debug
  variant — use it locally too if you're provisioning fresh.
- `ONLY_LOGGING=OFF` uses the committed public-testnet `src/private_settings.h` (`ON` collides with
  TESTNET on the `broadcastedComputorSeeds` shim).
- **The canonical recipe is `.github/workflows/windows-dyn-build.yml`** — keep it and this doc in sync.

## The loop (what to ask Claude to do)
> Build `--target Qubic`. On the first MSVC error, fix it (prefer a `WIN32`-guarded CMake/extension
> change so linux/macOS stay byte-identical), rebuild, repeat — until `Qubic.exe` links. Tip: build
> serially (no `/m`) when an error is hard to find; `/m` interleaves output and buries the failing target.

## Rules
- **Guard every Windows change** with `if(WIN32)` / `#ifdef _WIN32` so the Linux + macOS builds are
  byte-identical (verify with `cmake -B <existing linux build dir>` — it must reconfigure unchanged).
- **`k12_engine.h` is Linux-only** (userfaultfd). On Windows the adapter (`runtime/state_backend.h`)
  uses `LITE_SC_CONTRACT_LEVEL` → demand-zero `qVirtualAlloc` (VirtualAlloc) contract state. The
  free-mismatch fixes (`liteSCOnWasmTakeover`, `liteSCFree`, `freePoolOrVirtual`) already route the
  Windows free path correctly — don't `freePool` a `qVirtualAlloc`/memfd pointer.
- Commit to `feat/dynamic-contracts`; push to **hackerby888**. Short commit msgs, no AI attribution.

## Done — build-system layer (iters 3–11)
| iter | error | fix |
|---|---|---|
| 3 | configure ok; build @ **libbacktrace** (autotools `./configure`+make) | guard the libbacktrace ExternalProject `if(NOT APPLE AND NOT WIN32)` (Windows uses boost windbg backend) |
| 4 | Windows fell into the **Linux link branch** (`-Wl,-Bstatic`, `dl`/`rt`) | add an `elseif(WIN32)` `target_link_libraries` (FetchContent targets, no backtrace, no GNU-ld/unix libs) |
| 5–10 | **`.asm` → nasm `-fwin32`** `MSB3721` (EDK2 SetJump/LongJump, WAMR `invokeNative_em64.asm` are MASM syntax) | force `LANGUAGE ASM_MASM`: EDK2 in `lib/platform_common/CMakeLists.txt`; **WAMR** per-file in `src/CMakeLists.txt` at the `vmlib` target |
| 11 | **`custom_stack.nasm`** ran `$NASM_EXECUTABLE -f elf64` → `'-f' not recognized` | deadcode (setjmp used); `elseif(WIN32)` uses the same no-op stub ARM uses |

## Done — qubic.cpp + extensions (iters 12–19, the MSVC compile of the single TU)
| iter | error | fix |
|---|---|---|
| 12 | `C2665 appendText` ×1000s — `L"..."` is native `wchar_t*`, `CHAR16` is `unsigned short` | **`/Zc:wchar_t-`** on the Qubic target (= the mainnet .sln setting; wchar_t == unsigned short == CHAR16). Deps unaffected: their APIs are std::string; `<filesystem>` internals are `extern "C"` |
| 13 | `C2760` syntax error in `runtime/qpi_services.h` at `->__transfer(...)` | the Windows SDK `specstrings.h` (via `<windows.h>`) defines a **function-like SAL macro `__transfer(formal)`** → `#undef __transfer` under `_MSC_VER` (no other SAL names collide — grepped) |
| 14 | `C2589 '(' illegal right of '::'` — `std::min/max` vs windows.h `min`/`max` macros | `/DNOMINMAX` on the Qubic target |
| 15 | `C1083 explorer_assets.generated.h` missing (regen.sh needs bash) | `regen.cmake` (cmake -P, no bash) + `else()` branch of the explorer custom command. NOTE: emits **byte arrays**, not raw strings — see iter 16 |
| 16 | `C2026 string too big` — MSVC caps a string literal at ~16K; css/js exceed it | regen.cmake emits `unsigned char[]` byte arrays + `const char* const` aliases (consumers unchanged) |
| 17 | `LNK2019 __imp_?...@trantor/drogon@` everywhere | vcpkg-manifest drogon's **dynamic** headers shadowed the FetchContent static ones (via FFI_INCLUDE_DIR) → trim `vcpkg.json` (see build notes above) |
| 18/19 | trantor/drogon `C1083 ares.h / openssl / zlib.h / brotli` after the trim | those C libs ARE consumed from vcpkg → manifest = libffi+openssl+c-ares+zlib+brotli exactly |

Also in this pass (pre-emptive, all `_WIN32`-guarded): `runtime/state_write_tracker.h` dirty-page tracker ported
(sigaction/mprotect → `AddVectoredExceptionHandler`/`VirtualProtect`, `__sync_*` → `_InterlockedExchange`);
`utils.h` `exec()` via `_popen`; `overload.h` `getCheckInData` compiled on Windows (http.h references it);
`http.h` + the `QubicHttpServer::start`/`watchAndCheckin` gates extended to `_WIN32` (RPC server now runs
on Windows — Phase 2 needs it); the leftover `SUFFIX ".efi"` removed (OS port = `Qubic.exe`).

## Done — Phase 2 runtime fixes (boot + tick)
| symptom | cause | fix |
|---|---|---|
| node boots, ticks 2-3×, then stalls at `tx=?` with the vote counter crawling (~25 votes/s; quorum needs 451) | **Windows default timer resolution is ~15.6ms** — every `sleep_for(1ms)` in the request/vote/transmit loops slept 15.6ms → throughput ~15× below Linux | `timeBeginPeriod(1)` in `initializeUefi()` (overload.h, `_MSC_VER`) + winmm |
| (belt-and-braces, same pass) | Nagle + delayed-ACK on the many small per-vote loopback sends | `TCP_NODELAY` on accepted + outgoing sockets (`_MSC_VER`) |
| stalls kept recurring ANYWAY — randomly at boot ("warm-up") or mid-run under tx load, request rate back at the ~256/s signature; interactive-console runs eventually recovered, qinit-spawned (detached, stdio→file) nodes never did | **Windows 11 power-throttles timer-resolution requests of "background" processes** (detached/occluded-console), silently ignoring `timeBeginPeriod` — the stall came and went with window state, which is why it looked nondeterministic | `SetProcessInformation(ProcessPowerThrottling, IGNORE_TIMER_RESOLUTION, StateMask=0)` right after `timeBeginPeriod(1)` — the request is then always honored. Node now ticks ~1/s (Linux parity) with no warm-up stall, even spawned by qinit |
| working set ~11.5 GB (target ~1.9 GB) | Windows has **no shared zero page** for committed private memory: the per-tick digest's READS of never-written state pages each fault in a unique physical zero page — the first full sweep (change flags boot 0xFF) pinned every contract state's whole reserve (wsmap: 4×1GB dyn reserves + QX 593MB + ... ≈ 11 GB, all 100% resident; linux/mac mmap reads hit the shared zero page) | `VirtualUnlock(state, size)` after the K12 in `hashContractState` — documented way to drop pages from the working set WITHOUT invalidating them (readers soft-fault back; clean zero pages reclaimed outright; digest unchanged). Working set now **~1.7 GB**, contract regions 0-resident (`tools/wsmap.c` measures per-region residency) |

Boot: `run\..\build-win\src\Release\Qubic.exe --peers 127.0.0.1 --node-mode 3 --ticking-delay 1000`,
then `GET http://127.0.0.1:41841/live/v1/tick-info` advances (vcpkg applocal copies ffi-8/openssl/...
DLLs next to the exe automatically).

## Done — Phase 3a: deploy + exec via qinit
`qinit test --bin build-win\src\Release\Qubic.exe --core <core-lite> --skip-verify` **passes** on
Windows (Counter @ dyn slot, wasm via wasi-sdk-29-windows, upload/deploy txs, WAMR exec, 1 pass/0 fail).
qinit-side Windows fixes (qinit repo): `node-ops.ts` `taskkill /F /IM Qubic.exe` + `tasklist` (pkill/
pgrep elsewhere), `Qubic.exe` binary name, no chmod; `test.tsx`/`verify.ts` `Bun.which` instead of
spawning `sh`; `deploy-ops.ts` tick-wait 90s → 300s (cold-node warm-up stall, below).

## Known issues (Windows, non-blocking)
- **Commit charge** ~16.7 GB → **FIXED (2026-06-13): now ~2.8 GB** (working set ~1.77 GB, Linux parity).
  Root cause: every big demand-zero reserve (contract states 10 GB, score 2 GB, commonBuffers 2 GB,
  peer 0.86 GB, processor 0.2 GB) was `MEM_COMMIT`ted up front, charging the full reserve against the
  commit limit (Linux relies on mmap overcommit + the shared zero page, so only *written* pages count).
  Fix = the "real lazy-commit scheme" this note called for: `qVirtualAllocLazy` (`MEM_RESERVE` +
  commit-on-write via a vectored handler, `overload.h`) + a page-aware `KangarooTwelvePaged`
  (`extensions/k12_paged.h`) so the per-tick digest doesn't fault untouched reserve pages back into
  commit. This was ALSO the CI tick-stall root cause (the 16.7 GB sat on a 16 GB CI VM's commit limit;
  the deploy's +1 GB WAMR arena tipped it over) — see `WINDOWS_CI_TICK_STALL.md`. Peer buffers stay
  eager (kernel `recv` write targets can't fault-commit). The page-aware digest is proven byte-identical
  to canonical K12 (`tools/k12paged_test.cpp`), so the cross-platform contract-state digest is unchanged.
  (The earlier "warm-up stall" and "2-4s tick rate" entries were the timer-resolution throttling
  above — gone since the opt-out; ticks run ~1/s from the first tick. qinit's longer deploy
  budgets, added while diagnosing, were kept as general slow-node robustness.)

## Phase 3b — consensus proof (next)
- Add **`windows-x64`** to the qinit-release smoke + **digest-equivalence** matrix
  (`GET /live/v1/dev/contract-digest?slot=N` must byte-match linux/macOS) → 4-way consensus proof.
- The windows-dyn-build CI now links; consider uploading Qubic.exe + the applocal DLLs as one artifact.
- ~~**BLOCKER (CI-only):** the smoke leg freezes at `tx=?` when deploy txs arrive~~ → **FIXED (2026-06-13).**
  Real root cause was NOT the timer/throttle theory but the **eager ~16.7 GB commit charge** sitting on the
  16 GB CI VM's commit limit (the deploy's +1 GB WAMR arena tipped it over). Fixed by the lazy-commit +
  page-aware-digest work in the Known-issues entry above (commit now ~2.8 GB). Validated locally (qinit test
  passes, commit/WS measured); final proof is the green `windows-latest` smoke + `digest-check` leg on push.
  Full corrected diagnosis: **`WINDOWS_CI_TICK_STALL.md`** (see the RESOLVED block at its top).

## Pointers
- Build recipe + CI: `.github/workflows/windows-dyn-build.yml` (push to `feat/dynamic-contracts` runs it).
- Dynamic-contract design: `src/extensions/DYNAMIC_CONTRACTS.md`, `WASM_CONTRACTS.md`.
- Linux/macOS node build (reference flags): `.github/workflows/qinit-release.yml`.

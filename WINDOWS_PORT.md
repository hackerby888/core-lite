# Windows (MSVC) port of the dynamic-contract node — continuation guide

Goal: build + run the **testnet dynamic/wasm node** on Windows with MSVC. This is the qubic-core-lite
fork (OS-process port). The dynamic-contract feature lives on branch **`feat/dynamic-contracts`** and is
wired through **CMake** (the mainnet `Qubic.sln` does NOT have the WAMR/libffi/dynamic glue). So Windows
builds via **cmake + the Visual Studio generator**, not the .sln.

All fixes so far are committed + pushed to `feat/dynamic-contracts` (remote **hackerby888**, NOT qubic
upstream). A fresh clone already has them — just continue from the latest MSVC error.

## How to build locally (fast loop — beats the CI by ~100×)

Prereqs: Visual Studio 2022 (Desktop C++ workload → MSVC `cl`, `ml64`, CMake), Git, and vcpkg
(`git clone https://github.com/microsoft/vcpkg && .\vcpkg\bootstrap-vcpkg.bat`). nasm: leave it on PATH
(Strawberry Perl ships it) — blosc2's `project()` declares `ASM_NASM`, but our `.asm` are routed to ml64
(see fixes below).

```bat
cmake -S . -B build-win -G "Visual Studio 17 2022" -A x64 ^
  -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake ^
  -DBUILD_BINARY=ON -DBUILD_TESTS=OFF -DENABLE_AVX512=OFF -DUSE_SANITIZER=OFF ^
  -DTESTNET=ON -DTESTNET_LITE_RAM=ON -DTESTNET_PREFILL_QUS=ON ^
  -DLITE_DYNAMIC_CONTRACTS=ON -DLITE_WASM_CONTRACTS=ON -DCMAKE_NO_USE_SWAP=ON ^
  -DADDON_TX_STATUS_REQUEST=ON -DONLY_LOGGING=OFF ^
  -DFFI_LIB_SHARED=C:/vcpkg/installed/x64-windows/lib/ffi.lib ^
  -DFFI_INCLUDE_DIR=C:/vcpkg/installed/x64-windows/include
cmake --build build-win --config Release --target Qubic
```
- vcpkg is **manifest mode** (root `vcpkg.json`: fmt/jsoncpp/drogon/libffi); the toolchain auto-installs
  them. fmt/jsoncpp/drogon/blosc2/WAMR are also **FetchContent** (built from source) — only **libffi**
  is consumed from vcpkg.
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
- **`k12_engine.h` is Linux-only** (userfaultfd). On Windows the adapter (`lite_sc_engine_adapter.h`)
  uses `LITE_SC_CONTRACT_LEVEL` → demand-zero `qVirtualAlloc` (VirtualAlloc) contract state. The
  free-mismatch fixes (`liteSCOnWasmTakeover`, `liteSCFree`, `freePoolOrVirtual`) already route the
  Windows free path correctly — don't `freePool` a `qVirtualAlloc`/memfd pointer.
- Commit to `feat/dynamic-contracts`; push to **hackerby888**. Short commit msgs, no AI attribution.

## Done so far (build-system layer — the deps, asm, link, no-Linux-isms)
| iter | error | fix |
|---|---|---|
| 3 | configure ok; build @ **libbacktrace** (autotools `./configure`+make) | guard the libbacktrace ExternalProject `if(NOT APPLE AND NOT WIN32)` (Windows uses boost windbg backend) |
| 4 | Windows fell into the **Linux link branch** (`-Wl,-Bstatic`, `dl`/`rt`) | add an `elseif(WIN32)` `target_link_libraries` (FetchContent targets, no backtrace, no GNU-ld/unix libs) |
| 5–10 | **`.asm` → nasm `-fwin32`** `MSB3721` (EDK2 SetJump/LongJump, WAMR `invokeNative_em64.asm` are MASM syntax) | nasm (Strawberry) on PATH makes CMake route `.asm` to nasm; can't drop nasm (blosc2's `project()` needs `ASM_NASM`). Force `LANGUAGE ASM_MASM`: EDK2 in `lib/platform_common/CMakeLists.txt`; **WAMR** per-file in `src/CMakeLists.txt` at the `vmlib` target (`foreach .asm → ASM_MASM`). |
| 11 | **`custom_stack.nasm`** ran `$NASM_EXECUTABLE -f elf64` (unset on Win + Linux format) → `'-f' not recognized` | it's deadcode (setjmp used); add `elseif(WIN32)` to use the same no-op stub ARM uses (`custom_stack_arm_stub.cpp`) |

Milestones reached: vcpkg deps + WAMR(windows) + libffi **configure ✓**; `platform_common`, `platform_efi`,
**`vmlib` (WAMR)** all **build ✓**. The whole build-system layer is essentially done.

## Next: `qubic.cpp` (the heavy part — do this on the Windows PC)
`qubic.cpp` is a single ~10k-line TU `#include`-ing the whole node + every `extensions/*.h`. It has never
compiled on MSVC. Expect a run of MSVC errors — Linux/POSIX-isms and GCC/clang intrinsics — each a small
guarded fix. Likely categories:
- **POSIX headers/calls**: `sys/socket.h`, `unistd.h`, `arpa/inet.h`, `pthread`, `mmap`/`munmap`,
  `sysconf` → Winsock2 / Windows equivalents. Much is already shimmed in `src/extensions/overload.h`
  (it has `_MSC_VER` branches: `qVirtualAlloc`=VirtualAlloc, etc.) — extend it.
- **Intrinsics**: `__builtin_*`, `__int128`, `__attribute__`, GCC/clang `_mm_*` usage; MSVC has `<intrin.h>`
  equivalents. Some live in `lib/platform_common` / `qintrin.h`.
- **`#pragma`/alignment/`__attribute__((packed))`** → MSVC `#pragma pack`.
- **boost::stacktrace** on Windows uses the windbg backend (needs `dbgeng.lib`/`ole32` linked).
- **drogon/Winsock init** for the RPC server.

Method: `cmake --build build-win --config Release --target Qubic` → fix the FIRST `error C####`/`LNK` →
repeat. Keep every change `#ifdef _WIN32`-guarded.

## Phase 2 — boot + tick (after it links)
```bat
mkdir run & cd run
..\build-win\src\Release\Qubic.exe --peers 127.0.0.1 --node-mode 3 --ticking-delay 1000
```
Confirm `GET http://127.0.0.1:41841/live/v1/tick-info` advances. Watch for Winsock/file-IO/threading
issues + the `VirtualAlloc`/`freePoolOrVirtual` paths. RAM target ~1.9 GB (demand-zero contract state).

## Phase 3 — deploy + exec (cross-platform consensus)
- Make qinit's `node-ops` cross-platform: `taskkill /IM Qubic.exe` / `tasklist` instead of `pkill`/`pgrep`
  (Qinit repo: `packages/cli/src/node-ops.ts`).
- `qinit test --bin build-win\src\Release\Qubic.exe --core <core> --skip-verify` → deploy + Get/Inc/Get.
- Then add **`windows-x64`** to the qinit-release smoke + **digest-equivalence** matrix
  (`GET /live/v1/dev/contract-digest?slot=N` must byte-match linux/macOS) → 4-way consensus proof.

## Pointers
- Build recipe + CI: `.github/workflows/windows-dyn-build.yml` (push to `feat/dynamic-contracts` runs it).
- Dynamic-contract design: `src/extensions/DYNAMIC_CONTRACTS.md`, `WASM_CONTRACTS.md`.
- Linux/macOS node build (reference flags): `.github/workflows/qinit-release.yml`.

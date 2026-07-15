#!/usr/bin/env bash
# qinit core-header snapshot: clang -M closure ∪ all contracts ∪ contract_def.h ∪ lite_contract_calls.h.
# Output <out>/core-headers.tar.gz (+ .sha256); layout mirrors the repo so -I resolves 1:1.
set -euo pipefail

CORE="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-$CORE/dist-snapshot}"
CLANG="${CLANG:-clang++-18}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cat > "$TMP/Stub.h" <<'EOF'
using namespace QPI;
struct CONTRACT_STATE2_TYPE {};
struct CONTRACT_STATE_TYPE : public ContractBase {
  struct StateData { uint64 x; };
  struct G_input {}; struct G_output { uint64 v; };
  PUBLIC_FUNCTION(G) { output.v = state.get().x; }
  REGISTER_USER_FUNCTIONS_AND_PROCEDURES() { REGISTER_USER_FUNCTION(G, 1); }
  INITIALIZE() { state.mut().x = 0; }
};
EOF

# Mirror qinit packages/build/src/recipe.ts genWrapper so the -M closure matches a real .so build.
cat > "$TMP/Stub.wrapper.cpp" <<EOF
#define NO_UEFI
#include <cstdint>
#include <cstddef>
#include <cstring>
#include <cstdlib>
#include <string>
#include <type_traits>
#include <utility>
#include <array>
#include <limits>
#define LITE_DYN_SO_BUILD
#include "contract_core/pre_qpi_def.h"
#include "contracts/qpi.h"
#include "contract_core/qpi_proposal_voting.h"
#include "oracle_core/oracle_interfaces_def.h"
#define CONTRACT_INDEX 28
#define CONTRACT_STATE_TYPE Stub
#define CONTRACT_STATE2_TYPE Stub2
#include "$TMP/Stub.h"
#include "contract_core/qpi_collection_impl.h"
#include "contract_core/qpi_linked_list_impl.h"
#define __acquireScratchpad __lite_cb_acquireScratchpad_unused
#define __releaseScratchpad __lite_cb_releaseScratchpad_unused
#include "contract_core/qpi_hash_map_impl.h"
#undef __acquireScratchpad
#undef __releaseScratchpad
#include "extensions/wasm/lite_dyn_abi.h"
EOF

SNAP="$TMP/core-headers"
mkdir -p "$SNAP"
copy() { local f="$1"; [ -f "$f" ] || return 0; local rel; rel="$(realpath --relative-to="$CORE" "$f")"; case "$rel" in ../*) return 0;; esac; mkdir -p "$SNAP/$(dirname "$rel")"; cp "$f" "$SNAP/$rel"; }

# clang -M => every header the compile touches; keep those under the repo.
DEPS="$("$CLANG" -std=c++20 -fPIC -mavx2 -I"$CORE" -I"$CORE/src" -M "$TMP/Stub.wrapper.cpp" | tr ' \\' '\n\n' | grep "^$CORE/" || true)"
for f in $DEPS; do copy "$f"; done

# WASM closure: contracts are compiled TO wasm, which pulls headers the native (.so, -mavx2) wrapper never
# references — lite_wasm_tu.h (swapped in for lite_dyn_abi.h), the force -include'd lite_wasm_intrinsics.h, and
# the simde/x86 m256i headers the wasm path takes (native uses real SSE). Compute that closure with the real
# wasm target+sysroot and add it, or the cached snapshot fails: 'lite_wasm_intrinsics.h file not found'.
SHIM="$CORE/src/extensions/wasm/lite_wasm_intrinsics.h"
sed -e 's|#define LITE_DYN_SO_BUILD|#define LITE_WASM_TU_BUILD|' \
    -e 's|#include "extensions/wasm/lite_dyn_abi.h"|#include "extensions/wasm/lite_wasm_tu.h"|' \
    "$TMP/Stub.wrapper.cpp" > "$TMP/Stub.wasm.wrapper.cpp"
if [ -n "${WASM_CLANG:-}" ]; then
  WDEPS="$("$WASM_CLANG" --target=wasm32-wasi -std=c++20 -fno-exceptions -fno-rtti \
    ${WASI_SYSROOT:+--sysroot="$WASI_SYSROOT"} -include "$SHIM" -I"$CORE" -I"$CORE/src" \
    -M "$TMP/Stub.wasm.wrapper.cpp" | tr ' \\' '\n\n' | grep "^$CORE/" || true)"
  for f in $WDEPS; do copy "$f"; done
else
  echo "WARN: WASM_CLANG unset — wasm header closure (simde / lite_wasm_*) NOT captured; snapshot incomplete for wasm" >&2
fi
copy "$SHIM"                                       # -include'd by the wasm compile
copy "$CORE/src/extensions/wasm/lite_wasm_tu.h"         # wasm TU binding (swapped in)

# Inter-contract: every contract header (callee types), the index map, the call-macro header.
for f in "$CORE"/src/contracts/*.h; do copy "$f"; done
copy "$CORE/src/contract_core/contract_def.h"
copy "$CORE/src/extensions/wasm/lite_contract_calls.h"

mkdir -p "$OUT"
tar czf "$OUT/core-headers.tar.gz" -C "$SNAP" .
sha256sum "$OUT/core-headers.tar.gz" | awk '{print $1}' > "$OUT/core-headers.sha256"
echo "snapshot: $OUT/core-headers.tar.gz ($(find "$SNAP" -type f | wc -l) files, sha256 $(cat "$OUT/core-headers.sha256"))"

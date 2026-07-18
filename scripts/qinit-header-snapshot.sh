#!/usr/bin/env bash
# qinit core-header snapshot: native/Wasm clang -M closures plus contracts and inter-contract support.
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

# Mirror qinit packages/build/src/recipe.ts native wrapper directly.
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
#define WASM_NATIVE_TU_BUILD
#include "contract_core/pre_qpi_def.h"
#include "contracts/qpi.h"
#include "contracts/math_lib.h"
#include "contract_core/qpi_proposal_voting.h"
#include "oracle_core/oracle_interfaces_def.h"
#define CONTRACT_INDEX 28
#define Stub_CONTRACT_INDEX 28
#define CONTRACT_STATE_TYPE Stub
#define CONTRACT_STATE2_TYPE Stub2
#include "extensions/wasm/sdk/intercontract_calls.h"
#include "$TMP/Stub.h"
#include "contract_core/qpi_collection_impl.h"
#include "contract_core/qpi_linked_list_impl.h"
#define __acquireScratchpad __wasm_native_cb_acquireScratchpad_unused
#define __releaseScratchpad __wasm_native_cb_releaseScratchpad_unused
#include "contract_core/qpi_hash_map_impl.h"
#undef __acquireScratchpad
#undef __releaseScratchpad
#include "extensions/wasm/shared/abi_types.h"
EOF

# Mirror the Wasm target directly. Keep this independent from the native wrapper so target-specific
# headers cannot drift through include-string replacement.
cat > "$TMP/Stub.wasm.wrapper.cpp" <<EOF
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
#define LITE_WASM_TU_BUILD
#include "contract_core/pre_qpi_def.h"
#include "contracts/qpi.h"
#include "contracts/math_lib.h"
#include "contract_core/qpi_proposal_voting.h"
#include "oracle_core/oracle_interfaces_def.h"
#define CONTRACT_INDEX 28
#define Stub_CONTRACT_INDEX 28
#define CONTRACT_STATE_TYPE Stub
#define CONTRACT_STATE2_TYPE Stub2
#include "extensions/wasm/sdk/intercontract_calls.h"
#include "extensions/wasm/sdk/qpi_support.h"
#include "$TMP/Stub.h"
#include "contract_core/qpi_collection_impl.h"
#include "contract_core/qpi_linked_list_impl.h"
#define __acquireScratchpad __wasm_native_cb_acquireScratchpad_unused
#define __releaseScratchpad __wasm_native_cb_releaseScratchpad_unused
#include "contract_core/qpi_hash_map_impl.h"
#undef __acquireScratchpad
#undef __releaseScratchpad
#include "extensions/wasm/sdk/module_runtime.h"
EOF

SNAP="$TMP/core-headers"
mkdir -p "$SNAP"
copy() { local f="$1"; [ -f "$f" ] || return 0; local rel; rel="$(realpath --relative-to="$CORE" "$f")"; case "$rel" in ../*) return 0;; esac; mkdir -p "$SNAP/$(dirname "$rel")"; cp "$f" "$SNAP/$rel"; }

# clang -M => every header the compile touches; keep those under the repo.
DEPS="$("$CLANG" -std=c++20 -fPIC -mavx2 -I"$CORE" -I"$CORE/src" -M "$TMP/Stub.wrapper.cpp" | tr ' \\' '\n\n' | grep "^$CORE/" || true)"
for f in $DEPS; do copy "$f"; done

# Wasm contracts also pull the SDK runtime, platform shim, and simde/x86 m256i headers. Compute that
# closure with the real target and sysroot.
SHIM="$CORE/src/extensions/wasm/sdk/platform_intrinsics.h"
if [ -n "${WASM_CLANG:-}" ]; then
  WDEPS="$("$WASM_CLANG" --target=wasm32-wasi -std=c++20 -fno-exceptions -fno-rtti \
    ${WASI_SYSROOT:+--sysroot="$WASI_SYSROOT"} -include "$SHIM" -I"$CORE" -I"$CORE/src" \
    -M "$TMP/Stub.wasm.wrapper.cpp" | tr ' \\' '\n\n' | grep "^$CORE/" || true)"
  for f in $WDEPS; do copy "$f"; done
else
  echo "WARN: WASM_CLANG unset — Wasm SDK header closure not captured; snapshot incomplete for Wasm" >&2
fi
copy "$SHIM" # force-included by the Wasm compile

# Inter-contract: every contract header (callee types), the index map, the call-macro header.
for f in "$CORE"/src/contracts/*.h; do copy "$f"; done
copy "$CORE/src/contract_core/contract_def.h"
copy "$CORE/src/extensions/wasm/sdk/intercontract_calls.h"

mkdir -p "$OUT"
tar czf "$OUT/core-headers.tar.gz" -C "$SNAP" .
sha256sum "$OUT/core-headers.tar.gz" | awk '{print $1}' > "$OUT/core-headers.sha256"
echo "snapshot: $OUT/core-headers.tar.gz ($(find "$SNAP" -type f | wc -l) files, sha256 $(cat "$OUT/core-headers.sha256"))"

#!/usr/bin/env bash
# Build qinit's native and Wasm core-header closure.
set -euo pipefail

CORE_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUTPUT_DIR="${1:-$CORE_ROOT/dist-snapshot}"
CLANG="${CLANG:-clang++-18}"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

cat > "$TEMP_DIR/Stub.h" <<'EOF'
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

# Mirror qinit's native build wrapper.
cat > "$TEMP_DIR/Stub.wrapper.cpp" <<EOF
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
#include "$TEMP_DIR/Stub.h"
#include "contract_core/qpi_collection_impl.h"
#include "contract_core/qpi_linked_list_impl.h"
#define __acquireScratchpad __wasm_native_cb_acquireScratchpad_unused
#define __releaseScratchpad __wasm_native_cb_releaseScratchpad_unused
#include "contract_core/qpi_hash_map_impl.h"
#undef __acquireScratchpad
#undef __releaseScratchpad
#include "extensions/wasm/shared/abi_types.h"
EOF

# Keep the Wasm wrapper independent from native include substitutions.
cat > "$TEMP_DIR/Stub.wasm.wrapper.cpp" <<EOF
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
#include "$TEMP_DIR/Stub.h"
#include "contract_core/qpi_collection_impl.h"
#include "contract_core/qpi_linked_list_impl.h"
#define __acquireScratchpad __wasm_native_cb_acquireScratchpad_unused
#define __releaseScratchpad __wasm_native_cb_releaseScratchpad_unused
#include "contract_core/qpi_hash_map_impl.h"
#undef __acquireScratchpad
#undef __releaseScratchpad
#include "extensions/wasm/sdk/module_runtime.h"
EOF

SNAPSHOT_DIR="$TEMP_DIR/core-headers"
mkdir -p "$SNAPSHOT_DIR"

copy_header()
{
  local source_file="$1"
  if [ ! -f "$source_file" ]; then
    return 0
  fi

  local relative_path
  relative_path="$(realpath --relative-to="$CORE_ROOT" "$source_file")"
  case "$relative_path" in
    ../*) return 0 ;;
  esac

  mkdir -p "$SNAPSHOT_DIR/$(dirname "$relative_path")"
  cp "$source_file" "$SNAPSHOT_DIR/$relative_path"
}

# Keep every repository header reached by the native wrapper.
NATIVE_DEPS="$("$CLANG" -std=c++20 -fPIC -mavx2 \
  -I"$CORE_ROOT" \
  -I"$CORE_ROOT/src" \
  -M "$TEMP_DIR/Stub.wrapper.cpp" \
  | tr ' \\' '\n\n' \
  | grep "^$CORE_ROOT/" || true)"
for source_file in $NATIVE_DEPS; do
  copy_header "$source_file"
done

# Resolve the Wasm closure with its real target and sysroot.
PLATFORM_SHIM="$CORE_ROOT/src/extensions/wasm/sdk/platform_intrinsics.h"
if [ -n "${WASM_CLANG:-}" ]; then
  WASM_DEPS="$("$WASM_CLANG" --target=wasm32-wasi -std=c++20 \
    -fno-exceptions \
    -fno-rtti \
    ${WASI_SYSROOT:+--sysroot="$WASI_SYSROOT"} \
    -include "$PLATFORM_SHIM" \
    -I"$CORE_ROOT" \
    -I"$CORE_ROOT/src" \
    -M "$TEMP_DIR/Stub.wasm.wrapper.cpp" \
    | tr ' \\' '\n\n' \
    | grep "^$CORE_ROOT/" || true)"
  for source_file in $WASM_DEPS; do
    copy_header "$source_file"
  done
else
  echo "WARN: WASM_CLANG unset — Wasm SDK header closure not captured; snapshot incomplete for Wasm" >&2
fi
copy_header "$PLATFORM_SHIM"

# Include contract types, the slot map, and inter-contract call macros.
for source_file in "$CORE_ROOT"/src/contracts/*.h; do
  copy_header "$source_file"
done
copy_header "$CORE_ROOT/src/contract_core/contract_def.h"
copy_header "$CORE_ROOT/src/extensions/wasm/sdk/intercontract_calls.h"

mkdir -p "$OUTPUT_DIR"
tar czf "$OUTPUT_DIR/core-headers.tar.gz" -C "$SNAPSHOT_DIR" .
sha256sum "$OUTPUT_DIR/core-headers.tar.gz" \
  | awk '{print $1}' > "$OUTPUT_DIR/core-headers.sha256"

HEADER_COUNT=$(find "$SNAPSHOT_DIR" -type f | wc -l)
SNAPSHOT_SHA=$(cat "$OUTPUT_DIR/core-headers.sha256")
echo "snapshot: $OUTPUT_DIR/core-headers.tar.gz ($HEADER_COUNT files, sha256 $SNAPSHOT_SHA)"

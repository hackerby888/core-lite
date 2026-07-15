# Wasm smart-contract extension

The extension is enabled with one feature switch and two explicit prerequisites:

```sh
cmake -S . -B build-node \
  -DTESTNET=ON \
  -DTESTNET_LITE_RAM=ON \
  -DLITE_WASM_SC=ON
```

CMake rejects `LITE_WASM_SC` without either prerequisite. WAMR always uses the classic interpreter with call-stack capture, and a build without the switch does not link WAMR or libffi.

The main files are:

- `lite_dynamic_contracts.h`: deployment wire decoding and host-service adapters.
- `lite_wasm_contracts.h`: WAMR loading, state takeover, registration, dispatch, and migration.
- `lite_wasm_imports.h`: the stable `"lhost"` import table.
- `lite_wasm_tu.h`: contract-side imports, exports, registration, and dispatch.
- `lite_wasm_debug.h`: trace-ring, trap, and state-diff support.

`LITE_WASM_TU_BUILD` remains the contract-side compilation guard. `LITE_SC_ENGINE`, `LITE_SC_CONTRACT_LEVEL`, and `LITE_SC_NO_ENGINE` are internal state-backend controls, not user-facing Wasm feature switches.

The `"lhost"` names and signatures, module exports, deployment transaction layout, state layout, libffi call shapes, migration order, and registration order are compatibility boundaries. Changes to them require an explicit ABI or wire-format migration.

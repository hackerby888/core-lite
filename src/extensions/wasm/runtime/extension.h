#pragma once

// Single node include for the Wasm smart-contract extension.
#ifdef LITE_WASM_SC

#include "extensions/wasm/shared/abi_metadata.h"
#include "extensions/wasm/shared/abi_types.h"
#include "extensions/wasm/runtime/state_backend.h"
#include "extensions/wasm/runtime/arena_scope.h"
#include "extensions/wasm/runtime/trace.h"
#include "extensions/wasm/runtime/state_write_tracker.h"
#include "extensions/wasm/runtime/oracle_services.h"
#include "extensions/wasm/runtime/qpi_services.h"
#include "extensions/wasm/runtime/host_services.h"
#include "extensions/wasm/runtime/contract_slots.h"
#include "extensions/wasm/runtime/deployment_protocol.h"
#include "extensions/wasm/runtime/lhost_registry.h"
#include "extensions/wasm/runtime/engine_state.h"
#include "extensions/wasm/runtime/dispatch.h"
#include "extensions/wasm/runtime/module_loader.h"
#include "extensions/wasm/runtime/registration.h"
#include "extensions/wasm/runtime/engine.h"
#include "extensions/wasm/runtime/deployment.h"

#endif // LITE_WASM_SC

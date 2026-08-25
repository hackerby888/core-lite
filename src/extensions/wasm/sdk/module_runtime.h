#pragma once

// Final post-contract include for a Qinit Wasm module.
#ifdef LITE_WASM_TU_BUILD

#include "extensions/wasm/sdk/lhost_imports.h"
#include "extensions/wasm/sdk/qpi_forwarders.h"
#include "extensions/wasm/sdk/registration.h"
#include "extensions/wasm/sdk/module_storage.h"
#include "extensions/wasm/sdk/dispatch.h"

#undef LH_IMPORT
#undef LH_EXPORT

#endif // LITE_WASM_TU_BUILD

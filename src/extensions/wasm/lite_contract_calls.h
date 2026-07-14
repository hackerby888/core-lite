#pragma once
// These source-compatible macros dispatch through the callee's deployed host table.
// Qinit supplies each generated input-type constant before including contract source.
#if defined(LITE_WASM_TU_BUILD)

int liteCallFunction(
    const void* callerContext,
    unsigned int calleeIndex,
    unsigned short inputType,
    const void* input,
    unsigned int inputSize,
    void* output,
    unsigned int outputSize);
int liteInvokeProcedure(
    const void* callerContext,
    unsigned int calleeIndex,
    unsigned short inputType,
    const void* input,
    unsigned int inputSize,
    void* output,
    unsigned int outputSize,
    long long invocationReward);

// Calls remain restricted to lower-index contracts.
#undef CALL_OTHER_CONTRACT_FUNCTION_E
#define CALL_OTHER_CONTRACT_FUNCTION_E(contractStateType, function, input, output, errorVar) \
    static_assert(contractStateType::__contract_index < CONTRACT_INDEX, "lite: can only call a lower-index contract"); \
    QPI::InterContractCallError errorVar = (QPI::InterContractCallError)liteCallFunction( \
        &qpi, contractStateType::__contract_index, contractStateType##_##function##_inputType, \
        &(input), sizeof(input), &(output), sizeof(output))

#undef CALL_OTHER_CONTRACT_FUNCTION
#define CALL_OTHER_CONTRACT_FUNCTION(contractStateType, function, input, output) \
    CALL_OTHER_CONTRACT_FUNCTION_E(contractStateType, function, input, output, interContractCallError)

#undef INVOKE_OTHER_CONTRACT_PROCEDURE_E
#define INVOKE_OTHER_CONTRACT_PROCEDURE_E(contractStateType, procedure, input, output, invocationReward, errorVar) \
    static_assert(contractStateType::__contract_index < CONTRACT_INDEX, "lite: can only call a lower-index contract"); \
    QPI::InterContractCallError errorVar = (QPI::InterContractCallError)liteInvokeProcedure( \
        &qpi, contractStateType::__contract_index, contractStateType##_##procedure##_inputType, \
        &(input), sizeof(input), &(output), sizeof(output), (invocationReward))

#undef INVOKE_OTHER_CONTRACT_PROCEDURE
#define INVOKE_OTHER_CONTRACT_PROCEDURE(contractStateType, procedure, input, output, invocationReward) \
    INVOKE_OTHER_CONTRACT_PROCEDURE_E(contractStateType, procedure, input, output, invocationReward, interContractCallError)

#endif // LITE_WASM_TU_BUILD

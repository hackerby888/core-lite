#pragma once

constexpr unsigned int CONTRACT_INVOCATION_BUFFER_ALIGNMENT = 8;

struct ContractInvocationBufferLayout
{
    unsigned int outputOffset;
    unsigned int localsOffset;
    unsigned int totalSize;
};

constexpr unsigned int alignContractInvocationBufferOffset(unsigned int offset)
{
    return (offset + CONTRACT_INVOCATION_BUFFER_ALIGNMENT - 1) & ~(CONTRACT_INVOCATION_BUFFER_ALIGNMENT - 1);
}

constexpr ContractInvocationBufferLayout contractInvocationBufferLayout(unsigned int inputSize, unsigned int outputSize, unsigned int localsSize)
{
    const unsigned int outputOffset = alignContractInvocationBufferOffset(inputSize);
    const unsigned int localsOffset = alignContractInvocationBufferOffset(outputOffset + outputSize);
    return { outputOffset, localsOffset, localsOffset + localsSize };
}

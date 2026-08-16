#define NO_UEFI

#include "contract_testing.h"

#include <cstdint>

namespace
{

constexpr unsigned int contractIndex = 0;
constexpr unsigned short inputType = 1;
constexpr unsigned char inputValue = 0x5A;
constexpr unsigned char outputValue = 0xA5;

struct AlignmentProbe
{
    unsigned int inputRemainder;
    unsigned int outputRemainder;
    unsigned int localsRemainder;
    bool inputCopied;
    bool outputZeroed;
    bool localsZeroed;
};

AlignmentProbe probe;

unsigned int alignmentRemainder(const void* buffer)
{
    return static_cast<unsigned int>(reinterpret_cast<std::uintptr_t>(buffer) % CONTRACT_INVOCATION_BUFFER_ALIGNMENT);
}

void recordProbe(void* input, void* output, void* locals)
{
    probe.inputRemainder = alignmentRemainder(input);
    probe.outputRemainder = alignmentRemainder(output);
    probe.localsRemainder = alignmentRemainder(locals);
    probe.inputCopied = *static_cast<unsigned char*>(input) == inputValue;
    probe.outputZeroed = *static_cast<unsigned char*>(output) == 0;
    probe.localsZeroed = *static_cast<unsigned char*>(locals) == 0;
    *static_cast<unsigned char*>(output) = outputValue;
}

void probeFunction(const QPI::QpiContextFunctionCall&, void*, void* input, void* output, void* locals)
{
    recordProbe(input, output, locals);
}

void probeProcedure(const QPI::QpiContextProcedureCall&, void*, void* input, void* output, void* locals)
{
    recordProbe(input, output, locals);
}

void probeNotification(const QPI::QpiContextProcedureCall&, void*, void* input, void*, void* locals)
{
    probe.inputRemainder = alignmentRemainder(input);
    probe.localsRemainder = alignmentRemainder(locals);
    probe.inputCopied = *static_cast<unsigned char*>(input) == inputValue;
    probe.localsZeroed = *static_cast<unsigned char*>(locals) == 0;
}

void expectAlignedFunctionOrProcedure()
{
    EXPECT_EQ(probe.inputRemainder, 0);
    EXPECT_EQ(probe.outputRemainder, 0);
    EXPECT_EQ(probe.localsRemainder, 0);
    EXPECT_TRUE(probe.inputCopied);
    EXPECT_TRUE(probe.outputZeroed);
    EXPECT_TRUE(probe.localsZeroed);
}

} // namespace

TEST(ContractInvocationAlignment, AlignsNativeFunctionProcedureAndNotificationBuffers)
{
    constexpr ContractInvocationBufferLayout layout = contractInvocationBufferLayout(1, 1, 1);
    static_assert(layout.outputOffset == 8);
    static_assert(layout.localsOffset == 16);
    static_assert(layout.totalSize == 17);

    ContractTesting environment;

    contractUserFunctions[contractIndex][inputType] = probeFunction;
    contractUserFunctionInputSizes[contractIndex][inputType] = 1;
    contractUserFunctionOutputSizes[contractIndex][inputType] = 1;
    contractUserFunctionLocalsSizes[contractIndex][inputType] = 1;

    QpiContextUserFunctionCall functionContext(contractIndex);
    EXPECT_EQ(functionContext.call(inputType, &inputValue, sizeof(inputValue)), NoContractError);
    expectAlignedFunctionOrProcedure();
    ASSERT_NE(functionContext.outputBuffer, nullptr);
    EXPECT_EQ(*reinterpret_cast<unsigned char*>(functionContext.outputBuffer), outputValue);
    functionContext.freeBuffer();

    probe = {};
    contractUserProcedures[contractIndex][inputType] = probeProcedure;
    contractUserProcedureInputSizes[contractIndex][inputType] = 1;
    contractUserProcedureOutputSizes[contractIndex][inputType] = 1;
    contractUserProcedureLocalsSizes[contractIndex][inputType] = 1;

    QpiContextUserProcedureCall procedureContext(contractIndex, NULL_ID, 0);
    procedureContext.call(inputType, &inputValue, sizeof(inputValue));
    expectAlignedFunctionOrProcedure();
    ASSERT_NE(procedureContext.outputBuffer, nullptr);
    EXPECT_EQ(*reinterpret_cast<unsigned char*>(procedureContext.outputBuffer), outputValue);
    procedureContext.freeBuffer();

    probe = {};
    const UserProcedureRegistry::UserProcedureData notification{
        probeNotification,
        contractIndex,
        1,
        1,
        0,
    };
    QpiContextUserProcedureNotificationCall notificationContext(notification);
    notificationContext.call(&inputValue);

    EXPECT_EQ(probe.inputRemainder, 0);
    EXPECT_EQ(probe.localsRemainder, 0);
    EXPECT_TRUE(probe.inputCopied);
    EXPECT_TRUE(probe.localsZeroed);
}

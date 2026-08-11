// Minimal Counter fixture for the engine ABI tests.
typedef unsigned long long u64;
typedef unsigned int u32;

typedef struct
{
    u64 count;
    u64 sum;
} State;

static State g_state;
static unsigned char io_scratch[64 * 1024];

__attribute__((export_name("contract_index"))) u32 contract_index(void)
{
    return 29;
}

__attribute__((export_name("state_addr"))) u32 state_addr(void)
{
    return (u32)(unsigned long)&g_state;
}

__attribute__((export_name("state_size"))) u32 state_size(void)
{
    return (u32)sizeof(State);
}

__attribute__((export_name("io_base"))) u32 io_base(void)
{
    return (u32)(unsigned long)&io_scratch[0];
}

__attribute__((export_name("reg_count"))) u32 reg_count(void)
{
    return 2;
}

typedef struct
{
    u32 inputType;
    u32 kind;
    u32 inSize;
    u32 outSize;
} EntryInfo;

__attribute__((export_name("reg_info")))
void reg_info(u32 index, EntryInfo* output)
{
    if (index == 0)
    {
        output->inputType = 1;
        output->kind = 0;
        output->inSize = 0;
        output->outSize = 8;
    }
    if (index == 1)
    {
        output->inputType = 2;
        output->kind = 1;
        output->inSize = 8;
        output->outSize = 8;
    }
}

__attribute__((export_name("reg_sysproc_mask"))) u32 reg_sysproc_mask(void)
{
    return (1u << 0) | (1u << 9);
}

__attribute__((export_name("sysproc_in_size"))) u32 sysproc_in_size(u32 systemProcedure)
{
    return systemProcedure == 9 ? 8 : 0;
}

__attribute__((export_name("sysproc_out_size"))) u32 sysproc_out_size(u32 systemProcedure)
{
    (void)systemProcedure;
    return 0;
}

__attribute__((export_name("sysproc_locals_size"))) u32 sysproc_locals_size(u32 systemProcedure)
{
    (void)systemProcedure;
    return 0;
}

__attribute__((export_name("dispatch")))
void dispatch(
    u32 kind,
    u32 inputType,
    u32 inputOffset,
    u32 outputOffset,
    u32 localsOffset)
{
    (void)localsOffset;
    if (kind == 2)
    {
        if (inputType == 0)
        {
            g_state.count = 4242;
        }
        else if (inputType == 9)
        {
            g_state.sum = *(u64*)(unsigned long)inputOffset;
        }
        return;
    }
    if (inputType == 1)
    {
        *(u64*)(unsigned long)outputOffset = g_state.count;
    }
    else if (inputType == 2)
    {
        const u64 increment = *(u64*)(unsigned long)inputOffset;
        g_state.count++;
        g_state.sum += increment;
        *(u64*)(unsigned long)outputOffset = g_state.count;
    }
}

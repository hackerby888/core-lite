// Trap fixture used by the WAMR backtrace test.
typedef unsigned int u32;
typedef unsigned long long u64;

static u64 g_state;

__attribute__((noinline)) static u64 do_div(u64 dividend, u64 divisor)
{
    return dividend / divisor;
}

__attribute__((export_name("dispatch")))
void dispatch(u32 kind, u32 inputType, u32 inputOffset, u32 outputOffset, u32 localsOffset)
{
    (void)kind;
    (void)inputOffset;
    (void)outputOffset;
    (void)localsOffset;
    g_state = do_div(7, inputType);
}

__attribute__((export_name("state_addr"))) u32 state_addr(void)
{
    return (u32)(unsigned long)&g_state;
}

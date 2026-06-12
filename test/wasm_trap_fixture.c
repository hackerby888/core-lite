// gtest fixture: a contract whose dispatch() traps (divide by zero) at a known site, for the classic-interp
// trap-backtrace test (WasmContracts.TrapBacktrace). Regenerate:
//   $WASI_SDK/clang --target=wasm32 -g -O2 -nostdlib -Wl,--no-entry \
//     -Wl,--export=dispatch -Wl,--export=state_addr -o trap.wasm test/wasm_trap_fixture.c
//   $WASI_SDK/llvm-strip --strip-debug trap.wasm -o trap.s.wasm && xxd -i trap.s.wasm > test/wasm_trap_fixture.h
typedef unsigned int u32; typedef unsigned long long u64;
static u64 g_state;
__attribute__((noinline)) static u64 do_div(u64 a, u64 b){ return a / b; }   // TRAP site: div by zero
__attribute__((export_name("dispatch")))
void dispatch(u32 kind,u32 it,u32 inOff,u32 outOff,u32 localsOff){
  (void)kind;(void)inOff;(void)outOff;(void)localsOff;
  g_state = do_div(7, it);   // call with it=0 -> divide by zero -> trap (frames: do_div <- dispatch)
}
__attribute__((export_name("state_addr"))) u32 state_addr(void){ return (u32)(unsigned long)&g_state; }

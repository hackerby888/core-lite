// arm build: custom_stack.{nasm,asm} is x86-64 assembly and is DEADCODE on the OS port
// (contracts run on normal thread stacks; error recovery uses setjmp/longjmp in contract_exec.h).
// The x86 nasm can't link into an arm binary, so provide a never-called stub of its one symbol.
extern "C" void __customStackSetupAndRunFunc(void*, void*, void*) {}

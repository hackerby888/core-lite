// gtest fixture: minimal Counter exporting the engine's dispatch/reg/state ABI (no QPI imports needed).
typedef unsigned long long u64; typedef unsigned int u32;
typedef struct { u64 count, sum; } State;
static State g_state;
static unsigned char io_scratch[64*1024];
__attribute__((export_name("state_addr"))) u32 state_addr(void){return (u32)(unsigned long)&g_state;}
__attribute__((export_name("state_size"))) u32 state_size(void){return (u32)sizeof(State);}
__attribute__((export_name("io_base")))    u32 io_base(void){return (u32)(unsigned long)&io_scratch[0];}
__attribute__((export_name("reg_count")))  u32 reg_count(void){return 2;}
typedef struct { u32 inputType, kind, inSize, outSize; } EntryInfo;
__attribute__((export_name("reg_info")))
void reg_info(u32 i, EntryInfo* o){
  if(i==0){o->inputType=1;o->kind=0;o->inSize=0;o->outSize=8;}   // G  function
  if(i==1){o->inputType=2;o->kind=1;o->inSize=8;o->outSize=8;}   // INC procedure
}
// system procedures: INITIALIZE (sp 0, no IO) + POST_INCOMING_TRANSFER (sp 9, 8-byte input).
__attribute__((export_name("reg_sysproc_mask")))    u32 reg_sysproc_mask(void){return (1u<<0)|(1u<<9);}
__attribute__((export_name("sysproc_in_size")))     u32 sysproc_in_size(u32 sp){return sp==9?8:0;}
__attribute__((export_name("sysproc_out_size")))    u32 sysproc_out_size(u32 sp){(void)sp;return 0;}
__attribute__((export_name("sysproc_locals_size"))) u32 sysproc_locals_size(u32 sp){(void)sp;return 0;}
__attribute__((export_name("dispatch")))
void dispatch(u32 kind,u32 it,u32 inOff,u32 outOff,u32 localsOff){
  (void)localsOff;
  if(kind==2){ // system procedure (it = sp id)
    if(it==0){g_state.count=4242;}                              // INITIALIZE
    else if(it==9){g_state.sum=*(u64*)(unsigned long)inOff;}    // POST_INCOMING_TRANSFER (input marshalled)
    return;
  }
  if(it==1){*(u64*)(unsigned long)outOff=g_state.count;}
  else if(it==2){u64 by=*(u64*)(unsigned long)inOff; g_state.count++; g_state.sum+=by; *(u64*)(unsigned long)outOff=g_state.count;}
}

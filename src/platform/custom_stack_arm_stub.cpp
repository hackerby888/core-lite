// Provide the unused custom-stack symbol for host builds that use system threads.
extern "C" void __customStackSetupAndRunFunc(void*, void*, void*)
{
}

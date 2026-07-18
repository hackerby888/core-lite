// Provide the unused custom-stack symbol for non-x86 builds.
extern "C" void __customStackSetupAndRunFunc(void*, void*, void*)
{
}

cmake_minimum_required(VERSION 3.15)
project(qubic_core CXX ASM_NASM)

# GoogleTest requires at least C++20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(BUILD_SHARED_LIBS OFF CACHE BOOL "Force static libraries" FORCE)
if(NOT WIN32)
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a") # Only look for static archives (unix); Windows uses .lib/.dll
endif()

# Enable FetchContent module
include(FetchContent)
include(ExternalProject)

# Include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/lib/)

# macOS: brew installs deps (boost/zstd/lz4/lzo/openssl) under the Homebrew prefix, which clang does
# NOT search by default. Add them so the build finds the headers/libs. (IS_MACOS => Linux unchanged.)
if(IS_MACOS)
    if(EXISTS /opt/homebrew)            # Apple Silicon
        set(BREW_PREFIX /opt/homebrew)
    else()                              # Intel mac
        set(BREW_PREFIX /usr/local)
    endif()
    include_directories(SYSTEM ${BREW_PREFIX}/include ${BREW_PREFIX}/opt/openssl@3/include)
    link_directories(${BREW_PREFIX}/lib ${BREW_PREFIX}/opt/openssl@3/lib)
endif()

if(IS_ARM)
    # custom_stack.nasm is x86-64 asm + deadcode on the OS port (setjmp is used instead). Skip it on
    # arm and link a never-called stub symbol. Also expose the x86 <cpuid.h> shim for arm.
    include_directories(${CMAKE_SOURCE_DIR}/lib/platform_common/arm_shims)
    set(CUSTOM_STACK_OBJ ${CMAKE_CURRENT_SOURCE_DIR}/platform/custom_stack_arm_stub.cpp)
elseif(WIN32)
    # custom_stack.nasm is x86-64 ELF asm + deadcode on the OS port (setjmp is used). MSVC has no nasm and
    # -f elf64 is Linux-only; link the same never-called no-op stub (name says arm, but it's generic).
    set(CUSTOM_STACK_OBJ ${CMAKE_CURRENT_SOURCE_DIR}/platform/custom_stack_arm_stub.cpp)
else()
    set(ASM_FILE "${CMAKE_CURRENT_SOURCE_DIR}/platform/custom_stack.nasm")
    set(ASM_OBJ "${CMAKE_CURRENT_BINARY_DIR}/custom_stack.obj")
    add_custom_command(
        OUTPUT ${ASM_OBJ}
        COMMAND ${NASM_EXECUTABLE} -f elf64 ${ASM_FILE} -o ${ASM_OBJ}
        DEPENDS ${ASM_FILE}
        COMMENT "Assembling ${ASM_FILE}"
    )
    set(CUSTOM_STACK_OBJ ${ASM_OBJ})
endif()

# Extra libraries
set(JSONCPP_WITH_POST_INSTALL OFF CACHE BOOL "" FORCE)
set(JSONCPP_WITH_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
        jsoncpp
        GIT_REPOSITORY https://github.com/open-source-parsers/jsoncpp.git
        GIT_TAG 1.9.6
)
FetchContent_MakeAvailable(jsoncpp)
set(JSONCPP_FOUND TRUE CACHE BOOL "" FORCE)
set(Jsoncpp_FOUND TRUE CACHE BOOL "" FORCE)
set(JSONCPP_INCLUDE_DIRS "${jsoncpp_SOURCE_DIR}/include" CACHE PATH "" FORCE)
set(JSONCPP_INCLUDE_DIR "${jsoncpp_SOURCE_DIR}/include" CACHE PATH "" FORCE)
set(JSONCPP_LIBRARIES jsoncpp_static CACHE STRING "" FORCE)
if(NOT TARGET Jsoncpp::Jsoncpp)
    add_library(Jsoncpp::Jsoncpp ALIAS jsoncpp_static)
endif()
include_directories(${jsoncpp_SOURCE_DIR}/include)

set(BUILD_POSTGRESQL OFF CACHE BOOL "" FORCE)
set(BUILD_MYSQL OFF CACHE BOOL "" FORCE)
set(BUILD_SQLITE OFF CACHE BOOL "" FORCE)
set(BUILD_REDIS OFF CACHE BOOL "" FORCE)
set(BUILD_DROGON_SHARED OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
        drogon
        GIT_REPOSITORY https://github.com/drogonframework/drogon.git
        GIT_TAG v1.9.11
)
FetchContent_MakeAvailable(drogon)

FetchContent_Declare(
        fmt
        GIT_REPOSITORY https://github.com/fmtlib/fmt.git
        GIT_TAG 12.1.0
)
FetchContent_MakeAvailable(fmt)

# libbacktrace is the boost::stacktrace backend on Linux. macOS uses boost's execinfo backend and Windows
# uses the windbg/dbghelp backend, so skip the autotools (./configure + make) libbacktrace build on both.
if(NOT APPLE AND NOT WIN32)
set(LIBBACKTRACE_PREFIX ${CMAKE_BINARY_DIR}/libbacktrace)
set(LIBBACKTRACE_BINARY_DIR ${LIBBACKTRACE_PREFIX}/src/libbacktrace_project-build)
set(LIBBACKTRACE_STATIC_LIB ${LIBBACKTRACE_BINARY_DIR}/.libs/libbacktrace.a)
set(LIBBACKTRACE_INCLUDE_DIR ${LIBBACKTRACE_PREFIX}/src/libbacktrace_project/)
file(MAKE_DIRECTORY ${LIBBACKTRACE_INCLUDE_DIR})
ExternalProject_Add(libbacktrace_project
        GIT_REPOSITORY https://github.com/ianlancetaylor/libbacktrace.git
        GIT_TAG master
        PREFIX ${LIBBACKTRACE_PREFIX}
        BINARY_DIR ${LIBBACKTRACE_BINARY_DIR}

        UPDATE_COMMAND ""
        LOG_CONFIGURE 1    # Hides the "checking..." lines
        LOG_BUILD 1        # Hides the "libtool: compile..." lines
        LOG_DOWNLOAD 1     # Hides the Git clone progress
        LOG_INSTALL 1      # Hides the install step output

        # Run the Autotools configuration
        CONFIGURE_COMMAND <SOURCE_DIR>/configure --with-pic --enable-static --disable-shared
        # Build the project
        BUILD_COMMAND make -j${nproc}
        # We don't want to install it to /usr/local, keep it in our build folder
        INSTALL_COMMAND ""
        BUILD_BYPRODUCTS ${LIBBACKTRACE_STATIC_LIB}
)
add_library(backtrace_static STATIC IMPORTED)
set_target_properties(backtrace_static PROPERTIES
        IMPORTED_LOCATION ${LIBBACKTRACE_STATIC_LIB}
        INTERFACE_INCLUDE_DIRECTORIES ${LIBBACKTRACE_INCLUDE_DIR}
)
add_dependencies(backtrace_static libbacktrace_project)
include_directories(SYSTEM ${LIBBACKTRACE_INCLUDE_DIR})
endif() # NOT APPLE AND NOT WIN32 (libbacktrace)

# Define the shared library (matches Visual Studio DynamicLibrary configuration)
add_executable(
  Qubic
  qubic.cpp
  ${CUSTOM_STACK_OBJ}
)

# Explorer asset bundling: regen embedded header when HTML/CSS/JS change.
if(NOT WIN32)
    set(EXPLORER_ASSETS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/extensions/http/static/explorer)
    set(EXPLORER_ASSETS_OUT ${CMAKE_CURRENT_SOURCE_DIR}/extensions/http/controller/explorer_assets.generated.h)
    add_custom_command(
        OUTPUT  ${EXPLORER_ASSETS_OUT}
        DEPENDS ${EXPLORER_ASSETS_DIR}/index.html
                ${EXPLORER_ASSETS_DIR}/style.css
                ${EXPLORER_ASSETS_DIR}/app.js
                ${EXPLORER_ASSETS_DIR}/regen.sh
        COMMAND bash ${EXPLORER_ASSETS_DIR}/regen.sh
        COMMENT "Regenerating explorer embedded assets header"
        VERBATIM
    )
    add_custom_target(explorer_assets_gen DEPENDS ${EXPLORER_ASSETS_OUT})
    add_dependencies(Qubic explorer_assets_gen)
else()
    # Windows: no bash — regen.cmake produces the identical header via `cmake -P`.
    set(EXPLORER_ASSETS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/extensions/http/static/explorer)
    set(EXPLORER_ASSETS_OUT ${CMAKE_CURRENT_SOURCE_DIR}/extensions/http/controller/explorer_assets.generated.h)
    add_custom_command(
        OUTPUT  ${EXPLORER_ASSETS_OUT}
        DEPENDS ${EXPLORER_ASSETS_DIR}/index.html
                ${EXPLORER_ASSETS_DIR}/style.css
                ${EXPLORER_ASSETS_DIR}/app.js
                ${EXPLORER_ASSETS_DIR}/regen.cmake
        COMMAND ${CMAKE_COMMAND} -DSRC=${EXPLORER_ASSETS_DIR} -DOUT=${EXPLORER_ASSETS_OUT} -P ${EXPLORER_ASSETS_DIR}/regen.cmake
        COMMENT "Regenerating explorer embedded assets header (cmake -P)"
        VERBATIM
    )
    add_custom_target(explorer_assets_gen DEPENDS ${EXPLORER_ASSETS_OUT})
    add_dependencies(Qubic explorer_assets_gen)
endif()

if(TESTNET)
    target_compile_definitions(Qubic PRIVATE TESTNET)
endif()

if(ONLY_LOGGING)
    target_compile_definitions(Qubic PRIVATE ONLY_LOGGING)
endif()

if(CMAKE_NO_USE_SWAP)
    target_compile_definitions(Qubic PRIVATE CMAKE_NO_USE_SWAP)
endif()

if(NO_RPC)
    target_compile_definitions(Qubic PRIVATE NO_RPC)
endif()

if(NO_ENABLE_QUBIC_LOGGING_EVENT)
    target_compile_definitions(Qubic PRIVATE NO_ENABLE_QUBIC_LOGGING_EVENT)
endif()

if(TESTNET)
    target_compile_definitions(Qubic PRIVATE TESTNET)
endif()

if(TESTNET_PREFILL_QUS)
    target_compile_definitions(Qubic PRIVATE TESTNET_PREFILL_QUS)
endif()

if(TESTNET_LITE_RAM)
    target_compile_definitions(Qubic PRIVATE TESTNET_LITE_RAM)
endif()

# Wasm smart contracts embed WAMR and libffi. The root CMakeLists enforces the testnet-lite-RAM prerequisites.
if(LITE_WASM_SC)
    target_compile_definitions(Qubic PRIVATE LITE_WASM_SC)
    enable_language(C)

    # WAMR_ROOT_DIR is fetched and SHA-pinned by the root CMakeLists.
    if(APPLE)
        set(WAMR_BUILD_PLATFORM "darwin")
    elseif(WIN32)
        set(WAMR_BUILD_PLATFORM "windows")
    else()
        set(WAMR_BUILD_PLATFORM "linux")
    endif()
    if(IS_ARM)
        set(WAMR_BUILD_TARGET "AARCH64")
    else()
        set(WAMR_BUILD_TARGET "X86_64")
    endif()
    set(WAMR_BUILD_INTERP 1)
    set(WAMR_BUILD_FAST_INTERP 0)
    set(WAMR_BUILD_DUMP_CALL_STACK 1)
    set(WAMR_BUILD_AOT 0)
    set(WAMR_BUILD_JIT 0)
    set(WAMR_BUILD_LIBC_BUILTIN 0)
    set(WAMR_BUILD_LIBC_WASI 0)
    set(WAMR_BUILD_MULTI_MODULE 0)
    set(WAMR_BUILD_REF_TYPES 1)
    # Use software bounds checks because Wasm runs on existing node threads.
    set(WAMR_DISABLE_HW_BOUND_CHECK 1)
    include(${WAMR_ROOT_DIR}/build-scripts/runtime_lib.cmake)
    add_library(vmlib STATIC ${WAMR_RUNTIME_LIB_SOURCE})
    if(WIN32)
        # WAMR's invokeNative_em64.asm is MASM syntax (the MSVC variant). ASM_NASM (enabled for blosc2's
        # project()) otherwise claims the .asm extension and assembles it as nasm -fwin32 -> MSB3721. Pin
        # WAMR's .asm sources to ml64 (MASM) here, in vmlib's own directory scope.
        foreach(_wsrc ${WAMR_RUNTIME_LIB_SOURCE})
            if(_wsrc MATCHES "\\.asm$")
                set_source_files_properties(${_wsrc} PROPERTIES LANGUAGE ASM_MASM)
            endif()
        endforeach()
    endif()
    target_include_directories(Qubic PRIVATE ${WAMR_ROOT_DIR}/core/iwasm/include)

    find_package(LibFFI REQUIRED)
    target_link_libraries(
        Qubic
        vmlib
        LibFFI::LibFFI
    )
    message(STATUS "LITE_WASM_SC: WAMR(${WAMR_BUILD_PLATFORM}/${WAMR_BUILD_TARGET}) + libffi")
endif()

if(ADDON_TX_STATUS_REQUEST)
    target_compile_definitions(Qubic PRIVATE ADDON_TX_STATUS_REQUEST=1)
endif()

# Set specific properties for assembly files to avoid C++ flags being applied
set_source_files_properties(platform/custom_stack.nasm PROPERTIES
    COMPILE_FLAGS ""
    LANGUAGE ASM_MASM
)

# Add additional compiler-specific flags for the main application
if((IS_CLANG OR IS_GCC) AND IS_ARM)
    # arm: -mrdrnd/-mbmi/-mlzcnt are x86-only (provided via SIMDe + qintrin shims); -fsigned-char needed.
    target_compile_options(Qubic PRIVATE
        -D_CONSOLE
        $<$<CONFIG:Release>:-DNDEBUG>
        -fsigned-char
        -fshort-wchar
        -w
    )
elseif(IS_CLANG OR IS_GCC)
    # Clang/GCC x86 flags (GCC requires the -m feature flag to emit rdrand/bmi/lzcnt intrinsics)
    target_compile_options(Qubic PRIVATE
        -D_CONSOLE
        $<$<CONFIG:Release>:-DNDEBUG>
        -mrdrnd
        -mbmi
        -mlzcnt
        -fshort-wchar
        -w
    )
elseif(IS_MSVC)
    # MSVC-specific flags. /Zc:wchar_t- = wchar_t is unsigned short (= CHAR16), same as the mainnet
    # Qubic.sln — L"..." literals are CHAR16* everywhere. Deps are unaffected (their APIs are
    # std::string-based; <filesystem>'s wchar_t internals are extern "C", so no mangling to clash).
    # NOMINMAX: <windows.h> (overload.h) min/max macros break the std::min/std::max call sites.
    target_compile_options(Qubic PRIVATE
       /D_CONSOLE
       /DNOMINMAX
       /Zc:wchar_t-
       $<$<CONFIG:Release>:/DNDEBUG>
    )
endif()

find_package(Threads REQUIRED)

apply_common_compiler_flags(Qubic)
if(APPLE)
    # macOS ld64: no -Bstatic/-Bdynamic, no librt/libdl (in libSystem), no libbacktrace.
    target_link_libraries(Qubic
            platform_common
            platform_efi
            jsoncpp_static drogon fmt::fmt
            Threads::Threads
            z
            zstd
            m
    )
elseif(WIN32)
# MSVC: no GNU-ld -Bstatic/-Bdynamic, no unix dl/rt/z/zstd/m, no libbacktrace (windbg backend). The
# FetchContent targets are the same; zstd is bundled inside blosc2. vmlib + ffi are linked in the wasm block.
target_link_libraries(Qubic
        platform_common
        platform_efi
        jsoncpp_static drogon fmt::fmt Blosc2::blosc2_static
        Threads::Threads
)
else()
target_link_libraries(Qubic
        platform_common
        platform_efi
        -Wl,-Bstatic
        jsoncpp_static drogon fmt::fmt backtrace_static Blosc2::blosc2_static
        -Wl,-Bdynamic
        Threads::Threads
        dl
        z
        zstd
        rt
        m
)
endif()

# Configure linker settings based on compiler
if(IS_MSVC)
    # OS-process node (qubic.cpp defines NO_UEFI unconditionally): keep the default .exe suffix.
    # (The old SUFFIX .efi predates the OS port; the EFI binary builds via Qubic.sln, not CMake.)
elseif(IS_CLANG)
    # Use default linker settings for Clang
endif()

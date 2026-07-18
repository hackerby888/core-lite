cmake_minimum_required(VERSION 3.15)
project(qubic_core_tests CXX C)

# GoogleTest requires at least C++20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.16.0
  )
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)


get_filename_component(PROJECT_ROOT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)

if(IS_ARM)
    # Prefer the ARM shim over the system x86 CPUID header.
    include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/../lib/platform_common/arm_shims)
endif()
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../lib/platform_common)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../lib/platform_os)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../lib/platform_efi) # Currently still needed due to various imports
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../src)
include_directories(${PROJECT_ROOT_DIR})


add_executable(
  qubic_core_tests
		assets.cpp
   		common_def.cpp
   		contract_core.cpp
   		contract_gqmprop.cpp
   		contract_msvault.cpp
   		contract_nostromo.cpp
   		contract_qbay.cpp
		contract_qbond.cpp
   		contract_qearn.cpp
   		contract_qswap.cpp
   		contract_qutil.cpp
   		contract_qvault.cpp
   		contract_qx.cpp
   		contract_rl.cpp
   		contract_testex.cpp
		contract_escrow.cpp
		contract_quottery.cpp
		contract_qrwa.cpp
		contract_qip.cpp
		contract_qusino.cpp
		custom_qubic_mining_storage.cpp
		fast_tx_window.cpp
   		file_io.cpp
   		fourq.cpp
   		kangaroo_twelve.cpp
   		m256.cpp
   		math_lib.cpp
   		network_messages.cpp
		pending_txs_pool.cpp
   		platform.cpp
   		qpi.cpp
   		qpi_collection.cpp
   		qpi_date_time.cpp
   		qpi_hash_map.cpp
		qpi_linked_list.cpp
   		revenue.cpp
   		score.cpp
   		score_cache.cpp
   		spectrum.cpp
   		stdlib_impl.cpp
   		tick_storage.cpp
   		time.cpp
   		tx_status_request.cpp
   		uint128.cpp
   		virtual_memory.cpp
   		vote_counter.cpp
		read_write_lock.cpp
		setjump.cpp
		oracle_engine.cpp
)

# Keep logging template sizes identical across every test translation unit.
target_compile_definitions(qubic_core_tests PRIVATE
    LOG_BUFFER_PAGE_SIZE=10000000ULL
    PMAP_LOG_PAGE_SIZE=1000000ULL
    IMAP_LOG_PAGE_SIZE=300ULL
    VM_NUM_CACHE_PAGE=1
)

# Apply test-specific compiler flags from the centralized detection module
apply_test_compiler_flags(qubic_core_tests)

# Add additional test-specific flags if needed
if((IS_CLANG OR IS_GCC) AND IS_ARM)
  # ARM receives x86 intrinsics through SIMDe.
  target_compile_options(qubic_core_tests PRIVATE -fshort-wchar -w)
elseif(IS_CLANG OR IS_GCC)
  target_compile_options(
          qubic_core_tests
          PRIVATE
          -mrdrnd -Wno-error
          -mbmi
          -mlzcnt
          -fshort-wchar
          -w
  )
endif()


target_link_libraries(
  qubic_core_tests PRIVATE
  GTest::gtest_main
  platform_common
  platform_os
  Blosc2::blosc2_static
)

# The contract-state engine test requires Linux userfaultfd and x86 RDRAND.
if(NOT WIN32 AND NOT APPLE AND NOT IS_ARM)
    target_sources(qubic_core_tests PRIVATE k12_engine.cpp)
    target_link_libraries(qubic_core_tests PRIVATE zstd)
endif()

# Build the Wasm ABI regression tests against the node's WAMR configuration.
if(LITE_WASM_SC)
    target_compile_definitions(qubic_core_tests PRIVATE
        LITE_WASM_SC
        TESTNET
        TESTNET_LITE_RAM
    )
    target_sources(qubic_core_tests PRIVATE wasm_contracts.cpp)

    if(NOT TARGET vmlib)
        if(APPLE)
            set(WAMR_BUILD_PLATFORM "darwin")
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
        set(WAMR_DISABLE_HW_BOUND_CHECK 1)
        include(${WAMR_ROOT_DIR}/build-scripts/runtime_lib.cmake)
        add_library(vmlib STATIC ${WAMR_RUNTIME_LIB_SOURCE})
    endif()
    target_include_directories(qubic_core_tests PRIVATE ${WAMR_ROOT_DIR}/core/iwasm/include)
    target_link_libraries(qubic_core_tests PRIVATE vmlib)
endif()

include(GoogleTest)
gtest_discover_tests(qubic_core_tests)

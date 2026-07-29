if(NOT DEFINED INPUT_FILE OR NOT DEFINED OUTPUT_FILE)
    message(FATAL_ERROR "INPUT_FILE and OUTPUT_FILE are required")
endif()
if(NOT EXISTS "${INPUT_FILE}")
    message(FATAL_ERROR "Missing binary asset: ${INPUT_FILE}")
endif()

file(READ "${INPUT_FILE}" _hex HEX)
file(SIZE "${INPUT_FILE}" _size)
file(SHA256 "${INPUT_FILE}" _sha256)

set(_contents
"// AUTO-GENERATED from data/bpp9000.task by cmake/EmbedBinary.cmake.
// SHA-256: ${_sha256}
#pragma once

alignas(64) inline constexpr unsigned char BPP9000_TASK_BYTES[] = {
")

string(LENGTH "${_hex}" _hex_length)
set(_offset 0)
while(_offset LESS _hex_length)
    math(EXPR _remaining "${_hex_length} - ${_offset}")
    if(_remaining GREATER 32)
        set(_chunk_length 32)
    else()
        set(_chunk_length "${_remaining}")
    endif()

    string(SUBSTRING "${_hex}" ${_offset} ${_chunk_length} _chunk)
    string(REGEX MATCHALL "[0-9a-f][0-9a-f]" _bytes "${_chunk}")
    list(TRANSFORM _bytes PREPEND "0x")
    list(JOIN _bytes ", " _line)
    string(APPEND _contents "    ${_line},\n")
    math(EXPR _offset "${_offset} + ${_chunk_length}")
endwhile()

string(APPEND _contents
"};
inline constexpr unsigned long long BPP9000_TASK_SIZE = sizeof(BPP9000_TASK_BYTES);
static_assert(BPP9000_TASK_SIZE == ${_size}ULL);
")

file(WRITE "${OUTPUT_FILE}" "${_contents}")

if(DEFINED EXPECTED_FILE)
    file(READ "${EXPECTED_FILE}" _expected)
    string(REPLACE "\r\n" "\n" _expected "${_expected}")
    if(NOT "${_expected}" STREQUAL "${_contents}")
        message(FATAL_ERROR
            "${EXPECTED_FILE} is stale; regenerate it from ${INPUT_FILE} with cmake/EmbedBinary.cmake")
    endif()
endif()

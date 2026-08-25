include_guard(GLOBAL)

function(qubic_add_homebrew_prefixes)
    if(NOT APPLE)
        return()
    endif()

    find_program(HOMEBREW_EXECUTABLE NAMES brew)
    mark_as_advanced(HOMEBREW_EXECUTABLE)

    if(NOT HOMEBREW_EXECUTABLE)
        return()
    endif()

    set(homebrew_prefixes)

    foreach(formula IN LISTS ARGN)
        execute_process(
            COMMAND "${HOMEBREW_EXECUTABLE}" --prefix "${formula}"
            RESULT_VARIABLE prefix_result
            OUTPUT_VARIABLE formula_prefix
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
        )

        if(prefix_result EQUAL 0 AND IS_DIRECTORY "${formula_prefix}")
            list(APPEND homebrew_prefixes "${formula_prefix}")
        endif()
    endforeach()

    if(NOT homebrew_prefixes)
        return()
    endif()

    list(PREPEND CMAKE_PREFIX_PATH ${homebrew_prefixes})
    list(REMOVE_DUPLICATES CMAKE_PREFIX_PATH)
    set(CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH}" PARENT_SCOPE)
endfunction()

# Normalize system and vcpkg libffi packages to LibFFI::LibFFI.

find_package(unofficial-libffi CONFIG QUIET)

if(TARGET unofficial::libffi::libffi)
    if(NOT TARGET LibFFI::LibFFI)
        add_library(LibFFI::LibFFI INTERFACE IMPORTED)
        set_property(
            TARGET LibFFI::LibFFI
            PROPERTY INTERFACE_LINK_LIBRARIES unofficial::libffi::libffi
        )
    endif()

    set(LibFFI_FOUND TRUE)
    return()
endif()

set(libffi_include_suffixes include)

if(CMAKE_LIBRARY_ARCHITECTURE)
    list(APPEND libffi_include_suffixes
        "${CMAKE_LIBRARY_ARCHITECTURE}"
        "include/${CMAKE_LIBRARY_ARCHITECTURE}"
    )
endif()

find_path(
    LibFFI_INCLUDE_DIR
    NAMES ffi.h
    PATH_SUFFIXES ${libffi_include_suffixes}
)

find_library(
    LibFFI_LIBRARY
    NAMES ffi
    PATH_SUFFIXES lib lib64
)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(
    LibFFI
    REQUIRED_VARS
        LibFFI_LIBRARY
        LibFFI_INCLUDE_DIR
)

mark_as_advanced(
    LibFFI_LIBRARY
    LibFFI_INCLUDE_DIR
)

if(LibFFI_FOUND AND NOT TARGET LibFFI::LibFFI)
    add_library(LibFFI::LibFFI UNKNOWN IMPORTED)
    set_target_properties(
        LibFFI::LibFFI
        PROPERTIES
            IMPORTED_LOCATION "${LibFFI_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${LibFFI_INCLUDE_DIR}"
    )
endif()

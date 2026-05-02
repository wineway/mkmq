set(MERCURY_ROOT "" CACHE PATH "Mercury source or install prefix")

find_path(MERCURY_INCLUDE_DIR
    NAMES mercury.h
    HINTS
        "${MERCURY_ROOT}"
    PATHS
        ENV MERCURY_ROOT
    PATH_SUFFIXES include src)

find_path(MERCURY_SOURCE_INCLUDE_DIR
    NAMES mercury_core.h
    HINTS
        "${MERCURY_ROOT}"
    PATHS
        ENV MERCURY_ROOT
    PATH_SUFFIXES src)

find_library(MERCURY_LIBRARY
    NAMES mercury
    HINTS
        "${MERCURY_ROOT}"
    PATHS
        ENV MERCURY_ROOT
    PATH_SUFFIXES lib lib64 build/src src)

if(MERCURY_SOURCE_INCLUDE_DIR)
    set(MERCURY_INCLUDE_DIRS
        "${MERCURY_INCLUDE_DIR}"
        "${MERCURY_SOURCE_INCLUDE_DIR}")
else()
    set(MERCURY_INCLUDE_DIRS "${MERCURY_INCLUDE_DIR}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Mercury
    REQUIRED_VARS MERCURY_INCLUDE_DIR MERCURY_LIBRARY)

if(Mercury_FOUND AND NOT TARGET Mercury::Mercury)
    add_library(Mercury::Mercury UNKNOWN IMPORTED)
    set_target_properties(Mercury::Mercury PROPERTIES
        IMPORTED_LOCATION "${MERCURY_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${MERCURY_INCLUDE_DIRS}")
endif()

mark_as_advanced(MERCURY_INCLUDE_DIR MERCURY_SOURCE_INCLUDE_DIR MERCURY_LIBRARY)

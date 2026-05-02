include(CheckCXXSourceCompiles)

set(_mkmq_saved_required_flags "${CMAKE_REQUIRED_FLAGS}")
set(CMAKE_REQUIRED_FLAGS "${CMAKE_REQUIRED_FLAGS} -msse4.2")
check_cxx_source_compiles([=[
    #include <cstdint>
    #include <nmmintrin.h>
    int main() {
        std::uint64_t crc = 0;
        crc = _mm_crc32_u64(crc, 0xfeedbeefULL);
        return static_cast<int>(crc);
    }
]=] MKMQ_HAVE_SSE42_CRC32C)
set(CMAKE_REQUIRED_FLAGS "${_mkmq_saved_required_flags}")

if(MKMQ_HAVE_SSE42_CRC32C)
    add_library(CRC32C::crc32c INTERFACE IMPORTED)
    set_target_properties(CRC32C::crc32c PROPERTIES
        INTERFACE_COMPILE_OPTIONS "-msse4.2"
        INTERFACE_COMPILE_DEFINITIONS "MKMQ_HAVE_SSE42_CRC32C=1")
    set(CRC32C_FOUND TRUE)
else()
    set(CRC32C_FOUND FALSE)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(CRC32C
    REQUIRED_VARS MKMQ_HAVE_SSE42_CRC32C
    FAIL_MESSAGE "CRC32C hardware support was not found. Build on x86_64 with SSE4.2 or install/provide a CRC32C backend.")

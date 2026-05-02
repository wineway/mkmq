#include "mkmq/crc32c.hpp"

#include <nmmintrin.h>

namespace mkmq {

std::uint32_t crc32c(const std::uint8_t* data, std::size_t size) {
    std::uint64_t crc = 0xffffffffU;
    while (size >= sizeof(std::uint64_t)) {
        std::uint64_t word = 0;
        __builtin_memcpy(&word, data, sizeof(word));
        crc = _mm_crc32_u64(crc, word);
        data += sizeof(word);
        size -= sizeof(word);
    }
    std::uint32_t crc32 = static_cast<std::uint32_t>(crc);
    while (size >= sizeof(std::uint32_t)) {
        std::uint32_t word = 0;
        __builtin_memcpy(&word, data, sizeof(word));
        crc32 = _mm_crc32_u32(crc32, word);
        data += sizeof(word);
        size -= sizeof(word);
    }
    while (size != 0) {
        crc32 = _mm_crc32_u8(crc32, *data);
        ++data;
        --size;
    }
    return crc32 ^ 0xffffffffU;
}

}  // namespace mkmq

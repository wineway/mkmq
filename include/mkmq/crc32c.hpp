#pragma once

#include <cstddef>
#include <cstdint>

namespace mkmq {

std::uint32_t crc32c(const std::uint8_t* data, std::size_t size);

}  // namespace mkmq

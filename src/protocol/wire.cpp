#include "mkmq/protocol/wire.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <utility>

namespace mkmq::protocol {

ProtocolError::ProtocolError(const std::string& message)
    : std::runtime_error(message) {}

WireReader::WireReader(const std::vector<std::uint8_t>& data)
    : data_(data.data()), size_(data.size()) {}

WireReader::WireReader(const std::uint8_t* data, std::size_t size)
    : data_(data), size_(size) {}

std::size_t WireReader::position() const noexcept {
    return pos_;
}

std::size_t WireReader::remaining() const noexcept {
    return size_ - pos_;
}

bool WireReader::empty() const noexcept {
    return remaining() == 0;
}

void WireReader::require(std::size_t size) const {
    if (size > remaining()) {
        throw ProtocolError("Kafka frame ended while decoding");
    }
}

std::int8_t WireReader::read_int8() {
    require(1);
    return static_cast<std::int8_t>(data_[pos_++]);
}

bool WireReader::read_bool() {
    return read_int8() != 0;
}

std::int16_t WireReader::read_int16() {
    require(2);
    const std::uint16_t value =
        (static_cast<std::uint16_t>(data_[pos_]) << 8) |
        static_cast<std::uint16_t>(data_[pos_ + 1]);
    pos_ += 2;
    return static_cast<std::int16_t>(value);
}

std::int32_t WireReader::read_int32() {
    require(4);
    const std::uint32_t value =
        (static_cast<std::uint32_t>(data_[pos_]) << 24) |
        (static_cast<std::uint32_t>(data_[pos_ + 1]) << 16) |
        (static_cast<std::uint32_t>(data_[pos_ + 2]) << 8) |
        static_cast<std::uint32_t>(data_[pos_ + 3]);
    pos_ += 4;
    return static_cast<std::int32_t>(value);
}

std::int64_t WireReader::read_int64() {
    require(8);
    std::uint64_t value = 0;
    for (int i = 0; i < 8; ++i) {
        value = (value << 8) | data_[pos_ + static_cast<std::size_t>(i)];
    }
    pos_ += 8;
    return static_cast<std::int64_t>(value);
}

std::uint32_t WireReader::read_unsigned_varint() {
    std::uint32_t value = 0;
    int shift = 0;
    while (shift < 32) {
        require(1);
        const std::uint8_t byte = data_[pos_++];
        value |= static_cast<std::uint32_t>(byte & 0x7fU) << shift;
        if ((byte & 0x80U) == 0) {
            return value;
        }
        shift += 7;
    }
    throw ProtocolError("Kafka unsigned varint is too large");
}

std::string WireReader::read_string() {
    const std::int16_t size = read_int16();
    if (size < 0) {
        throw ProtocolError("Kafka string cannot be null");
    }
    const auto bytes = read_raw(static_cast<std::size_t>(size));
    return std::string(bytes.begin(), bytes.end());
}

std::optional<std::string> WireReader::read_nullable_string() {
    const std::int16_t size = read_int16();
    if (size < 0) {
        return std::nullopt;
    }
    const auto bytes = read_raw(static_cast<std::size_t>(size));
    return std::string(bytes.begin(), bytes.end());
}

std::string WireReader::read_compact_string() {
    const std::uint32_t size_plus_one = read_unsigned_varint();
    if (size_plus_one == 0) {
        throw ProtocolError("Kafka compact string cannot be null");
    }
    const auto bytes = read_raw(size_plus_one - 1);
    return std::string(bytes.begin(), bytes.end());
}

std::optional<std::string> WireReader::read_compact_nullable_string() {
    const std::uint32_t size_plus_one = read_unsigned_varint();
    if (size_plus_one == 0) {
        return std::nullopt;
    }
    const auto bytes = read_raw(size_plus_one - 1);
    return std::string(bytes.begin(), bytes.end());
}

std::vector<std::uint8_t> WireReader::read_bytes() {
    const std::int32_t size = read_int32();
    if (size < 0) {
        throw ProtocolError("Kafka bytes cannot be null");
    }
    return read_raw(static_cast<std::size_t>(size));
}

std::optional<std::vector<std::uint8_t>> WireReader::read_nullable_bytes() {
    const std::int32_t size = read_int32();
    if (size < 0) {
        return std::nullopt;
    }
    return read_raw(static_cast<std::size_t>(size));
}

std::optional<std::vector<std::uint8_t>> WireReader::read_compact_nullable_bytes() {
    const std::uint32_t size_plus_one = read_unsigned_varint();
    if (size_plus_one == 0) {
        return std::nullopt;
    }
    return read_raw(size_plus_one - 1);
}

std::vector<std::uint8_t> WireReader::read_raw(std::size_t size) {
    require(size);
    std::vector<std::uint8_t> out(data_ + pos_, data_ + pos_ + size);
    pos_ += size;
    return out;
}

std::int32_t WireReader::read_array_count(bool flexible, bool nullable) {
    if (!flexible) {
        const std::int32_t count = read_int32();
        if (count < 0 && !nullable) {
            throw ProtocolError("Kafka array cannot be null");
        }
        return count;
    }

    const std::uint32_t count_plus_one = read_unsigned_varint();
    if (count_plus_one == 0) {
        if (!nullable) {
            throw ProtocolError("Kafka compact array cannot be null");
        }
        return -1;
    }
    if (count_plus_one > static_cast<std::uint32_t>(std::numeric_limits<std::int32_t>::max())) {
        throw ProtocolError("Kafka compact array is too large");
    }
    return static_cast<std::int32_t>(count_plus_one - 1);
}

void WireReader::skip_tagged_fields() {
    const std::uint32_t fields = read_unsigned_varint();
    std::uint32_t previous_tag = 0;
    for (std::uint32_t i = 0; i < fields; ++i) {
        const std::uint32_t tag = read_unsigned_varint();
        if (i > 0 && tag <= previous_tag) {
            throw ProtocolError("Kafka tagged fields are not strictly increasing");
        }
        previous_tag = tag;
        const std::uint32_t size = read_unsigned_varint();
        skip(size);
    }
}

void WireReader::skip(std::size_t size) {
    require(size);
    pos_ += size;
}

const std::vector<std::uint8_t>& WireWriter::bytes() const noexcept {
    return data_;
}

std::vector<std::uint8_t> WireWriter::take_bytes() {
    return std::move(data_);
}

std::size_t WireWriter::size() const noexcept {
    return data_.size();
}

void WireWriter::write_int8(std::int8_t value) {
    data_.push_back(static_cast<std::uint8_t>(value));
}

void WireWriter::write_bool(bool value) {
    write_int8(value ? 1 : 0);
}

void WireWriter::write_int16(std::int16_t value) {
    const auto raw = static_cast<std::uint16_t>(value);
    data_.push_back(static_cast<std::uint8_t>((raw >> 8) & 0xffU));
    data_.push_back(static_cast<std::uint8_t>(raw & 0xffU));
}

void WireWriter::write_int32(std::int32_t value) {
    const auto raw = static_cast<std::uint32_t>(value);
    data_.push_back(static_cast<std::uint8_t>((raw >> 24) & 0xffU));
    data_.push_back(static_cast<std::uint8_t>((raw >> 16) & 0xffU));
    data_.push_back(static_cast<std::uint8_t>((raw >> 8) & 0xffU));
    data_.push_back(static_cast<std::uint8_t>(raw & 0xffU));
}

void WireWriter::write_int64(std::int64_t value) {
    const auto raw = static_cast<std::uint64_t>(value);
    for (int shift = 56; shift >= 0; shift -= 8) {
        data_.push_back(static_cast<std::uint8_t>((raw >> shift) & 0xffU));
    }
}

void WireWriter::write_unsigned_varint(std::uint32_t value) {
    while ((value & 0xffffff80U) != 0) {
        data_.push_back(static_cast<std::uint8_t>((value & 0x7fU) | 0x80U));
        value >>= 7;
    }
    data_.push_back(static_cast<std::uint8_t>(value));
}

void WireWriter::write_string(const std::string& value) {
    if (value.size() > static_cast<std::size_t>(std::numeric_limits<std::int16_t>::max())) {
        throw ProtocolError("Kafka string is too large");
    }
    write_int16(static_cast<std::int16_t>(value.size()));
    write_raw(reinterpret_cast<const std::uint8_t*>(value.data()), value.size());
}

void WireWriter::write_nullable_string(const std::optional<std::string>& value) {
    if (!value.has_value()) {
        write_int16(-1);
        return;
    }
    write_string(*value);
}

void WireWriter::write_compact_string(const std::string& value) {
    write_unsigned_varint(static_cast<std::uint32_t>(value.size() + 1));
    write_raw(reinterpret_cast<const std::uint8_t*>(value.data()), value.size());
}

void WireWriter::write_compact_nullable_string(const std::optional<std::string>& value) {
    if (!value.has_value()) {
        write_unsigned_varint(0);
        return;
    }
    write_compact_string(*value);
}

void WireWriter::write_bytes(const std::vector<std::uint8_t>& value) {
    write_int32(static_cast<std::int32_t>(value.size()));
    write_raw(value);
}

void WireWriter::write_nullable_bytes(const std::optional<std::vector<std::uint8_t>>& value) {
    if (!value.has_value()) {
        write_int32(-1);
        return;
    }
    write_bytes(*value);
}

void WireWriter::write_compact_nullable_bytes(const std::optional<std::vector<std::uint8_t>>& value) {
    if (!value.has_value()) {
        write_unsigned_varint(0);
        return;
    }
    write_unsigned_varint(static_cast<std::uint32_t>(value->size() + 1));
    write_raw(*value);
}

void WireWriter::write_raw(const std::vector<std::uint8_t>& value) {
    write_raw(value.data(), value.size());
}

void WireWriter::write_raw(const std::uint8_t* data, std::size_t size) {
    data_.insert(data_.end(), data, data + size);
}

void WireWriter::write_array_count(std::int32_t count, bool flexible) {
    if (count < 0) {
        throw ProtocolError("Kafka array count cannot be negative");
    }
    if (flexible) {
        write_unsigned_varint(static_cast<std::uint32_t>(count + 1));
    } else {
        write_int32(count);
    }
}

void WireWriter::write_nullable_array_count(std::int32_t count, bool flexible) {
    if (count < 0) {
        if (flexible) {
            write_unsigned_varint(0);
        } else {
            write_int32(-1);
        }
        return;
    }
    write_array_count(count, flexible);
}

void WireWriter::write_empty_tagged_fields() {
    write_unsigned_varint(0);
}

void WireWriter::patch_int32(std::size_t offset, std::int32_t value) {
    if (offset + 4 > data_.size()) {
        throw ProtocolError("cannot patch int32 outside buffer");
    }
    const auto raw = static_cast<std::uint32_t>(value);
    data_[offset] = static_cast<std::uint8_t>((raw >> 24) & 0xffU);
    data_[offset + 1] = static_cast<std::uint8_t>((raw >> 16) & 0xffU);
    data_[offset + 2] = static_cast<std::uint8_t>((raw >> 8) & 0xffU);
    data_[offset + 3] = static_cast<std::uint8_t>(raw & 0xffU);
}

}  // namespace mkmq::protocol

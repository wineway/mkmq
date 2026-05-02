#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace mkmq::protocol {

class ProtocolError : public std::runtime_error {
public:
    explicit ProtocolError(const std::string& message);
};

class WireReader {
public:
    explicit WireReader(const std::vector<std::uint8_t>& data);
    WireReader(const std::uint8_t* data, std::size_t size);

    std::size_t position() const noexcept;
    std::size_t remaining() const noexcept;
    bool empty() const noexcept;

    std::int8_t read_int8();
    bool read_bool();
    std::int16_t read_int16();
    std::int32_t read_int32();
    std::int64_t read_int64();
    std::uint32_t read_unsigned_varint();
    std::string read_string();
    std::optional<std::string> read_nullable_string();
    std::string read_compact_string();
    std::optional<std::string> read_compact_nullable_string();
    std::vector<std::uint8_t> read_bytes();
    std::optional<std::vector<std::uint8_t>> read_nullable_bytes();
    std::optional<std::vector<std::uint8_t>> read_compact_nullable_bytes();
    std::vector<std::uint8_t> read_raw(std::size_t size);

    std::int32_t read_array_count(bool flexible, bool nullable = false);
    void skip_tagged_fields();
    void skip(std::size_t size);

private:
    const std::uint8_t* data_;
    std::size_t size_;
    std::size_t pos_{0};

    void require(std::size_t size) const;
};

class WireWriter {
public:
    const std::vector<std::uint8_t>& bytes() const noexcept;
    std::vector<std::uint8_t> take_bytes();
    std::size_t size() const noexcept;

    void write_int8(std::int8_t value);
    void write_bool(bool value);
    void write_int16(std::int16_t value);
    void write_int32(std::int32_t value);
    void write_int64(std::int64_t value);
    void write_unsigned_varint(std::uint32_t value);
    void write_string(const std::string& value);
    void write_nullable_string(const std::optional<std::string>& value);
    void write_compact_string(const std::string& value);
    void write_compact_nullable_string(const std::optional<std::string>& value);
    void write_bytes(const std::vector<std::uint8_t>& value);
    void write_nullable_bytes(const std::optional<std::vector<std::uint8_t>>& value);
    void write_compact_nullable_bytes(const std::optional<std::vector<std::uint8_t>>& value);
    void write_raw(const std::vector<std::uint8_t>& value);
    void write_raw(const std::uint8_t* data, std::size_t size);
    void write_array_count(std::int32_t count, bool flexible);
    void write_nullable_array_count(std::int32_t count, bool flexible);
    void write_empty_tagged_fields();
    void patch_int32(std::size_t offset, std::int32_t value);

private:
    std::vector<std::uint8_t> data_;
};

}  // namespace mkmq::protocol

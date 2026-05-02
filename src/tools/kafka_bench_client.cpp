#include "mkmq/crc32c.hpp"
#include "mkmq/protocol/header.hpp"
#include "mkmq/protocol/wire.hpp"

#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

namespace {

struct Options {
    std::string host{"127.0.0.1"};
    std::uint16_t port{9092};
    std::string topic{"orders"};
    std::int32_t partition{0};
    std::int32_t messages{10000};
    std::int32_t payload_size{128};
    std::int16_t acks{1};
    std::int32_t fetch_empty_wait_ms{0};
    std::int64_t fetch_offset{0};
    std::optional<std::int16_t> expect_fetch_error;
    std::optional<std::int16_t> expect_metadata_error;
    std::optional<std::int16_t> expect_list_offsets_error;
    std::optional<std::size_t> expect_fetch_min_bytes;
    std::optional<std::size_t> expect_fetch_max_bytes;
    std::optional<std::int16_t> expect_produce_error;
    std::optional<std::string> transactional_id;
    bool idempotent_batch{false};
    bool metadata_only{false};
    bool list_offsets_only{false};
    bool fetch_only{false};
    bool smoke{false};
};

std::string require_value(int& i, int argc, char** argv, const char* name) {
    if (i + 1 >= argc) {
        throw std::runtime_error(std::string("missing value for ") + name);
    }
    return argv[++i];
}

Options parse_options(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--host") {
            options.host = require_value(i, argc, argv, "--host");
        } else if (arg == "--port") {
            options.port = static_cast<std::uint16_t>(std::stoi(require_value(i, argc, argv, "--port")));
        } else if (arg == "--topic") {
            options.topic = require_value(i, argc, argv, "--topic");
        } else if (arg == "--partition") {
            options.partition = std::stoi(require_value(i, argc, argv, "--partition"));
        } else if (arg == "--messages") {
            options.messages = std::stoi(require_value(i, argc, argv, "--messages"));
        } else if (arg == "--payload-size") {
            options.payload_size = std::stoi(require_value(i, argc, argv, "--payload-size"));
        } else if (arg == "--acks") {
            options.acks = static_cast<std::int16_t>(std::stoi(require_value(i, argc, argv, "--acks")));
        } else if (arg == "--fetch-empty-wait-ms") {
            options.fetch_empty_wait_ms = std::stoi(require_value(i, argc, argv, "--fetch-empty-wait-ms"));
        } else if (arg == "--fetch-offset") {
            options.fetch_offset = std::stoll(require_value(i, argc, argv, "--fetch-offset"));
        } else if (arg == "--expect-fetch-error") {
            options.expect_fetch_error = static_cast<std::int16_t>(std::stoi(require_value(i, argc, argv, "--expect-fetch-error")));
        } else if (arg == "--expect-metadata-error") {
            options.expect_metadata_error = static_cast<std::int16_t>(std::stoi(require_value(i, argc, argv, "--expect-metadata-error")));
        } else if (arg == "--expect-list-offsets-error") {
            options.expect_list_offsets_error = static_cast<std::int16_t>(std::stoi(require_value(i, argc, argv, "--expect-list-offsets-error")));
        } else if (arg == "--expect-fetch-min-bytes") {
            options.expect_fetch_min_bytes = static_cast<std::size_t>(std::stoull(require_value(i, argc, argv, "--expect-fetch-min-bytes")));
        } else if (arg == "--expect-fetch-max-bytes") {
            options.expect_fetch_max_bytes = static_cast<std::size_t>(std::stoull(require_value(i, argc, argv, "--expect-fetch-max-bytes")));
        } else if (arg == "--expect-produce-error") {
            options.expect_produce_error = static_cast<std::int16_t>(std::stoi(require_value(i, argc, argv, "--expect-produce-error")));
        } else if (arg == "--transactional-id") {
            options.transactional_id = require_value(i, argc, argv, "--transactional-id");
        } else if (arg == "--idempotent-batch") {
            options.idempotent_batch = true;
        } else if (arg == "--metadata-only") {
            options.metadata_only = true;
        } else if (arg == "--list-offsets-only") {
            options.list_offsets_only = true;
        } else if (arg == "--fetch-only") {
            options.fetch_only = true;
        } else if (arg == "--smoke") {
            options.smoke = true;
            options.messages = 1;
        } else if (arg == "--help") {
            std::cout << "Usage: mkmq_kafka_bench [--host HOST] [--port PORT] [--topic TOPIC]\n"
                         "                        [--partition N] [--messages N]\n"
                         "                        [--payload-size N] [--acks 0|1|-1]\n"
                         "                        [--fetch-empty-wait-ms N]\n"
                         "                        [--fetch-only] [--fetch-offset N]\n"
                         "                        [--metadata-only] [--list-offsets-only]\n"
                         "                        [--expect-fetch-error N]\n"
                         "                        [--expect-metadata-error N]\n"
                         "                        [--expect-list-offsets-error N]\n"
                         "                        [--expect-fetch-min-bytes N]\n"
                         "                        [--expect-fetch-max-bytes N]\n"
                         "                        [--expect-produce-error N]\n"
                         "                        [--transactional-id ID]\n"
                         "                        [--idempotent-batch] [--smoke]\n";
            std::exit(0);
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }
    if (options.messages <= 0 || options.payload_size < 0 || options.acks < -1 || options.acks > 1 || options.fetch_empty_wait_ms < 0) {
        throw std::runtime_error("messages must be positive, payload-size non-negative, acks one of 0, 1, -1, and fetch wait non-negative");
    }
    return options;
}

bool read_exact(int fd, void* data, std::size_t size) {
    auto* out = static_cast<std::uint8_t*>(data);
    std::size_t done = 0;
    while (done < size) {
        const auto n = ::recv(fd, out + done, size - done, 0);
        if (n == 0) {
            return false;
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        done += static_cast<std::size_t>(n);
    }
    return true;
}

bool write_exact(int fd, const void* data, std::size_t size) {
    const auto* in = static_cast<const std::uint8_t*>(data);
    std::size_t done = 0;
    while (done < size) {
        const auto n = ::send(fd, in + done, size - done, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        done += static_cast<std::size_t>(n);
    }
    return true;
}

std::int32_t decode_i32(const std::uint8_t bytes[4]) {
    return static_cast<std::int32_t>(
        (static_cast<std::uint32_t>(bytes[0]) << 24) |
        (static_cast<std::uint32_t>(bytes[1]) << 16) |
        (static_cast<std::uint32_t>(bytes[2]) << 8) |
        static_cast<std::uint32_t>(bytes[3]));
}

class Socket {
public:
    Socket(const std::string& host, std::uint16_t port) {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) {
            throw std::runtime_error("socket failed");
        }
        int yes = 1;
        (void) ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
            throw std::runtime_error("host must be an IPv4 address");
        }
        if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            throw std::runtime_error("connect failed: " + std::string(std::strerror(errno)));
        }
    }

    ~Socket() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    void send_frame(const std::vector<std::uint8_t>& frame) {
        if (!write_exact(fd_, frame.data(), frame.size())) {
            throw std::runtime_error("send failed");
        }
    }

    std::vector<std::uint8_t> recv_frame() {
        std::uint8_t size_bytes[4]{};
        if (!read_exact(fd_, size_bytes, sizeof(size_bytes))) {
            throw std::runtime_error("failed to read response size");
        }
        const auto size = decode_i32(size_bytes);
        if (size <= 0 || size > 100 * 1024 * 1024) {
            throw std::runtime_error("invalid response frame size");
        }
        std::vector<std::uint8_t> frame(static_cast<std::size_t>(size) + 4);
        std::memcpy(frame.data(), size_bytes, sizeof(size_bytes));
        if (!read_exact(fd_, frame.data() + 4, static_cast<std::size_t>(size))) {
            throw std::runtime_error("failed to read response payload");
        }
        return frame;
    }

private:
    int fd_{-1};
};

std::vector<std::uint8_t> wrap_request(
    std::int16_t api_key,
    std::int16_t api_version,
    std::int32_t correlation_id,
    const std::vector<std::uint8_t>& body) {
    mkmq::protocol::WireWriter writer;
    writer.write_int32(0);
    writer.write_int16(api_key);
    writer.write_int16(api_version);
    writer.write_int32(correlation_id);
    writer.write_nullable_string(std::optional<std::string>("mkmq-bench"));
    writer.write_empty_tagged_fields();
    writer.write_raw(body);
    writer.patch_int32(0, static_cast<std::int32_t>(writer.size() - 4));
    return writer.take_bytes();
}

void expect_correlation(const std::vector<std::uint8_t>& frame, std::int32_t correlation_id) {
    mkmq::protocol::WireReader reader(frame);
    (void) reader.read_int32();
    const auto actual = reader.read_int32();
    if (actual != correlation_id) {
        throw std::runtime_error("unexpected correlation id in response");
    }
}

void skip_response_header_tags(std::vector<std::uint8_t> const& frame, std::int32_t correlation_id, mkmq::protocol::WireReader& reader) {
    (void) frame;
    (void) reader.read_int32();
    const auto actual = reader.read_int32();
    if (actual != correlation_id) {
        throw std::runtime_error("unexpected correlation id in response");
    }
    reader.skip_tagged_fields();
}

std::int16_t first_produce_error(const std::vector<std::uint8_t>& frame, std::int32_t correlation_id) {
    mkmq::protocol::WireReader reader(frame);
    skip_response_header_tags(frame, correlation_id, reader);
    const auto topic_count = reader.read_array_count(true);
    if (topic_count <= 0) {
        throw std::runtime_error("Produce response had no topics");
    }
    (void) reader.read_compact_string();
    const auto partition_count = reader.read_array_count(true);
    if (partition_count <= 0) {
        throw std::runtime_error("Produce response had no partitions");
    }
    (void) reader.read_int32();
    return reader.read_int16();
}

std::int16_t first_metadata_error(const std::vector<std::uint8_t>& frame, std::int32_t correlation_id) {
    mkmq::protocol::WireReader reader(frame);
    skip_response_header_tags(frame, correlation_id, reader);
    (void) reader.read_int32();
    const auto broker_count = reader.read_array_count(true);
    for (std::int32_t i = 0; i < broker_count; ++i) {
        (void) reader.read_int32();
        (void) reader.read_compact_string();
        (void) reader.read_int32();
        (void) reader.read_compact_nullable_string();
        reader.skip_tagged_fields();
    }
    (void) reader.read_compact_nullable_string();
    (void) reader.read_int32();
    const auto topic_count = reader.read_array_count(true);
    if (topic_count <= 0) {
        throw std::runtime_error("Metadata response had no topics");
    }
    return reader.read_int16();
}

struct FetchSummary {
    std::int16_t error_code{0};
    std::int64_t high_watermark{0};
    std::vector<std::uint8_t> records;
};

FetchSummary first_fetch_summary(const std::vector<std::uint8_t>& frame, std::int32_t correlation_id) {
    mkmq::protocol::WireReader reader(frame);
    skip_response_header_tags(frame, correlation_id, reader);
    (void) reader.read_int32();
    (void) reader.read_int16();
    (void) reader.read_int32();
    const auto topic_count = reader.read_array_count(true);
    if (topic_count <= 0) {
        throw std::runtime_error("Fetch response had no topics");
    }
    (void) reader.read_compact_string();
    const auto partition_count = reader.read_array_count(true);
    if (partition_count <= 0) {
        throw std::runtime_error("Fetch response had no partitions");
    }
    (void) reader.read_int32();
    FetchSummary summary;
    summary.error_code = reader.read_int16();
    summary.high_watermark = reader.read_int64();
    (void) reader.read_int64();
    (void) reader.read_int64();
    const auto aborted_count = reader.read_array_count(true, true);
    for (std::int32_t i = 0; i < aborted_count; ++i) {
        (void) reader.read_int64();
        (void) reader.read_int64();
        reader.skip_tagged_fields();
    }
    (void) reader.read_int32();
    summary.records = reader.read_compact_nullable_bytes().value_or(std::vector<std::uint8_t>{});
    return summary;
}

std::int16_t first_fetch_error(const std::vector<std::uint8_t>& frame, std::int32_t correlation_id) {
    return first_fetch_summary(frame, correlation_id).error_code;
}

std::pair<std::int16_t, std::int64_t> first_list_offset(
    const std::vector<std::uint8_t>& frame,
    std::int32_t correlation_id) {
    mkmq::protocol::WireReader reader(frame);
    skip_response_header_tags(frame, correlation_id, reader);
    (void) reader.read_int32();
    const auto topic_count = reader.read_array_count(true);
    if (topic_count <= 0) {
        throw std::runtime_error("ListOffsets response had no topics");
    }
    (void) reader.read_compact_string();
    const auto partition_count = reader.read_array_count(true);
    if (partition_count <= 0) {
        throw std::runtime_error("ListOffsets response had no partitions");
    }
    (void) reader.read_int32();
    const auto error = reader.read_int16();
    (void) reader.read_int64();
    const auto offset = reader.read_int64();
    return {error, offset};
}

std::vector<std::uint8_t> make_api_versions_body() {
    mkmq::protocol::WireWriter writer;
    writer.write_compact_string("mkmq-bench");
    writer.write_compact_string("0.1");
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}

std::vector<std::uint8_t> make_metadata_body(const Options& options) {
    mkmq::protocol::WireWriter writer;
    writer.write_array_count(1, true);
    writer.write_compact_string(options.topic);
    writer.write_empty_tagged_fields();
    writer.write_bool(false);
    writer.write_bool(false);
    writer.write_bool(false);
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}

void put_i64_be(std::vector<std::uint8_t>& bytes, std::size_t pos, std::int64_t value) {
    const auto raw = static_cast<std::uint64_t>(value);
    for (int shift = 56; shift >= 0; shift -= 8) {
        bytes[pos++] = static_cast<std::uint8_t>((raw >> shift) & 0xffU);
    }
}

void put_i32_be(std::vector<std::uint8_t>& bytes, std::size_t pos, std::int32_t value) {
    const auto raw = static_cast<std::uint32_t>(value);
    bytes[pos] = static_cast<std::uint8_t>((raw >> 24) & 0xffU);
    bytes[pos + 1] = static_cast<std::uint8_t>((raw >> 16) & 0xffU);
    bytes[pos + 2] = static_cast<std::uint8_t>((raw >> 8) & 0xffU);
    bytes[pos + 3] = static_cast<std::uint8_t>(raw & 0xffU);
}

void put_i16_be(std::vector<std::uint8_t>& bytes, std::size_t pos, std::int16_t value) {
    const auto raw = static_cast<std::uint16_t>(value);
    bytes[pos] = static_cast<std::uint8_t>((raw >> 8) & 0xffU);
    bytes[pos + 1] = static_cast<std::uint8_t>(raw & 0xffU);
}

void append_unsigned_varint(std::vector<std::uint8_t>& bytes, std::uint64_t value) {
    while ((value & ~0x7fULL) != 0) {
        bytes.push_back(static_cast<std::uint8_t>((value & 0x7fU) | 0x80U));
        value >>= 7;
    }
    bytes.push_back(static_cast<std::uint8_t>(value));
}

void append_varint(std::vector<std::uint8_t>& bytes, std::int32_t value) {
    const auto raw = static_cast<std::uint32_t>(value);
    append_unsigned_varint(bytes, static_cast<std::uint64_t>((raw << 1U) ^ static_cast<std::uint32_t>(value >> 31)));
}

void append_varlong(std::vector<std::uint8_t>& bytes, std::int64_t value) {
    const auto raw = static_cast<std::uint64_t>(value);
    append_unsigned_varint(bytes, (raw << 1U) ^ static_cast<std::uint64_t>(value >> 63));
}

std::vector<std::uint8_t> make_record_batch(const Options& options) {
    const auto payload_size = options.payload_size;
    std::vector<std::uint8_t> record_body;
    record_body.reserve(static_cast<std::size_t>(payload_size) + 16);
    record_body.push_back(0);
    append_varlong(record_body, 0);
    append_varint(record_body, 0);
    append_varint(record_body, -1);
    append_varint(record_body, payload_size);
    for (std::int32_t i = 0; i < payload_size; ++i) {
        record_body.push_back(static_cast<std::uint8_t>(i));
    }
    append_varint(record_body, 0);

    std::vector<std::uint8_t> records;
    records.reserve(record_body.size() + 5);
    append_varint(records, static_cast<std::int32_t>(record_body.size()));
    records.insert(records.end(), record_body.begin(), record_body.end());

    const auto size = static_cast<std::size_t>(61 + records.size());
    std::vector<std::uint8_t> batch(size, 0);
    const std::int32_t batch_length = static_cast<std::int32_t>(size - 12);
    put_i32_be(batch, 8, batch_length);
    batch[16] = 2;
    put_i64_be(batch, 43, options.idempotent_batch ? 1 : -1);
    put_i16_be(batch, 51, options.idempotent_batch ? 0 : -1);
    put_i32_be(batch, 53, options.idempotent_batch ? 0 : -1);
    put_i32_be(batch, 57, 1);
    std::copy(records.begin(), records.end(), batch.begin() + 61);
    put_i32_be(batch, 17, static_cast<std::int32_t>(mkmq::crc32c(batch.data() + 21, batch.size() - 21)));
    return batch;
}

std::vector<std::uint8_t> make_produce_body(const Options& options, std::int16_t acks) {
    mkmq::protocol::WireWriter writer;
    writer.write_compact_nullable_string(options.transactional_id);
    writer.write_int16(acks);
    writer.write_int32(100);
    writer.write_array_count(1, true);
    writer.write_compact_string(options.topic);
    writer.write_array_count(1, true);
    writer.write_int32(options.partition);
    writer.write_compact_nullable_bytes(make_record_batch(options));
    writer.write_empty_tagged_fields();
    writer.write_empty_tagged_fields();
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}

std::vector<std::uint8_t> make_fetch_body(const Options& options, std::int64_t offset, std::int32_t max_wait_ms = 0) {
    mkmq::protocol::WireWriter writer;
    writer.write_int32(-1);
    writer.write_int32(max_wait_ms);
    writer.write_int32(1);
    writer.write_int32(1024 * 1024);
    writer.write_int8(0);
    writer.write_int32(0);
    writer.write_int32(-1);
    writer.write_array_count(1, true);
    writer.write_compact_string(options.topic);
    writer.write_array_count(1, true);
    writer.write_int32(options.partition);
    writer.write_int32(0);
    writer.write_int64(offset);
    writer.write_int32(-1);
    writer.write_int64(-1);
    writer.write_int32(1024 * 1024);
    writer.write_empty_tagged_fields();
    writer.write_empty_tagged_fields();
    writer.write_array_count(0, true);
    writer.write_compact_string("");
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}

std::vector<std::uint8_t> make_list_offsets_body(const Options& options, std::int64_t timestamp) {
    mkmq::protocol::WireWriter writer;
    writer.write_int32(-1);
    writer.write_int8(0);
    writer.write_array_count(1, true);
    writer.write_compact_string(options.topic);
    writer.write_array_count(1, true);
    writer.write_int32(options.partition);
    writer.write_int32(-1);
    writer.write_int64(timestamp);
    writer.write_empty_tagged_fields();
    writer.write_empty_tagged_fields();
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}

void run_smoke(Socket& socket, const Options& options) {
    std::int32_t correlation = 1;
    socket.send_frame(wrap_request(18, 3, correlation, make_api_versions_body()));
    expect_correlation(socket.recv_frame(), correlation++);
    socket.send_frame(wrap_request(3, 9, correlation, make_metadata_body(options)));
    const auto metadata_error = first_metadata_error(socket.recv_frame(), correlation);
    const auto expected_metadata_error = options.expect_metadata_error.value_or(0);
    if (metadata_error != expected_metadata_error) {
        throw std::runtime_error("Metadata returned Kafka error " + std::to_string(metadata_error) +
                                 ", expected " + std::to_string(expected_metadata_error));
    }
    ++correlation;
    if (options.metadata_only) {
        std::cout << "metadata_error=" << metadata_error << '\n';
        return;
    }
    socket.send_frame(wrap_request(0, 9, correlation, make_produce_body(options, options.acks)));
    if (options.acks != 0) {
        const auto error = first_produce_error(socket.recv_frame(), correlation);
        const auto expected_error = options.expect_produce_error.value_or(0);
        if (error != expected_error) {
            throw std::runtime_error("Produce smoke returned Kafka error " + std::to_string(error));
        }
    }
    ++correlation;
    if (options.expect_produce_error.value_or(0) != 0) {
        return;
    }
    socket.send_frame(wrap_request(1, 12, correlation, make_fetch_body(options, 0)));
    expect_correlation(socket.recv_frame(), correlation++);

    socket.send_frame(wrap_request(2, 6, correlation, make_list_offsets_body(options, -2)));
    const auto [earliest_error, earliest] = first_list_offset(socket.recv_frame(), correlation);
    if (earliest_error != 0 || earliest != 0) {
        throw std::runtime_error("ListOffsets earliest returned error " + std::to_string(earliest_error) +
                                 " offset " + std::to_string(earliest));
    }
    ++correlation;

    socket.send_frame(wrap_request(2, 6, correlation, make_list_offsets_body(options, -1)));
    const auto [latest_error, latest] = first_list_offset(socket.recv_frame(), correlation);
    if (latest_error != 0 || latest < 1) {
        throw std::runtime_error("ListOffsets latest returned error " + std::to_string(latest_error) +
                                 " offset " + std::to_string(latest));
    }
    ++correlation;

    if (options.fetch_empty_wait_ms > 0) {
        const auto start = std::chrono::steady_clock::now();
        socket.send_frame(wrap_request(1, 12, correlation, make_fetch_body(options, 1, options.fetch_empty_wait_ms)));
        const auto error = first_fetch_error(socket.recv_frame(), correlation);
        const auto stop = std::chrono::steady_clock::now();
        if (error != 0) {
            throw std::runtime_error("Fetch empty wait returned Kafka error " + std::to_string(error));
        }
        std::cout << "fetch_empty_wait_us="
                  << std::chrono::duration<double, std::micro>(stop - start).count()
                  << '\n';
        ++correlation;
    }
}

void run_list_offsets_only(Socket& socket, const Options& options) {
    std::int32_t correlation = 1;
    socket.send_frame(wrap_request(18, 3, correlation, make_api_versions_body()));
    expect_correlation(socket.recv_frame(), correlation++);
    socket.send_frame(wrap_request(3, 9, correlation, make_metadata_body(options)));
    expect_correlation(socket.recv_frame(), correlation++);
    socket.send_frame(wrap_request(2, 6, correlation, make_list_offsets_body(options, -1)));
    const auto [error, offset] = first_list_offset(socket.recv_frame(), correlation);
    const auto expected_error = options.expect_list_offsets_error.value_or(0);
    if (error != expected_error) {
        throw std::runtime_error("ListOffsets returned Kafka error " + std::to_string(error) +
                                 ", expected " + std::to_string(expected_error));
    }
    std::cout << "list_offsets_error=" << error
              << " list_offsets_selected=" << offset
              << '\n';
}

void run_fetch_only(Socket& socket, const Options& options) {
    std::int32_t correlation = 1;
    socket.send_frame(wrap_request(18, 3, correlation, make_api_versions_body()));
    expect_correlation(socket.recv_frame(), correlation++);
    socket.send_frame(wrap_request(3, 9, correlation, make_metadata_body(options)));
    expect_correlation(socket.recv_frame(), correlation++);
    socket.send_frame(wrap_request(1, 12, correlation, make_fetch_body(options, options.fetch_offset)));
    const auto summary = first_fetch_summary(socket.recv_frame(), correlation++);

    if (options.expect_fetch_error.has_value() && summary.error_code != *options.expect_fetch_error) {
        throw std::runtime_error("Fetch returned Kafka error " + std::to_string(summary.error_code) +
                                 ", expected " + std::to_string(*options.expect_fetch_error));
    }
    if (!options.expect_fetch_error.has_value() && summary.error_code != 0) {
        throw std::runtime_error("Fetch returned Kafka error " + std::to_string(summary.error_code));
    }
    if (options.expect_fetch_min_bytes.has_value() && summary.records.size() < *options.expect_fetch_min_bytes) {
        throw std::runtime_error("Fetch returned " + std::to_string(summary.records.size()) +
                                 " record bytes, expected at least " + std::to_string(*options.expect_fetch_min_bytes));
    }
    if (options.expect_fetch_max_bytes.has_value() && summary.records.size() > *options.expect_fetch_max_bytes) {
        throw std::runtime_error("Fetch returned " + std::to_string(summary.records.size()) +
                                 " record bytes, expected at most " + std::to_string(*options.expect_fetch_max_bytes));
    }
    std::cout << "fetch_error=" << summary.error_code
              << " fetch_high_watermark=" << summary.high_watermark
              << " fetch_record_bytes=" << summary.records.size()
              << '\n';
}

void run_benchmark(Socket& socket, const Options& options) {
    std::int32_t correlation = 1000;
    if (options.acks == 0) {
        throw std::runtime_error("benchmark mode requires a Produce response; use --acks 1 or --acks -1");
    }
    std::vector<double> micros;
    micros.reserve(static_cast<std::size_t>(options.messages));
    const auto body = make_produce_body(options, options.acks);
    for (std::int32_t i = 0; i < options.messages; ++i) {
        const auto frame = wrap_request(0, 9, correlation, body);
        const auto start = std::chrono::steady_clock::now();
        socket.send_frame(frame);
        const auto error = first_produce_error(socket.recv_frame(), correlation);
        if (error != 0) {
            throw std::runtime_error("Produce benchmark returned Kafka error " + std::to_string(error));
        }
        const auto stop = std::chrono::steady_clock::now();
        micros.push_back(std::chrono::duration<double, std::micro>(stop - start).count());
        ++correlation;
    }

    std::sort(micros.begin(), micros.end());
    const auto pct = [&](double p) {
        const auto index = std::min<std::size_t>(
            micros.size() - 1,
            static_cast<std::size_t>((p / 100.0) * static_cast<double>(micros.size() - 1)));
        return micros[index];
    };
    std::cout << "messages=" << options.messages
              << " payload=" << options.payload_size
              << " p50_us=" << pct(50)
              << " p99_us=" << pct(99)
              << " max_us=" << micros.back()
              << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const auto options = parse_options(argc, argv);
        Socket socket(options.host, options.port);
        if (options.metadata_only) {
            run_smoke(socket, options);
        } else if (options.list_offsets_only) {
            run_list_offsets_only(socket, options);
        } else if (options.fetch_only) {
            run_fetch_only(socket, options);
        } else {
            run_smoke(socket, options);
        }
        if (!options.smoke && !options.fetch_only && !options.metadata_only && !options.list_offsets_only && !options.expect_produce_error.has_value()) {
            run_benchmark(socket, options);
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "mkmq_kafka_bench: " << error.what() << '\n';
        return 1;
    }
}

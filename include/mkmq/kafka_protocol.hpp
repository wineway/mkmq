#pragma once

#include "mkmq/config.hpp"
#include "mkmq/protocol/api.hpp"

#include <seastar/core/future.hh>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mkmq {

struct ProducePartitionRequest {
    std::string topic;
    std::int32_t partition{0};
    std::vector<std::uint8_t> records;
};

struct ProduceRequest {
    std::int16_t acks{1};
    std::int32_t timeout_ms{0};
    std::int16_t rejected_error_code{0};
    std::vector<ProducePartitionRequest> partitions;
};

struct ProducePartitionResult {
    std::string topic;
    std::int32_t partition{0};
    std::int16_t error_code{0};
    std::int64_t base_offset{0};
    std::int64_t log_end_offset{0};
    std::int64_t log_start_offset{0};
};

struct FetchPartitionRequest {
    std::string topic;
    std::int32_t partition{0};
    std::int64_t offset{0};
    std::int32_t max_wait_ms{0};
    std::int32_t min_bytes{0};
    std::int32_t max_bytes{0};
};

struct FetchPartitionResult {
    std::int16_t error_code{0};
    std::int64_t high_watermark{0};
    std::int64_t log_start_offset{0};
    std::int64_t log_end_offset{0};
    std::vector<std::uint8_t> records;
};

struct ListOffsetPartitionRequest {
    std::string topic;
    std::int32_t partition{0};
    std::int64_t timestamp{-1};
};

struct ListOffsetPartitionResult {
    std::string topic;
    std::int32_t partition{0};
    std::int16_t error_code{0};
    std::int64_t earliest{0};
    std::int64_t latest{0};
    std::int64_t selected{0};
};

class BrokerShard;

class KafkaProtocol {
public:
    explicit KafkaProtocol(BrokerShard& broker);

    seastar::future<std::optional<std::vector<std::uint8_t>>> handle_frame(
        std::vector<std::uint8_t> payload);

private:
    BrokerShard& broker_;
};

// Rewrite the batch headers of `records` in place so each batch's base
// offset starts at `base_offset` (and CRCs are recomputed). Takes the vector
// by value so callers can std::move into it: the previous
// `const&` + internal full-copy was an unconditional memcpy on every produce
// regardless of whether the caller already owned the bytes.
std::vector<std::uint8_t> rewrite_record_batch_offsets(
    std::vector<std::uint8_t> records,
    std::int64_t base_offset);

}  // namespace mkmq

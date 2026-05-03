#pragma once
// Internal-only helpers shared between broker_shard.cpp and
// partition_log_shard.cpp. NOT a public header — do not #include from anything
// under include/.

#include "mkmq/broker_shard.hpp"
#include "mkmq/config.hpp"
#include "mkmq/protocol/api.hpp"
#include "mkmq/protocol/wire.hpp"

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace mkmq::detail {

// ---- File-format magics and hard-coded sizes -------------------------------
inline constexpr char kLogMagic[8] = {'M', 'K', 'M', 'Q', 'L', 'O', 'G', '1'};
inline constexpr char kIndexMagic[8] = {'M', 'K', 'M', 'Q', 'I', 'D', 'X', '1'};
inline constexpr std::uint64_t kIndexEntryPayloadSize = 48;
inline constexpr std::int32_t kReplicaCatchupMaxBytes = 1024 * 1024;

// ---- Byte-level helpers ----------------------------------------------------
// Native-endian memcpy-based integer I/O used by the log/index on-disk format.
constexpr std::uint64_t round_up(std::uint64_t value, std::uint64_t alignment) noexcept {
    return ((value + alignment - 1) / alignment) * alignment;
}

inline void put_i32(std::uint8_t* out, std::int32_t value) noexcept {
    std::memcpy(out, &value, sizeof(value));
}
inline void put_i64(std::uint8_t* out, std::int64_t value) noexcept {
    std::memcpy(out, &value, sizeof(value));
}
inline std::int32_t get_i32(const char* in) noexcept {
    std::int32_t value = 0;
    std::memcpy(&value, in, sizeof(value));
    return value;
}
inline std::int64_t get_i64(const char* in) noexcept {
    std::int64_t value = 0;
    std::memcpy(&value, in, sizeof(value));
    return value;
}

// Kafka wire uses big-endian for the 4-byte frame length prefix.
inline std::int32_t decode_frame_size(const char* bytes) noexcept {
    return static_cast<std::int32_t>(
        (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[0])) << 24) |
        (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[1])) << 16) |
        (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[2])) << 8) |
        static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[3])));
}

// ---- Kafka frame stream helpers --------------------------------------------
seastar::future<> write_response(
    seastar::output_stream<char>& out,
    const std::vector<std::uint8_t>& response);

constexpr std::int16_t partition_missing() noexcept {
    return static_cast<std::int16_t>(protocol::ErrorCode::UnknownTopicOrPartition);
}

// Peek the 2-byte ApiKey from a request payload.
inline std::int16_t request_api_key(const std::vector<std::uint8_t>& payload) noexcept {
    if (payload.size() < 2) {
        return -1;
    }
    return static_cast<std::int16_t>(
        (static_cast<std::uint16_t>(payload[0]) << 8) |
        static_cast<std::uint16_t>(payload[1]));
}

// ---- Filesystem / segment naming -------------------------------------------
std::string sanitized_topic_name(std::string name);
std::string segment_prefix(const PartitionConfig& config);
std::string segment_name(const PartitionConfig& config, std::int64_t base_offset);
std::optional<std::int64_t> segment_base_offset_from_name(
    const PartitionConfig& config, const std::string& filename);

// ---- Replication wire codec ------------------------------------------------
ReplicaAppendRequest decode_replica_append(const std::vector<std::uint8_t>& payload);
std::vector<std::uint8_t> encode_replica_append(const ReplicaAppendRequest& request);
std::vector<std::uint8_t> encode_replica_append_response(
    std::int16_t error_code, std::int64_t log_end_offset);

ReplicaFetchRequest decode_replica_fetch(const std::vector<std::uint8_t>& payload);
std::vector<std::uint8_t> encode_replica_fetch(const ReplicaFetchRequest& request);
std::vector<std::uint8_t> encode_replica_fetch_response(const ReplicaFetchResult& result);
ReplicaFetchResult decode_replica_fetch_response(const std::vector<std::uint8_t>& payload);

ProducePartitionResult decode_replica_append_response(
    const ReplicaAppendRequest& request,
    const std::vector<std::uint8_t>& payload);

// ---- Reload validation -----------------------------------------------------
std::vector<PartitionConfig> all_partitions(const BrokerConfig& config);

// Throws std::runtime_error when the new config attempts a change that SIGHUP
// reload does not support (broker endpoints, partition deletion, shard
// migration, replica/ISR changes, etc.).
void validate_reload(const BrokerConfig& old_config, const BrokerConfig& new_config);

}  // namespace mkmq::detail

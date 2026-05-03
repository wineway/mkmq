#include "broker_shard_detail.hpp"

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>

namespace mkmq::detail {

// ---- Kafka frame stream helpers --------------------------------------------
seastar::future<> write_response(
    seastar::output_stream<char>& out,
    const std::vector<std::uint8_t>& response) {
    return out.write(reinterpret_cast<const char*>(response.data()), response.size()).then([&out] {
        return out.flush();
    });
}

// ---- Filesystem / segment naming -------------------------------------------
std::string sanitized_topic_name(std::string name) {
    for (char& ch : name) {
        if (ch == '/' || ch == ':') {
            ch = '_';
        }
    }
    return name;
}

std::string segment_prefix(const PartitionConfig& config) {
    return sanitized_topic_name(config.topic) + "-" + std::to_string(config.partition) + "-";
}

std::string segment_name(const PartitionConfig& config, std::int64_t base_offset) {
    std::ostringstream out;
    out << segment_prefix(config)
        << std::setw(20) << std::setfill('0') << base_offset
        << ".segment";
    return out.str();
}

std::optional<std::int64_t> segment_base_offset_from_name(
    const PartitionConfig& config,
    const std::string& filename) {
    const auto prefix = segment_prefix(config);
    constexpr std::string_view suffix = ".segment";
    if (filename.size() <= prefix.size() + suffix.size() ||
        filename.rfind(prefix, 0) != 0 ||
        filename.substr(filename.size() - suffix.size()) != suffix) {
        return std::nullopt;
    }
    const auto digits = filename.substr(prefix.size(), filename.size() - prefix.size() - suffix.size());
    if (digits.empty() || !std::all_of(digits.begin(), digits.end(), [](char ch) { return ch >= '0' && ch <= '9'; })) {
        return std::nullopt;
    }
    return std::stoll(digits);
}

// ---- Replication wire codec ------------------------------------------------
namespace {
void write_string(protocol::WireWriter& writer, const std::string& value) {
    writer.write_string(value);
}
}  // namespace

ReplicaAppendRequest decode_replica_append(const std::vector<std::uint8_t>& payload) {
    protocol::WireReader reader(payload);
    ReplicaAppendRequest request;
    request.topic = reader.read_string();
    request.partition = reader.read_int32();
    request.base_offset = reader.read_int64();
    request.leader_high_watermark = reader.read_int64();
    request.records = reader.read_bytes();
    return request;
}

std::vector<std::uint8_t> encode_replica_append(const ReplicaAppendRequest& request) {
    protocol::WireWriter writer;
    write_string(writer, request.topic);
    writer.write_int32(request.partition);
    writer.write_int64(request.base_offset);
    writer.write_int64(request.leader_high_watermark);
    writer.write_bytes(request.records);
    return writer.take_bytes();
}

std::vector<std::uint8_t> encode_replica_append_response(
    std::int16_t error_code, std::int64_t log_end_offset) {
    protocol::WireWriter writer;
    writer.write_int16(error_code);
    writer.write_int64(log_end_offset);
    return writer.take_bytes();
}

ReplicaFetchRequest decode_replica_fetch(const std::vector<std::uint8_t>& payload) {
    protocol::WireReader reader(payload);
    ReplicaFetchRequest request;
    request.topic = reader.read_string();
    request.partition = reader.read_int32();
    request.offset = reader.read_int64();
    request.max_bytes = reader.read_int32();
    return request;
}

std::vector<std::uint8_t> encode_replica_fetch(const ReplicaFetchRequest& request) {
    protocol::WireWriter writer;
    write_string(writer, request.topic);
    writer.write_int32(request.partition);
    writer.write_int64(request.offset);
    writer.write_int32(request.max_bytes);
    return writer.take_bytes();
}

std::vector<std::uint8_t> encode_replica_fetch_response(const ReplicaFetchResult& result) {
    protocol::WireWriter writer;
    writer.write_int16(result.error_code);
    writer.write_int64(result.leader_high_watermark);
    writer.write_int64(result.log_end_offset);
    writer.write_bytes(result.records);
    return writer.take_bytes();
}

ReplicaFetchResult decode_replica_fetch_response(const std::vector<std::uint8_t>& payload) {
    protocol::WireReader reader(payload);
    ReplicaFetchResult result;
    result.error_code = reader.read_int16();
    result.leader_high_watermark = reader.read_int64();
    result.log_end_offset = reader.read_int64();
    result.records = reader.read_bytes();
    return result;
}

ProducePartitionResult decode_replica_append_response(
    const ReplicaAppendRequest& request,
    const std::vector<std::uint8_t>& payload) {
    protocol::WireReader reader(payload);
    ProducePartitionResult result;
    result.topic = request.topic;
    result.partition = request.partition;
    result.error_code = reader.read_int16();
    result.base_offset = request.base_offset;
    result.log_start_offset = 0;
    (void) reader.read_int64();
    return result;
}

// ---- Reload validation -----------------------------------------------------
std::vector<PartitionConfig> all_partitions(const BrokerConfig& config) {
    std::vector<PartitionConfig> partitions;
    for (const auto& topic : config.topics) {
        partitions.insert(partitions.end(), topic.partitions.begin(), topic.partitions.end());
    }
    return partitions;
}

void validate_reload(const BrokerConfig& old_config, const BrokerConfig& new_config) {
    if (old_config.node_id != new_config.node_id) {
        throw std::runtime_error("SIGHUP reload cannot change node_id");
    }
    if (old_config.data_dir != new_config.data_dir) {
        throw std::runtime_error("SIGHUP reload cannot change data_dir");
    }
    if (old_config.segment_bytes != new_config.segment_bytes) {
        throw std::runtime_error("SIGHUP reload cannot change segment_bytes");
    }
    if (old_config.brokers.size() != new_config.brokers.size()) {
        throw std::runtime_error("SIGHUP reload cannot add or remove brokers yet");
    }
    for (const auto& broker : old_config.brokers) {
        const auto updated = find_broker(new_config, broker.id);
        if (!updated.has_value() ||
            updated->kafka_host != broker.kafka_host ||
            updated->kafka_port != broker.kafka_port ||
            updated->mercury_address != broker.mercury_address) {
            throw std::runtime_error("SIGHUP reload cannot change broker endpoints yet");
        }
    }
    for (const auto& partition : all_partitions(old_config)) {
        const auto updated = find_partition(new_config, partition.topic, partition.partition);
        if (!updated.has_value()) {
            throw std::runtime_error("SIGHUP reload cannot delete topic partitions");
        }
        if (updated->shard != partition.shard) {
            throw std::runtime_error("SIGHUP reload cannot migrate partitions across shards");
        }
        if (updated->replicas != partition.replicas || updated->isr != partition.isr) {
            throw std::runtime_error("SIGHUP reload cannot change replicas or ISR yet");
        }
        if (updated->migration_generation != partition.migration_generation ||
            updated->migration_target_shard != partition.migration_target_shard ||
            updated->migration_target_leader != partition.migration_target_leader) {
            throw std::runtime_error("SIGHUP reload accepts migration schema but does not execute migrations yet");
        }
    }
}

}  // namespace mkmq::detail

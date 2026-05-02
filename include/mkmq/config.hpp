#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace mkmq {

enum class Durability {
    Write,
    Fsync,
};

enum class FetchVisibility {
    HighWatermark,
    LeaderLocalEnd,
};

struct BrokerEndpoint {
    std::int32_t id{0};
    std::string kafka_host{"127.0.0.1"};
    std::uint16_t kafka_port{9092};
    std::string mercury_address{"ofi+tcp;ofi_rxm://127.0.0.1:3344"};
};

struct PartitionConfig {
    std::string topic;
    std::int32_t partition{0};
    unsigned shard{0};
    std::vector<std::int32_t> replicas;
    std::vector<std::int32_t> isr;
    Durability durability{Durability::Write};
    std::uint32_t queue_limit{8192};
    std::uint32_t max_frame_bytes{1024 * 1024};
    FetchVisibility fetch_visibility{FetchVisibility::HighWatermark};
    bool allow_follower_fetch{false};
    std::int32_t leader{0};
    std::uint64_t migration_generation{0};
    unsigned migration_target_shard{0};
    std::int32_t migration_target_leader{-1};
};

struct TopicConfig {
    std::string name;
    std::vector<PartitionConfig> partitions;
};

struct BrokerConfig {
    std::int32_t node_id{0};
    std::uint64_t cluster_generation{0};
    std::filesystem::path data_dir{"mkmq-data"};
    std::uint16_t metrics_port{9642};
    std::uint32_t fetch_max_wait_ms_cap{100};
    std::uint32_t produce_wait_ms_cap{100};
    std::uint64_t segment_bytes{1024ULL * 1024ULL * 1024ULL};
    std::vector<BrokerEndpoint> brokers;
    std::vector<TopicConfig> topics;
};

BrokerConfig load_config_file(const std::filesystem::path& path);
BrokerConfig load_config_yaml(const std::string& yaml);
std::optional<PartitionConfig> find_partition(
    const BrokerConfig& config,
    const std::string& topic,
    std::int32_t partition);
std::optional<BrokerEndpoint> find_broker(const BrokerConfig& config, std::int32_t node_id);

std::string to_string(Durability durability);
std::string to_string(FetchVisibility visibility);

}  // namespace mkmq

#include "mkmq/config.hpp"

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <set>
#include <stdexcept>

namespace mkmq {
namespace {

YAML::Node require_node(const YAML::Node& node, const char* field) {
    if (!node[field]) {
        throw std::runtime_error(std::string("config requires ") + field);
    }
    return node[field];
}

void require_partition_field(const YAML::Node& node, const std::string& topic, std::int32_t partition, const char* field) {
    if (!node[field]) {
        throw std::runtime_error("partition " + topic + "/" + std::to_string(partition) + " requires " + field);
    }
}

Durability parse_durability(const YAML::Node& node) {
    const auto value = node.as<std::string>();
    if (value == "write") {
        return Durability::Write;
    }
    if (value == "fsync") {
        return Durability::Fsync;
    }
    throw std::runtime_error("durability must be 'write' or 'fsync'");
}

FetchVisibility parse_visibility(const YAML::Node& node) {
    const auto value = node.as<std::string>();
    if (value == "high_watermark") {
        return FetchVisibility::HighWatermark;
    }
    if (value == "leader_local_end") {
        return FetchVisibility::LeaderLocalEnd;
    }
    throw std::runtime_error("fetch_visibility must be 'high_watermark' or 'leader_local_end'");
}

template <typename T>
std::vector<T> parse_vector(const YAML::Node& node, const char* field) {
    if (!node || !node.IsSequence()) {
        throw std::runtime_error(std::string(field) + " must be a YAML sequence");
    }
    std::vector<T> values;
    for (const auto& item : node) {
        values.push_back(item.as<T>());
    }
    return values;
}

void validate_config(const BrokerConfig& config) {
    std::set<std::int32_t> broker_ids;
    for (const auto& broker : config.brokers) {
        if (!broker_ids.insert(broker.id).second) {
            throw std::runtime_error("duplicate broker id: " + std::to_string(broker.id));
        }
        if (broker.kafka_host.empty() || broker.kafka_port == 0 || broker.mercury_address.empty()) {
            throw std::runtime_error("broker " + std::to_string(broker.id) + " requires kafka and Mercury endpoints");
        }
    }
    if (broker_ids.find(config.node_id) == broker_ids.end()) {
        throw std::runtime_error("node_id does not match any configured broker");
    }
    if (config.segment_bytes == 0) {
        throw std::runtime_error("segment_bytes must be positive");
    }
    std::set<std::pair<std::string, std::int32_t>> partitions;
    for (const auto& topic : config.topics) {
        if (topic.name.empty()) {
            throw std::runtime_error("topic name must not be empty");
        }
        for (const auto& partition : topic.partitions) {
            if (!partitions.insert({partition.topic, partition.partition}).second) {
                throw std::runtime_error("duplicate partition " + partition.topic + "/" + std::to_string(partition.partition));
            }
            if (partition.replicas.empty()) {
                throw std::runtime_error("partition " + partition.topic + "/" + std::to_string(partition.partition) + " requires at least one replica");
            }
            if (partition.isr.empty()) {
                throw std::runtime_error("partition " + partition.topic + "/" + std::to_string(partition.partition) + " requires at least one ISR");
            }
            const auto in_replicas = [&partition](std::int32_t id) {
                return std::find(partition.replicas.begin(), partition.replicas.end(), id) != partition.replicas.end();
            };
            if (!in_replicas(partition.leader)) {
                throw std::runtime_error("partition " + partition.topic + "/" + std::to_string(partition.partition) + " leader must be in replicas");
            }
            for (auto replica : partition.replicas) {
                if (broker_ids.find(replica) == broker_ids.end()) {
                    throw std::runtime_error("partition " + partition.topic + "/" + std::to_string(partition.partition) + " references unknown replica broker");
                }
            }
            for (auto isr : partition.isr) {
                if (!in_replicas(isr)) {
                    throw std::runtime_error("partition " + partition.topic + "/" + std::to_string(partition.partition) + " ISR must be a subset of replicas");
                }
            }
            if (partition.queue_limit == 0 || partition.max_frame_bytes == 0) {
                throw std::runtime_error("partition " + partition.topic + "/" + std::to_string(partition.partition) + " queue_limit and max_frame_bytes must be positive");
            }
            if (partition.migration_generation != 0 &&
                (partition.migration_target_leader < 0 || broker_ids.find(partition.migration_target_leader) == broker_ids.end())) {
                throw std::runtime_error("partition " + partition.topic + "/" + std::to_string(partition.partition) + " migration target_leader must reference a broker");
            }
        }
    }
}

}  // namespace

BrokerConfig parse_config_node(const YAML::Node& root) {
    BrokerConfig config;
    config.node_id = require_node(root, "node_id").as<std::int32_t>();
    config.cluster_generation = require_node(root, "cluster_generation").as<std::uint64_t>();
    config.data_dir = require_node(root, "data_dir").as<std::string>();
    config.metrics_port = root["metrics_port"] ? root["metrics_port"].as<std::uint16_t>() : 9642;
    config.fetch_max_wait_ms_cap = root["fetch_max_wait_ms_cap"] ? root["fetch_max_wait_ms_cap"].as<std::uint32_t>() : 100;
    config.produce_wait_ms_cap = root["produce_wait_ms_cap"] ? root["produce_wait_ms_cap"].as<std::uint32_t>() : 100;
    config.segment_bytes = root["segment_bytes"] ? root["segment_bytes"].as<std::uint64_t>() : 1024ULL * 1024ULL * 1024ULL;

    if (!root["brokers"] || !root["brokers"].IsSequence()) {
        throw std::runtime_error("config requires brokers sequence");
    }
    for (const auto& node : root["brokers"]) {
        BrokerEndpoint broker;
        broker.id = require_node(node, "id").as<std::int32_t>();
        broker.kafka_host = require_node(node, "kafka_host").as<std::string>();
        broker.kafka_port = require_node(node, "kafka_port").as<std::uint16_t>();
        broker.mercury_address = require_node(node, "mercury_address").as<std::string>();
        config.brokers.push_back(std::move(broker));
    }

    if (!root["topics"] || !root["topics"].IsSequence()) {
        throw std::runtime_error("config requires topics sequence");
    }
    for (const auto& topic_node : root["topics"]) {
        TopicConfig topic;
        topic.name = topic_node["name"].as<std::string>();
        if (!topic_node["partitions"] || !topic_node["partitions"].IsSequence()) {
            throw std::runtime_error("topic " + topic.name + " requires partitions sequence");
        }
        for (const auto& partition_node : topic_node["partitions"]) {
            PartitionConfig partition;
            partition.topic = topic.name;
            partition.partition = require_node(partition_node, "id").as<std::int32_t>();
            require_partition_field(partition_node, topic.name, partition.partition, "shard");
            require_partition_field(partition_node, topic.name, partition.partition, "leader");
            require_partition_field(partition_node, topic.name, partition.partition, "replicas");
            require_partition_field(partition_node, topic.name, partition.partition, "isr");
            require_partition_field(partition_node, topic.name, partition.partition, "durability");
            require_partition_field(partition_node, topic.name, partition.partition, "queue_limit");
            require_partition_field(partition_node, topic.name, partition.partition, "max_frame_bytes");
            require_partition_field(partition_node, topic.name, partition.partition, "fetch_visibility");
            require_partition_field(partition_node, topic.name, partition.partition, "allow_follower_fetch");
            partition.shard = partition_node["shard"].as<unsigned>();
            partition.replicas = parse_vector<std::int32_t>(partition_node["replicas"], "replicas");
            partition.isr = parse_vector<std::int32_t>(partition_node["isr"], "isr");
            partition.leader = partition_node["leader"].as<std::int32_t>();
            partition.durability = parse_durability(partition_node["durability"]);
            partition.queue_limit = partition_node["queue_limit"].as<std::uint32_t>();
            partition.max_frame_bytes = partition_node["max_frame_bytes"].as<std::uint32_t>();
            partition.fetch_visibility = parse_visibility(partition_node["fetch_visibility"]);
            partition.allow_follower_fetch = partition_node["allow_follower_fetch"].as<bool>();
            if (partition_node["migration"]) {
                const auto migration = partition_node["migration"];
                partition.migration_generation = require_node(migration, "generation").as<std::uint64_t>();
                partition.migration_target_shard = require_node(migration, "target_shard").as<unsigned>();
                partition.migration_target_leader = require_node(migration, "target_leader").as<std::int32_t>();
            }
            topic.partitions.push_back(std::move(partition));
        }
        config.topics.push_back(std::move(topic));
    }

    validate_config(config);
    return config;
}

BrokerConfig load_config_file(const std::filesystem::path& path) {
    return parse_config_node(YAML::LoadFile(path.string()));
}

BrokerConfig load_config_yaml(const std::string& yaml) {
    return parse_config_node(YAML::Load(yaml));
}

std::optional<PartitionConfig> find_partition(
    const BrokerConfig& config,
    const std::string& topic,
    std::int32_t partition) {
    for (const auto& topic_config : config.topics) {
        if (topic_config.name != topic) {
            continue;
        }
        const auto it = std::find_if(
            topic_config.partitions.begin(),
            topic_config.partitions.end(),
            [partition](const PartitionConfig& candidate) {
                return candidate.partition == partition;
            });
        if (it != topic_config.partitions.end()) {
            return *it;
        }
    }
    return std::nullopt;
}

std::optional<BrokerEndpoint> find_broker(const BrokerConfig& config, std::int32_t node_id) {
    const auto it = std::find_if(
        config.brokers.begin(),
        config.brokers.end(),
        [node_id](const BrokerEndpoint& broker) {
            return broker.id == node_id;
        });
    if (it == config.brokers.end()) {
        return std::nullopt;
    }
    return *it;
}

std::string to_string(Durability durability) {
    return durability == Durability::Fsync ? "fsync" : "write";
}

std::string to_string(FetchVisibility visibility) {
    return visibility == FetchVisibility::LeaderLocalEnd ? "leader_local_end" : "high_watermark";
}

}  // namespace mkmq

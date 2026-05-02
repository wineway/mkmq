#include "mkmq/generated/kafka_codec.hpp"

// Generated from Kafka JSON schemas at commit faa8b4870f for the fixed mkmq
// API surface. Regenerate with tools/generate_kafka_schemas.py.

#include <utility>

namespace mkmq::generated::codec {

MetadataRequest decode_metadata_request(protocol::WireReader& reader) {
    MetadataRequest request;
    const auto topic_count = reader.read_array_count(true, true);
    request.all_topics = topic_count < 0;
    for (std::int32_t i = 0; i < topic_count; ++i) {
        MetadataRequestTopic topic;
        topic.name = reader.read_compact_string();
        reader.skip_tagged_fields();
        request.topics.push_back(std::move(topic));
    }
    request.allow_auto_topic_creation = reader.read_bool();
    request.include_cluster_authorized_operations = reader.read_bool();
    request.include_topic_authorized_operations = reader.read_bool();
    reader.skip_tagged_fields();
    return request;
}

ProduceRequest decode_produce_request(protocol::WireReader& reader) {
    ProduceRequest request;
    request.transactional_id = reader.read_compact_nullable_string();
    request.acks = reader.read_int16();
    request.timeout_ms = reader.read_int32();
    const auto topic_count = reader.read_array_count(true);
    for (std::int32_t i = 0; i < topic_count; ++i) {
        ProduceRequestTopic topic;
        topic.name = reader.read_compact_string();
        const auto partition_count = reader.read_array_count(true);
        for (std::int32_t j = 0; j < partition_count; ++j) {
            ProduceRequestPartition partition;
            partition.partition_index = reader.read_int32();
            partition.records = reader.read_compact_nullable_bytes().value_or(std::vector<std::uint8_t>{});
            reader.skip_tagged_fields();
            topic.partitions.push_back(std::move(partition));
        }
        reader.skip_tagged_fields();
        request.topics.push_back(std::move(topic));
    }
    reader.skip_tagged_fields();
    return request;
}

FetchRequest decode_fetch_request(protocol::WireReader& reader) {
    FetchRequest request;
    request.replica_id = reader.read_int32();
    request.max_wait_ms = reader.read_int32();
    request.min_bytes = reader.read_int32();
    request.max_bytes = reader.read_int32();
    request.isolation_level = reader.read_int8();
    request.session_id = reader.read_int32();
    request.session_epoch = reader.read_int32();
    const auto topic_count = reader.read_array_count(true);
    for (std::int32_t i = 0; i < topic_count; ++i) {
        FetchRequestTopic topic;
        topic.topic = reader.read_compact_string();
        const auto partition_count = reader.read_array_count(true);
        for (std::int32_t j = 0; j < partition_count; ++j) {
            FetchRequestPartition partition;
            partition.partition = reader.read_int32();
            partition.current_leader_epoch = reader.read_int32();
            partition.fetch_offset = reader.read_int64();
            partition.last_fetched_epoch = reader.read_int32();
            partition.log_start_offset = reader.read_int64();
            partition.partition_max_bytes = reader.read_int32();
            reader.skip_tagged_fields();
            topic.partitions.push_back(std::move(partition));
        }
        reader.skip_tagged_fields();
        request.topics.push_back(std::move(topic));
    }
    const auto forgotten_topic_count = reader.read_array_count(true);
    for (std::int32_t i = 0; i < forgotten_topic_count; ++i) {
        (void) reader.read_compact_string();
        const auto partition_count = reader.read_array_count(true);
        for (std::int32_t j = 0; j < partition_count; ++j) {
            (void) reader.read_int32();
        }
        reader.skip_tagged_fields();
    }
    request.rack_id = reader.read_compact_string();
    reader.skip_tagged_fields();
    return request;
}

ListOffsetsRequest decode_list_offsets_request(protocol::WireReader& reader) {
    ListOffsetsRequest request;
    request.replica_id = reader.read_int32();
    request.isolation_level = reader.read_int8();
    const auto topic_count = reader.read_array_count(true);
    for (std::int32_t i = 0; i < topic_count; ++i) {
        ListOffsetsRequestTopic topic;
        topic.name = reader.read_compact_string();
        const auto partition_count = reader.read_array_count(true);
        for (std::int32_t j = 0; j < partition_count; ++j) {
            ListOffsetsRequestPartition partition;
            partition.partition_index = reader.read_int32();
            partition.current_leader_epoch = reader.read_int32();
            partition.timestamp = reader.read_int64();
            reader.skip_tagged_fields();
            topic.partitions.push_back(std::move(partition));
        }
        reader.skip_tagged_fields();
        request.topics.push_back(std::move(topic));
    }
    reader.skip_tagged_fields();
    return request;
}

std::vector<std::uint8_t> encode_api_versions_response(const ApiVersionsResponse& response) {
    protocol::WireWriter writer;
    writer.write_int16(response.error_code);
    writer.write_array_count(static_cast<std::int32_t>(response.api_keys.size()), true);
    for (const auto& api : response.api_keys) {
        writer.write_int16(api.api_key);
        writer.write_int16(api.min_version);
        writer.write_int16(api.max_version);
        writer.write_empty_tagged_fields();
    }
    writer.write_int32(response.throttle_time_ms);
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}

std::vector<std::uint8_t> encode_metadata_response(const MetadataResponse& response) {
    protocol::WireWriter writer;
    writer.write_int32(response.throttle_time_ms);
    writer.write_array_count(static_cast<std::int32_t>(response.brokers.size()), true);
    for (const auto& broker : response.brokers) {
        writer.write_int32(broker.node_id);
        writer.write_compact_string(broker.host);
        writer.write_int32(broker.port);
        writer.write_compact_nullable_string(broker.rack);
        writer.write_empty_tagged_fields();
    }
    writer.write_compact_nullable_string(response.cluster_id);
    writer.write_int32(response.controller_id);
    writer.write_array_count(static_cast<std::int32_t>(response.topics.size()), true);
    for (const auto& topic : response.topics) {
        writer.write_int16(topic.error_code);
        writer.write_compact_string(topic.name);
        writer.write_bool(topic.is_internal);
        writer.write_array_count(static_cast<std::int32_t>(topic.partitions.size()), true);
        for (const auto& partition : topic.partitions) {
            writer.write_int16(partition.error_code);
            writer.write_int32(partition.partition_index);
            writer.write_int32(partition.leader_id);
            writer.write_int32(partition.leader_epoch);
            writer.write_array_count(static_cast<std::int32_t>(partition.replica_nodes.size()), true);
            for (auto node : partition.replica_nodes) {
                writer.write_int32(node);
            }
            writer.write_array_count(static_cast<std::int32_t>(partition.isr_nodes.size()), true);
            for (auto node : partition.isr_nodes) {
                writer.write_int32(node);
            }
            writer.write_array_count(static_cast<std::int32_t>(partition.offline_replicas.size()), true);
            for (auto node : partition.offline_replicas) {
                writer.write_int32(node);
            }
            writer.write_empty_tagged_fields();
        }
        writer.write_int32(topic.topic_authorized_operations);
        writer.write_empty_tagged_fields();
    }
    writer.write_int32(response.cluster_authorized_operations);
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}

std::vector<std::uint8_t> encode_produce_response(const ProduceResponse& response) {
    protocol::WireWriter writer;
    writer.write_array_count(static_cast<std::int32_t>(response.topics.size()), true);
    for (const auto& topic : response.topics) {
        writer.write_compact_string(topic.name);
        writer.write_array_count(static_cast<std::int32_t>(topic.partitions.size()), true);
        for (const auto& partition : topic.partitions) {
            writer.write_int32(partition.partition_index);
            writer.write_int16(partition.error_code);
            writer.write_int64(partition.base_offset);
            writer.write_int64(partition.log_append_time_ms);
            writer.write_int64(partition.log_start_offset);
            writer.write_array_count(static_cast<std::int32_t>(partition.record_errors.size()), true);
            for (auto error : partition.record_errors) {
                writer.write_int32(error);
                writer.write_compact_nullable_string(std::nullopt);
                writer.write_empty_tagged_fields();
            }
            writer.write_compact_nullable_string(partition.error_message);
            writer.write_empty_tagged_fields();
        }
        writer.write_empty_tagged_fields();
    }
    writer.write_int32(response.throttle_time_ms);
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}

std::vector<std::uint8_t> encode_fetch_response(const FetchResponse& response) {
    protocol::WireWriter writer;
    writer.write_int32(response.throttle_time_ms);
    writer.write_int16(response.error_code);
    writer.write_int32(response.session_id);
    writer.write_array_count(static_cast<std::int32_t>(response.topics.size()), true);
    for (const auto& topic : response.topics) {
        writer.write_compact_string(topic.topic);
        writer.write_array_count(static_cast<std::int32_t>(topic.partitions.size()), true);
        for (const auto& partition : topic.partitions) {
            writer.write_int32(partition.partition_index);
            writer.write_int16(partition.error_code);
            writer.write_int64(partition.high_watermark);
            writer.write_int64(partition.last_stable_offset);
            writer.write_int64(partition.log_start_offset);
            if (!partition.aborted_transactions.has_value()) {
                writer.write_nullable_array_count(-1, true);
            } else {
                writer.write_array_count(static_cast<std::int32_t>(partition.aborted_transactions->size()), true);
                for (auto producer_id : *partition.aborted_transactions) {
                    writer.write_int64(producer_id);
                    writer.write_int64(-1);
                    writer.write_empty_tagged_fields();
                }
            }
            writer.write_int32(partition.preferred_read_replica);
            writer.write_compact_nullable_bytes(partition.records);
            writer.write_empty_tagged_fields();
        }
        writer.write_empty_tagged_fields();
    }
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}

std::vector<std::uint8_t> encode_list_offsets_response(const ListOffsetsResponse& response) {
    protocol::WireWriter writer;
    writer.write_int32(response.throttle_time_ms);
    writer.write_array_count(static_cast<std::int32_t>(response.topics.size()), true);
    for (const auto& topic : response.topics) {
        writer.write_compact_string(topic.name);
        writer.write_array_count(static_cast<std::int32_t>(topic.partitions.size()), true);
        for (const auto& partition : topic.partitions) {
            writer.write_int32(partition.partition_index);
            writer.write_int16(partition.error_code);
            writer.write_int64(partition.timestamp);
            writer.write_int64(partition.offset);
            writer.write_int32(partition.leader_epoch);
            writer.write_empty_tagged_fields();
        }
        writer.write_empty_tagged_fields();
    }
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}

}  // namespace mkmq::generated::codec

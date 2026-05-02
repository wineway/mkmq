#pragma once

// Generated from Kafka JSON schemas at commit faa8b4870f for the fixed mkmq
// API surface. Regenerate with tools/generate_kafka_schemas.py.

#include "mkmq/protocol/wire.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mkmq::generated::codec {

struct ApiVersionsResponseApi {
    std::int16_t api_key{0};
    std::int16_t min_version{0};
    std::int16_t max_version{0};
};

struct ApiVersionsResponse {
    std::int16_t error_code{0};
    std::vector<ApiVersionsResponseApi> api_keys;
    std::int32_t throttle_time_ms{0};
};

struct MetadataRequestTopic {
    std::string name;
};

struct MetadataRequest {
    bool all_topics{false};
    std::vector<MetadataRequestTopic> topics;
    bool allow_auto_topic_creation{false};
    bool include_cluster_authorized_operations{false};
    bool include_topic_authorized_operations{false};
};

struct MetadataResponseBroker {
    std::int32_t node_id{0};
    std::string host;
    std::int32_t port{0};
    std::optional<std::string> rack;
};

struct MetadataResponsePartition {
    std::int16_t error_code{0};
    std::int32_t partition_index{0};
    std::int32_t leader_id{-1};
    std::int32_t leader_epoch{0};
    std::vector<std::int32_t> replica_nodes;
    std::vector<std::int32_t> isr_nodes;
    std::vector<std::int32_t> offline_replicas;
};

struct MetadataResponseTopic {
    std::int16_t error_code{0};
    std::string name;
    bool is_internal{false};
    std::vector<MetadataResponsePartition> partitions;
    std::int32_t topic_authorized_operations{-2147483648};
};

struct MetadataResponse {
    std::int32_t throttle_time_ms{0};
    std::vector<MetadataResponseBroker> brokers;
    std::optional<std::string> cluster_id;
    std::int32_t controller_id{-1};
    std::vector<MetadataResponseTopic> topics;
    std::int32_t cluster_authorized_operations{-2147483648};
};

struct ProduceRequestPartition {
    std::int32_t partition_index{0};
    std::vector<std::uint8_t> records;
};

struct ProduceRequestTopic {
    std::string name;
    std::vector<ProduceRequestPartition> partitions;
};

struct ProduceRequest {
    std::optional<std::string> transactional_id;
    std::int16_t acks{1};
    std::int32_t timeout_ms{0};
    std::vector<ProduceRequestTopic> topics;
};

struct ProduceResponsePartition {
    std::int32_t partition_index{0};
    std::int16_t error_code{0};
    std::int64_t base_offset{0};
    std::int64_t log_append_time_ms{-1};
    std::int64_t log_start_offset{0};
    std::vector<std::int32_t> record_errors;
    std::optional<std::string> error_message;
};

struct ProduceResponseTopic {
    std::string name;
    std::vector<ProduceResponsePartition> partitions;
};

struct ProduceResponse {
    std::vector<ProduceResponseTopic> topics;
    std::int32_t throttle_time_ms{0};
};

struct FetchRequestPartition {
    std::int32_t partition{0};
    std::int32_t current_leader_epoch{-1};
    std::int64_t fetch_offset{0};
    std::int32_t last_fetched_epoch{-1};
    std::int64_t log_start_offset{-1};
    std::int32_t partition_max_bytes{0};
};

struct FetchRequestTopic {
    std::string topic;
    std::vector<FetchRequestPartition> partitions;
};

struct FetchRequest {
    std::int32_t replica_id{-1};
    std::int32_t max_wait_ms{0};
    std::int32_t min_bytes{0};
    std::int32_t max_bytes{0};
    std::int8_t isolation_level{0};
    std::int32_t session_id{0};
    std::int32_t session_epoch{-1};
    std::vector<FetchRequestTopic> topics;
    std::string rack_id;
};

struct FetchResponsePartition {
    std::int32_t partition_index{0};
    std::int16_t error_code{0};
    std::int64_t high_watermark{0};
    std::int64_t last_stable_offset{0};
    std::int64_t log_start_offset{0};
    std::optional<std::vector<std::int64_t>> aborted_transactions;
    std::int32_t preferred_read_replica{-1};
    std::vector<std::uint8_t> records;
};

struct FetchResponseTopic {
    std::string topic;
    std::vector<FetchResponsePartition> partitions;
};

struct FetchResponse {
    std::int32_t throttle_time_ms{0};
    std::int16_t error_code{0};
    std::int32_t session_id{0};
    std::vector<FetchResponseTopic> topics;
};

struct ListOffsetsRequestPartition {
    std::int32_t partition_index{0};
    std::int32_t current_leader_epoch{-1};
    std::int64_t timestamp{-1};
};

struct ListOffsetsRequestTopic {
    std::string name;
    std::vector<ListOffsetsRequestPartition> partitions;
};

struct ListOffsetsRequest {
    std::int32_t replica_id{-1};
    std::int8_t isolation_level{0};
    std::vector<ListOffsetsRequestTopic> topics;
};

struct ListOffsetsResponsePartition {
    std::int32_t partition_index{0};
    std::int16_t error_code{0};
    std::int64_t timestamp{-1};
    std::int64_t offset{0};
    std::int32_t leader_epoch{0};
};

struct ListOffsetsResponseTopic {
    std::string name;
    std::vector<ListOffsetsResponsePartition> partitions;
};

struct ListOffsetsResponse {
    std::int32_t throttle_time_ms{0};
    std::vector<ListOffsetsResponseTopic> topics;
};

MetadataRequest decode_metadata_request(protocol::WireReader& reader);
ProduceRequest decode_produce_request(protocol::WireReader& reader);
FetchRequest decode_fetch_request(protocol::WireReader& reader);
ListOffsetsRequest decode_list_offsets_request(protocol::WireReader& reader);

std::vector<std::uint8_t> encode_api_versions_response(const ApiVersionsResponse& response);
std::vector<std::uint8_t> encode_metadata_response(const MetadataResponse& response);
std::vector<std::uint8_t> encode_produce_response(const ProduceResponse& response);
std::vector<std::uint8_t> encode_fetch_response(const FetchResponse& response);
std::vector<std::uint8_t> encode_list_offsets_response(const ListOffsetsResponse& response);

}  // namespace mkmq::generated::codec

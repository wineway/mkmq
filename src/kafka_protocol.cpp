#include "mkmq/kafka_protocol.hpp"

#include "mkmq/broker_shard.hpp"
#include "mkmq/crc32c.hpp"
#include "mkmq/generated/kafka_codec.hpp"
#include "mkmq/generated/kafka_schema.hpp"
#include "mkmq/protocol/header.hpp"
#include "mkmq/protocol/wire.hpp"

#include <seastar/core/future-util.hh>

#include <algorithm>
#include <cstddef>
#include <map>
#include <utility>

namespace mkmq {
namespace {

using protocol::ApiKey;
using protocol::ErrorCode;
using protocol::RequestHeader;
using protocol::WireReader;

constexpr std::int32_t kNoAuthorizedOps = -2147483648;

void write_i32_be(std::vector<std::uint8_t>& out, std::size_t pos, std::int32_t value) {
    const auto raw = static_cast<std::uint32_t>(value);
    out[pos] = static_cast<std::uint8_t>((raw >> 24) & 0xffU);
    out[pos + 1] = static_cast<std::uint8_t>((raw >> 16) & 0xffU);
    out[pos + 2] = static_cast<std::uint8_t>((raw >> 8) & 0xffU);
    out[pos + 3] = static_cast<std::uint8_t>(raw & 0xffU);
}

void write_i64_be(std::vector<std::uint8_t>& out, std::size_t pos, std::int64_t value) {
    const auto raw = static_cast<std::uint64_t>(value);
    for (int shift = 56; shift >= 0; shift -= 8) {
        out[pos++] = static_cast<std::uint8_t>((raw >> shift) & 0xffU);
    }
}

std::int32_t read_i32_be(const std::vector<std::uint8_t>& in, std::size_t pos) {
    return static_cast<std::int32_t>(
        (static_cast<std::uint32_t>(in[pos]) << 24) |
        (static_cast<std::uint32_t>(in[pos + 1]) << 16) |
        (static_cast<std::uint32_t>(in[pos + 2]) << 8) |
        static_cast<std::uint32_t>(in[pos + 3]));
}

generated::codec::ApiVersionsResponse api_versions_response(ErrorCode error) {
    generated::codec::ApiVersionsResponse response;
    response.error_code = static_cast<std::int16_t>(error);
    const auto& apis = generated::fixed_kafka_apis();
    for (const auto& api : apis) {
        response.api_keys.push_back({
            api.api_key,
            api.min_version,
            api.max_version,
        });
    }
    return response;
}

generated::codec::MetadataResponse metadata_response(BrokerShard& broker, const generated::codec::MetadataRequest& request) {
    generated::codec::MetadataResponse response;
    response.cluster_id = std::optional<std::string>("mkmq-cluster");
    response.controller_id = broker.config().node_id;
    response.cluster_authorized_operations = kNoAuthorizedOps;
    for (const auto& node : broker.config().brokers) {
        response.brokers.push_back({
            node.id,
            node.kafka_host,
            node.kafka_port,
            std::nullopt,
        });
    }

    std::vector<TopicConfig> topics;
    if (request.all_topics) {
        topics = broker.topics();
    } else {
        for (const auto& requested : request.topics) {
            TopicConfig topic;
            topic.name = requested.name;
            for (const auto& configured : broker.topics()) {
                if (configured.name == requested.name) {
                    topic = configured;
                    break;
                }
            }
            topics.push_back(std::move(topic));
        }
    }

    for (const auto& topic : topics) {
        const bool missing = topic.partitions.empty();
        generated::codec::MetadataResponseTopic out_topic;
        out_topic.error_code = missing ? static_cast<std::int16_t>(ErrorCode::UnknownTopicOrPartition) : 0;
        out_topic.name = topic.name;
        out_topic.is_internal = false;
        out_topic.topic_authorized_operations = kNoAuthorizedOps;
        for (const auto& partition : topic.partitions) {
            const auto error_code = missing
                ? static_cast<std::int16_t>(ErrorCode::UnknownTopicOrPartition)
                : static_cast<std::int16_t>(0);
            out_topic.partitions.push_back({
                error_code,
                partition.partition,
                missing ? -1 : partition.leader,
                0,
                partition.replicas,
                partition.isr,
                {},
            });
        }
        response.topics.push_back(std::move(out_topic));
    }
    return response;
}

std::int64_t read_i64_be_at(const std::vector<std::uint8_t>& in, std::size_t pos) {
    std::uint64_t raw = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        raw = (raw << 8) | static_cast<std::uint64_t>(in[pos + i]);
    }
    return static_cast<std::int64_t>(raw);
}

std::int16_t read_i16_be_at(const std::vector<std::uint8_t>& in, std::size_t pos) {
    return static_cast<std::int16_t>(
        (static_cast<std::uint16_t>(in[pos]) << 8) |
        static_cast<std::uint16_t>(in[pos + 1]));
}

bool has_idempotent_record_batch(const std::vector<std::uint8_t>& records) {
    std::size_t pos = 0;
    while (pos + 61 <= records.size()) {
        const auto batch_length = read_i32_be(records, pos + 8);
        if (batch_length <= 0) {
            return false;
        }
        const auto total_batch_size = static_cast<std::size_t>(batch_length) + 12;
        if (pos + total_batch_size > records.size()) {
            return false;
        }
        const auto producer_id = read_i64_be_at(records, pos + 43);
        const auto producer_epoch = read_i16_be_at(records, pos + 51);
        const auto base_sequence = read_i32_be(records, pos + 53);
        if (producer_id != -1 || producer_epoch != -1 || base_sequence != -1) {
            return true;
        }
        pos += total_batch_size;
    }
    return false;
}

ProduceRequest to_broker_produce_request(const generated::codec::ProduceRequest& wire) {
    ProduceRequest request;
    request.acks = wire.acks;
    request.timeout_ms = wire.timeout_ms;
    if (wire.transactional_id.has_value()) {
        request.rejected_error_code = static_cast<std::int16_t>(ErrorCode::InvalidRequest);
    }
    for (const auto& topic : wire.topics) {
        for (const auto& wire_partition : topic.partitions) {
            ProducePartitionRequest partition;
            partition.topic = topic.name;
            partition.partition = wire_partition.partition_index;
            partition.records = wire_partition.records;
            if (request.rejected_error_code == 0 && has_idempotent_record_batch(partition.records)) {
                request.rejected_error_code = static_cast<std::int16_t>(ErrorCode::InvalidRequest);
            }
            request.partitions.push_back(std::move(partition));
        }
    }
    return request;
}

std::vector<ProducePartitionResult> rejected_produce_results(const ProduceRequest& request) {
    std::vector<ProducePartitionResult> results;
    results.reserve(request.partitions.size());
    for (const auto& partition : request.partitions) {
        ProducePartitionResult result;
        result.topic = partition.topic;
        result.partition = partition.partition;
        result.error_code = request.rejected_error_code;
        results.push_back(std::move(result));
    }
    return results;
}

generated::codec::ProduceResponse produce_response(const std::vector<ProducePartitionResult>& responses) {
    generated::codec::ProduceResponse response;
    std::map<std::string, std::vector<ProducePartitionResult>> grouped;
    for (const auto& response : responses) {
        grouped[response.topic].push_back(response);
    }
    for (const auto& [topic, partitions] : grouped) {
        generated::codec::ProduceResponseTopic out_topic;
        out_topic.name = topic;
        for (const auto& partition : partitions) {
            out_topic.partitions.push_back({
                partition.partition,
                partition.error_code,
                partition.base_offset,
                -1,
                partition.log_start_offset,
                {},
                std::nullopt,
            });
        }
        response.topics.push_back(std::move(out_topic));
    }
    return response;
}

std::vector<FetchPartitionRequest> to_broker_fetch_request(const generated::codec::FetchRequest& wire) {
    std::vector<FetchPartitionRequest> partitions;
    for (const auto& topic : wire.topics) {
        for (const auto& wire_partition : topic.partitions) {
            FetchPartitionRequest partition;
            partition.topic = topic.topic;
            partition.partition = wire_partition.partition;
            partition.offset = wire_partition.fetch_offset;
            partition.max_wait_ms = wire.max_wait_ms;
            partition.min_bytes = wire.min_bytes;
            partition.max_bytes = wire_partition.partition_max_bytes;
            if (partition.max_bytes <= 0) {
                partition.max_bytes = wire.max_bytes;
            }
            partitions.push_back(std::move(partition));
        }
    }
    return partitions;
}

generated::codec::FetchResponse fetch_response(
    const std::vector<std::pair<FetchPartitionRequest, FetchPartitionResult>>& responses) {
    generated::codec::FetchResponse response;
    std::map<std::string, std::vector<std::pair<FetchPartitionRequest, FetchPartitionResult>>> grouped;
    for (const auto& response : responses) {
        grouped[response.first.topic].push_back(response);
    }
    for (const auto& [topic, partitions] : grouped) {
        generated::codec::FetchResponseTopic out_topic;
        out_topic.topic = topic;
        for (const auto& [request, result] : partitions) {
            out_topic.partitions.push_back({
                request.partition,
                result.error_code,
                result.high_watermark,
                result.high_watermark,
                result.log_start_offset,
                std::nullopt,
                -1,
                result.records,
            });
        }
        response.topics.push_back(std::move(out_topic));
    }
    return response;
}

std::vector<ListOffsetPartitionRequest> to_broker_list_offsets_request(const generated::codec::ListOffsetsRequest& wire) {
    std::vector<ListOffsetPartitionRequest> partitions;
    for (const auto& topic : wire.topics) {
        for (const auto& wire_partition : topic.partitions) {
            ListOffsetPartitionRequest partition;
            partition.topic = topic.name;
            partition.partition = wire_partition.partition_index;
            partition.timestamp = wire_partition.timestamp;
            partitions.push_back(std::move(partition));
        }
    }
    return partitions;
}

generated::codec::ListOffsetsResponse list_offsets_response(const std::vector<ListOffsetPartitionResult>& responses) {
    generated::codec::ListOffsetsResponse response;
    std::map<std::string, std::vector<ListOffsetPartitionResult>> grouped;
    for (const auto& response : responses) {
        grouped[response.topic].push_back(response);
    }
    for (const auto& [topic, partitions] : grouped) {
        generated::codec::ListOffsetsResponseTopic out_topic;
        out_topic.name = topic;
        for (const auto& partition : partitions) {
            out_topic.partitions.push_back({
                partition.partition,
                partition.error_code,
                -1,
                partition.selected,
                0,
            });
        }
        response.topics.push_back(std::move(out_topic));
    }
    return response;
}

}  // namespace

KafkaProtocol::KafkaProtocol(BrokerShard& broker)
    : broker_(broker) {}

seastar::future<std::optional<std::vector<std::uint8_t>>> KafkaProtocol::handle_frame(
    std::vector<std::uint8_t> payload) {
    WireReader reader(payload);
    RequestHeader header = protocol::parse_request_header(reader);

    if (header.api_key == ApiKey::ApiVersions) {
        const bool supported = protocol::is_supported(ApiKey::ApiVersions, header.api_version);
        RequestHeader response_header = header;
        return seastar::make_ready_future<std::optional<std::vector<std::uint8_t>>>(
            protocol::make_response_frame(
                response_header,
                generated::codec::encode_api_versions_response(
                    api_versions_response(supported ? ErrorCode::None : ErrorCode::UnsupportedVersion))));
    }

    if (!protocol::is_supported(header.api_key, header.api_version)) {
        throw protocol::ProtocolError("unsupported fixed Kafka API version for " + protocol::api_name(header.api_key));
    }

    switch (header.api_key) {
        case ApiKey::Metadata: {
            const auto request = generated::codec::decode_metadata_request(reader);
            return seastar::make_ready_future<std::optional<std::vector<std::uint8_t>>>(
                protocol::make_response_frame(header, generated::codec::encode_metadata_response(metadata_response(broker_, request))));
        }
        case ApiKey::Produce: {
            auto request = to_broker_produce_request(generated::codec::decode_produce_request(reader));
            if (request.rejected_error_code != 0) {
                if (request.acks == 0) {
                    return seastar::make_ready_future<std::optional<std::vector<std::uint8_t>>>(std::nullopt);
                }
                return seastar::make_ready_future<std::optional<std::vector<std::uint8_t>>>(
                    protocol::make_response_frame(header, generated::codec::encode_produce_response(produce_response(rejected_produce_results(request)))));
            }
            return seastar::do_with(std::move(request), std::vector<ProducePartitionResult>{}, [this, header](auto& produce, auto& results) {
                results.reserve(produce.partitions.size());
                return seastar::do_for_each(produce.partitions, [this, &produce, &results](ProducePartitionRequest& partition) {
                    return broker_.append(std::move(partition), produce.acks, produce.timeout_ms).then([&results](ProducePartitionResult result) {
                        results.push_back(std::move(result));
                    });
                }).then([header, &produce, &results] {
                    if (produce.acks == 0) {
                        return seastar::make_ready_future<std::optional<std::vector<std::uint8_t>>>(std::nullopt);
                    }
                    return seastar::make_ready_future<std::optional<std::vector<std::uint8_t>>>(
                        protocol::make_response_frame(header, generated::codec::encode_produce_response(produce_response(results))));
                });
            });
        }
        case ApiKey::Fetch: {
            auto partitions = to_broker_fetch_request(generated::codec::decode_fetch_request(reader));
            return seastar::do_with(std::move(partitions), std::vector<std::pair<FetchPartitionRequest, FetchPartitionResult>>{}, [this, header](auto& fetches, auto& results) {
                results.reserve(fetches.size());
                return seastar::do_for_each(fetches, [this, &results](FetchPartitionRequest& partition) {
                    FetchPartitionRequest original = partition;
                    return broker_.fetch(std::move(partition)).then([original = std::move(original), &results](FetchPartitionResult result) mutable {
                        results.emplace_back(std::move(original), std::move(result));
                    });
                }).then([header, &results] {
                    return seastar::make_ready_future<std::optional<std::vector<std::uint8_t>>>(
                        protocol::make_response_frame(header, generated::codec::encode_fetch_response(fetch_response(results))));
                });
            });
        }
        case ApiKey::ListOffsets: {
            auto partitions = to_broker_list_offsets_request(generated::codec::decode_list_offsets_request(reader));
            return seastar::do_with(std::move(partitions), std::vector<ListOffsetPartitionResult>{}, [this, header](auto& offsets, auto& results) {
                results.reserve(offsets.size());
                return seastar::do_for_each(offsets, [this, &results](ListOffsetPartitionRequest& partition) {
                    return broker_.list_offsets(std::move(partition)).then([&results](ListOffsetPartitionResult result) {
                        results.push_back(std::move(result));
                    });
                }).then([header, &results] {
                    return seastar::make_ready_future<std::optional<std::vector<std::uint8_t>>>(
                        protocol::make_response_frame(header, generated::codec::encode_list_offsets_response(list_offsets_response(results))));
                });
            });
        }
        default:
            throw protocol::ProtocolError("Kafka API is outside the fixed first release scope");
    }
}

std::vector<std::uint8_t> rewrite_record_batch_offsets(
    const std::vector<std::uint8_t>& records,
    std::int64_t base_offset) {
    std::vector<std::uint8_t> rewritten = records;
    std::size_t pos = 0;
    std::int64_t next_base = base_offset;
    while (pos + 57 <= rewritten.size()) {
        const auto batch_length = read_i32_be(rewritten, pos + 8);
        if (batch_length <= 0) {
            break;
        }
        const auto batch_size = static_cast<std::size_t>(batch_length) + 12;
        if (pos + batch_size > rewritten.size()) {
            break;
        }
        write_i64_be(rewritten, pos, next_base);
        const auto crc = crc32c(rewritten.data() + pos + 21, batch_size - 21);
        write_i32_be(rewritten, pos + 17, static_cast<std::int32_t>(crc));
        const auto last_offset_delta = read_i32_be(rewritten, pos + 23);
        next_base += std::max<std::int64_t>(1, static_cast<std::int64_t>(last_offset_delta) + 1);
        pos += batch_size;
    }
    return rewritten;
}

}  // namespace mkmq

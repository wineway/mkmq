#!/usr/bin/env python3
"""Generate the fixed Kafka schema manifest and codec artifacts used by mkmq.

The runtime protocol structs are deliberately narrowed to one flexible version
per API. This generator validates the selected Kafka JSON schema files, records
the exact Kafka source commit, and renders the checked-in fixed codec artifacts
when requested.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
from dataclasses import dataclass


FIXED = {
    "ApiVersions": 3,
    "Metadata": 9,
    "Produce": 9,
    "Fetch": 12,
    "ListOffsets": 6,
}

ADVERTISED_MIN = {
    "ApiVersions": 0,
    "Produce": 3,
    "Fetch": 4,
}

EXPECTED_FIELDS = {
    "ApiVersionsRequest": ["ClientSoftwareName", "ClientSoftwareVersion"],
    "ApiVersionsResponse": ["ErrorCode", "ApiKeys", "ThrottleTimeMs"],
    "MetadataRequest": [
        "Topics",
        "AllowAutoTopicCreation",
        "IncludeClusterAuthorizedOperations",
        "IncludeTopicAuthorizedOperations",
    ],
    "MetadataResponse": [
        "ThrottleTimeMs",
        "Brokers",
        "ClusterId",
        "ControllerId",
        "Topics",
        "ClusterAuthorizedOperations",
    ],
    "ProduceRequest": ["TransactionalId", "Acks", "TimeoutMs", "TopicData"],
    "ProduceResponse": ["Responses", "ThrottleTimeMs"],
    "FetchRequest": [
        "ReplicaId",
        "MaxWaitMs",
        "MinBytes",
        "MaxBytes",
        "IsolationLevel",
        "SessionId",
        "SessionEpoch",
        "Topics",
        "ForgottenTopicsData",
        "RackId",
    ],
    "FetchResponse": ["ThrottleTimeMs", "ErrorCode", "SessionId", "Responses"],
    "ListOffsetsRequest": ["ReplicaId", "IsolationLevel", "Topics"],
    "ListOffsetsResponse": ["ThrottleTimeMs", "Topics"],
}


@dataclass(frozen=True)
class FixedApi:
    api_key: int
    name: str
    version: int
    min_version: int
    request_flexible: bool
    response_flexible: bool


def version_in_range(expr: str, version: int) -> bool:
    if not expr:
        return False
    for part in (piece.strip() for piece in expr.split(",")):
        if not part or part == "none":
            continue
        if part == str(version):
            return True
        if part.endswith("+"):
            try:
                if version >= int(part[:-1]):
                    return True
            except ValueError:
                continue
        if "-" in part:
            start, end = part.split("-", 1)
            try:
                if int(start) <= version <= int(end):
                    return True
            except ValueError:
                continue
    return False


def schema_fields(schema: dict, version: int) -> list[str]:
    out = []
    for field in schema.get("fields", []):
        if version_in_range(field.get("versions", ""), version):
            out.append(field["name"])
    return out


def validate_schema_shape(name: str, schema: dict, version: int) -> None:
    schema_name = schema["name"]
    expected = EXPECTED_FIELDS.get(schema_name)
    if expected is None:
        raise RuntimeError(f"missing expected field shape for {schema_name}")
    actual = schema_fields(schema, version)
    missing = [field for field in expected if field not in actual]
    if missing:
        raise RuntimeError(f"{schema_name} v{version} missing expected fields: {', '.join(missing)}")
    if schema["name"] != f"{name}{'Request' if schema_name.endswith('Request') else 'Response'}":
        raise RuntimeError(f"unexpected schema name {schema_name} for {name}")


def render_schema_header() -> str:
    return """#pragma once

// Generated from Kafka JSON schemas. Regenerate with tools/generate_kafka_schemas.py.

#include <cstdint>
#include <string_view>
#include <vector>

namespace mkmq::generated {

struct FixedKafkaApi {
    std::int16_t api_key;
    std::string_view name;
    std::int16_t min_version;
    std::int16_t max_version;
    bool flexible;
};

std::string_view kafka_schema_source_commit();
const std::vector<FixedKafkaApi>& fixed_kafka_apis();

}  // namespace mkmq::generated
"""


def render_schema_source(commit: str, apis: list[FixedApi]) -> str:
    rows = "\n".join(
        f'        {{{api.api_key}, "{api.name}", {api.min_version}, {api.version}, {"true" if api.request_flexible and api.response_flexible else "false"}}},'
        for api in apis
    )
    return f"""#include "mkmq/generated/kafka_schema.hpp"

// Generated from Kafka JSON schemas at commit {commit}. Regenerate with
// tools/generate_kafka_schemas.py.

namespace mkmq::generated {{

std::string_view kafka_schema_source_commit() {{
    return "{commit}";
}}

const std::vector<FixedKafkaApi>& fixed_kafka_apis() {{
    static const std::vector<FixedKafkaApi> apis{{
{rows}
    }};
    return apis;
}}

}}  // namespace mkmq::generated
"""


def render_codec_header(commit: str) -> str:
    return f"""#pragma once

// Generated from Kafka JSON schemas at commit {commit} for the fixed mkmq
// API surface. Regenerate with tools/generate_kafka_schemas.py.

#include "mkmq/protocol/wire.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mkmq::generated::codec {{

struct ApiVersionsResponseApi {{
    std::int16_t api_key{{0}};
    std::int16_t min_version{{0}};
    std::int16_t max_version{{0}};
}};

struct ApiVersionsResponse {{
    std::int16_t error_code{{0}};
    std::vector<ApiVersionsResponseApi> api_keys;
    std::int32_t throttle_time_ms{{0}};
}};

struct MetadataRequestTopic {{
    std::string name;
}};

struct MetadataRequest {{
    bool all_topics{{false}};
    std::vector<MetadataRequestTopic> topics;
    bool allow_auto_topic_creation{{false}};
    bool include_cluster_authorized_operations{{false}};
    bool include_topic_authorized_operations{{false}};
}};

struct MetadataResponseBroker {{
    std::int32_t node_id{{0}};
    std::string host;
    std::int32_t port{{0}};
    std::optional<std::string> rack;
}};

struct MetadataResponsePartition {{
    std::int16_t error_code{{0}};
    std::int32_t partition_index{{0}};
    std::int32_t leader_id{{-1}};
    std::int32_t leader_epoch{{0}};
    std::vector<std::int32_t> replica_nodes;
    std::vector<std::int32_t> isr_nodes;
    std::vector<std::int32_t> offline_replicas;
}};

struct MetadataResponseTopic {{
    std::int16_t error_code{{0}};
    std::string name;
    bool is_internal{{false}};
    std::vector<MetadataResponsePartition> partitions;
    std::int32_t topic_authorized_operations{{-2147483648}};
}};

struct MetadataResponse {{
    std::int32_t throttle_time_ms{{0}};
    std::vector<MetadataResponseBroker> brokers;
    std::optional<std::string> cluster_id;
    std::int32_t controller_id{{-1}};
    std::vector<MetadataResponseTopic> topics;
    std::int32_t cluster_authorized_operations{{-2147483648}};
}};

struct ProduceRequestPartition {{
    std::int32_t partition_index{{0}};
    std::vector<std::uint8_t> records;
}};

struct ProduceRequestTopic {{
    std::string name;
    std::vector<ProduceRequestPartition> partitions;
}};

struct ProduceRequest {{
    std::optional<std::string> transactional_id;
    std::int16_t acks{{1}};
    std::int32_t timeout_ms{{0}};
    std::vector<ProduceRequestTopic> topics;
}};

struct ProduceResponsePartition {{
    std::int32_t partition_index{{0}};
    std::int16_t error_code{{0}};
    std::int64_t base_offset{{0}};
    std::int64_t log_append_time_ms{{-1}};
    std::int64_t log_start_offset{{0}};
    std::vector<std::int32_t> record_errors;
    std::optional<std::string> error_message;
}};

struct ProduceResponseTopic {{
    std::string name;
    std::vector<ProduceResponsePartition> partitions;
}};

struct ProduceResponse {{
    std::vector<ProduceResponseTopic> topics;
    std::int32_t throttle_time_ms{{0}};
}};

struct FetchRequestPartition {{
    std::int32_t partition{{0}};
    std::int32_t current_leader_epoch{{-1}};
    std::int64_t fetch_offset{{0}};
    std::int32_t last_fetched_epoch{{-1}};
    std::int64_t log_start_offset{{-1}};
    std::int32_t partition_max_bytes{{0}};
}};

struct FetchRequestTopic {{
    std::string topic;
    std::vector<FetchRequestPartition> partitions;
}};

struct FetchRequest {{
    std::int32_t replica_id{{-1}};
    std::int32_t max_wait_ms{{0}};
    std::int32_t min_bytes{{0}};
    std::int32_t max_bytes{{0}};
    std::int8_t isolation_level{{0}};
    std::int32_t session_id{{0}};
    std::int32_t session_epoch{{-1}};
    std::vector<FetchRequestTopic> topics;
    std::string rack_id;
}};

struct FetchResponsePartition {{
    std::int32_t partition_index{{0}};
    std::int16_t error_code{{0}};
    std::int64_t high_watermark{{0}};
    std::int64_t last_stable_offset{{0}};
    std::int64_t log_start_offset{{0}};
    std::optional<std::vector<std::int64_t>> aborted_transactions;
    std::int32_t preferred_read_replica{{-1}};
    std::vector<std::uint8_t> records;
}};

struct FetchResponseTopic {{
    std::string topic;
    std::vector<FetchResponsePartition> partitions;
}};

struct FetchResponse {{
    std::int32_t throttle_time_ms{{0}};
    std::int16_t error_code{{0}};
    std::int32_t session_id{{0}};
    std::vector<FetchResponseTopic> topics;
}};

struct ListOffsetsRequestPartition {{
    std::int32_t partition_index{{0}};
    std::int32_t current_leader_epoch{{-1}};
    std::int64_t timestamp{{-1}};
}};

struct ListOffsetsRequestTopic {{
    std::string name;
    std::vector<ListOffsetsRequestPartition> partitions;
}};

struct ListOffsetsRequest {{
    std::int32_t replica_id{{-1}};
    std::int8_t isolation_level{{0}};
    std::vector<ListOffsetsRequestTopic> topics;
}};

struct ListOffsetsResponsePartition {{
    std::int32_t partition_index{{0}};
    std::int16_t error_code{{0}};
    std::int64_t timestamp{{-1}};
    std::int64_t offset{{0}};
    std::int32_t leader_epoch{{0}};
}};

struct ListOffsetsResponseTopic {{
    std::string name;
    std::vector<ListOffsetsResponsePartition> partitions;
}};

struct ListOffsetsResponse {{
    std::int32_t throttle_time_ms{{0}};
    std::vector<ListOffsetsResponseTopic> topics;
}};

MetadataRequest decode_metadata_request(protocol::WireReader& reader);
ProduceRequest decode_produce_request(protocol::WireReader& reader);
FetchRequest decode_fetch_request(protocol::WireReader& reader);
ListOffsetsRequest decode_list_offsets_request(protocol::WireReader& reader);

std::vector<std::uint8_t> encode_api_versions_response(const ApiVersionsResponse& response);
std::vector<std::uint8_t> encode_metadata_response(const MetadataResponse& response);
std::vector<std::uint8_t> encode_produce_response(const ProduceResponse& response);
std::vector<std::uint8_t> encode_fetch_response(const FetchResponse& response);
std::vector<std::uint8_t> encode_list_offsets_response(const ListOffsetsResponse& response);

}}  // namespace mkmq::generated::codec
"""


def render_codec_source(commit: str) -> str:
    return f"""#include "mkmq/generated/kafka_codec.hpp"

// Generated from Kafka JSON schemas at commit {commit} for the fixed mkmq
// API surface. Regenerate with tools/generate_kafka_schemas.py.

#include <utility>

namespace mkmq::generated::codec {{

MetadataRequest decode_metadata_request(protocol::WireReader& reader) {{
    MetadataRequest request;
    const auto topic_count = reader.read_array_count(true, true);
    request.all_topics = topic_count < 0;
    for (std::int32_t i = 0; i < topic_count; ++i) {{
        MetadataRequestTopic topic;
        topic.name = reader.read_compact_string();
        reader.skip_tagged_fields();
        request.topics.push_back(std::move(topic));
    }}
    request.allow_auto_topic_creation = reader.read_bool();
    request.include_cluster_authorized_operations = reader.read_bool();
    request.include_topic_authorized_operations = reader.read_bool();
    reader.skip_tagged_fields();
    return request;
}}

ProduceRequest decode_produce_request(protocol::WireReader& reader) {{
    ProduceRequest request;
    request.transactional_id = reader.read_compact_nullable_string();
    request.acks = reader.read_int16();
    request.timeout_ms = reader.read_int32();
    const auto topic_count = reader.read_array_count(true);
    for (std::int32_t i = 0; i < topic_count; ++i) {{
        ProduceRequestTopic topic;
        topic.name = reader.read_compact_string();
        const auto partition_count = reader.read_array_count(true);
        for (std::int32_t j = 0; j < partition_count; ++j) {{
            ProduceRequestPartition partition;
            partition.partition_index = reader.read_int32();
            partition.records = reader.read_compact_nullable_bytes().value_or(std::vector<std::uint8_t>{{}});
            reader.skip_tagged_fields();
            topic.partitions.push_back(std::move(partition));
        }}
        reader.skip_tagged_fields();
        request.topics.push_back(std::move(topic));
    }}
    reader.skip_tagged_fields();
    return request;
}}

FetchRequest decode_fetch_request(protocol::WireReader& reader) {{
    FetchRequest request;
    request.replica_id = reader.read_int32();
    request.max_wait_ms = reader.read_int32();
    request.min_bytes = reader.read_int32();
    request.max_bytes = reader.read_int32();
    request.isolation_level = reader.read_int8();
    request.session_id = reader.read_int32();
    request.session_epoch = reader.read_int32();
    const auto topic_count = reader.read_array_count(true);
    for (std::int32_t i = 0; i < topic_count; ++i) {{
        FetchRequestTopic topic;
        topic.topic = reader.read_compact_string();
        const auto partition_count = reader.read_array_count(true);
        for (std::int32_t j = 0; j < partition_count; ++j) {{
            FetchRequestPartition partition;
            partition.partition = reader.read_int32();
            partition.current_leader_epoch = reader.read_int32();
            partition.fetch_offset = reader.read_int64();
            partition.last_fetched_epoch = reader.read_int32();
            partition.log_start_offset = reader.read_int64();
            partition.partition_max_bytes = reader.read_int32();
            reader.skip_tagged_fields();
            topic.partitions.push_back(std::move(partition));
        }}
        reader.skip_tagged_fields();
        request.topics.push_back(std::move(topic));
    }}
    const auto forgotten_topic_count = reader.read_array_count(true);
    for (std::int32_t i = 0; i < forgotten_topic_count; ++i) {{
        (void) reader.read_compact_string();
        const auto partition_count = reader.read_array_count(true);
        for (std::int32_t j = 0; j < partition_count; ++j) {{
            (void) reader.read_int32();
        }}
        reader.skip_tagged_fields();
    }}
    request.rack_id = reader.read_compact_string();
    reader.skip_tagged_fields();
    return request;
}}

ListOffsetsRequest decode_list_offsets_request(protocol::WireReader& reader) {{
    ListOffsetsRequest request;
    request.replica_id = reader.read_int32();
    request.isolation_level = reader.read_int8();
    const auto topic_count = reader.read_array_count(true);
    for (std::int32_t i = 0; i < topic_count; ++i) {{
        ListOffsetsRequestTopic topic;
        topic.name = reader.read_compact_string();
        const auto partition_count = reader.read_array_count(true);
        for (std::int32_t j = 0; j < partition_count; ++j) {{
            ListOffsetsRequestPartition partition;
            partition.partition_index = reader.read_int32();
            partition.current_leader_epoch = reader.read_int32();
            partition.timestamp = reader.read_int64();
            reader.skip_tagged_fields();
            topic.partitions.push_back(std::move(partition));
        }}
        reader.skip_tagged_fields();
        request.topics.push_back(std::move(topic));
    }}
    reader.skip_tagged_fields();
    return request;
}}

std::vector<std::uint8_t> encode_api_versions_response(const ApiVersionsResponse& response) {{
    protocol::WireWriter writer;
    writer.write_int16(response.error_code);
    writer.write_array_count(static_cast<std::int32_t>(response.api_keys.size()), true);
    for (const auto& api : response.api_keys) {{
        writer.write_int16(api.api_key);
        writer.write_int16(api.min_version);
        writer.write_int16(api.max_version);
        writer.write_empty_tagged_fields();
    }}
    writer.write_int32(response.throttle_time_ms);
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}}

std::vector<std::uint8_t> encode_metadata_response(const MetadataResponse& response) {{
    protocol::WireWriter writer;
    writer.write_int32(response.throttle_time_ms);
    writer.write_array_count(static_cast<std::int32_t>(response.brokers.size()), true);
    for (const auto& broker : response.brokers) {{
        writer.write_int32(broker.node_id);
        writer.write_compact_string(broker.host);
        writer.write_int32(broker.port);
        writer.write_compact_nullable_string(broker.rack);
        writer.write_empty_tagged_fields();
    }}
    writer.write_compact_nullable_string(response.cluster_id);
    writer.write_int32(response.controller_id);
    writer.write_array_count(static_cast<std::int32_t>(response.topics.size()), true);
    for (const auto& topic : response.topics) {{
        writer.write_int16(topic.error_code);
        writer.write_compact_string(topic.name);
        writer.write_bool(topic.is_internal);
        writer.write_array_count(static_cast<std::int32_t>(topic.partitions.size()), true);
        for (const auto& partition : topic.partitions) {{
            writer.write_int16(partition.error_code);
            writer.write_int32(partition.partition_index);
            writer.write_int32(partition.leader_id);
            writer.write_int32(partition.leader_epoch);
            writer.write_array_count(static_cast<std::int32_t>(partition.replica_nodes.size()), true);
            for (auto node : partition.replica_nodes) {{
                writer.write_int32(node);
            }}
            writer.write_array_count(static_cast<std::int32_t>(partition.isr_nodes.size()), true);
            for (auto node : partition.isr_nodes) {{
                writer.write_int32(node);
            }}
            writer.write_array_count(static_cast<std::int32_t>(partition.offline_replicas.size()), true);
            for (auto node : partition.offline_replicas) {{
                writer.write_int32(node);
            }}
            writer.write_empty_tagged_fields();
        }}
        writer.write_int32(topic.topic_authorized_operations);
        writer.write_empty_tagged_fields();
    }}
    writer.write_int32(response.cluster_authorized_operations);
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}}

std::vector<std::uint8_t> encode_produce_response(const ProduceResponse& response) {{
    protocol::WireWriter writer;
    writer.write_array_count(static_cast<std::int32_t>(response.topics.size()), true);
    for (const auto& topic : response.topics) {{
        writer.write_compact_string(topic.name);
        writer.write_array_count(static_cast<std::int32_t>(topic.partitions.size()), true);
        for (const auto& partition : topic.partitions) {{
            writer.write_int32(partition.partition_index);
            writer.write_int16(partition.error_code);
            writer.write_int64(partition.base_offset);
            writer.write_int64(partition.log_append_time_ms);
            writer.write_int64(partition.log_start_offset);
            writer.write_array_count(static_cast<std::int32_t>(partition.record_errors.size()), true);
            for (auto error : partition.record_errors) {{
                writer.write_int32(error);
                writer.write_compact_nullable_string(std::nullopt);
                writer.write_empty_tagged_fields();
            }}
            writer.write_compact_nullable_string(partition.error_message);
            writer.write_empty_tagged_fields();
        }}
        writer.write_empty_tagged_fields();
    }}
    writer.write_int32(response.throttle_time_ms);
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}}

std::vector<std::uint8_t> encode_fetch_response(const FetchResponse& response) {{
    protocol::WireWriter writer;
    writer.write_int32(response.throttle_time_ms);
    writer.write_int16(response.error_code);
    writer.write_int32(response.session_id);
    writer.write_array_count(static_cast<std::int32_t>(response.topics.size()), true);
    for (const auto& topic : response.topics) {{
        writer.write_compact_string(topic.topic);
        writer.write_array_count(static_cast<std::int32_t>(topic.partitions.size()), true);
        for (const auto& partition : topic.partitions) {{
            writer.write_int32(partition.partition_index);
            writer.write_int16(partition.error_code);
            writer.write_int64(partition.high_watermark);
            writer.write_int64(partition.last_stable_offset);
            writer.write_int64(partition.log_start_offset);
            if (!partition.aborted_transactions.has_value()) {{
                writer.write_nullable_array_count(-1, true);
            }} else {{
                writer.write_array_count(static_cast<std::int32_t>(partition.aborted_transactions->size()), true);
                for (auto producer_id : *partition.aborted_transactions) {{
                    writer.write_int64(producer_id);
                    writer.write_int64(-1);
                    writer.write_empty_tagged_fields();
                }}
            }}
            writer.write_int32(partition.preferred_read_replica);
            writer.write_compact_nullable_bytes(partition.records);
            writer.write_empty_tagged_fields();
        }}
        writer.write_empty_tagged_fields();
    }}
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}}

std::vector<std::uint8_t> encode_list_offsets_response(const ListOffsetsResponse& response) {{
    protocol::WireWriter writer;
    writer.write_int32(response.throttle_time_ms);
    writer.write_array_count(static_cast<std::int32_t>(response.topics.size()), true);
    for (const auto& topic : response.topics) {{
        writer.write_compact_string(topic.name);
        writer.write_array_count(static_cast<std::int32_t>(topic.partitions.size()), true);
        for (const auto& partition : topic.partitions) {{
            writer.write_int32(partition.partition_index);
            writer.write_int16(partition.error_code);
            writer.write_int64(partition.timestamp);
            writer.write_int64(partition.offset);
            writer.write_int32(partition.leader_epoch);
            writer.write_empty_tagged_fields();
        }}
        writer.write_empty_tagged_fields();
    }}
    writer.write_empty_tagged_fields();
    return writer.take_bytes();
}}

}}  // namespace mkmq::generated::codec
"""


def write_if_requested(path: str | None, text: str) -> None:
    if path:
        pathlib.Path(path).write_text(text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-root", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--schema-header")
    parser.add_argument("--schema-source")
    parser.add_argument("--codec-header")
    parser.add_argument("--codec-source")
    args = parser.parse_args()

    root = pathlib.Path(args.kafka_root)
    schema_dir = root / "clients/src/main/resources/common/message"
    commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "--short=10", "HEAD"], text=True).strip()

    apis = []
    for name, version in FIXED.items():
        request = json.loads((schema_dir / f"{name}Request.json").read_text())
        response = json.loads((schema_dir / f"{name}Response.json").read_text())
        for kind, schema in (("request", request), ("response", response)):
            if not version_in_range(schema.get("validVersions", ""), version):
                raise RuntimeError(f"{name} {kind} schema does not support v{version}")
            validate_schema_shape(name, schema, version)
        request_flexible = version_in_range(request.get("flexibleVersions", ""), version)
        response_flexible = version_in_range(response.get("flexibleVersions", ""), version)
        if not request_flexible or not response_flexible:
            raise RuntimeError(f"{name} v{version} must be flexible on request and response")
        apis.append(FixedApi(request["apiKey"], name, version, ADVERTISED_MIN.get(name, version), request_flexible, response_flexible))

    out = pathlib.Path(args.out)
    out.write_text(
        "// Generated from Kafka schemas at commit " + commit + "\n"
        + "\n".join(
            f"// {api.api_key}: {api.name} v{api.version} request-flex={api.request_flexible} response-flex={api.response_flexible}"
            for api in apis)
        + "\n",
        encoding="utf-8",
    )

    write_if_requested(args.schema_header, render_schema_header())
    write_if_requested(args.schema_source, render_schema_source(commit, apis))
    write_if_requested(args.codec_header, render_codec_header(commit))
    write_if_requested(args.codec_source, render_codec_source(commit))


if __name__ == "__main__":
    main()

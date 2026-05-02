#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace mkmq::protocol {

enum class ApiKey : std::int16_t {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    ApiVersions = 18,
};

enum class ErrorCode : std::int16_t {
    None = 0,
    OffsetOutOfRange = 1,
    UnknownTopicOrPartition = 3,
    InvalidRequest = 42,
    UnsupportedVersion = 35,
    NotLeaderOrFollower = 6,
    RequestTimedOut = 7,
    NotEnoughReplicas = 19,
    RecordListTooLarge = 18,
    PolicyViolation = 44,
};

struct ApiVersionRange {
    ApiKey key;
    std::int16_t min_version;
    std::int16_t max_version;
};

bool is_known_api(std::int16_t api_key);
std::string api_name(ApiKey key);
std::int16_t api_id(ApiKey key);
ApiKey api_key_from_id(std::int16_t id);

std::vector<ApiVersionRange> supported_api_versions();
bool is_supported(ApiKey key, std::int16_t version);
bool is_flexible(ApiKey key, std::int16_t version);
std::int16_t fixed_version(ApiKey key);
std::int16_t request_header_version(ApiKey key, std::int16_t version);
std::int16_t response_header_version(ApiKey key, std::int16_t version);

}  // namespace mkmq::protocol

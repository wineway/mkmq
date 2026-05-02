#include "mkmq/protocol/api.hpp"

#include <algorithm>
#include <stdexcept>

namespace mkmq::protocol {

namespace {

struct ApiInfo {
    ApiKey key;
    const char* name;
    std::int16_t min_version;
    std::int16_t max_version;
    std::int16_t flexible_start;
};

constexpr ApiInfo kApis[] = {
    {ApiKey::Produce, "Produce", 9, 9, 9},
    {ApiKey::Fetch, "Fetch", 12, 12, 12},
    {ApiKey::ListOffsets, "ListOffsets", 6, 6, 6},
    {ApiKey::Metadata, "Metadata", 9, 9, 9},
    {ApiKey::ApiVersions, "ApiVersions", 0, 3, 3},
};

const ApiInfo* find_info(ApiKey key) {
    const auto it = std::find_if(std::begin(kApis), std::end(kApis), [key](const ApiInfo& info) {
        return info.key == key;
    });
    return it == std::end(kApis) ? nullptr : &*it;
}

}  // namespace

bool is_known_api(std::int16_t api_key) {
    return std::any_of(std::begin(kApis), std::end(kApis), [api_key](const ApiInfo& info) {
        return static_cast<std::int16_t>(info.key) == api_key;
    });
}

std::string api_name(ApiKey key) {
    const ApiInfo* info = find_info(key);
    return info == nullptr ? "Unknown" : info->name;
}

std::int16_t api_id(ApiKey key) {
    return static_cast<std::int16_t>(key);
}

ApiKey api_key_from_id(std::int16_t id) {
    if (!is_known_api(id)) {
        throw std::invalid_argument("unknown Kafka API key");
    }
    return static_cast<ApiKey>(id);
}

std::vector<ApiVersionRange> supported_api_versions() {
    std::vector<ApiVersionRange> versions;
    versions.reserve(std::size(kApis));
    for (const ApiInfo& info : kApis) {
        versions.push_back({info.key, info.min_version, info.max_version});
    }
    return versions;
}

bool is_supported(ApiKey key, std::int16_t version) {
    const ApiInfo* info = find_info(key);
    return info != nullptr && version >= info->min_version && version <= info->max_version;
}

bool is_flexible(ApiKey key, std::int16_t version) {
    const ApiInfo* info = find_info(key);
    return info != nullptr && version >= info->flexible_start;
}

std::int16_t fixed_version(ApiKey key) {
    const ApiInfo* info = find_info(key);
    if (info == nullptr) {
        throw std::invalid_argument("unknown Kafka API key");
    }
    return info->max_version;
}

std::int16_t request_header_version(ApiKey key, std::int16_t version) {
    return is_flexible(key, version) ? 2 : 1;
}

std::int16_t response_header_version(ApiKey key, std::int16_t version) {
    if (key == ApiKey::ApiVersions) {
        return 0;
    }
    return is_flexible(key, version) ? 1 : 0;
}

}  // namespace mkmq::protocol

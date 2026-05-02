#pragma once

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

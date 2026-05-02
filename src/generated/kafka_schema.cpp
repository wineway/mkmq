#include "mkmq/generated/kafka_schema.hpp"

// Generated from Kafka JSON schemas at commit faa8b4870f. Regenerate with
// tools/generate_kafka_schemas.py.

namespace mkmq::generated {

std::string_view kafka_schema_source_commit() {
    return "faa8b4870f";
}

const std::vector<FixedKafkaApi>& fixed_kafka_apis() {
    static const std::vector<FixedKafkaApi> apis{
        {18, "ApiVersions", 0, 3, true},
        {3, "Metadata", 9, 9, true},
        {0, "Produce", 3, 9, true},
        {1, "Fetch", 4, 12, true},
        {2, "ListOffsets", 6, 6, true},
    };
    return apis;
}

}  // namespace mkmq::generated

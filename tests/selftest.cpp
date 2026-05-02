#include "mkmq/config.hpp"
#include "mkmq/generated/kafka_schema.hpp"
#include "mkmq/kafka_protocol.hpp"
#include "mkmq/protocol/api.hpp"
#include "mkmq/protocol/header.hpp"
#include "mkmq/protocol/wire.hpp"

#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>

#include <cassert>

namespace {

std::vector<std::uint8_t> minimal_record_batch() {
    std::vector<std::uint8_t> batch(61, 0);
    batch[8] = 0;
    batch[9] = 0;
    batch[10] = 0;
    batch[11] = 49;
    batch[16] = 2;
    batch[26] = 0;
    batch[57] = 0;
    batch[58] = 0;
    batch[59] = 0;
    batch[60] = 0;
    return batch;
}

void wire_test() {
    mkmq::protocol::WireWriter writer;
    writer.write_int16(18);
    writer.write_int16(3);
    writer.write_int32(42);
    writer.write_nullable_string(std::optional<std::string>("client-a"));
    writer.write_empty_tagged_fields();

    mkmq::protocol::WireReader reader(writer.bytes());
    const auto header = mkmq::protocol::parse_request_header(reader);
    assert(header.api_key == mkmq::protocol::ApiKey::ApiVersions);
    assert(header.api_version == 3);
    assert(header.correlation_id == 42);
    assert(reader.empty());
}

void schema_test() {
    assert(mkmq::generated::kafka_schema_source_commit() == "faa8b4870f");
    assert(mkmq::protocol::is_supported(mkmq::protocol::ApiKey::Produce, 9));
    assert(!mkmq::protocol::is_supported(mkmq::protocol::ApiKey::Produce, 8));
    assert(mkmq::generated::fixed_kafka_apis().size() == 5);
}

void rewrite_test() {
    const auto rewritten = mkmq::rewrite_record_batch_offsets(minimal_record_batch(), 123);
    assert(rewritten[0] == 0);
    assert(rewritten[7] == 123);
    assert(rewritten[17] != 0 || rewritten[18] != 0 || rewritten[19] != 0 || rewritten[20] != 0);
}

void config_test() {
    const auto config = mkmq::load_config_yaml(
        "node_id: 0\n"
        "cluster_generation: 1\n"
        "data_dir: /tmp/mkmq-selftest\n"
        "brokers:\n"
        "  - id: 0\n"
        "    kafka_host: 127.0.0.1\n"
        "    kafka_port: 19092\n"
        "    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3344\n"
        "topics:\n"
        "  - name: orders\n"
        "    partitions:\n"
        "      - id: 0\n"
        "        shard: 0\n"
        "        leader: 0\n"
        "        replicas: [0]\n"
        "        isr: [0]\n"
        "        durability: write\n"
        "        queue_limit: 8192\n"
        "        max_frame_bytes: 1048576\n"
        "        fetch_visibility: high_watermark\n"
        "        allow_follower_fetch: false\n");
    assert(config.cluster_generation == 1);
    assert(mkmq::find_partition(config, "orders", 0).has_value());
}

}  // namespace

int main(int argc, char** argv) {
    seastar::app_template app;
    return app.run(argc, argv, [] {
        wire_test();
        schema_test();
        rewrite_test();
        config_test();
        return seastar::make_ready_future<>();
    });
}

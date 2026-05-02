#include "mkmq/broker_shard.hpp"

#include "mkmq/protocol/api.hpp"
#include "mkmq/protocol/wire.hpp"

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/core/with_timeout.hh>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <exception>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>

namespace mkmq {
namespace {

constexpr std::uint64_t kLogFrameAlignment = 4096;
constexpr char kLogMagic[8] = {'M', 'K', 'M', 'Q', 'L', 'O', 'G', '1'};
constexpr char kIndexMagic[8] = {'M', 'K', 'M', 'Q', 'I', 'D', 'X', '1'};
constexpr std::uint64_t kIndexEntrySize = 4096;
constexpr std::int32_t kReplicaCatchupMaxBytes = 1024 * 1024;

std::uint64_t round_up(std::uint64_t value, std::uint64_t alignment) {
    return ((value + alignment - 1) / alignment) * alignment;
}

void put_i32(std::uint8_t* out, std::int32_t value) {
    std::memcpy(out, &value, sizeof(value));
}

void put_i64(std::uint8_t* out, std::int64_t value) {
    std::memcpy(out, &value, sizeof(value));
}

std::int32_t get_i32(const char* in) {
    std::int32_t value = 0;
    std::memcpy(&value, in, sizeof(value));
    return value;
}

std::int64_t get_i64(const char* in) {
    std::int64_t value = 0;
    std::memcpy(&value, in, sizeof(value));
    return value;
}

std::int32_t decode_frame_size(const char* bytes) {
    return static_cast<std::int32_t>(
        (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[0])) << 24) |
        (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[1])) << 16) |
        (static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[2])) << 8) |
        static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[3])));
}

seastar::future<> write_response(seastar::output_stream<char>& out, const std::vector<std::uint8_t>& response) {
    return out.write(reinterpret_cast<const char*>(response.data()), response.size()).then([&out] {
        return out.flush();
    });
}

std::int16_t partition_missing() {
    return static_cast<std::int16_t>(protocol::ErrorCode::UnknownTopicOrPartition);
}

std::int16_t request_api_key(const std::vector<std::uint8_t>& payload) {
    if (payload.size() < 2) {
        return -1;
    }
    return static_cast<std::int16_t>(
        (static_cast<std::uint16_t>(payload[0]) << 8) |
        static_cast<std::uint16_t>(payload[1]));
}

std::string sanitized_topic_name(std::string name) {
    for (char& ch : name) {
        if (ch == '/' || ch == ':') {
            ch = '_';
        }
    }
    return name;
}

std::string segment_prefix(const PartitionConfig& config) {
    return sanitized_topic_name(config.topic) + "-" + std::to_string(config.partition) + "-";
}

std::string segment_name(const PartitionConfig& config, std::int64_t base_offset) {
    std::ostringstream out;
    out << segment_prefix(config)
        << std::setw(20) << std::setfill('0') << base_offset
        << ".segment";
    return out.str();
}

std::optional<std::int64_t> segment_base_offset_from_name(
    const PartitionConfig& config,
    const std::string& filename) {
    const auto prefix = segment_prefix(config);
    constexpr std::string_view suffix = ".segment";
    if (filename.size() <= prefix.size() + suffix.size() ||
        filename.rfind(prefix, 0) != 0 ||
        filename.substr(filename.size() - suffix.size()) != suffix) {
        return std::nullopt;
    }
    const auto digits = filename.substr(prefix.size(), filename.size() - prefix.size() - suffix.size());
    if (digits.empty() || !std::all_of(digits.begin(), digits.end(), [](char ch) { return ch >= '0' && ch <= '9'; })) {
        return std::nullopt;
    }
    return std::stoll(digits);
}

void write_string(protocol::WireWriter& writer, const std::string& value) {
    writer.write_string(value);
}

ReplicaAppendRequest decode_replica_append(const std::vector<std::uint8_t>& payload) {
    protocol::WireReader reader(payload);
    ReplicaAppendRequest request;
    request.topic = reader.read_string();
    request.partition = reader.read_int32();
    request.base_offset = reader.read_int64();
    request.leader_high_watermark = reader.read_int64();
    request.records = reader.read_bytes();
    return request;
}

std::vector<std::uint8_t> encode_replica_append(const ReplicaAppendRequest& request) {
    protocol::WireWriter writer;
    write_string(writer, request.topic);
    writer.write_int32(request.partition);
    writer.write_int64(request.base_offset);
    writer.write_int64(request.leader_high_watermark);
    writer.write_bytes(request.records);
    return writer.take_bytes();
}

std::vector<std::uint8_t> encode_replica_append_response(std::int16_t error_code, std::int64_t log_end_offset) {
    protocol::WireWriter writer;
    writer.write_int16(error_code);
    writer.write_int64(log_end_offset);
    return writer.take_bytes();
}

ReplicaFetchRequest decode_replica_fetch(const std::vector<std::uint8_t>& payload) {
    protocol::WireReader reader(payload);
    ReplicaFetchRequest request;
    request.topic = reader.read_string();
    request.partition = reader.read_int32();
    request.offset = reader.read_int64();
    request.max_bytes = reader.read_int32();
    return request;
}

std::vector<std::uint8_t> encode_replica_fetch(const ReplicaFetchRequest& request) {
    protocol::WireWriter writer;
    write_string(writer, request.topic);
    writer.write_int32(request.partition);
    writer.write_int64(request.offset);
    writer.write_int32(request.max_bytes);
    return writer.take_bytes();
}

std::vector<std::uint8_t> encode_replica_fetch_response(const ReplicaFetchResult& result) {
    protocol::WireWriter writer;
    writer.write_int16(result.error_code);
    writer.write_int64(result.leader_high_watermark);
    writer.write_int64(result.log_end_offset);
    writer.write_bytes(result.records);
    return writer.take_bytes();
}

ReplicaFetchResult decode_replica_fetch_response(const std::vector<std::uint8_t>& payload) {
    protocol::WireReader reader(payload);
    ReplicaFetchResult result;
    result.error_code = reader.read_int16();
    result.leader_high_watermark = reader.read_int64();
    result.log_end_offset = reader.read_int64();
    result.records = reader.read_bytes();
    return result;
}

ProducePartitionResult decode_replica_append_response(
    const ReplicaAppendRequest& request,
    const std::vector<std::uint8_t>& payload) {
    protocol::WireReader reader(payload);
    ProducePartitionResult result;
    result.topic = request.topic;
    result.partition = request.partition;
    result.error_code = reader.read_int16();
    result.base_offset = request.base_offset;
    result.log_start_offset = 0;
    (void) reader.read_int64();
    return result;
}

std::vector<PartitionConfig> all_partitions(const BrokerConfig& config) {
    std::vector<PartitionConfig> partitions;
    for (const auto& topic : config.topics) {
        partitions.insert(partitions.end(), topic.partitions.begin(), topic.partitions.end());
    }
    return partitions;
}

void validate_reload(const BrokerConfig& old_config, const BrokerConfig& new_config) {
    if (old_config.node_id != new_config.node_id) {
        throw std::runtime_error("SIGHUP reload cannot change node_id");
    }
    if (old_config.data_dir != new_config.data_dir) {
        throw std::runtime_error("SIGHUP reload cannot change data_dir");
    }
    if (old_config.segment_bytes != new_config.segment_bytes) {
        throw std::runtime_error("SIGHUP reload cannot change segment_bytes");
    }
    if (old_config.brokers.size() != new_config.brokers.size()) {
        throw std::runtime_error("SIGHUP reload cannot add or remove brokers yet");
    }
    for (const auto& broker : old_config.brokers) {
        const auto updated = find_broker(new_config, broker.id);
        if (!updated.has_value() ||
            updated->kafka_host != broker.kafka_host ||
            updated->kafka_port != broker.kafka_port ||
            updated->mercury_address != broker.mercury_address) {
            throw std::runtime_error("SIGHUP reload cannot change broker endpoints yet");
        }
    }
    for (const auto& partition : all_partitions(old_config)) {
        const auto updated = find_partition(new_config, partition.topic, partition.partition);
        if (!updated.has_value()) {
            throw std::runtime_error("SIGHUP reload cannot delete topic partitions");
        }
        if (updated->shard != partition.shard) {
            throw std::runtime_error("SIGHUP reload cannot migrate partitions across shards");
        }
        if (updated->replicas != partition.replicas || updated->isr != partition.isr) {
            throw std::runtime_error("SIGHUP reload cannot change replicas or ISR yet");
        }
        if (updated->migration_generation != partition.migration_generation ||
            updated->migration_target_shard != partition.migration_target_shard ||
            updated->migration_target_leader != partition.migration_target_leader) {
            throw std::runtime_error("SIGHUP reload accepts migration schema but does not execute migrations yet");
        }
    }
}

}  // namespace

bool TopicPartition::operator<(const TopicPartition& other) const noexcept {
    if (topic != other.topic) {
        return topic < other.topic;
    }
    return partition < other.partition;
}

seastar::future<> PartitionLogShard::start(
    PartitionConfig config,
    std::filesystem::path root,
    std::uint64_t segment_bytes) {
    config_ = std::move(config);
    root_ = std::move(root);
    segment_bytes_ = segment_bytes;
    return seastar::recursive_touch_directory(root_.string()).then([this] {
        return recover();
    }).then([this] {
        return open_active_segment();
    }).then([this] {
        return open_index();
    });
}

seastar::future<> PartitionLogShard::stop() {
    std::vector<seastar::future<>> closes;
    if (file_.has_value()) {
        auto file = std::move(*file_);
        file_.reset();
        closes.push_back(file.close());
    }
    if (index_file_.has_value()) {
        auto file = std::move(*index_file_);
        index_file_.reset();
        closes.push_back(file.close());
    }
    return seastar::when_all_succeed(closes.begin(), closes.end()).discard_result();
}

const PartitionConfig& PartitionLogShard::config() const noexcept {
    return config_;
}

PartitionOffsets PartitionLogShard::offsets() const noexcept {
    return offsets_;
}

std::uint32_t PartitionLogShard::queue_depth() const noexcept {
    return queue_depth_;
}

bool PartitionLogShard::degraded() const noexcept {
    return degraded_;
}

std::int64_t PartitionLogShard::replica_lag() const noexcept {
    return replica_lag_;
}

std::uint64_t PartitionLogShard::disk_writes() const noexcept {
    return disk_writes_;
}

std::uint64_t PartitionLogShard::disk_fsyncs() const noexcept {
    return disk_fsyncs_;
}

LatencyHistogram PartitionLogShard::disk_write_latency() const {
    return disk_write_latency_;
}

LatencyHistogram PartitionLogShard::disk_fsync_latency() const {
    return disk_fsync_latency_;
}

void PartitionLogShard::update_config(PartitionConfig config) {
    config_ = std::move(config);
}

seastar::future<ProducePartitionResult> PartitionLogShard::append(
    std::vector<std::uint8_t> records,
    std::int16_t acks) {
    ProducePartitionResult response;
    response.topic = config_.topic;
    response.partition = config_.partition;
    response.log_start_offset = offsets_.earliest;

    if (records.size() > config_.max_frame_bytes) {
        response.error_code = static_cast<std::int16_t>(protocol::ErrorCode::RecordListTooLarge);
        return seastar::make_ready_future<ProducePartitionResult>(std::move(response));
    }
    if (queue_depth_ >= config_.queue_limit) {
        response.error_code = static_cast<std::int16_t>(protocol::ErrorCode::RequestTimedOut);
        return seastar::make_ready_future<ProducePartitionResult>(std::move(response));
    }
    if (acks < -1 || acks > 1) {
        response.error_code = static_cast<std::int16_t>(protocol::ErrorCode::InvalidRequest);
        return seastar::make_ready_future<ProducePartitionResult>(std::move(response));
    }

    ++queue_depth_;
    const std::int64_t base_offset = offsets_.latest;
    auto rewritten = rewrite_record_batch_offsets(records, base_offset);
    const std::int64_t count = estimate_record_count(rewritten);
    const std::int64_t last_offset = count == 0 ? base_offset - 1 : base_offset + count - 1;
    offsets_.latest = base_offset + count;
    if (config_.isr.size() <= 1) {
        offsets_.high_watermark = offsets_.latest;
    }

    RecordSet record_set{base_offset, last_offset, std::move(rewritten)};
    response.base_offset = base_offset;
    response.log_end_offset = offsets_.latest;
    response.log_start_offset = offsets_.earliest;

    return write_record_set(record_set).then([this, response = std::move(response)]() mutable {
        return maybe_flush().then([response = std::move(response)]() mutable {
            return seastar::make_ready_future<ProducePartitionResult>(std::move(response));
        });
    }).finally([this] {
        --queue_depth_;
    });
}

seastar::future<ProducePartitionResult> PartitionLogShard::append_replica(ReplicaAppendRequest request) {
    ProducePartitionResult response;
    response.topic = request.topic;
    response.partition = request.partition;
    response.base_offset = request.base_offset;
    response.log_start_offset = offsets_.earliest;

    if (request.records.size() > config_.max_frame_bytes) {
        response.error_code = static_cast<std::int16_t>(protocol::ErrorCode::RecordListTooLarge);
        return seastar::make_ready_future<ProducePartitionResult>(std::move(response));
    }
    if (request.base_offset != offsets_.latest) {
        response.error_code = static_cast<std::int16_t>(protocol::ErrorCode::OffsetOutOfRange);
        return seastar::make_ready_future<ProducePartitionResult>(std::move(response));
    }
    if (queue_depth_ >= config_.queue_limit) {
        response.error_code = static_cast<std::int16_t>(protocol::ErrorCode::RequestTimedOut);
        return seastar::make_ready_future<ProducePartitionResult>(std::move(response));
    }

    ++queue_depth_;
    const auto count = estimate_record_count(request.records);
    RecordSet record_set{
        request.base_offset,
        count == 0 ? request.base_offset - 1 : request.base_offset + count - 1,
        std::move(request.records),
    };
    offsets_.latest = request.base_offset + count;
    response.log_end_offset = offsets_.latest;
    offsets_.high_watermark = std::min(offsets_.latest, request.leader_high_watermark);
    replica_lag_ = std::max<std::int64_t>(0, request.leader_high_watermark - offsets_.high_watermark);

    return write_record_set(record_set).then([this, response = std::move(response)]() mutable {
        return maybe_flush().then([response = std::move(response)]() mutable {
            return seastar::make_ready_future<ProducePartitionResult>(std::move(response));
        });
    }).finally([this] {
        --queue_depth_;
    });
}

seastar::future<FetchPartitionResult> PartitionLogShard::fetch(std::int64_t offset, std::int32_t max_bytes) {
    FetchPartitionResult result;
    const std::int64_t visible_end = config_.fetch_visibility == FetchVisibility::HighWatermark
        ? offsets_.high_watermark
        : offsets_.latest;
    result.high_watermark = visible_end;
    result.log_start_offset = offsets_.earliest;
    result.log_end_offset = offsets_.latest;
    if (offset < offsets_.earliest || offset > offsets_.latest) {
        result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::OffsetOutOfRange);
        return seastar::make_ready_future<FetchPartitionResult>(std::move(result));
    }

    const std::size_t limit = max_bytes <= 0 ? config_.max_frame_bytes : static_cast<std::size_t>(max_bytes);
    return read_records_from_index(offset, visible_end, limit).then(
        [result = std::move(result)](std::vector<std::uint8_t> records) mutable {
            result.records = std::move(records);
            return seastar::make_ready_future<FetchPartitionResult>(std::move(result));
        });
}

seastar::future<ReplicaFetchResult> PartitionLogShard::fetch_for_replica(std::int64_t offset, std::int32_t max_bytes) {
    ReplicaFetchResult result;
    result.leader_high_watermark = offsets_.high_watermark;
    result.log_end_offset = offsets_.latest;
    if (offset < offsets_.earliest || offset > offsets_.latest) {
        result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::OffsetOutOfRange);
        return seastar::make_ready_future<ReplicaFetchResult>(std::move(result));
    }
    const std::size_t limit = max_bytes <= 0 ? config_.max_frame_bytes : static_cast<std::size_t>(max_bytes);
    return read_records_from_index(offset, offsets_.latest, limit).then(
        [result = std::move(result)](std::vector<std::uint8_t> records) mutable {
            result.records = std::move(records);
            return seastar::make_ready_future<ReplicaFetchResult>(std::move(result));
        });
}

ListOffsetPartitionResult PartitionLogShard::list_offsets(std::int64_t timestamp) const {
    ListOffsetPartitionResult result;
    result.topic = config_.topic;
    result.partition = config_.partition;
    result.earliest = offsets_.earliest;
    result.latest = offsets_.latest;
    result.selected = timestamp == -2 ? offsets_.earliest : offsets_.latest;
    return result;
}

void PartitionLogShard::mark_replicated_through(std::int64_t offset) {
    offsets_.high_watermark = std::min(offsets_.latest, std::max(offsets_.high_watermark, offset));
    replica_lag_ = std::max<std::int64_t>(0, offsets_.latest - offsets_.high_watermark);
    degraded_ = false;
}

void PartitionLogShard::mark_degraded() {
    degraded_ = true;
    replica_lag_ = std::max<std::int64_t>(0, offsets_.latest - offsets_.high_watermark);
}

std::filesystem::path PartitionLogShard::segment_path(std::int64_t base_offset) const {
    return root_ / segment_name(config_, base_offset);
}

std::filesystem::path PartitionLogShard::index_path() const {
    return root_ / (sanitized_topic_name(config_.topic) + "-" + std::to_string(config_.partition) + ".index");
}

seastar::future<> PartitionLogShard::recover() {
    segments_.clear();
    offsets_ = {};
    active_segment_base_offset_ = 0;
    write_position_ = 0;
    index_position_ = 0;

    return recover_from_index().then([this](bool recovered) {
        if (recovered) {
            return seastar::make_ready_future<>();
        }
        return recover_from_segments();
    });
}

seastar::future<bool> PartitionLogShard::recover_from_index() {
    seastar::file index;
    try {
        index = co_await seastar::open_file_dma(index_path().string(), seastar::open_flags::ro);
    } catch (...) {
        co_return false;
    }

    bool recovered = false;
    std::exception_ptr error;
    try {
        const auto size = co_await index.size();
        for (std::uint64_t position = 0; position + kIndexEntrySize <= size; position += kIndexEntrySize) {
            auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(kIndexEntrySize, kLogFrameAlignment);
            const auto bytes_read = co_await index.dma_read(position, buffer.get(), kIndexEntrySize);
            if (bytes_read != kIndexEntrySize) {
                break;
            }
            auto entry = decode_index_entry(buffer.get());
            if (!entry.has_value()) {
                break;
            }
            note_index_entry(*entry);
            offsets_.latest = std::max(offsets_.latest, entry->last_offset + 1);
            offsets_.high_watermark = offsets_.latest;
            index_position_ += kIndexEntrySize;
            recovered = true;
        }
    } catch (...) {
        error = std::current_exception();
    }
    co_await index.close();
    if (error) {
        std::rethrow_exception(error);
    }

    if (recovered) {
        const auto& active = segments_.back();
        active_segment_base_offset_ = active.base_offset;
        write_position_ = active.write_position;
    }
    co_return recovered;
}

seastar::future<> PartitionLogShard::recover_from_segments() {
    auto segment_files = co_await list_segment_files();
    seastar::file rebuilt_index = co_await seastar::open_file_dma(
        index_path().string(),
        seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate);
    std::uint64_t rebuilt_index_position = 0;
    std::exception_ptr error;
    try {
        for (const auto& [segment_base, path] : segment_files) {
            co_await recover_segment_file(segment_base, path, rebuilt_index, rebuilt_index_position);
        }
    } catch (...) {
        error = std::current_exception();
    }
    co_await rebuilt_index.close();
    if (error) {
        std::rethrow_exception(error);
    }

    if (segment_files.empty()) {
        active_segment_base_offset_ = offsets_.latest;
        write_position_ = 0;
    } else if (!segments_.empty()) {
        const auto& active = segments_.back();
        active_segment_base_offset_ = active.base_offset;
        write_position_ = active.write_position;
    }
    co_return;
}

seastar::future<std::vector<std::pair<std::int64_t, std::filesystem::path>>> PartitionLogShard::list_segment_files() {
    std::vector<std::pair<std::int64_t, std::filesystem::path>> segments;
    seastar::file dir;
    try {
        dir = co_await seastar::open_directory(root_.string());
    } catch (...) {
        co_return segments;
    }

    std::exception_ptr error;
    try {
        co_await dir.list_directory([this, &segments](seastar::directory_entry entry) -> seastar::future<> {
            if (entry.type.has_value() && *entry.type != seastar::directory_entry_type::regular) {
                co_return;
            }
            const auto base_offset = segment_base_offset_from_name(config_, std::string(entry.name.c_str()));
            if (base_offset.has_value()) {
                segments.emplace_back(*base_offset, root_ / entry.name.c_str());
            }
            co_return;
        }).done();
    } catch (...) {
        error = std::current_exception();
    }
    co_await dir.close();
    if (error) {
        std::rethrow_exception(error);
    }

    std::sort(segments.begin(), segments.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });
    co_return segments;
}

seastar::future<> PartitionLogShard::recover_segment_file(
    std::int64_t segment_base,
    const std::filesystem::path& path,
    seastar::file& rebuilt_index,
    std::uint64_t& rebuilt_index_position) {
    seastar::file segment = co_await seastar::open_file_dma(path.string(), seastar::open_flags::ro);
    std::exception_ptr error;
    try {
        const auto size = co_await segment.size();
        std::uint64_t position = 0;
        while (position + kLogFrameAlignment <= size) {
            auto header_buffer = seastar::allocate_aligned_buffer<std::uint8_t>(kLogFrameAlignment, kLogFrameAlignment);
            const auto header_bytes = co_await segment.dma_read(position, header_buffer.get(), kLogFrameAlignment);
            if (header_bytes != kLogFrameAlignment ||
                std::memcmp(header_buffer.get(), kLogMagic, sizeof(kLogMagic)) != 0) {
                break;
            }
            const auto* raw_header = reinterpret_cast<const char*>(header_buffer.get());
            const std::int64_t base_offset = get_i64(raw_header + 8);
            const std::int32_t record_bytes = get_i32(raw_header + 16);
            if (record_bytes <= 0) {
                break;
            }
            const auto frame_size = round_up(20 + static_cast<std::uint64_t>(record_bytes), kLogFrameAlignment);
            if (position + frame_size > size) {
                break;
            }

            std::vector<std::uint8_t> records(static_cast<std::size_t>(record_bytes));
            if (frame_size == kLogFrameAlignment) {
                std::memcpy(records.data(), header_buffer.get() + 20, records.size());
            } else {
                auto frame_buffer = seastar::allocate_aligned_buffer<std::uint8_t>(frame_size, kLogFrameAlignment);
                const auto frame_bytes = co_await segment.dma_read(position, frame_buffer.get(), frame_size);
                if (frame_bytes != frame_size ||
                    std::memcmp(frame_buffer.get(), kLogMagic, sizeof(kLogMagic)) != 0 ||
                    get_i64(reinterpret_cast<const char*>(frame_buffer.get()) + 8) != base_offset ||
                    get_i32(reinterpret_cast<const char*>(frame_buffer.get()) + 16) != record_bytes) {
                    break;
                }
                std::memcpy(records.data(), frame_buffer.get() + 20, records.size());
            }

            const auto count = estimate_record_count(records);
            IndexEntry index_entry{
                base_offset,
                base_offset + count - 1,
                segment_base,
                position,
                record_bytes,
            };
            note_index_entry(index_entry);
            co_await write_rebuilt_index_entry(rebuilt_index, index_entry, rebuilt_index_position);
            rebuilt_index_position += kIndexEntrySize;
            index_position_ += kIndexEntrySize;
            offsets_.latest = base_offset + count;
            offsets_.high_watermark = offsets_.latest;
            position += frame_size;
        }
    } catch (...) {
        error = std::current_exception();
    }
    co_await segment.close();
    if (error) {
        std::rethrow_exception(error);
    }
    co_return;
}

seastar::future<> PartitionLogShard::open_active_segment() {
    seastar::file_open_options options;
    options.extent_allocation_size_hint = segment_bytes_;
    options.durable = config_.durability == Durability::Fsync;
    return seastar::open_file_dma(
        segment_path(active_segment_base_offset_).string(),
        seastar::open_flags::rw | seastar::open_flags::create,
        options).then([this](seastar::file file) {
            file_.emplace(std::move(file));
        });
}

seastar::future<> PartitionLogShard::open_index() {
    seastar::file_open_options options;
    options.durable = config_.durability == Durability::Fsync;
    return seastar::open_file_dma(
        index_path().string(),
        seastar::open_flags::rw | seastar::open_flags::create,
        options).then([this](seastar::file file) {
            index_file_.emplace(std::move(file));
        });
}

seastar::future<> PartitionLogShard::ensure_segment_for_write(std::int64_t base_offset, std::uint64_t frame_size) {
    if (!file_.has_value()) {
        active_segment_base_offset_ = base_offset;
        write_position_ = 0;
        return open_active_segment();
    }
    if (write_position_ == 0 || write_position_ + frame_size <= segment_bytes_) {
        return seastar::make_ready_future<>();
    }
    auto old_file = std::move(*file_);
    file_.reset();
    return old_file.close().then([this, base_offset] {
        active_segment_base_offset_ = base_offset;
        write_position_ = 0;
        return open_active_segment();
    });
}

seastar::future<> PartitionLogShard::write_record_set(const RecordSet& record_set) {
    const auto started = std::chrono::steady_clock::now();
    const std::uint64_t header_size = 20;
    const std::uint64_t frame_size = round_up(header_size + record_set.bytes.size(), kLogFrameAlignment);
    auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(frame_size, kLogFrameAlignment);
    std::memset(buffer.get(), 0, frame_size);
    std::memcpy(buffer.get(), kLogMagic, sizeof(kLogMagic));
    put_i64(buffer.get() + 8, record_set.base_offset);
    put_i32(buffer.get() + 16, static_cast<std::int32_t>(record_set.bytes.size()));
    if (!record_set.bytes.empty()) {
        std::memcpy(buffer.get() + header_size, record_set.bytes.data(), record_set.bytes.size());
    }

    return ensure_segment_for_write(record_set.base_offset, frame_size).then(
        [this,
         frame_size,
         base_offset = record_set.base_offset,
         last_offset = record_set.last_offset,
         record_bytes = static_cast<std::int32_t>(record_set.bytes.size()),
         buffer = std::move(buffer),
         started]() mutable {
            const auto position = write_position_;
            const auto segment_base = active_segment_base_offset_;
            write_position_ += frame_size;
            if (!file_.has_value()) {
                return seastar::make_exception_future<>(std::runtime_error("partition segment file is not open"));
            }
            auto raw = buffer.get();
            return file_->dma_write(position, raw, frame_size).then(
                [this,
                 base_offset,
                 last_offset,
                 record_bytes,
                 segment_base,
                 position,
                 started,
                 buffer = std::move(buffer)](std::size_t) mutable {
                    ++disk_writes_;
                    disk_write_latency_.add(std::chrono::steady_clock::now() - started);
                    return write_index_entry(base_offset, last_offset, record_bytes, segment_base, position);
                });
        });
}

seastar::future<> PartitionLogShard::write_index_entry(
    std::int64_t base_offset,
    std::int64_t last_offset,
    std::int32_t record_bytes,
    std::int64_t segment_base,
    std::uint64_t segment_position) {
    if (!index_file_.has_value()) {
        return seastar::make_exception_future<>(std::runtime_error("partition index file is not open"));
    }
    auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(kIndexEntrySize, kLogFrameAlignment);
    IndexEntry entry{base_offset, last_offset, segment_base, segment_position, record_bytes};
    encode_index_entry(buffer.get(), entry);
    const auto position = index_position_;
    index_position_ += kIndexEntrySize;
    auto raw = buffer.get();
    return index_file_->dma_write(position, raw, kIndexEntrySize).then(
        [this, entry, buffer = std::move(buffer)](std::size_t) mutable {
            note_index_entry(entry);
        });
}

seastar::future<std::optional<PartitionLogShard::IndexEntry>> PartitionLogShard::read_index_entry(
    std::uint64_t position) {
    if (!index_file_.has_value() || position >= index_position_) {
        return seastar::make_ready_future<std::optional<IndexEntry>>(std::nullopt);
    }
    auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(kIndexEntrySize, kLogFrameAlignment);
    auto raw = buffer.get();
    return index_file_->dma_read(position, raw, kIndexEntrySize).then(
        [buffer = std::move(buffer)](std::size_t bytes_read) mutable {
            if (bytes_read != kIndexEntrySize) {
                return seastar::make_ready_future<std::optional<IndexEntry>>(std::nullopt);
            }
            return seastar::make_ready_future<std::optional<IndexEntry>>(decode_index_entry(buffer.get()));
        });
}

seastar::future<std::uint64_t> PartitionLogShard::find_index_position_for_offset(std::int64_t offset) {
    const auto entry_count = index_position_ / kIndexEntrySize;
    return seastar::do_with(
        std::uint64_t{0},
        entry_count,
        entry_count,
        [this, offset](std::uint64_t& low, std::uint64_t& high, std::uint64_t& candidate) {
            return seastar::repeat([this, offset, &low, &high, &candidate] {
                if (low >= high) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                const auto mid = low + ((high - low) / 2);
                return read_index_entry(mid * kIndexEntrySize).then(
                    [offset, mid, &low, &high, &candidate](std::optional<IndexEntry> entry) {
                        if (!entry.has_value()) {
                            high = mid;
                            return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                        }
                        if (entry->last_offset < offset) {
                            low = mid + 1;
                        } else {
                            candidate = mid;
                            high = mid;
                        }
                        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                    });
            }).then([&candidate] {
                return seastar::make_ready_future<std::uint64_t>(candidate * kIndexEntrySize);
            });
        });
}

seastar::future<std::vector<std::uint8_t>> PartitionLogShard::read_segment_record(const IndexEntry& entry) {
    const std::uint64_t header_size = 20;
    const auto frame_size = round_up(header_size + static_cast<std::uint64_t>(entry.record_bytes), kLogFrameAlignment);
    auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(frame_size, kLogFrameAlignment);
    auto raw = buffer.get();
    return seastar::open_file_dma(segment_path(entry.segment_base).string(), seastar::open_flags::ro).then(
        [entry, frame_size, raw, buffer = std::move(buffer)](seastar::file file) mutable {
            return file.dma_read(entry.segment_position, raw, frame_size).then(
                [entry, frame_size, file = std::move(file), buffer = std::move(buffer)](std::size_t bytes_read) mutable {
                    std::exception_ptr error;
                    std::vector<std::uint8_t> records;
                    try {
                        if (bytes_read != frame_size ||
                            std::memcmp(buffer.get(), kLogMagic, sizeof(kLogMagic)) != 0 ||
                            get_i64(reinterpret_cast<const char*>(buffer.get()) + 8) != entry.base_offset ||
                            get_i32(reinterpret_cast<const char*>(buffer.get()) + 16) != entry.record_bytes) {
                            throw std::runtime_error("partition segment frame is corrupt");
                        }
                        records.resize(static_cast<std::size_t>(entry.record_bytes));
                        if (!records.empty()) {
                            std::memcpy(records.data(), buffer.get() + 20, records.size());
                        }
                    } catch (...) {
                        error = std::current_exception();
                    }
                    return file.close().then([records = std::move(records), error]() mutable {
                        if (error) {
                            return seastar::make_exception_future<std::vector<std::uint8_t>>(error);
                        }
                        return seastar::make_ready_future<std::vector<std::uint8_t>>(std::move(records));
                    });
                });
        });
}

seastar::future<std::vector<std::uint8_t>> PartitionLogShard::read_records_from_index(
    std::int64_t offset,
    std::int64_t visible_end,
    std::size_t limit) {
    return seastar::do_with(
        std::vector<std::uint8_t>{},
        std::uint64_t{0},
        [this, offset, visible_end, limit](std::vector<std::uint8_t>& records, std::uint64_t& position) {
            return find_index_position_for_offset(offset).then([&position](std::uint64_t found_position) {
                position = found_position;
            }).then([this, offset, visible_end, limit, &records, &position] {
                return seastar::repeat([this, offset, visible_end, limit, &records, &position] {
                    return read_index_entry(position).then(
                        [this, offset, visible_end, limit, &records, &position](std::optional<IndexEntry> entry) {
                            if (!entry.has_value()) {
                                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                            }
                            position += kIndexEntrySize;
                            if (entry->last_offset < offset) {
                                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                            }
                            if (entry->base_offset >= visible_end || entry->last_offset >= visible_end) {
                                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                            }
                            const auto record_bytes = static_cast<std::size_t>(entry->record_bytes);
                            if (!records.empty() && records.size() + record_bytes > limit) {
                                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                            }
                            return read_segment_record(*entry).then(
                                [limit, &records](std::vector<std::uint8_t> record_bytes) {
                                    records.insert(records.end(), record_bytes.begin(), record_bytes.end());
                                    return seastar::make_ready_future<seastar::stop_iteration>(
                                        records.size() >= limit ? seastar::stop_iteration::yes : seastar::stop_iteration::no);
                                });
                        });
                });
            }).then([&records] {
                return seastar::make_ready_future<std::vector<std::uint8_t>>(std::move(records));
            });
        });
}

void PartitionLogShard::note_index_entry(const IndexEntry& entry) {
    const auto frame_size = round_up(20 + static_cast<std::uint64_t>(entry.record_bytes), kLogFrameAlignment);
    const auto next_position = entry.segment_position + frame_size;
    if (segments_.empty() || segments_.back().base_offset != entry.segment_base) {
        segments_.push_back(SegmentInfo{entry.segment_base, entry.last_offset, next_position});
        return;
    }
    auto& segment = segments_.back();
    segment.last_offset = std::max(segment.last_offset, entry.last_offset);
    segment.write_position = std::max(segment.write_position, next_position);
}

seastar::future<> PartitionLogShard::write_rebuilt_index_entry(
    seastar::file& index_file,
    const IndexEntry& entry,
    std::uint64_t position) {
    auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(kIndexEntrySize, kLogFrameAlignment);
    encode_index_entry(buffer.get(), entry);
    auto raw = buffer.get();
    return index_file.dma_write(position, raw, kIndexEntrySize).then(
        [buffer = std::move(buffer)](std::size_t) mutable {});
}

std::optional<PartitionLogShard::IndexEntry> PartitionLogShard::decode_index_entry(const std::uint8_t* bytes) {
    if (std::memcmp(bytes, kIndexMagic, sizeof(kIndexMagic)) != 0) {
        return std::nullopt;
    }
    const auto* raw_entry = reinterpret_cast<const char*>(bytes);
    IndexEntry entry{
        get_i64(raw_entry + 8),
        get_i64(raw_entry + 16),
        get_i64(raw_entry + 24),
        static_cast<std::uint64_t>(get_i64(raw_entry + 32)),
        get_i32(raw_entry + 40),
    };
    if (entry.record_bytes <= 0 || entry.last_offset < entry.base_offset) {
        return std::nullopt;
    }
    return entry;
}

void PartitionLogShard::encode_index_entry(std::uint8_t* bytes, const IndexEntry& entry) {
    std::memset(bytes, 0, kIndexEntrySize);
    std::memcpy(bytes, kIndexMagic, sizeof(kIndexMagic));
    put_i64(bytes + 8, entry.base_offset);
    put_i64(bytes + 16, entry.last_offset);
    put_i64(bytes + 24, entry.segment_base);
    put_i64(bytes + 32, static_cast<std::int64_t>(entry.segment_position));
    put_i32(bytes + 40, entry.record_bytes);
}

seastar::future<> PartitionLogShard::maybe_flush() {
    if (config_.durability != Durability::Fsync) {
        return seastar::make_ready_future<>();
    }
    if (!file_.has_value()) {
        return seastar::make_exception_future<>(std::runtime_error("partition segment file is not open"));
    }
    const auto started = std::chrono::steady_clock::now();
    return file_->flush().then([this, started] {
            ++disk_fsyncs_;
            disk_fsync_latency_.add(std::chrono::steady_clock::now() - started);
            if (index_file_.has_value()) {
                return index_file_->flush();
            }
            return seastar::make_ready_future<>();
        });
}

std::int64_t PartitionLogShard::estimate_record_count(const std::vector<std::uint8_t>& records) {
    if (records.empty()) {
        return 0;
    }
    std::int64_t total = 0;
    std::size_t pos = 0;
    while (pos + 27 <= records.size()) {
        const std::uint32_t batch_length =
            (static_cast<std::uint32_t>(records[pos + 8]) << 24) |
            (static_cast<std::uint32_t>(records[pos + 9]) << 16) |
            (static_cast<std::uint32_t>(records[pos + 10]) << 8) |
            static_cast<std::uint32_t>(records[pos + 11]);
        const std::size_t total_batch_size = static_cast<std::size_t>(batch_length) + 12;
        if (batch_length == 0 || pos + total_batch_size > records.size()) {
            break;
        }
        const std::int32_t last_offset_delta =
            static_cast<std::int32_t>(
                (static_cast<std::uint32_t>(records[pos + 23]) << 24) |
                (static_cast<std::uint32_t>(records[pos + 24]) << 16) |
                (static_cast<std::uint32_t>(records[pos + 25]) << 8) |
                static_cast<std::uint32_t>(records[pos + 26]));
        total += std::max<std::int64_t>(1, static_cast<std::int64_t>(last_offset_delta) + 1);
        pos += total_batch_size;
    }
    return total == 0 ? 1 : total;
}

BrokerShard::BrokerShard()
    : protocol_(*this)
    , maintenance_timer_([this] {
          (void) seastar::with_gate(gate_, [this] {
              return maintenance_once();
          }).handle_exception([](std::exception_ptr) {}).finally([this] {
              arm_maintenance_timer();
          });
      }) {}

seastar::future<> BrokerShard::start(BrokerConfig config) {
    config_ = std::move(config);
    register_metrics();

    std::vector<seastar::future<>> starts;
    for (const auto& topic : config_.topics) {
        for (const auto& partition : topic.partitions) {
            if (partition.shard != seastar::this_shard_id()) {
                continue;
            }
            TopicPartition tp{partition.topic, partition.partition};
            starts.push_back(partitions_[tp].start(partition, config_.data_dir / ("shard-" + std::to_string(seastar::this_shard_id())), config_.segment_bytes));
        }
    }

    const auto broker = find_broker(config_, config_.node_id);
    if (!broker.has_value()) {
        return seastar::make_exception_future<>(std::runtime_error("local broker is missing from config"));
    }

    return seastar::when_all_succeed(starts.begin(), starts.end()).discard_result().then([this, broker] {
        return mercury_.start(broker->mercury_address, true);
    }).then([this, broker] {
        register_mercury_rpcs();
        seastar::listen_options opts;
        opts.reuse_address = true;
        opts.lba = seastar::server_socket::load_balancing_algorithm::connection_distribution;
        listener_.emplace(seastar::listen(
            seastar::socket_address{seastar::net::inet_address(broker->kafka_host), broker->kafka_port},
            opts));
        (void) seastar::with_gate(gate_, [this] {
            return accept_loop();
        });
        arm_maintenance_timer();
        return seastar::make_ready_future<>();
    });
}

seastar::future<> BrokerShard::stop() {
    stopping_ = true;
    maintenance_timer_.cancel();
    if (listener_.has_value()) {
        listener_->abort_accept();
    }
    return gate_.close().then([this] {
        std::vector<seastar::future<>> stops;
        for (auto& [_, partition] : partitions_) {
            stops.push_back(partition.stop());
        }
        return seastar::when_all_succeed(stops.begin(), stops.end()).discard_result();
    }).then([this] {
        return mercury_.stop();
    });
}

seastar::future<> BrokerShard::apply_config(BrokerConfig config) {
    try {
        validate_reload(config_, config);
    } catch (...) {
        return seastar::make_exception_future<>(std::current_exception());
    }

    std::vector<seastar::future<>> starts;
    for (const auto& topic : config.topics) {
        for (const auto& partition : topic.partitions) {
            if (partition.shard != seastar::this_shard_id()) {
                continue;
            }
            TopicPartition tp{partition.topic, partition.partition};
            auto it = partitions_.find(tp);
            if (it == partitions_.end()) {
                starts.push_back(partitions_[tp].start(
                    partition,
                    config.data_dir / ("shard-" + std::to_string(seastar::this_shard_id())),
                    config.segment_bytes));
            } else {
                it->second.update_config(partition);
            }
        }
    }
    return seastar::when_all_succeed(starts.begin(), starts.end()).discard_result().then(
        [this, config = std::move(config)]() mutable {
            config_ = std::move(config);
        });
}

void BrokerShard::set_peers(seastar::sharded<BrokerShard>* peers) noexcept {
    peers_ = peers;
}

const BrokerConfig& BrokerShard::config() const noexcept {
    return config_;
}

seastar::future<std::optional<std::vector<std::uint8_t>>> BrokerShard::handle_kafka_frame(
    std::vector<std::uint8_t> payload) {
    ++kafka_requests_;
    const auto api_key = request_api_key(payload);
    const auto started = std::chrono::steady_clock::now();
    return protocol_.handle_frame(std::move(payload)).then(
        [this, api_key, started](std::optional<std::vector<std::uint8_t>> response) mutable {
            const auto elapsed = std::chrono::steady_clock::now() - started;
            kafka_request_latency_.add(elapsed);
            if (api_key == protocol::api_id(protocol::ApiKey::Produce)) {
                produce_latency_.add(elapsed);
            } else if (api_key == protocol::api_id(protocol::ApiKey::Fetch)) {
                fetch_latency_.add(elapsed);
            }
            return seastar::make_ready_future<std::optional<std::vector<std::uint8_t>>>(std::move(response));
        }).handle_exception([this, started](std::exception_ptr ep) {
        ++kafka_errors_;
        kafka_request_latency_.add(std::chrono::steady_clock::now() - started);
        return seastar::make_exception_future<std::optional<std::vector<std::uint8_t>>>(ep);
    });
}

seastar::future<ProducePartitionResult> BrokerShard::append(
    ProducePartitionRequest request,
    std::int16_t acks,
    std::int32_t timeout_ms) {
    const auto owner = owner_shard(request.topic, request.partition);
    if (!owner.has_value()) {
        ++produce_errors_;
        ProducePartitionResult result;
        result.topic = std::move(request.topic);
        result.partition = request.partition;
        result.error_code = partition_missing();
        return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
    }
    if (*owner != seastar::this_shard_id()) {
        return peers_->invoke_on(*owner, [request = std::move(request), acks, timeout_ms](BrokerShard& shard) mutable {
            return shard.append(std::move(request), acks, timeout_ms);
        });
    }
    auto it = partitions_.find({request.topic, request.partition});
    if (it == partitions_.end()) {
        ++produce_errors_;
        ProducePartitionResult result;
        result.topic = std::move(request.topic);
        result.partition = request.partition;
        result.error_code = partition_missing();
        return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
    }
    if (it->second.config().leader != config_.node_id) {
        ProducePartitionResult result;
        result.topic = std::move(request.topic);
        result.partition = request.partition;
        result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::NotLeaderOrFollower);
        return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
    }
    auto records = rewrite_record_batch_offsets(request.records, it->second.offsets().latest);
    const auto partition_config = it->second.config();
    auto& partition_log = it->second;
    auto operation = seastar::with_gate(gate_, [this, &partition_log, partition_config, records = std::move(records), acks]() mutable {
        return partition_log.append(records, acks).then(
            [this, tp = TopicPartition{partition_config.topic, partition_config.partition}, partition_config, records = std::move(records), acks](ProducePartitionResult result) mutable {
            if (result.error_code != 0) {
                return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
            }
            if (partition_config.isr.size() <= 1) {
                return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
            }
            auto replication = replicate_to_isr(partition_config, result, records);
            if (acks == -1) {
                return replication.then([this, tp, result]() mutable {
                    auto it = partitions_.find(tp);
                    if (it != partitions_.end()) {
                        it->second.mark_replicated_through(it->second.offsets().latest);
                    }
                    return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
                }).handle_exception([this, tp, result](std::exception_ptr) mutable {
                    ++replica_errors_;
                    auto it = partitions_.find(tp);
                    if (it != partitions_.end()) {
                        it->second.mark_degraded();
                    }
                    ProducePartitionResult failed = std::move(result);
                    failed.error_code = static_cast<std::int16_t>(protocol::ErrorCode::NotEnoughReplicas);
                    return seastar::make_ready_future<ProducePartitionResult>(std::move(failed));
                });
            }
            (void) seastar::with_gate(gate_, [this, tp, replication = std::move(replication)]() mutable {
                return std::move(replication).then([this, tp] {
                    auto it = partitions_.find(tp);
                    if (it != partitions_.end()) {
                        it->second.mark_replicated_through(it->second.offsets().latest);
                    }
                }).handle_exception([this, tp](std::exception_ptr) {
                    ++replica_errors_;
                    auto it = partitions_.find(tp);
                    if (it != partitions_.end()) {
                        it->second.mark_degraded();
                    }
                });
            });
            return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
        });
    });
    const auto cap_ms = static_cast<std::int32_t>(config_.produce_wait_ms_cap);
    const auto wait_ms = timeout_ms <= 0 ? cap_ms : std::min(timeout_ms, cap_ms);
    if (wait_ms <= 0) {
        return operation;
    }
    return seastar::with_timeout(
        seastar::lowres_clock::now() + std::chrono::milliseconds(wait_ms),
        std::move(operation)).handle_exception_type(
            [this, topic = partition_config.topic, partition = partition_config.partition](const seastar::timed_out_error&) {
                ++produce_errors_;
                ProducePartitionResult result;
                result.topic = topic;
                result.partition = partition;
                result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::RequestTimedOut);
                return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
            });
}

seastar::future<FetchPartitionResult> BrokerShard::fetch(FetchPartitionRequest request) {
    const auto owner = owner_shard(request.topic, request.partition);
    if (!owner.has_value()) {
        ++fetch_errors_;
        FetchPartitionResult result;
        result.error_code = partition_missing();
        return seastar::make_ready_future<FetchPartitionResult>(std::move(result));
    }
    if (*owner != seastar::this_shard_id()) {
        return peers_->invoke_on(*owner, [request = std::move(request)](BrokerShard& shard) mutable {
            return shard.fetch(std::move(request));
        });
    }
    auto it = partitions_.find({request.topic, request.partition});
    if (it == partitions_.end()) {
        ++fetch_errors_;
        FetchPartitionResult result;
        result.error_code = partition_missing();
        return seastar::make_ready_future<FetchPartitionResult>(std::move(result));
    }
    const auto& partition = it->second.config();
    const bool local_is_replica = std::find(partition.replicas.begin(), partition.replicas.end(), config_.node_id) != partition.replicas.end();
    if (!local_is_replica || (partition.leader != config_.node_id && !partition.allow_follower_fetch)) {
        FetchPartitionResult result;
        result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::NotLeaderOrFollower);
        return seastar::make_ready_future<FetchPartitionResult>(std::move(result));
    }
    auto& partition_log = it->second;
    return partition_log.fetch(request.offset, request.max_bytes).then(
        [this, request = std::move(request), &partition_log](FetchPartitionResult result) mutable {
            const auto wait_ms = std::min<std::int32_t>(
                std::max<std::int32_t>(0, request.max_wait_ms),
                static_cast<std::int32_t>(config_.fetch_max_wait_ms_cap));
            if (result.error_code != 0 ||
                !result.records.empty() ||
                wait_ms <= 0 ||
                request.min_bytes <= 0 ||
                request.offset < result.log_end_offset) {
                return seastar::make_ready_future<FetchPartitionResult>(std::move(result));
            }
            return seastar::sleep(std::chrono::milliseconds(wait_ms)).then(
                [&partition_log, request = std::move(request)] {
                    return partition_log.fetch(request.offset, request.max_bytes);
                });
        });
}

seastar::future<ListOffsetPartitionResult> BrokerShard::list_offsets(ListOffsetPartitionRequest request) {
    const auto owner = owner_shard(request.topic, request.partition);
    if (!owner.has_value()) {
        ListOffsetPartitionResult result;
        result.topic = std::move(request.topic);
        result.partition = request.partition;
        result.error_code = partition_missing();
        return seastar::make_ready_future<ListOffsetPartitionResult>(std::move(result));
    }
    if (*owner != seastar::this_shard_id()) {
        return peers_->invoke_on(*owner, [request = std::move(request)](BrokerShard& shard) mutable {
            return shard.list_offsets(std::move(request));
        });
    }
    const auto it = partitions_.find({request.topic, request.partition});
    if (it == partitions_.end()) {
        ListOffsetPartitionResult result;
        result.topic = std::move(request.topic);
        result.partition = request.partition;
        result.error_code = partition_missing();
        return seastar::make_ready_future<ListOffsetPartitionResult>(std::move(result));
    }
    return seastar::make_ready_future<ListOffsetPartitionResult>(it->second.list_offsets(request.timestamp));
}

seastar::future<ProducePartitionResult> BrokerShard::append_replica(ReplicaAppendRequest request) {
    ++replica_appends_;
    const auto owner = owner_shard(request.topic, request.partition);
    if (!owner.has_value()) {
        ++replica_errors_;
        ProducePartitionResult result;
        result.topic = std::move(request.topic);
        result.partition = request.partition;
        result.error_code = partition_missing();
        return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
    }
    if (*owner != seastar::this_shard_id()) {
        return peers_->invoke_on(*owner, [request = std::move(request)](BrokerShard& shard) mutable {
            return shard.append_replica(std::move(request));
        });
    }
    auto it = partitions_.find({request.topic, request.partition});
    if (it == partitions_.end()) {
        ++replica_errors_;
        ProducePartitionResult result;
        result.topic = std::move(request.topic);
        result.partition = request.partition;
        result.error_code = partition_missing();
        return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
    }
    const auto& partition = it->second.config();
    const bool local_is_replica = std::find(partition.replicas.begin(), partition.replicas.end(), config_.node_id) != partition.replicas.end();
    if (!local_is_replica) {
        ++replica_errors_;
        ProducePartitionResult result;
        result.topic = std::move(request.topic);
        result.partition = request.partition;
        result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::NotLeaderOrFollower);
        return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
    }
    return it->second.append_replica(std::move(request));
}

seastar::future<ReplicaFetchResult> BrokerShard::fetch_for_replica(ReplicaFetchRequest request) {
    const auto owner = owner_shard(request.topic, request.partition);
    if (!owner.has_value()) {
        ReplicaFetchResult result;
        result.error_code = partition_missing();
        return seastar::make_ready_future<ReplicaFetchResult>(std::move(result));
    }
    if (*owner != seastar::this_shard_id()) {
        return peers_->invoke_on(*owner, [request = std::move(request)](BrokerShard& shard) mutable {
            return shard.fetch_for_replica(std::move(request));
        });
    }
    auto it = partitions_.find({request.topic, request.partition});
    if (it == partitions_.end()) {
        ReplicaFetchResult result;
        result.error_code = partition_missing();
        return seastar::make_ready_future<ReplicaFetchResult>(std::move(result));
    }
    const auto& partition = it->second.config();
    if (partition.leader != config_.node_id) {
        ReplicaFetchResult result;
        result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::NotLeaderOrFollower);
        return seastar::make_ready_future<ReplicaFetchResult>(std::move(result));
    }
    return it->second.fetch_for_replica(request.offset, request.max_bytes);
}

std::vector<TopicConfig> BrokerShard::topics() const {
    return config_.topics;
}

std::optional<PartitionConfig> BrokerShard::partition_config(const std::string& topic, std::int32_t partition) const {
    return find_partition(config_, topic, partition);
}

seastar::future<> BrokerShard::accept_loop() {
    return seastar::keep_doing([this] {
        return listener_->accept().then([this](seastar::accept_result accepted) {
            (void) seastar::with_gate(gate_, [this, socket = std::move(accepted.connection)]() mutable {
                return handle_connection(std::move(socket));
            });
        });
    }).handle_exception_type([](const std::system_error&) {
        return seastar::make_ready_future<>();
    });
}

seastar::future<> BrokerShard::handle_connection(seastar::connected_socket socket) {
    auto in = socket.input();
    auto out = socket.output();
    return seastar::do_with(std::move(in), std::move(out), [this](auto& in, auto& out) {
        return seastar::repeat([this, &in, &out] {
            if (stopping_) {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            }
            return in.read_exactly(4).then([this, &in, &out](seastar::temporary_buffer<char> size_buf) {
                if (size_buf.size() != 4) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                const auto frame_size = decode_frame_size(size_buf.get());
                if (frame_size <= 0 || frame_size > 100 * 1024 * 1024) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                return in.read_exactly(static_cast<std::size_t>(frame_size)).then(
                    [this, &out, frame_size](seastar::temporary_buffer<char> frame) {
                        if (frame.size() != static_cast<std::size_t>(frame_size)) {
                            return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                        }
                        std::vector<std::uint8_t> payload(frame.size());
                        std::memcpy(payload.data(), frame.get(), frame.size());
                        return handle_kafka_frame(std::move(payload)).then([&out](auto response) {
                            if (!response.has_value()) {
                                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                            }
                            return write_response(out, *response).then([] {
                                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                            });
                        });
                    });
            }).handle_exception([](std::exception_ptr) {
                return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
            });
        }).finally([&out] {
            return out.close();
        });
    });
}

void BrokerShard::register_metrics() {
    namespace sm = seastar::metrics;
    metrics_.add_group("mkmq", {
        sm::make_counter("kafka_requests", sm::description("Kafka requests handled on this shard"), [this] { return kafka_requests_; }),
        sm::make_counter("kafka_errors", sm::description("Kafka request errors on this shard"), [this] { return kafka_errors_; }),
        sm::make_counter("produce_errors", sm::description("Produce partition errors on this shard"), [this] { return produce_errors_; }),
        sm::make_counter("fetch_errors", sm::description("Fetch partition errors on this shard"), [this] { return fetch_errors_; }),
        sm::make_counter("replica_appends", sm::description("Replica append RPCs handled on this shard"), [this] { return replica_appends_; }),
        sm::make_counter("replica_errors", sm::description("Replica append and replication errors on this shard"), [this] { return replica_errors_; }),
        sm::make_counter("broker_health_checks", sm::description("Mercury health checks issued by this shard"), [this] { return health_checks_; }),
        sm::make_counter("broker_health_failures", sm::description("Mercury health check failures observed by this shard"), [this] { return health_failures_; }),
        sm::make_counter("replica_catchup_attempts", sm::description("Follower catch-up attempts issued by this shard"), [this] { return catchup_attempts_; }),
        sm::make_counter("replica_catchup_errors", sm::description("Follower catch-up failures observed by this shard"), [this] { return catchup_errors_; }),
        sm::make_counter("replica_catchup_records", sm::description("Records appended through follower catch-up on this shard"), [this] { return catchup_records_; }),
        sm::make_counter("mercury_rpc_forwards", sm::description("Mercury RPC forwards issued by this shard"), [this] {
            return mercury_.rpc_forwards();
        }),
        sm::make_counter("mercury_rpc_receives", sm::description("Mercury RPC requests received by this shard"), [this] {
            return mercury_.rpc_receives();
        }),
        sm::make_counter("mercury_rpc_errors", sm::description("Mercury RPC errors observed by this shard"), [this] {
            return mercury_.rpc_errors();
        }),
        sm::make_counter("mercury_rpc_timeouts", sm::description("Mercury RPC forward timeouts observed by this shard"), [this] {
            return mercury_.rpc_timeouts();
        }),
        sm::make_counter("mercury_bulk_transfers", sm::description("Mercury bulk transfers completed by this shard"), [this] {
            return mercury_.bulk_transfers();
        }),
        sm::make_counter("mercury_bulk_bytes", sm::description("Mercury bulk payload bytes received by this shard"), [this] {
            return mercury_.bulk_bytes();
        }),
        sm::make_counter("mercury_bulk_errors", sm::description("Mercury bulk transfer errors observed by this shard"), [this] {
            return mercury_.bulk_errors();
        }),
        sm::make_counter("disk_writes", sm::description("Local partition DMA log writes on this shard"), [this] {
            std::uint64_t count = 0;
            for (const auto& [_, partition] : partitions_) {
                count += partition.disk_writes();
            }
            return count;
        }),
        sm::make_counter("disk_fsyncs", sm::description("Local partition log fsync operations on this shard"), [this] {
            std::uint64_t count = 0;
            for (const auto& [_, partition] : partitions_) {
                count += partition.disk_fsyncs();
            }
            return count;
        }),
        sm::make_histogram("kafka_request_latency", sm::description("Kafka request handling latency in microseconds"), [this] {
            return kafka_request_latency_.to_metrics_histogram();
        }),
        sm::make_histogram("produce_latency", sm::description("Produce request handling latency in microseconds"), [this] {
            return produce_latency_.to_metrics_histogram();
        }),
        sm::make_histogram("fetch_latency", sm::description("Fetch request handling latency in microseconds"), [this] {
            return fetch_latency_.to_metrics_histogram();
        }),
        sm::make_histogram("disk_write_latency", sm::description("DMA log write latency in microseconds"), [this] {
            LatencyHistogram histogram;
            for (const auto& [_, partition] : partitions_) {
                histogram.merge(partition.disk_write_latency());
            }
            return histogram.to_metrics_histogram();
        }),
        sm::make_histogram("disk_fsync_latency", sm::description("Log fsync latency in microseconds"), [this] {
            LatencyHistogram histogram;
            for (const auto& [_, partition] : partitions_) {
                histogram.merge(partition.disk_fsync_latency());
            }
            return histogram.to_metrics_histogram();
        }),
        sm::make_histogram("mercury_rpc_forward_latency", sm::description("Mercury RPC forward latency in microseconds"), [this] {
            return mercury_.rpc_forward_latency().to_metrics_histogram();
        }),
        sm::make_histogram("mercury_rpc_handler_latency", sm::description("Mercury RPC handler latency in microseconds"), [this] {
            return mercury_.rpc_handler_latency().to_metrics_histogram();
        }),
        sm::make_histogram("mercury_bulk_latency", sm::description("Mercury bulk pull latency in microseconds"), [this] {
            return mercury_.bulk_latency().to_metrics_histogram();
        }),
        sm::make_gauge("degraded_partitions", sm::description("Local partitions with failed ISR replication"), [this] {
            std::uint64_t count = 0;
            for (const auto& [_, partition] : partitions_) {
                if (partition.degraded()) {
                    ++count;
                }
            }
            return count;
        }),
        sm::make_gauge("replica_lag", sm::description("Total local partition replica lag in records"), [this] {
            std::int64_t lag = 0;
            for (const auto& [_, partition] : partitions_) {
                lag += partition.replica_lag();
            }
            return lag;
        }),
        sm::make_gauge("queue_depth", sm::description("Total local partition append queue depth"), [this] {
            std::uint64_t depth = 0;
            for (const auto& [_, partition] : partitions_) {
                depth += partition.queue_depth();
            }
            return depth;
        }),
    });
}

void BrokerShard::register_mercury_rpcs() {
    mercury_.register_rpc("replica_append", [this](std::vector<std::uint8_t> payload) {
        auto request = decode_replica_append(payload);
        return append_replica(request).then([request = std::move(request)](ProducePartitionResult result) {
            const auto log_end_offset = result.error_code == 0 ? result.log_end_offset : request.base_offset;
            return seastar::make_ready_future<std::vector<std::uint8_t>>(
                encode_replica_append_response(result.error_code, log_end_offset));
        });
    });
    mercury_.register_rpc("replica_fetch", [this](std::vector<std::uint8_t> payload) {
        auto request = decode_replica_fetch(payload);
        return fetch_for_replica(std::move(request)).then([](ReplicaFetchResult result) {
            return seastar::make_ready_future<std::vector<std::uint8_t>>(
                encode_replica_fetch_response(result));
        });
    });
    mercury_.register_rpc("health_ping", [](std::vector<std::uint8_t>) {
        return seastar::make_ready_future<std::vector<std::uint8_t>>(std::vector<std::uint8_t>{});
    });
}

void BrokerShard::arm_maintenance_timer() {
    if (!stopping_) {
        maintenance_timer_.arm(std::chrono::seconds(1));
    }
}

seastar::future<> BrokerShard::maintenance_once() {
    std::vector<seastar::future<>> futures;
    for (const auto& broker : config_.brokers) {
        if (broker.id != config_.node_id) {
            futures.push_back(check_peer_health(broker.id));
        }
    }
    for (const auto& [_, partition] : partitions_) {
        const auto& partition_config = partition.config();
        const bool local_is_replica =
            std::find(partition_config.replicas.begin(), partition_config.replicas.end(), config_.node_id) != partition_config.replicas.end();
        if (local_is_replica && partition_config.leader != config_.node_id) {
            futures.push_back(catch_up_follower_partition(partition_config));
        }
    }
    return seastar::when_all_succeed(futures.begin(), futures.end()).discard_result();
}

seastar::future<> BrokerShard::check_peer_health(std::int32_t broker_id) {
    const auto broker = find_broker(config_, broker_id);
    if (!broker.has_value()) {
        ++health_failures_;
        return seastar::make_ready_future<>();
    }
    ++health_checks_;
    return mercury_.forward(broker->mercury_address, "health_ping", {}).discard_result().handle_exception([this](std::exception_ptr) {
        ++health_failures_;
    });
}

seastar::future<> BrokerShard::catch_up_follower_partition(const PartitionConfig& partition) {
    const auto leader = partition.leader;
    const auto it = partitions_.find({partition.topic, partition.partition});
    if (it == partitions_.end()) {
        return seastar::make_ready_future<>();
    }
    const auto local_end = it->second.offsets().latest;
    ReplicaFetchRequest request;
    request.topic = partition.topic;
    request.partition = partition.partition;
    request.offset = local_end;
    request.max_bytes = kReplicaCatchupMaxBytes;
    ++catchup_attempts_;
    return send_replica_fetch(leader, std::move(request)).then(
        [this, local_end, partition](ReplicaFetchResult result) mutable {
            if (result.error_code != 0 || result.records.empty()) {
                return seastar::make_ready_future<>();
            }
            ReplicaAppendRequest append;
            append.topic = partition.topic;
            append.partition = partition.partition;
            append.base_offset = local_end;
            append.leader_high_watermark = result.leader_high_watermark;
            append.records = std::move(result.records);
            return append_replica(std::move(append)).then([this, partition, local_end](ProducePartitionResult append_result) {
                if (append_result.error_code != 0) {
                    ++catchup_errors_;
                    return;
                }
                catchup_records_ += static_cast<std::uint64_t>(std::max<std::int64_t>(0, append_result.log_end_offset - local_end));
                auto it = partitions_.find({partition.topic, partition.partition});
                if (it != partitions_.end() && it->second.offsets().latest >= append_result.log_end_offset) {
                    it->second.mark_replicated_through(it->second.offsets().latest);
                }
            });
        }).handle_exception([this](std::exception_ptr) {
            ++catchup_errors_;
        });
}

std::optional<unsigned> BrokerShard::owner_shard(const std::string& topic, std::int32_t partition) const {
    const auto config = find_partition(config_, topic, partition);
    if (!config.has_value()) {
        return std::nullopt;
    }
    return config->shard;
}

seastar::future<> BrokerShard::replicate_to_isr(
    const PartitionConfig& partition,
    const ProducePartitionResult& local_result,
    const std::vector<std::uint8_t>& records) {
    std::vector<seastar::future<>> replicas;
    for (const auto broker_id : partition.isr) {
        if (broker_id == config_.node_id) {
            continue;
        }
        ReplicaAppendRequest request;
        request.topic = partition.topic;
        request.partition = partition.partition;
        request.base_offset = local_result.base_offset;
        request.leader_high_watermark = local_result.log_end_offset;
        request.records = records;
        replicas.push_back(send_replica_append(broker_id, std::move(request)));
    }
    if (replicas.empty()) {
        return seastar::make_ready_future<>();
    }
    return seastar::when_all_succeed(replicas.begin(), replicas.end()).discard_result();
}

seastar::future<> BrokerShard::send_replica_append(std::int32_t broker_id, ReplicaAppendRequest request) {
    const auto broker = find_broker(config_, broker_id);
    if (!broker.has_value()) {
        return seastar::make_exception_future<>(std::runtime_error("replica broker is not configured"));
    }
    return mercury_.forward(broker->mercury_address, "replica_append", encode_replica_append(request)).then(
        [request = std::move(request)](std::vector<std::uint8_t> payload) {
            const auto result = decode_replica_append_response(request, payload);
            if (result.error_code != 0) {
                return seastar::make_exception_future<>(std::runtime_error("replica append failed"));
            }
            return seastar::make_ready_future<>();
        });
}

seastar::future<ReplicaFetchResult> BrokerShard::send_replica_fetch(std::int32_t broker_id, ReplicaFetchRequest request) {
    const auto broker = find_broker(config_, broker_id);
    if (!broker.has_value()) {
        return seastar::make_exception_future<ReplicaFetchResult>(std::runtime_error("replica broker is not configured"));
    }
    return mercury_.forward(broker->mercury_address, "replica_fetch", encode_replica_fetch(request)).then(
        [](std::vector<std::uint8_t> payload) {
            return seastar::make_ready_future<ReplicaFetchResult>(decode_replica_fetch_response(payload));
        });
}

}  // namespace mkmq

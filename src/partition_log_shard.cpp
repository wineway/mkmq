// All PartitionLogShard methods live here. Kept separate from BrokerShard to
// keep the per-shard log implementation (DMA segment I/O, index recovery,
// fsync) cleanly isolated from the Kafka front-end and replication glue.

#include "broker_shard_detail.hpp"
#include "mkmq/broker_shard.hpp"
#include "mkmq/kafka_protocol.hpp"

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <exception>
#include <stdexcept>

namespace mkmq {

using detail::get_i32;
using detail::get_i64;
using detail::kIndexMagic;
using detail::kIndexEntryPayloadSize;
using detail::kLogMagic;
using detail::put_i32;
using detail::put_i64;
using detail::round_up;
using detail::sanitized_topic_name;
using detail::segment_base_offset_from_name;
using detail::segment_name;

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
    index_entries_.clear();
    index_write_error_ = nullptr;
    return seastar::recursive_touch_directory(root_.string()).then([this] {
        return recover();
    }).then([this] {
        return open_active_segment();
    }).then([this] {
        return open_index();
    });
}

void PartitionLogShard::update_dma_alignment(const seastar::file& file) noexcept {
    log_frame_alignment_ = std::max<std::uint64_t>(1, file.disk_write_dma_alignment());
    index_entry_size_ = round_up(kIndexEntryPayloadSize, log_frame_alignment_);
    memory_dma_alignment_ = std::max<std::uint64_t>(1, file.memory_dma_alignment());
}

seastar::future<> PartitionLogShard::stop() {
    // Drain any in-flight index DMA writes scheduled via schedule_index_entry_write
    // before we close the index file out from under them. If the gate was
    // already closed (stop called twice), treat it as a no-op.
    auto drain = index_write_gate_.is_closed()
        ? seastar::make_ready_future<>()
        : index_write_gate_.close();
    return drain.then([this] {
        // Drop all cached segment handles. Each shared_ptr's deleter enqueues
        // the close() onto segment_close_gate_, which we drain below.
        segment_cache_.clear();
        if (segment_close_gate_.is_closed()) {
            return seastar::make_ready_future<>();
        }
        return segment_close_gate_.close();
    }).finally([this] {
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
    });
}

const PartitionConfig& PartitionLogShard::config() const noexcept { return config_; }
PartitionOffsets PartitionLogShard::offsets() const noexcept { return offsets_; }
std::uint32_t PartitionLogShard::queue_depth() const noexcept { return queue_depth_; }
bool PartitionLogShard::degraded() const noexcept { return degraded_; }
std::int64_t PartitionLogShard::replica_lag() const noexcept { return replica_lag_; }
std::uint64_t PartitionLogShard::disk_writes() const noexcept { return disk_writes_; }
std::uint64_t PartitionLogShard::disk_fsyncs() const noexcept { return disk_fsyncs_; }
LatencyHistogram PartitionLogShard::disk_write_latency() const { return disk_write_latency_; }
LatencyHistogram PartitionLogShard::disk_fsync_latency() const { return disk_fsync_latency_; }

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
    // Contract: `records` arrive already rewritten to this base_offset. Every
    // in-tree caller (BrokerShard::append) rewrites before invoking us, and
    // because each shard's reactor is single-threaded there is no chance for
    // the partition's `offsets_.latest` to drift between that rewrite and this
    // function body. Re-running the rewrite here would double the allocation
    // and CRC32C cost on every produce — unacceptable on the HFT hot path.
    const std::int64_t count = estimate_record_count(records);
    const std::int64_t last_offset = count == 0 ? base_offset - 1 : base_offset + count - 1;
    offsets_.latest = base_offset + count;
    if (config_.isr.size() <= 1) {
        offsets_.high_watermark = offsets_.latest;
    }

    RecordSet record_set{base_offset, last_offset, std::move(records)};
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

// ---- Filesystem paths ------------------------------------------------------
std::filesystem::path PartitionLogShard::segment_path(std::int64_t base_offset) const {
    return root_ / segment_name(config_, base_offset);
}

std::filesystem::path PartitionLogShard::index_path() const {
    return root_ / (sanitized_topic_name(config_.topic) + "-" + std::to_string(config_.partition) + ".index");
}

// ---- Recovery --------------------------------------------------------------
seastar::future<> PartitionLogShard::recover() {
    segments_.clear();
    index_entries_.clear();
    offsets_ = {};
    active_segment_base_offset_ = 0;
    write_position_ = 0;
    index_position_ = 0;

    return recover_from_index().then([this](bool recovered) {
        if (recovered) {
            return recovered_index_covers_segments().then([this](bool complete) {
                if (complete) {
                    return seastar::make_ready_future<>();
                }
                segments_.clear();
                index_entries_.clear();
                offsets_ = {};
                active_segment_base_offset_ = 0;
                write_position_ = 0;
                index_position_ = 0;
                return recover_from_segments();
            });
        }
        return recover_from_segments();
    });
}

seastar::future<bool> PartitionLogShard::recover_from_index() {
    seastar::file index;
    try {
        index = co_await seastar::open_file_dma(index_path().string(), seastar::open_flags::ro);
        update_dma_alignment(index);
    } catch (...) {
        co_return false;
    }

    bool recovered = false;
    std::exception_ptr error;
    try {
        const auto size = co_await index.size();
        for (std::uint64_t position = 0; position + index_entry_size_ <= size; position += index_entry_size_) {
            auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(index_entry_size_, memory_dma_alignment_);
            const auto bytes_read = co_await index.dma_read(position, buffer.get(), index_entry_size_);
            if (bytes_read != index_entry_size_) {
                break;
            }
            auto entry = decode_index_entry(buffer.get());
            if (!entry.has_value()) {
                break;
            }
            note_index_entry(*entry);
            offsets_.latest = std::max(offsets_.latest, entry->last_offset + 1);
            offsets_.high_watermark = offsets_.latest;
            index_position_ += index_entry_size_;
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

seastar::future<bool> PartitionLogShard::recovered_index_covers_segments() {
    if (segments_.empty()) {
        co_return true;
    }

    const auto segment_files = co_await list_segment_files();
    if (segment_files.size() != segments_.size()) {
        co_return false;
    }

    for (std::size_t i = 0; i < segment_files.size(); ++i) {
        const auto& [segment_base, path] = segment_files[i];
        if (segment_base != segments_[i].base_offset) {
            co_return false;
        }
        seastar::file segment = co_await seastar::open_file_dma(path.string(), seastar::open_flags::ro);
        const auto size = co_await segment.size();
        co_await segment.close();
        if (size != segments_[i].write_position) {
            co_return false;
        }
    }

    co_return true;
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
    update_dma_alignment(segment);
    std::exception_ptr error;
    try {
        const auto size = co_await segment.size();
        std::uint64_t position = 0;
        while (position + log_frame_alignment_ <= size) {
            auto header_buffer = seastar::allocate_aligned_buffer<std::uint8_t>(log_frame_alignment_, memory_dma_alignment_);
            const auto header_bytes = co_await segment.dma_read(position, header_buffer.get(), log_frame_alignment_);
            if (header_bytes != log_frame_alignment_ ||
                std::memcmp(header_buffer.get(), kLogMagic, sizeof(kLogMagic)) != 0) {
                break;
            }
            const auto* raw_header = reinterpret_cast<const char*>(header_buffer.get());
            const std::int64_t base_offset = get_i64(raw_header + 8);
            const std::int32_t record_bytes = get_i32(raw_header + 16);
            if (record_bytes <= 0) {
                break;
            }
            const auto frame_size = round_up(20 + static_cast<std::uint64_t>(record_bytes), log_frame_alignment_);
            if (position + frame_size > size) {
                break;
            }

            std::vector<std::uint8_t> records(static_cast<std::size_t>(record_bytes));
            if (frame_size == log_frame_alignment_) {
                std::memcpy(records.data(), header_buffer.get() + 20, records.size());
            } else {
                auto frame_buffer = seastar::allocate_aligned_buffer<std::uint8_t>(frame_size, memory_dma_alignment_);
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
            rebuilt_index_position += index_entry_size_;
            index_position_ += index_entry_size_;
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

// ---- Segment / index file I/O ----------------------------------------------
seastar::future<> PartitionLogShard::open_active_segment() {
    seastar::file_open_options options;
    options.extent_allocation_size_hint = segment_bytes_;
    options.durable = config_.durability == Durability::Fsync;
    return seastar::open_file_dma(
        segment_path(active_segment_base_offset_).string(),
        seastar::open_flags::rw | seastar::open_flags::create,
        options).then([this](seastar::file file) {
            update_dma_alignment(file);
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
            update_dma_alignment(file);
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
    // Segment rollover. Instead of closing the old rw handle immediately (which
    // would race with in-flight reads holding a shared aliased copy), hand it
    // to the segment read cache. Future reads of this now-sealed segment hit
    // the cache, and close() eventually happens via the shared_ptr deleter
    // once cache + readers all release.
    const auto sealed_base = active_segment_base_offset_;
    auto old_file = std::move(*file_);
    file_.reset();
    auto wrapped = seastar::make_lw_shared<CachedSegmentFile>(
        std::move(old_file), &segment_close_gate_);
    while (segment_cache_.size() >= kSegmentCacheMax) {
        segment_cache_.pop_back();
    }
    segment_cache_.push_front(SegmentCacheEntry{sealed_base, std::move(wrapped)});
    active_segment_base_offset_ = base_offset;
    write_position_ = 0;
    return open_active_segment();
}

seastar::future<> PartitionLogShard::write_record_set(const RecordSet& record_set) {
    const auto started = std::chrono::steady_clock::now();
    const std::uint64_t header_size = 20;
    const std::uint64_t frame_size = round_up(header_size + record_set.bytes.size(), log_frame_alignment_);
    auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(frame_size, memory_dma_alignment_);
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
                    schedule_index_entry_write(base_offset, last_offset, record_bytes, segment_base, position);
                    return seastar::make_ready_future<>();
                });
        });
}

void PartitionLogShard::schedule_index_entry_write(
    std::int64_t base_offset,
    std::int64_t last_offset,
    std::int32_t record_bytes,
    std::int64_t segment_base,
    std::uint64_t segment_position) {
    if (!index_file_.has_value()) {
        index_write_error_ = std::make_exception_ptr(std::runtime_error("partition index file is not open"));
        return;
    }
    const IndexEntry entry{base_offset, last_offset, segment_base, segment_position, record_bytes};
    const auto position = index_position_;
    index_position_ += index_entry_size_;
    // Install into the in-memory authoritative index synchronously. Reads never
    // wait on the disk write completing — the vector is the source of truth.
    note_index_entry(entry);
    if (index_write_gate_.is_closed()) {
        return;
    }
    // Each entry's file position is pre-assigned, so there is no ordering
    // requirement between concurrent writes. Fire them all in parallel and let
    // Seastar's IO scheduler coalesce.
    (void) seastar::with_gate(index_write_gate_, [this, entry, position] {
        return write_index_entry_at(entry, position).handle_exception(
            [this](std::exception_ptr error) {
                // Stash the first error we see; wait_for_index_writes() (called
                // at stop / maybe_flush) will rethrow it.
                if (!index_write_error_) {
                    index_write_error_ = error;
                }
            });
    });
}

seastar::future<> PartitionLogShard::write_index_entry_at(const IndexEntry& entry, std::uint64_t position) {
    if (!index_file_.has_value()) {
        return seastar::make_exception_future<>(std::runtime_error("partition index file is not open"));
    }
    auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(index_entry_size_, memory_dma_alignment_);
    encode_index_entry(buffer.get(), entry);
    auto raw = buffer.get();
    return index_file_->dma_write(position, raw, index_entry_size_).then(
        [buffer = std::move(buffer)](std::size_t) mutable {});
}

seastar::future<> PartitionLogShard::wait_for_index_writes() {
    // stop() drains the gate; callers outside of stop just need to see any
    // latched error. Draining on every maybe_flush would defeat the whole
    // point of firing index writes in parallel.
    if (index_write_error_) {
        return seastar::make_exception_future<>(index_write_error_);
    }
    return seastar::make_ready_future<>();
}

// ---- Index reading / record reading ----------------------------------------
std::size_t PartitionLogShard::find_index_for_offset(std::int64_t offset) const noexcept {
    // index_entries_ is monotonically increasing in (base_offset, last_offset)
    // — append order == offset order. Find the first entry that covers `offset`
    // (i.e. last_offset >= offset). A linear memory lower_bound at O(log n)
    // replaces the previous DMA-per-step on-disk binary search.
    const auto it = std::lower_bound(
        index_entries_.begin(), index_entries_.end(), offset,
        [](const IndexEntry& entry, std::int64_t target) noexcept {
            return entry.last_offset < target;
        });
    return static_cast<std::size_t>(it - index_entries_.begin());
}

PartitionLogShard::CachedSegmentFile::~CachedSegmentFile() {
    // close_gate == nullptr marks an aliasing handle over the active segment —
    // file_ owns the actual close, so we just drop our seastar::file value and
    // decrement file_impl's (non-atomic) refcount.
    if (close_gate == nullptr) {
        return;
    }
    try {
        (void) seastar::with_gate(*close_gate, [f = std::move(file)]() mutable {
            return f.close();
        });
    } catch (const seastar::gate_closed_exception&) {
        // Shutdown raced us. The reactor is tearing down; drop the handle and
        // let file_impl's refcount drop to zero. close() wasn't made, so the
        // FD may leak for the remainder of the process — but we're exiting.
    }
}

seastar::future<PartitionLogShard::CachedSegmentPtr> PartitionLogShard::get_segment_file_for_read(
    std::int64_t segment_base) {
    // Active segment: return an aliasing handle (close_gate = nullptr). The
    // copied seastar::file shares file_impl with file_ via its internal
    // non-atomic intrusive refcount; when the reader's lw_shared_ptr drops,
    // the aliasing wrapper's dtor is a no-op, preserving file_'s ownership of
    // the FD and its close in stop()/rollover.
    if (segment_base == active_segment_base_offset_ && file_.has_value()) {
        return seastar::make_ready_future<CachedSegmentPtr>(
            seastar::make_lw_shared<CachedSegmentFile>(*file_, nullptr));
    }
    // Cache hit: move to MRU and alias the cached handle.
    for (auto it = segment_cache_.begin(); it != segment_cache_.end(); ++it) {
        if (it->segment_base == segment_base) {
            if (it != segment_cache_.begin()) {
                segment_cache_.splice(segment_cache_.begin(), segment_cache_, it);
            }
            return seastar::make_ready_future<CachedSegmentPtr>(segment_cache_.front().file);
        }
    }
    // Cache miss: open ro and install. Evicting the LRU just drops the cache's
    // lw_shared_ptr; if readers still hold refs, the dtor (and thus close())
    // defers until the last reader releases.
    return seastar::open_file_dma(segment_path(segment_base).string(), seastar::open_flags::ro).then(
        [this, segment_base](seastar::file file) {
            auto wrapped = seastar::make_lw_shared<CachedSegmentFile>(
                std::move(file), &segment_close_gate_);
            while (segment_cache_.size() >= kSegmentCacheMax) {
                segment_cache_.pop_back();
            }
            segment_cache_.push_front(SegmentCacheEntry{segment_base, wrapped});
            return seastar::make_ready_future<CachedSegmentPtr>(std::move(wrapped));
        });
}

seastar::future<std::vector<std::uint8_t>> PartitionLogShard::read_segment_record(const IndexEntry& entry) {
    const std::uint64_t header_size = 20;
    const auto frame_size = round_up(header_size + static_cast<std::uint64_t>(entry.record_bytes), log_frame_alignment_);
    auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(frame_size, memory_dma_alignment_);
    auto raw = buffer.get();
    return get_segment_file_for_read(entry.segment_base).then(
        [entry, frame_size, raw, buffer = std::move(buffer)](CachedSegmentPtr handle) mutable {
            // Keep `handle` alive across the dma_read via the continuation's
            // capture — lw_shared_ptr ref holds the FD open even if the cache
            // evicts under us.
            return handle->file.dma_read(entry.segment_position, raw, frame_size).then(
                [entry, frame_size, handle, buffer = std::move(buffer)](std::size_t bytes_read) mutable {
                    if (bytes_read != frame_size ||
                        std::memcmp(buffer.get(), kLogMagic, sizeof(kLogMagic)) != 0 ||
                        get_i64(reinterpret_cast<const char*>(buffer.get()) + 8) != entry.base_offset ||
                        get_i32(reinterpret_cast<const char*>(buffer.get()) + 16) != entry.record_bytes) {
                        return seastar::make_exception_future<std::vector<std::uint8_t>>(
                            std::runtime_error("partition segment frame is corrupt"));
                    }
                    std::vector<std::uint8_t> records(static_cast<std::size_t>(entry.record_bytes));
                    if (!records.empty()) {
                        std::memcpy(records.data(), buffer.get() + 20, records.size());
                    }
                    return seastar::make_ready_future<std::vector<std::uint8_t>>(std::move(records));
                });
        });
}

seastar::future<std::vector<std::uint8_t>> PartitionLogShard::read_records_from_index(
    std::int64_t offset,
    std::int64_t visible_end,
    std::size_t limit) {
    // Authoritative index is in memory — no waiting on in-flight DMA index
    // writes, no per-step DMA on a binary search. Snapshot the starting
    // position synchronously and drive the read loop off of index_entries_.
    const auto start_idx = find_index_for_offset(offset);
    return seastar::do_with(
        std::vector<std::uint8_t>{},
        start_idx,
        [this, offset, visible_end, limit](std::vector<std::uint8_t>& records, std::size_t& idx) {
            return seastar::repeat([this, offset, visible_end, limit, &records, &idx] {
                if (idx >= index_entries_.size()) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                const IndexEntry entry = index_entries_[idx++];
                if (entry.last_offset < offset) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::no);
                }
                if (entry.base_offset >= visible_end || entry.last_offset >= visible_end) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                const auto record_bytes = static_cast<std::size_t>(entry.record_bytes);
                if (!records.empty() && records.size() + record_bytes > limit) {
                    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
                }
                return read_segment_record(entry).then(
                    [limit, &records](std::vector<std::uint8_t> record_bytes) {
                        records.insert(records.end(), record_bytes.begin(), record_bytes.end());
                        return seastar::make_ready_future<seastar::stop_iteration>(
                            records.size() >= limit ? seastar::stop_iteration::yes : seastar::stop_iteration::no);
                    });
            }).then([&records] {
                return seastar::make_ready_future<std::vector<std::uint8_t>>(std::move(records));
            });
        });
}

void PartitionLogShard::note_index_entry(const IndexEntry& entry) {
    // Mirror every index entry into the in-memory vector so fetch / seek paths
    // never touch the index file on the hot path. Entries arrive in append
    // order (== offset order) from every caller: append's
    // schedule_index_entry_write, recover_from_index's sequential scan, and
    // recover_from_segments' per-frame walk.
    index_entries_.push_back(entry);
    const auto frame_size = round_up(20 + static_cast<std::uint64_t>(entry.record_bytes), log_frame_alignment_);
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
    auto buffer = seastar::allocate_aligned_buffer<std::uint8_t>(index_entry_size_, memory_dma_alignment_);
    encode_index_entry(buffer.get(), entry);
    auto raw = buffer.get();
    return index_file.dma_write(position, raw, index_entry_size_).then(
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

void PartitionLogShard::encode_index_entry(std::uint8_t* bytes, const IndexEntry& entry) const {
    std::memset(bytes, 0, index_entry_size_);
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

}  // namespace mkmq

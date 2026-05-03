#pragma once

#include "mkmq/config.hpp"
#include "mkmq/kafka_protocol.hpp"
#include "mkmq/mercury_shard.hpp"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/internal/estimated_histogram.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>

#include <cstdint>
#include <filesystem>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace mkmq {

using LatencyHistogram = seastar::metrics::internal::time_estimated_histogram;

struct TopicPartition {
    std::string topic;
    std::int32_t partition{0};

    bool operator<(const TopicPartition& other) const noexcept;
};

struct PartitionOffsets {
    std::int64_t earliest{0};
    std::int64_t latest{0};
    std::int64_t high_watermark{0};
};

struct ReplicaAppendRequest {
    std::string topic;
    std::int32_t partition{0};
    std::int64_t base_offset{0};
    std::int64_t leader_high_watermark{0};
    std::vector<std::uint8_t> records;
};

struct ReplicaFetchRequest {
    std::string topic;
    std::int32_t partition{0};
    std::int64_t offset{0};
    std::int32_t max_bytes{0};
};

struct ReplicaFetchResult {
    std::int16_t error_code{0};
    std::int64_t leader_high_watermark{0};
    std::int64_t log_end_offset{0};
    std::vector<std::uint8_t> records;
};

class PartitionLogShard {
public:
    seastar::future<> start(PartitionConfig config, std::filesystem::path root, std::uint64_t segment_bytes);
    seastar::future<> stop();

    const PartitionConfig& config() const noexcept;
    PartitionOffsets offsets() const noexcept;
    std::uint32_t queue_depth() const noexcept;
    bool degraded() const noexcept;
    std::int64_t replica_lag() const noexcept;
    std::uint64_t disk_writes() const noexcept;
    std::uint64_t disk_fsyncs() const noexcept;
    LatencyHistogram disk_write_latency() const;
    LatencyHistogram disk_fsync_latency() const;
    void update_config(PartitionConfig config);

    seastar::future<ProducePartitionResult> append(std::vector<std::uint8_t> records, std::int16_t acks);
    seastar::future<ProducePartitionResult> append_replica(ReplicaAppendRequest request);
    seastar::future<FetchPartitionResult> fetch(std::int64_t offset, std::int32_t max_bytes);
    seastar::future<ReplicaFetchResult> fetch_for_replica(std::int64_t offset, std::int32_t max_bytes);
    ListOffsetPartitionResult list_offsets(std::int64_t timestamp) const;
    void mark_replicated_through(std::int64_t offset);
    void mark_degraded();

    // Parse the Kafka record batch header chain to recover the delta between
    // `records`'s base_offset and its last_offset. Exposed so BrokerShard can
    // compute the log_end_offset synchronously before the DMA write starts —
    // lets leader-side replication dispatch run in parallel with the local
    // write instead of sequencing behind it.
    static std::int64_t estimate_record_count(const std::vector<std::uint8_t>& records);

private:
    struct RecordSet {
        std::int64_t base_offset{0};
        std::int64_t last_offset{-1};
        std::vector<std::uint8_t> bytes;
    };
    struct IndexEntry {
        std::int64_t base_offset{0};
        std::int64_t last_offset{-1};
        std::int64_t segment_base{0};
        std::uint64_t segment_position{0};
        std::int32_t record_bytes{0};
    };
    struct SegmentInfo {
        std::int64_t base_offset{0};
        std::int64_t last_offset{-1};
        std::uint64_t write_position{0};
    };

    std::filesystem::path segment_path(std::int64_t base_offset) const;
    std::filesystem::path index_path() const;
    seastar::future<> recover();
    seastar::future<bool> recover_from_index();
    seastar::future<bool> recovered_index_covers_segments();
    seastar::future<> recover_from_segments();
    seastar::future<std::vector<std::pair<std::int64_t, std::filesystem::path>>> list_segment_files();
    seastar::future<> recover_segment_file(
        std::int64_t segment_base,
        const std::filesystem::path& path,
        seastar::file& rebuilt_index,
        std::uint64_t& rebuilt_index_position);
    seastar::future<> open_active_segment();
    seastar::future<> open_index();
    seastar::future<> ensure_segment_for_write(std::int64_t base_offset, std::uint64_t frame_size);
    seastar::future<> write_record_set(const RecordSet& record_set);
    void schedule_index_entry_write(
        std::int64_t base_offset,
        std::int64_t last_offset,
        std::int32_t record_bytes,
        std::int64_t segment_base,
        std::uint64_t segment_position);
    seastar::future<> write_index_entry_at(const IndexEntry& entry, std::uint64_t position);
    seastar::future<> wait_for_index_writes();
    // Authoritative in-memory index lookup: returns the first index_entries_
    // position whose last_offset >= offset, or index_entries_.size() if none.
    // O(log n) std::lower_bound — no DMA read on the fetch hot path.
    std::size_t find_index_for_offset(std::int64_t offset) const noexcept;
    // RAII wrapper around a seastar::file that defers close() to destruction.
    // seastar::file itself uses non-atomic intrusive refcounting but does not
    // auto-close on destruction — callers must call close() explicitly. This
    // wrapper turns "close when last holder releases" into a destructor, and
    // routes the async close() through segment_close_gate_ so stop() can
    // drain the pending closes before tearing the reactor down.
    //
    // If close_gate is nullptr the destructor is a no-op: used for the active
    // segment where file_ owns the close.
    struct CachedSegmentFile {
        seastar::file file;
        seastar::gate* close_gate;
        CachedSegmentFile(seastar::file f, seastar::gate* g) noexcept
            : file(std::move(f)), close_gate(g) {}
        CachedSegmentFile(const CachedSegmentFile&) = delete;
        CachedSegmentFile& operator=(const CachedSegmentFile&) = delete;
        ~CachedSegmentFile();
    };
    using CachedSegmentPtr = seastar::lw_shared_ptr<CachedSegmentFile>;
    seastar::future<CachedSegmentPtr> get_segment_file_for_read(std::int64_t segment_base);
    seastar::future<std::vector<std::uint8_t>> read_records_from_index(
        std::int64_t offset,
        std::int64_t visible_end,
        std::size_t limit);
    seastar::future<> write_rebuilt_index_entry(
        seastar::file& index_file,
        const IndexEntry& entry,
        std::uint64_t position);
    void update_dma_alignment(const seastar::file& file) noexcept;
    void note_index_entry(const IndexEntry& entry);
    seastar::future<> maybe_flush();
    static std::optional<IndexEntry> decode_index_entry(const std::uint8_t* bytes);
    void encode_index_entry(std::uint8_t* bytes, const IndexEntry& entry) const;

    PartitionConfig config_;
    std::filesystem::path root_;
    std::uint64_t segment_bytes_{0};
    std::vector<SegmentInfo> segments_;
    PartitionOffsets offsets_;
    std::uint32_t queue_depth_{0};
    std::int64_t active_segment_base_offset_{0};
    std::uint64_t write_position_{0};
    std::uint64_t log_frame_alignment_{4096};
    std::uint64_t index_entry_size_{4096};
    std::uint64_t memory_dma_alignment_{4096};
    bool degraded_{false};
    std::int64_t replica_lag_{0};
    std::optional<seastar::file> file_;
    std::optional<seastar::file> index_file_;
    // LRU cache of read-only handles for non-active segments. The active
    // segment is always served directly from file_ (copy of the rw handle),
    // bypassing the cache. Front is MRU, back is LRU. Evicted entries have
    // their shared_ptr dropped — if a reader still holds one the underlying
    // seastar::file stays alive until the reader's copy dies, then the
    // gate-driven deleter closes it.
    struct SegmentCacheEntry {
        std::int64_t segment_base;
        CachedSegmentPtr file;
    };
    std::list<SegmentCacheEntry> segment_cache_;
    static constexpr std::size_t kSegmentCacheMax = 32;
    seastar::gate segment_close_gate_;
    std::uint64_t index_position_{0};
    // Authoritative in-memory mirror of the on-disk index, always in append
    // order (== offset order). Fetch / seek paths consult this directly and
    // never touch the index file on the hot path.
    std::vector<IndexEntry> index_entries_;
    // Gate around asynchronous index DMA writes. Every schedule_index_entry_write
    // enters via with_gate; stop() closes it to drain in-flight writes. Having
    // a gate — rather than the previous tail-chain shared_future — lets N
    // concurrent appends issue their index DMA writes in parallel (positions
    // are pre-assigned, so there's no ordering requirement).
    seastar::gate index_write_gate_;
    std::exception_ptr index_write_error_;
    // Group-commit fsync state. `pending_flush_` represents the next fsync
    // that will land on the current file_ (armed behind a yield()). All
    // appends that call maybe_flush() within one reactor tick share this
    // same future so N demands coalesce into 1 real fsync. flush_scheduled_
    // is cleared when the yield fires (before the actual flush starts), so
    // subsequent appends arm a fresh pending_flush_ for the next batch.
    seastar::shared_future<> pending_flush_;
    bool flush_scheduled_{false};
    std::uint64_t disk_writes_{0};
    std::uint64_t disk_fsyncs_{0};
    LatencyHistogram disk_write_latency_;
    LatencyHistogram disk_fsync_latency_;
};

class BrokerShard {
public:
    BrokerShard();

    seastar::future<> start(BrokerConfig config);
    seastar::future<> stop();
    seastar::future<> apply_config(BrokerConfig config);
    void set_peers(seastar::sharded<BrokerShard>* peers) noexcept;

    const BrokerConfig& config() const noexcept;
    seastar::future<std::optional<std::vector<std::uint8_t>>> handle_kafka_frame(
        std::vector<std::uint8_t> payload);

    seastar::future<ProducePartitionResult> append(
        ProducePartitionRequest request,
        std::int16_t acks,
        std::int32_t timeout_ms);
    seastar::future<FetchPartitionResult> fetch(FetchPartitionRequest request);
    seastar::future<ListOffsetPartitionResult> list_offsets(ListOffsetPartitionRequest request);
    seastar::future<ProducePartitionResult> append_replica(ReplicaAppendRequest request);

    std::vector<TopicConfig> topics() const;
    std::optional<PartitionConfig> partition_config(const std::string& topic, std::int32_t partition) const;

private:
    // Per-partition slot. `log` is owned via unique_ptr so that:
    //   (1) PartitionLogShard addresses are stable across `partitions_` vector
    //       reallocations — every captured `PartitionLogShard*` in an
    //       in-flight future remains valid, and
    //   (2) the flat vector stays cache-friendly for iteration without paying
    //       sizeof(PartitionLogShard) per element move on insert.
    struct PartitionEntry {
        TopicPartition key;
        std::unique_ptr<PartitionLogShard> log;
    };

    seastar::future<> accept_loop();
    seastar::future<> handle_connection(seastar::connected_socket socket);
    void register_metrics();
    void register_mercury_rpcs();
    void arm_maintenance_timer();
    seastar::future<> maintenance_once();
    seastar::future<> check_peer_health(std::int32_t broker_id);
    seastar::future<> catch_up_follower_partition(const PartitionConfig& partition);
    seastar::future<ReplicaFetchResult> fetch_for_replica(ReplicaFetchRequest request);
    std::optional<unsigned> owner_shard(const std::string& topic, std::int32_t partition) const;
    seastar::future<> replicate_to_isr(
        const PartitionConfig& partition,
        const ProducePartitionResult& local_result,
        const std::vector<std::uint8_t>& records);
    seastar::future<ReplicaFetchResult> send_replica_fetch(std::int32_t broker_id, ReplicaFetchRequest request);

    // Hot-path partition lookup: O(log n) binary search with a single-slot
    // pointer cache that skips the search on repeat calls for the same
    // (topic, partition) — the common HFT steady state. Returns nullptr when
    // the key is not locally owned.
    PartitionLogShard* find_partition_log(std::string_view topic, std::int32_t partition) noexcept;
    const PartitionLogShard* find_partition_log(std::string_view topic, std::int32_t partition) const noexcept;
    PartitionLogShard& emplace_partition_log(TopicPartition key);

    BrokerConfig config_;
    seastar::sharded<BrokerShard>* peers_{nullptr};
    std::vector<PartitionEntry> partitions_;  // sorted by key
    mutable PartitionEntry* hot_partition_{nullptr};
    MercuryShard mercury_;
    KafkaProtocol protocol_;
    bool stopping_{false};
    std::optional<seastar::server_socket> listener_;
    seastar::timer<> maintenance_timer_;
    seastar::gate gate_;
    seastar::metrics::metric_groups metrics_;
    std::uint64_t kafka_requests_{0};
    std::uint64_t kafka_errors_{0};
    std::uint64_t produce_errors_{0};
    std::uint64_t fetch_errors_{0};
    std::uint64_t replica_appends_{0};
    std::uint64_t replica_errors_{0};
    std::uint64_t health_checks_{0};
    std::uint64_t health_failures_{0};
    std::uint64_t catchup_attempts_{0};
    std::uint64_t catchup_errors_{0};
    std::uint64_t catchup_records_{0};
    LatencyHistogram kafka_request_latency_;
    LatencyHistogram produce_latency_;
    LatencyHistogram fetch_latency_;
};

}  // namespace mkmq

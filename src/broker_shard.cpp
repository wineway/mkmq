// BrokerShard: Kafka front-end, replication orchestration, per-shard metrics,
// and the control plane (start/stop/apply_config/maintenance).
// PartitionLogShard lives in partition_log_shard.cpp; shared anon-namespace
// helpers live in broker_shard_detail.{hpp,cpp}.

#include "broker_shard_detail.hpp"
#include "mkmq/broker_shard.hpp"
#include "mkmq/kafka_protocol.hpp"
#include "mkmq/protocol/api.hpp"
#include "mkmq/protocol/wire.hpp"

#include <seastar/core/coroutine.hh>
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
#include <memory>
#include <stdexcept>
#include <utility>

namespace mkmq {

namespace {

// Hoist these into the anonymous namespace of this TU so we can use unqualified
// names (detail::foo is noisy in the hot dispatch paths).
using detail::decode_frame_size;
using detail::decode_replica_append;
using detail::decode_replica_append_response;
using detail::decode_replica_fetch;
using detail::decode_replica_fetch_response;
using detail::encode_replica_append;
using detail::encode_replica_append_response;
using detail::encode_replica_fetch;
using detail::encode_replica_fetch_response;
using detail::kReplicaCatchupMaxBytes;
using detail::partition_missing;
using detail::request_api_key;
using detail::validate_reload;
using detail::write_response;

// The Kafka 4-byte frame length prefix is capped at 100 MiB — the same upper
// bound we already enforced pre-split.
constexpr std::int32_t kMaxKafkaFrameBytes = 100 * 1024 * 1024;

// Helper used by produce / fetch / list_offsets / append_replica to build
// uniform "unknown partition" responses without repeating the 5-line pattern.
template <class Result>
Result make_missing_partition_result(std::string topic, std::int32_t partition) {
    Result result;
    result.topic = std::move(topic);
    result.partition = partition;
    result.error_code = partition_missing();
    return result;
}

}  // namespace

// ---------------------------------------------------------------------------
// Lifecycle / control plane
// ---------------------------------------------------------------------------

BrokerShard::BrokerShard()
    : protocol_(*this)
    , maintenance_timer_([this] {
          (void) seastar::with_gate(gate_, [this] {
              return maintenance_once();
          }).handle_exception([](std::exception_ptr) {}).finally([this] {
              arm_maintenance_timer();
          });
      }) {}

// ---------------------------------------------------------------------------
// Partition table helpers
// ---------------------------------------------------------------------------
//
// `partitions_` is a sorted flat vector. At 3-way replication with ~16
// partitions per shard, an std::map imposes 4+ cache-missing pointer chases
// per `find`; a compact vector (with stable PartitionLogShard addresses via
// unique_ptr) fits in 1–2 cache lines. The single-slot `hot_partition_`
// pointer cache short-circuits the binary search in the common HFT steady
// state where the same (topic, partition) is hit repeatedly.

PartitionLogShard* BrokerShard::find_partition_log(
    std::string_view topic, std::int32_t partition) noexcept {
    if (hot_partition_ != nullptr
        && hot_partition_->key.partition == partition
        && hot_partition_->key.topic == topic) {
        return hot_partition_->log.get();
    }
    const auto it = std::lower_bound(
        partitions_.begin(), partitions_.end(),
        std::pair<std::string_view, std::int32_t>{topic, partition},
        [](const PartitionEntry& entry,
           const std::pair<std::string_view, std::int32_t>& key) noexcept {
            if (entry.key.topic != key.first) {
                return entry.key.topic < key.first;
            }
            return entry.key.partition < key.second;
        });
    if (it != partitions_.end()
        && it->key.topic == topic
        && it->key.partition == partition) {
        hot_partition_ = &*it;
        return it->log.get();
    }
    return nullptr;
}

const PartitionLogShard* BrokerShard::find_partition_log(
    std::string_view topic, std::int32_t partition) const noexcept {
    // Delegate to the non-const overload; we can treat the cache as mutable
    // since BrokerShard is single-threaded per Seastar shard.
    return const_cast<BrokerShard*>(this)->find_partition_log(topic, partition);
}

PartitionLogShard& BrokerShard::emplace_partition_log(TopicPartition key) {
    const auto it = std::lower_bound(
        partitions_.begin(), partitions_.end(),
        std::pair<std::string_view, std::int32_t>{key.topic, key.partition},
        [](const PartitionEntry& entry,
           const std::pair<std::string_view, std::int32_t>& k) noexcept {
            if (entry.key.topic != k.first) {
                return entry.key.topic < k.first;
            }
            return entry.key.partition < k.second;
        });
    if (it != partitions_.end()
        && it->key.topic == key.topic
        && it->key.partition == key.partition) {
        return *it->log;
    }
    // Insert invalidates iterators and any `hot_partition_` we may still hold.
    hot_partition_ = nullptr;
    const auto inserted = partitions_.insert(
        it, PartitionEntry{std::move(key), std::make_unique<PartitionLogShard>()});
    return *inserted->log;
}

seastar::future<> BrokerShard::start(BrokerConfig config) {
    config_ = std::move(config);
    register_metrics();

    std::vector<seastar::future<>> starts;
    for (const auto& topic : config_.topics) {
        for (const auto& partition : topic.partitions) {
            if (partition.shard != seastar::this_shard_id()) {
                continue;
            }
            auto& log = emplace_partition_log({partition.topic, partition.partition});
            starts.push_back(log.start(
                partition,
                config_.data_dir / ("shard-" + std::to_string(seastar::this_shard_id())),
                config_.segment_bytes));
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
        stops.reserve(partitions_.size());
        for (auto& entry : partitions_) {
            stops.push_back(entry.log->stop());
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
            if (auto* existing = find_partition_log(partition.topic, partition.partition)) {
                existing->update_config(partition);
            } else {
                auto& new_log = emplace_partition_log({partition.topic, partition.partition});
                starts.push_back(new_log.start(
                    partition,
                    config.data_dir / ("shard-" + std::to_string(seastar::this_shard_id())),
                    config.segment_bytes));
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

std::vector<TopicConfig> BrokerShard::topics() const {
    return config_.topics;
}

std::optional<PartitionConfig> BrokerShard::partition_config(
    const std::string& topic, std::int32_t partition) const {
    return find_partition(config_, topic, partition);
}

std::optional<unsigned> BrokerShard::owner_shard(
    const std::string& topic, std::int32_t partition) const {
    // Direct scan rather than find_partition() — that helper returns
    // std::optional<PartitionConfig>, which copies the entire struct
    // (topic string + replicas + isr vectors). owner_shard() is called on
    // every produce/fetch/list_offsets, so the copy is pure overhead on the
    // hot path. Read only the scalar we need.
    for (const auto& topic_config : config_.topics) {
        if (topic_config.name != topic) {
            continue;
        }
        for (const auto& p : topic_config.partitions) {
            if (p.partition == partition) {
                return p.shard;
            }
        }
        return std::nullopt;  // matching topic but no matching partition
    }
    return std::nullopt;
}

// ---------------------------------------------------------------------------
// Kafka dispatch
// ---------------------------------------------------------------------------

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
        return seastar::make_ready_future<ProducePartitionResult>(
            make_missing_partition_result<ProducePartitionResult>(
                std::move(request.topic), request.partition));
    }
    if (*owner != seastar::this_shard_id()) {
        return peers_->invoke_on(*owner,
            [request = std::move(request), acks, timeout_ms](BrokerShard& shard) mutable {
                return shard.append(std::move(request), acks, timeout_ms);
            });
    }
    auto* partition_log = find_partition_log(request.topic, request.partition);
    if (partition_log == nullptr) {
        ++produce_errors_;
        return seastar::make_ready_future<ProducePartitionResult>(
            make_missing_partition_result<ProducePartitionResult>(
                std::move(request.topic), request.partition));
    }
    if (partition_log->config().leader != config_.node_id) {
        ProducePartitionResult result;
        result.topic = std::move(request.topic);
        result.partition = request.partition;
        result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::NotLeaderOrFollower);
        return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
    }
    auto records = rewrite_record_batch_offsets(request.records, partition_log->offsets().latest);
    // lw_shared_ptr (non-atomic refcount) — BrokerShard is single-threaded per
    // Seastar shard, so std::shared_ptr's atomic cmpxchg was pure waste on the
    // hot path.
    auto partition_config = seastar::make_lw_shared<const PartitionConfig>(partition_log->config());

    // Compute the append plan synchronously: base_offset and log_end_offset
    // are fully determined by `offsets().latest` + the record-batch headers
    // before any disk I/O starts. Capturing them here lets us dispatch ISR
    // replication in PARALLEL with the local DMA write instead of after it —
    // acks=-1 tail latency becomes max(local_disk, net_rtt + remote_disk)
    // instead of their sum. replicate_to_isr() encodes its wire payload
    // synchronously before returning the future, so we only need `records`
    // to be live at the call site; after that the future owns the encoded
    // bytes and it's safe to move `records` into the local append.
    const std::int64_t planned_base = partition_log->offsets().latest;
    const std::int64_t planned_count = PartitionLogShard::estimate_record_count(records);
    const std::int64_t planned_log_end = planned_base + planned_count;
    ProducePartitionResult replica_stub;
    replica_stub.topic = partition_config->topic;
    replica_stub.partition = partition_config->partition;
    replica_stub.base_offset = planned_base;
    replica_stub.log_end_offset = planned_log_end;

    auto operation = seastar::with_gate(gate_, [
        this, partition_log, partition_config,
        records = std::move(records), acks, replica_stub]() mutable {
        seastar::future<> replicate_fut = partition_config->isr.size() > 1
            ? replicate_to_isr(*partition_config, replica_stub, records)
            : seastar::make_ready_future<>();
        auto write_fut = partition_log->append(std::move(records), acks);

        if (acks == -1) {
            return std::move(write_fut).then(
                [this, partition_config, rf = std::move(replicate_fut)](
                    ProducePartitionResult result) mutable {
                    if (result.error_code != 0) {
                        // Local rejected the write (queue full, too large,
                        // etc.). Swallow the replica future — followers may
                        // have accepted, but we're reporting the local
                        // refusal to the client.
                        (void) std::move(rf).handle_exception([](std::exception_ptr) {});
                        return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
                    }
                    return std::move(rf).then(
                        [this, partition_config, result]() mutable {
                            if (auto* log = find_partition_log(partition_config->topic, partition_config->partition)) {
                                log->mark_replicated_through(log->offsets().latest);
                            }
                            return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
                        }).handle_exception([this, partition_config, result](std::exception_ptr) mutable {
                            ++replica_errors_;
                            if (auto* log = find_partition_log(partition_config->topic, partition_config->partition)) {
                                log->mark_degraded();
                            }
                            ProducePartitionResult failed = std::move(result);
                            failed.error_code = static_cast<std::int16_t>(protocol::ErrorCode::NotEnoughReplicas);
                            return seastar::make_ready_future<ProducePartitionResult>(std::move(failed));
                        });
                });
        }
        // acks=0 / acks=1: return as soon as the local write completes. The
        // replication future runs to completion behind the gate and feeds the
        // degraded / HW signals the same way it did pre-refactor; it just
        // happens to have started earlier (in parallel with the write), so
        // follower lag recovers slightly faster too.
        (void) seastar::with_gate(gate_,
            [this, partition_config, rf = std::move(replicate_fut)]() mutable {
                return std::move(rf).then([this, partition_config] {
                    if (auto* log = find_partition_log(partition_config->topic, partition_config->partition)) {
                        log->mark_replicated_through(log->offsets().latest);
                    }
                }).handle_exception([this, partition_config](std::exception_ptr) {
                    ++replica_errors_;
                    if (auto* log = find_partition_log(partition_config->topic, partition_config->partition)) {
                        log->mark_degraded();
                    }
                });
            });
        return std::move(write_fut);
    });
    const auto cap_ms = static_cast<std::int32_t>(config_.produce_wait_ms_cap);
    const auto wait_ms = timeout_ms <= 0 ? cap_ms : std::min(timeout_ms, cap_ms);
    if (wait_ms <= 0) {
        return operation;
    }
    return seastar::with_timeout(
        seastar::lowres_clock::now() + std::chrono::milliseconds(wait_ms),
        std::move(operation)).handle_exception_type(
            [this, partition_config](const seastar::timed_out_error&) {
                ++produce_errors_;
                ProducePartitionResult result;
                result.topic = partition_config->topic;
                result.partition = partition_config->partition;
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
    auto* partition_log = find_partition_log(request.topic, request.partition);
    if (partition_log == nullptr) {
        ++fetch_errors_;
        FetchPartitionResult result;
        result.error_code = partition_missing();
        return seastar::make_ready_future<FetchPartitionResult>(std::move(result));
    }
    const auto& partition = partition_log->config();
    const bool local_is_replica =
        std::find(partition.replicas.begin(), partition.replicas.end(), config_.node_id) != partition.replicas.end();
    if (!local_is_replica || (partition.leader != config_.node_id && !partition.allow_follower_fetch)) {
        FetchPartitionResult result;
        result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::NotLeaderOrFollower);
        return seastar::make_ready_future<FetchPartitionResult>(std::move(result));
    }
    return partition_log->fetch(request.offset, request.max_bytes).then(
        [this, request = std::move(request), partition_log](FetchPartitionResult result) mutable {
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
                [partition_log, request = std::move(request)] {
                    return partition_log->fetch(request.offset, request.max_bytes);
                });
        });
}

seastar::future<ListOffsetPartitionResult> BrokerShard::list_offsets(ListOffsetPartitionRequest request) {
    const auto owner = owner_shard(request.topic, request.partition);
    if (!owner.has_value()) {
        return seastar::make_ready_future<ListOffsetPartitionResult>(
            make_missing_partition_result<ListOffsetPartitionResult>(
                std::move(request.topic), request.partition));
    }
    if (*owner != seastar::this_shard_id()) {
        return peers_->invoke_on(*owner, [request = std::move(request)](BrokerShard& shard) mutable {
            return shard.list_offsets(std::move(request));
        });
    }
    auto* partition_log = find_partition_log(request.topic, request.partition);
    if (partition_log == nullptr) {
        return seastar::make_ready_future<ListOffsetPartitionResult>(
            make_missing_partition_result<ListOffsetPartitionResult>(
                std::move(request.topic), request.partition));
    }
    return seastar::make_ready_future<ListOffsetPartitionResult>(partition_log->list_offsets(request.timestamp));
}

// ---------------------------------------------------------------------------
// Replication: server-side handlers + client-side Mercury forwarders
// ---------------------------------------------------------------------------

seastar::future<ProducePartitionResult> BrokerShard::append_replica(ReplicaAppendRequest request) {
    ++replica_appends_;
    const auto owner = owner_shard(request.topic, request.partition);
    if (!owner.has_value()) {
        ++replica_errors_;
        return seastar::make_ready_future<ProducePartitionResult>(
            make_missing_partition_result<ProducePartitionResult>(
                std::move(request.topic), request.partition));
    }
    if (*owner != seastar::this_shard_id()) {
        return peers_->invoke_on(*owner, [request = std::move(request)](BrokerShard& shard) mutable {
            return shard.append_replica(std::move(request));
        });
    }
    auto* partition_log = find_partition_log(request.topic, request.partition);
    if (partition_log == nullptr) {
        ++replica_errors_;
        return seastar::make_ready_future<ProducePartitionResult>(
            make_missing_partition_result<ProducePartitionResult>(
                std::move(request.topic), request.partition));
    }
    const auto& partition = partition_log->config();
    const bool local_is_replica =
        std::find(partition.replicas.begin(), partition.replicas.end(), config_.node_id) != partition.replicas.end();
    if (!local_is_replica) {
        ++replica_errors_;
        ProducePartitionResult result;
        result.topic = std::move(request.topic);
        result.partition = request.partition;
        result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::NotLeaderOrFollower);
        return seastar::make_ready_future<ProducePartitionResult>(std::move(result));
    }
    return partition_log->append_replica(std::move(request));
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
    auto* partition_log = find_partition_log(request.topic, request.partition);
    if (partition_log == nullptr) {
        ReplicaFetchResult result;
        result.error_code = partition_missing();
        return seastar::make_ready_future<ReplicaFetchResult>(std::move(result));
    }
    const auto& partition = partition_log->config();
    if (partition.leader != config_.node_id) {
        ReplicaFetchResult result;
        result.error_code = static_cast<std::int16_t>(protocol::ErrorCode::NotLeaderOrFollower);
        return seastar::make_ready_future<ReplicaFetchResult>(std::move(result));
    }
    return partition_log->fetch_for_replica(request.offset, request.max_bytes);
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

seastar::future<> BrokerShard::replicate_to_isr(
    const PartitionConfig& partition,
    const ProducePartitionResult& local_result,
    const std::vector<std::uint8_t>& records) {
    // Collect Mercury endpoints for followers first. A follower whose broker
    // endpoint is missing from config fails fast (matches pre-refactor
    // behavior from send_replica_append); this also lets us know how many
    // peers we'll end up sending to so the last one can move-not-copy the
    // shared encoded payload.
    struct FollowerEndpoint {
        std::int32_t broker_id;
        std::string mercury_address;
    };
    std::vector<FollowerEndpoint> followers;
    followers.reserve(partition.isr.size());
    std::vector<seastar::future<>> replicas;
    replicas.reserve(partition.isr.size());
    for (const auto broker_id : partition.isr) {
        if (broker_id == config_.node_id) {
            continue;
        }
        const auto broker = find_broker(config_, broker_id);
        if (!broker.has_value()) {
            replicas.push_back(seastar::make_exception_future<>(
                std::runtime_error("replica broker is not configured")));
            continue;
        }
        followers.push_back({broker_id, broker->mercury_address});
    }
    if (followers.empty()) {
        if (replicas.empty()) {
            return seastar::make_ready_future<>();
        }
        return seastar::when_all_succeed(replicas.begin(), replicas.end()).discard_result();
    }

    // Encode the replica_append payload exactly once. Every follower in the
    // ISR fan-out receives identical wire bytes — same base_offset, same
    // records — so repeating the encode per-follower (which used to be the
    // case via send_replica_append → encode_replica_append) was pure waste
    // (records-sized memcpy + std::vector allocation per follower). At the
    // last iteration we std::move the buffer into Mercury to save the final
    // copy as well.
    ReplicaAppendRequest header;
    header.topic = partition.topic;
    header.partition = partition.partition;
    header.base_offset = local_result.base_offset;
    header.leader_high_watermark = local_result.log_end_offset;
    header.records = records;  // one copy, shared across N followers
    auto encoded = encode_replica_append(header);
    header.records.clear();
    header.records.shrink_to_fit();

    for (std::size_t i = 0; i < followers.size(); ++i) {
        const bool is_last = (i + 1 == followers.size());
        auto payload = is_last ? std::move(encoded) : encoded;  // copy for all but last
        replicas.push_back(
            mercury_.forward(followers[i].mercury_address, "replica_append", std::move(payload)).then(
                [](std::vector<std::uint8_t> response_payload) {
                    // The replica_append response is {int16 error, int64 log_end_offset};
                    // we only need to know whether it succeeded.
                    protocol::WireReader reader(response_payload);
                    const auto error_code = reader.read_int16();
                    if (error_code != 0) {
                        return seastar::make_exception_future<>(
                            std::runtime_error("replica append failed"));
                    }
                    return seastar::make_ready_future<>();
                }));
    }
    return seastar::when_all_succeed(replicas.begin(), replicas.end()).discard_result();
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

// ---------------------------------------------------------------------------
// Connection handling
// ---------------------------------------------------------------------------

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
    // Pin TCP_NODELAY on every accepted connection. Without this, Linux will
    // coalesce a produce frame + its response into the same TCP window and add
    // ~40 ms of Nagle delay to the tail of every acks=1/-1 exchange — lethal
    // for an HFT-oriented produce path. Cheap syscall done once per connect.
    socket.set_nodelay(true);

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
                if (frame_size <= 0 || frame_size > kMaxKafkaFrameBytes) {
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

// ---------------------------------------------------------------------------
// Maintenance: health pings + follower catch-up (1s timer)
// ---------------------------------------------------------------------------

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
    for (const auto& entry : partitions_) {
        const auto& partition_config = entry.log->config();
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
    auto* log = find_partition_log(partition.topic, partition.partition);
    if (log == nullptr) {
        return seastar::make_ready_future<>();
    }
    const auto local_end = log->offsets().latest;
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
                if (auto* log = find_partition_log(partition.topic, partition.partition);
                    log != nullptr && log->offsets().latest >= append_result.log_end_offset) {
                    log->mark_replicated_through(log->offsets().latest);
                }
            });
        }).handle_exception([this](std::exception_ptr) {
            ++catchup_errors_;
        });
}

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

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
            for (const auto& entry : partitions_) {
                count += entry.log->disk_writes();
            }
            return count;
        }),
        sm::make_counter("disk_fsyncs", sm::description("Local partition log fsync operations on this shard"), [this] {
            std::uint64_t count = 0;
            for (const auto& entry : partitions_) {
                count += entry.log->disk_fsyncs();
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
            for (const auto& entry : partitions_) {
                histogram.merge(entry.log->disk_write_latency());
            }
            return histogram.to_metrics_histogram();
        }),
        sm::make_histogram("disk_fsync_latency", sm::description("Log fsync latency in microseconds"), [this] {
            LatencyHistogram histogram;
            for (const auto& entry : partitions_) {
                histogram.merge(entry.log->disk_fsync_latency());
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
            for (const auto& entry : partitions_) {
                if (entry.log->degraded()) {
                    ++count;
                }
            }
            return count;
        }),
        sm::make_gauge("replica_lag", sm::description("Total local partition replica lag in records"), [this] {
            std::int64_t lag = 0;
            for (const auto& entry : partitions_) {
                lag += entry.log->replica_lag();
            }
            return lag;
        }),
        sm::make_gauge("queue_depth", sm::description("Total local partition append queue depth"), [this] {
            std::uint64_t depth = 0;
            for (const auto& entry : partitions_) {
                depth += entry.log->queue_depth();
            }
            return depth;
        }),
    });
}

}  // namespace mkmq

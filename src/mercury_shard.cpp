#include "mkmq/mercury_shard.hpp"

#if MKMQ_HAVE_MERCURY
#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_proc.h>
#include <mercury_proc_bulk.h>
#endif

#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace mkmq {

#if MKMQ_HAVE_MERCURY

namespace {

constexpr hg_size_t kBulkTransferThreshold = 64 * 1024;
constexpr std::size_t kHandlePoolLimitPerPeerRpc = 128;
constexpr auto kMercuryIdleProgressInterval = std::chrono::microseconds(5);
constexpr auto kMercuryActiveProgressInterval = std::chrono::microseconds(1);
constexpr auto kForwardTimeout = std::chrono::milliseconds(100);
constexpr unsigned int kMercuryTriggerBatch = 64;
constexpr std::size_t kMercuryProgressRounds = 8;

// Transparent hasher that lets us look up std::string-keyed maps with a
// std::string_view (no temporary string allocation at every lookup).
struct StringHash {
    using is_transparent = void;
    using hash_type = std::hash<std::string_view>;
    std::size_t operator()(std::string_view sv) const noexcept {
        return hash_type{}(sv);
    }
    std::size_t operator()(const std::string& s) const noexcept {
        return hash_type{}(s);
    }
    std::size_t operator()(const char* s) const noexcept {
        return hash_type{}(s);
    }
};

enum class HgTransferMode : std::int32_t {
    Eager = 0,
    Bulk = 1,
};

struct HgBytes {
    hg_size_t size{0};
    void* data{nullptr};
};

struct HgRequest {
    std::int32_t mode{static_cast<std::int32_t>(HgTransferMode::Eager)};
    HgBytes eager;
    hg_size_t bulk_size{0};
    hg_bulk_t bulk_handle{HG_BULK_NULL};
};

struct HgReply {
    std::int32_t status{0};
    HgBytes payload;
};

}  // namespace

#endif  // MKMQ_HAVE_MERCURY

// ---------------------------------------------------------------------------
// Impl — owns all Mercury-specific state. Public MercuryShard methods are
// thin forwarders defined at the bottom of this file.
// ---------------------------------------------------------------------------
struct MercuryShard::Impl {
#if MKMQ_HAVE_MERCURY
    struct LookupState;
    struct ClientRpcState;
    struct ServerRpcState;

    // One registered RPC, indexed by both name and Mercury id.
    struct RpcEntry {
        std::string name;
        hg_id_t id{0};
        RpcHandler handler;
    };

    // Per-peer handle pool. One slot per RPC id. In a typical replica-fan-out
    // deployment a peer sees ≤ 3 RPC ids, so a linear scan over a small flat
    // vector is faster and more cache-friendly than std::unordered_map.
    struct HandlePool {
        hg_id_t rpc_id{0};
        std::vector<hg_handle_t> handles;
    };

    // Per-peer client-side state. Erased atomically when the peer endpoint is
    // dropped (transport error, shutdown, etc.).
    struct PeerState {
        hg_addr_t addr{HG_ADDR_NULL};
        std::vector<HandlePool> pools;  // small, typically ≤ 3 entries
        std::unique_ptr<LookupState> pending_lookup;
    };

    // In-flight address-lookup record. Multiple producers collide on the same
    // peer; `waiters` fans out when the lookup callback fires.
    struct LookupState {
        Impl* impl{nullptr};
        std::string peer;
        std::vector<std::unique_ptr<ClientRpcState>> waiters;
        bool cancelled{false};
    };

    // Client-side per-RPC bookkeeping. Lives as a unique_ptr that is released
    // across each Mercury callback boundary.
    struct ClientRpcState {
        explicit ClientRpcState(Impl* owner)
            : impl(owner),
              timeout_timer([this] {
                  if (!promise_done) {
                      promise.set_exception(std::runtime_error(
                          "Mercury RPC timed out for peer: " + peer));
                      promise_done = true;
                      impl->on_rpc_error();
                      impl->on_rpc_timeout();
                      impl->record_forward_latency_if_needed(*this);
                  }
              }) {}

        Impl* impl;
        seastar::promise<std::vector<std::uint8_t>> promise;
        seastar::timer<> timeout_timer;
        std::chrono::steady_clock::time_point started;
        hg_addr_t addr{HG_ADDR_NULL};
        hg_handle_t handle{HG_HANDLE_NULL};
        hg_id_t rpc_id{0};
        std::string peer;
        std::vector<std::uint8_t> request;
        HgRequest input;
        hg_bulk_t origin_bulk{HG_BULK_NULL};
        HgReply output;
        bool promise_done{false};
        bool latency_recorded{false};
        bool recycle_handle{false};
        bool drop_peer_on_cleanup{false};
    };

    // Server-side per-RPC bookkeeping.
    struct ServerRpcState {
        explicit ServerRpcState(Impl* owner) : impl(owner) {}

        Impl* impl;
        hg_handle_t handle{HG_HANDLE_NULL};
        RpcEntry* rpc{nullptr};
        HgRequest input;
        bool input_loaded{false};
        hg_bulk_t local_bulk{HG_BULK_NULL};
        std::vector<std::uint8_t> request;
        std::vector<std::uint8_t> response;
        HgReply output;
        std::chrono::steady_clock::time_point started;
    };

    // ----- counters & histograms -------------------------------------------
    void on_rpc_forward() noexcept { ++rpc_forwards; }
    void on_rpc_receive() noexcept { ++rpc_receives; }
    void on_rpc_error() noexcept { ++rpc_errors; }
    void on_rpc_timeout() noexcept { ++rpc_timeouts; }
    void on_bulk_transfer(std::uint64_t bytes) noexcept {
        ++bulk_transfers;
        bulk_bytes += bytes;
    }
    void on_bulk_error() noexcept { ++bulk_errors; }

    void record_forward_latency_if_needed(ClientRpcState& state) {
        if (!state.latency_recorded) {
            rpc_forward_latency.add(std::chrono::steady_clock::now() - state.started);
            state.latency_recorded = true;
        }
    }
    void record_handler_latency(const ServerRpcState& state) {
        rpc_handler_latency.add(std::chrono::steady_clock::now() - state.started);
    }
    void record_bulk_latency(const ServerRpcState& state) {
        bulk_latency.add(std::chrono::steady_clock::now() - state.started);
    }

    // ----- RPC registry -----------------------------------------------------
    // Hot path: `forward(rpc_name)` is called with the same RPC name
    // repeatedly. The last-used pointer short-circuits the map lookup entirely
    // in the steady state.
    RpcEntry* find_rpc_by_name(std::string_view name) {
        if (last_rpc_ != nullptr && last_rpc_->name == name) {
            return last_rpc_;
        }
        const auto it = rpcs_by_name.find(name);
        if (it == rpcs_by_name.end()) {
            return nullptr;
        }
        last_rpc_ = it->second.get();
        return last_rpc_;
    }
    RpcEntry* find_rpc_by_id(hg_id_t id) {
        // Server-side lookup. With typically ≤ 3 registered RPCs a linear scan
        // beats a hash table in both latency and cache footprint.
        for (const auto& [eid, entry] : rpcs_by_id) {
            if (eid == id) {
                return entry;
            }
        }
        return nullptr;
    }

    void register_handler(std::string_view name, RpcHandler handler) {
        auto it = rpcs_by_name.find(name);
        RpcEntry* entry;
        if (it == rpcs_by_name.end()) {
            auto new_entry = std::make_unique<RpcEntry>();
            new_entry->name = std::string(name);
            entry = new_entry.get();
            rpcs_by_name.emplace(entry->name, std::move(new_entry));
        } else {
            entry = it->second.get();
        }
        entry->handler = std::move(handler);
        if (hg_class != nullptr && entry->id == 0) {
            bind_rpc_to_mercury(*entry);
        }
    }

    // Allocate a Mercury id for an already-registered RpcEntry.
    void bind_rpc_to_mercury(RpcEntry& entry);

    // ----- peer cache -------------------------------------------------------
    PeerState& peer_state(std::string_view peer) {
        const auto it = peers.find(peer);
        if (it != peers.end()) {
            return it->second;
        }
        auto [new_it, _] = peers.emplace(std::string(peer), PeerState{});
        return new_it->second;
    }
    PeerState* find_peer(std::string_view peer) {
        const auto it = peers.find(peer);
        return it == peers.end() ? nullptr : &it->second;
    }

    hg_handle_t take_cached_handle(PeerState& peer, hg_id_t rpc_id) {
        // Linear scan over ≤ 3 entries; faster than a hash table in practice.
        for (auto& pool : peer.pools) {
            if (pool.rpc_id == rpc_id) {
                if (pool.handles.empty()) {
                    return HG_HANDLE_NULL;
                }
                auto handle = pool.handles.back();
                pool.handles.pop_back();
                return handle;
            }
        }
        return HG_HANDLE_NULL;
    }

    void recycle_handle(std::string_view peer, hg_id_t rpc_id, hg_handle_t handle) {
        const auto it = peers.find(peer);
        if (it == peers.end()) {
            (void) HG_Destroy(handle);
            return;
        }
        auto& peer_entry = it->second;
        for (auto& pool : peer_entry.pools) {
            if (pool.rpc_id == rpc_id) {
                if (pool.handles.size() >= kHandlePoolLimitPerPeerRpc) {
                    (void) HG_Destroy(handle);
                } else {
                    pool.handles.push_back(handle);
                }
                return;
            }
        }
        peer_entry.pools.push_back(HandlePool{rpc_id, {handle}});
    }

    // Drop transport-level state for one peer while preserving any pending
    // address lookup (the lookup callback will still need to find us).
    void drop_peer(std::string_view peer) {
        const auto it = peers.find(peer);
        if (it == peers.end()) {
            return;
        }
        for (auto& pool : it->second.pools) {
            for (auto h : pool.handles) {
                (void) HG_Destroy(h);
            }
        }
        it->second.pools.clear();
        if (it->second.addr != HG_ADDR_NULL) {
            (void) HG_Addr_free(hg_class, it->second.addr);
            it->second.addr = HG_ADDR_NULL;
        }
        if (!it->second.pending_lookup) {
            peers.erase(it);
        }
    }

    // Tear down all peer state, failing any pending lookup waiters.
    void clear_all_peers();

    // Retrieve a lookup the user code expects to still be indexed. Returns
    // nullptr if the lookup has been moved into `orphaned_lookups` already.
    std::unique_ptr<LookupState> take_pending_lookup(std::string_view peer) {
        const auto it = peers.find(peer);
        if (it == peers.end() || !it->second.pending_lookup) {
            return nullptr;
        }
        auto lookup = std::move(it->second.pending_lookup);
        if (it->second.addr == HG_ADDR_NULL && it->second.pools.empty()) {
            peers.erase(it);
        }
        return lookup;
    }

    std::unique_ptr<LookupState> take_orphaned_lookup(LookupState* raw) {
        const auto it = std::find_if(
            orphaned_lookups.begin(), orphaned_lookups.end(),
            [raw](const auto& p) { return p.get() == raw; });
        if (it == orphaned_lookups.end()) {
            return nullptr;
        }
        auto state = std::move(*it);
        orphaned_lookups.erase(it);
        return state;
    }

    // ----- per-call helpers -------------------------------------------------
    void start_client_forward(std::unique_ptr<ClientRpcState> state);
    void fail_client(std::unique_ptr<ClientRpcState> state, const std::string& msg);
    void cleanup_client(ClientRpcState& state);
    void respond_server(std::unique_ptr<ServerRpcState> state);
    void run_server_handler(std::unique_ptr<ServerRpcState> state);
    void cleanup_server_bulk(ServerRpcState& state);

    // Called from the static dispatch callback after it resolves the shard.
    int handle_rpc(hg_handle_t handle);

    // ----- Mercury callbacks (static, required by Mercury C ABI) ------------
    static hg_return_t forward_cb(const struct hg_cb_info* info);
    static hg_return_t lookup_cb(const struct hg_cb_info* info);
    static hg_return_t respond_cb(const struct hg_cb_info* info);
    static hg_return_t bulk_pull_cb(const struct hg_cb_info* info);
    static hg_return_t dispatch_cb(hg_handle_t handle);

    // ----- progress loop ----------------------------------------------------
    void arm_progress_timer(bool recently_active = false) {
        if (running) {
            progress_timer.arm(recently_active
                ? kMercuryActiveProgressInterval
                : kMercuryIdleProgressInterval);
        }
    }
    bool progress_once();

    // ----- state ------------------------------------------------------------
    Impl()
        : progress_timer([this] {
              const bool progressed = progress_once();
              arm_progress_timer(progressed);
          }) {}

    bool running{false};
    std::string address;
    seastar::timer<> progress_timer;

    hg_class_t* hg_class{nullptr};
    hg_context_t* hg_context{nullptr};

    // Transparent string-view lookup avoids constructing a temporary string on
    // every `forward(name, ...)` call.
    std::unordered_map<std::string, std::unique_ptr<RpcEntry>,
                       StringHash, std::equal_to<>> rpcs_by_name;
    // Typically ≤ 3 registered RPCs; linear scan beats a hash table.
    std::vector<std::pair<hg_id_t, RpcEntry*>> rpcs_by_id;
    // Last RpcEntry resolved by name; a single pointer cache that skips the
    // map lookup when the same RPC is called repeatedly (the common case).
    RpcEntry* last_rpc_{nullptr};

    std::unordered_map<std::string, PeerState,
                       StringHash, std::equal_to<>> peers;
    // Lookups that were in flight when stop() was called; kept alive until the
    // Mercury callback fires. Linear size is bounded by peer count.
    std::vector<std::unique_ptr<LookupState>> orphaned_lookups;

    std::uint64_t rpc_forwards{0};
    std::uint64_t rpc_receives{0};
    std::uint64_t rpc_errors{0};
    std::uint64_t rpc_timeouts{0};
    std::uint64_t bulk_transfers{0};
    std::uint64_t bulk_bytes{0};
    std::uint64_t bulk_errors{0};
    seastar::metrics::internal::time_estimated_histogram rpc_forward_latency;
    seastar::metrics::internal::time_estimated_histogram rpc_handler_latency;
    seastar::metrics::internal::time_estimated_histogram bulk_latency;
#else
    // Minimal stub members when Mercury support is compiled out. We still need
    // the timer so that stop()/start() have consistent structure, but we never
    // arm it.
    Impl() = default;

    bool running{false};
    std::string address;
    std::uint64_t rpc_forwards{0};
    std::uint64_t rpc_receives{0};
    std::uint64_t rpc_errors{0};
    std::uint64_t rpc_timeouts{0};
    std::uint64_t bulk_transfers{0};
    std::uint64_t bulk_bytes{0};
    std::uint64_t bulk_errors{0};
    seastar::metrics::internal::time_estimated_histogram rpc_forward_latency;
    seastar::metrics::internal::time_estimated_histogram rpc_handler_latency;
    seastar::metrics::internal::time_estimated_histogram bulk_latency;
    std::unordered_map<std::string, RpcHandler> deferred_handlers;
#endif
};

#if MKMQ_HAVE_MERCURY

// ---------------------------------------------------------------------------
// Mercury <proc> codecs for our eager/bulk envelope.
// ---------------------------------------------------------------------------
namespace {

hg_return_t proc_hg_bytes(hg_proc_t proc, void* data) {
    auto* bytes = static_cast<HgBytes*>(data);
    auto ret = hg_proc_hg_size_t(proc, &bytes->size);
    if (ret != HG_SUCCESS) {
        return ret;
    }

    switch (hg_proc_get_op(proc)) {
        case HG_ENCODE:
            if (bytes->size > 0 && bytes->data == nullptr) {
                return HG_INVALID_ARG;
            }
            return bytes->size == 0 ? HG_SUCCESS : hg_proc_raw(proc, bytes->data, bytes->size);
        case HG_DECODE:
            if (bytes->size == 0) {
                bytes->data = nullptr;
                return HG_SUCCESS;
            }
            bytes->data = std::malloc(static_cast<std::size_t>(bytes->size));
            if (bytes->data == nullptr) {
                return HG_NOMEM;
            }
            ret = hg_proc_raw(proc, bytes->data, bytes->size);
            if (ret != HG_SUCCESS) {
                std::free(bytes->data);
                bytes->data = nullptr;
                bytes->size = 0;
            }
            return ret;
        case HG_FREE:
            std::free(bytes->data);
            bytes->data = nullptr;
            bytes->size = 0;
            return HG_SUCCESS;
        default:
            return HG_PROTOCOL_ERROR;
    }
}

hg_return_t proc_hg_reply(hg_proc_t proc, void* data) {
    auto* reply = static_cast<HgReply*>(data);
    auto ret = hg_proc_int32_t(proc, &reply->status);
    if (ret != HG_SUCCESS) {
        return ret;
    }
    return proc_hg_bytes(proc, &reply->payload);
}

hg_return_t proc_hg_request(hg_proc_t proc, void* data) {
    auto* request = static_cast<HgRequest*>(data);
    auto ret = hg_proc_int32_t(proc, &request->mode);
    if (ret != HG_SUCCESS) {
        return ret;
    }
    ret = proc_hg_bytes(proc, &request->eager);
    if (ret != HG_SUCCESS) {
        return ret;
    }
    ret = hg_proc_hg_size_t(proc, &request->bulk_size);
    if (ret != HG_SUCCESS) {
        return ret;
    }
    if (request->mode == static_cast<std::int32_t>(HgTransferMode::Bulk)) {
        return hg_proc_hg_bulk_t(proc, &request->bulk_handle);
    }
    if (hg_proc_get_op(proc) == HG_DECODE) {
        request->bulk_handle = HG_BULK_NULL;
    }
    return HG_SUCCESS;
}

}  // namespace

// ---------------------------------------------------------------------------
// Impl method definitions.
// ---------------------------------------------------------------------------

void MercuryShard::Impl::bind_rpc_to_mercury(RpcEntry& entry) {
    const auto id = HG_Register_name(
        hg_class,
        entry.name.c_str(),
        proc_hg_request,
        proc_hg_reply,
        &Impl::dispatch_cb);
    if (id == 0) {
        throw std::runtime_error("HG_Register_name failed for " + entry.name);
    }
    const auto ret = HG_Register_data(hg_class, id, this, nullptr);
    if (ret != HG_SUCCESS) {
        throw std::runtime_error(std::string("HG_Register_data failed: ") + HG_Error_to_string(ret));
    }
    entry.id = id;
    rpcs_by_id.push_back({id, &entry});
}

void MercuryShard::Impl::clear_all_peers() {
    for (auto& [_, peer] : peers) {
        if (peer.pending_lookup) {
            auto lookup = std::move(peer.pending_lookup);
            lookup->cancelled = true;
            for (auto& waiter : lookup->waiters) {
                if (!waiter->promise_done) {
                    waiter->promise.set_exception(std::runtime_error(
                        "Mercury shard stopped during address lookup"));
                    waiter->promise_done = true;
                }
                cleanup_client(*waiter);
            }
            lookup->waiters.clear();
            orphaned_lookups.push_back(std::move(lookup));
        }
        for (auto& pool : peer.pools) {
            for (auto h : pool.handles) {
                (void) HG_Destroy(h);
            }
        }
        if (peer.addr != HG_ADDR_NULL) {
            (void) HG_Addr_free(hg_class, peer.addr);
        }
    }
    peers.clear();
}

void MercuryShard::Impl::cleanup_client(ClientRpcState& state) {
    if (state.origin_bulk != HG_BULK_NULL) {
        (void) HG_Bulk_free(state.origin_bulk);
        state.origin_bulk = HG_BULK_NULL;
    }
    if (state.handle != HG_HANDLE_NULL) {
        if (state.recycle_handle) {
            recycle_handle(state.peer, state.rpc_id, state.handle);
        } else {
            (void) HG_Destroy(state.handle);
        }
        state.handle = HG_HANDLE_NULL;
    }
    if (state.drop_peer_on_cleanup) {
        drop_peer(state.peer);
    }
    state.addr = HG_ADDR_NULL;
}

void MercuryShard::Impl::fail_client(std::unique_ptr<ClientRpcState> state, const std::string& msg) {
    state->timeout_timer.cancel();
    if (!state->promise_done) {
        state->promise.set_exception(std::runtime_error(msg));
        state->promise_done = true;
    }
    on_rpc_error();
    record_forward_latency_if_needed(*state);
    cleanup_client(*state);
}

void MercuryShard::Impl::start_client_forward(std::unique_ptr<ClientRpcState> state) {
    if (state->promise_done) {
        cleanup_client(*state);
        return;
    }

    auto& peer = peer_state(state->peer);
    state->handle = take_cached_handle(peer, state->rpc_id);
    if (state->handle != HG_HANDLE_NULL) {
        if (HG_Reset(state->handle, state->addr, state->rpc_id) != HG_SUCCESS) {
            (void) HG_Destroy(state->handle);
            state->handle = HG_HANDLE_NULL;
        }
    }
    if (state->handle == HG_HANDLE_NULL) {
        const auto ret = HG_Create(hg_context, state->addr, state->rpc_id, &state->handle);
        if (ret != HG_SUCCESS) {
            state->drop_peer_on_cleanup = true;
            fail_client(std::move(state), std::string("HG_Create failed: ") + HG_Error_to_string(ret));
            return;
        }
    }

    const auto ret = HG_Forward(state->handle, &Impl::forward_cb, state.get(), &state->input);
    if (ret != HG_SUCCESS) {
        state->drop_peer_on_cleanup = true;
        fail_client(std::move(state), std::string("HG_Forward failed: ") + HG_Error_to_string(ret));
        return;
    }
    state->timeout_timer.arm(kForwardTimeout);
    (void) state.release();  // Ownership passes to mercury_forward_cb.

    // Kick the transport immediately so the request goes on the wire without
    // waiting up to the next progress-timer tick (≈1 µs). The progress timer
    // will still run; this is purely a latency optimization on the fast path.
    (void) HG_Progress(hg_context, 0);
}

void MercuryShard::Impl::respond_server(std::unique_ptr<ServerRpcState> state) {
    record_handler_latency(*state);
    state->output.payload.size = static_cast<hg_size_t>(state->response.size());
    state->output.payload.data = state->response.empty() ? nullptr : state->response.data();
    const auto ret = HG_Respond(state->handle, &Impl::respond_cb, state.get(), &state->output);
    if (ret == HG_SUCCESS) {
        (void) state.release();  // Ownership passes to mercury_respond_cb.
    } else if (state->handle != HG_HANDLE_NULL) {
        (void) HG_Destroy(state->handle);
    }
}

void MercuryShard::Impl::run_server_handler(std::unique_ptr<ServerRpcState> state) {
    auto& handler = state->rpc->handler;
    (void) handler(std::move(state->request)).then_wrapped(
        [this, state = std::move(state)](seastar::future<std::vector<std::uint8_t>> fut) mutable {
            try {
                state->response = fut.get();
                state->output.status = 0;
            } catch (...) {
                state->response.clear();
                state->output.status = -1;
                on_rpc_error();
            }
            respond_server(std::move(state));
        }).handle_exception([](std::exception_ptr) {});
}

void MercuryShard::Impl::cleanup_server_bulk(ServerRpcState& state) {
    if (state.local_bulk != HG_BULK_NULL) {
        (void) HG_Bulk_free(state.local_bulk);
        state.local_bulk = HG_BULK_NULL;
    }
    if (state.input_loaded) {
        (void) HG_Free_input(state.handle, &state.input);
        state.input_loaded = false;
    }
}

int MercuryShard::Impl::handle_rpc(hg_handle_t handle) {
    on_rpc_receive();
    const auto started = std::chrono::steady_clock::now();
    const auto* info = HG_Get_info(handle);
    if (info == nullptr) {
        on_rpc_error();
        (void) HG_Destroy(handle);
        return HG_INVALID_ARG;
    }
    auto* rpc = find_rpc_by_id(info->id);
    if (rpc == nullptr || !rpc->handler) {
        on_rpc_error();
        (void) HG_Destroy(handle);
        return HG_NOENTRY;
    }

    auto state = std::make_unique<ServerRpcState>(this);
    state->handle = handle;
    state->rpc = rpc;
    state->started = started;

    auto ret = HG_Get_input(handle, &state->input);
    if (ret != HG_SUCCESS) {
        on_rpc_error();
        (void) HG_Destroy(handle);
        return ret;
    }
    state->input_loaded = true;

    if (state->input.mode != static_cast<std::int32_t>(HgTransferMode::Bulk)) {
        const auto* bytes = static_cast<const std::uint8_t*>(state->input.eager.data);
        if (bytes != nullptr && state->input.eager.size > 0) {
            state->request.assign(bytes, bytes + static_cast<std::size_t>(state->input.eager.size));
        }
        cleanup_server_bulk(*state);
        run_server_handler(std::move(state));
        return HG_SUCCESS;
    }

    // Bulk pull path: fail fast on a malformed descriptor, otherwise stage a
    // local buffer and issue a pull.
    if (state->input.bulk_size == 0 || state->input.bulk_handle == HG_BULK_NULL) {
        on_rpc_error();
        on_bulk_error();
        cleanup_server_bulk(*state);
        state->output.status = -1;
        respond_server(std::move(state));
        return HG_SUCCESS;
    }
    state->request.resize(static_cast<std::size_t>(state->input.bulk_size));
    void* buffer = state->request.data();
    const hg_size_t size = state->input.bulk_size;
    ret = HG_Bulk_create(hg_class, 1, &buffer, &size, HG_BULK_WRITE_ONLY, &state->local_bulk);
    if (ret != HG_SUCCESS) {
        on_rpc_error();
        on_bulk_error();
        cleanup_server_bulk(*state);
        state->output.status = -1;
        respond_server(std::move(state));
        return HG_SUCCESS;
    }
    ret = HG_Bulk_transfer(
        hg_context,
        &Impl::bulk_pull_cb,
        state.get(),
        HG_BULK_PULL,
        info->addr,
        state->input.bulk_handle,
        0,
        state->local_bulk,
        0,
        size,
        HG_OP_ID_IGNORE);
    if (ret != HG_SUCCESS) {
        on_rpc_error();
        on_bulk_error();
        cleanup_server_bulk(*state);
        state->output.status = -1;
        respond_server(std::move(state));
        return HG_SUCCESS;
    }
    (void) state.release();  // Ownership passes to mercury_bulk_pull_cb.
    return HG_SUCCESS;
}

bool MercuryShard::Impl::progress_once() {
    if (!running || hg_context == nullptr) {
        return false;
    }
    bool progressed = false;
    unsigned int actual_count = 0;
    for (std::size_t round = 0; round < kMercuryProgressRounds; ++round) {
        bool triggered = false;
        do {
            const auto ret = HG_Trigger(hg_context, 0, kMercuryTriggerBatch, &actual_count);
            if (ret != HG_SUCCESS) {
                actual_count = 0;
                break;
            }
            if (actual_count > 0) {
                triggered = true;
                progressed = true;
            }
        } while (actual_count > 0);

        if (HG_Progress(hg_context, 0) == HG_SUCCESS) {
            progressed = true;
            continue;
        }
        if (!triggered) {
            break;
        }
    }
    return progressed;
}

// ---------------------------------------------------------------------------
// Static Mercury callbacks — defined as Impl members so they can touch its
// private nested types. Each is called from Mercury's progress loop.
// ---------------------------------------------------------------------------

hg_return_t MercuryShard::Impl::respond_cb(const struct hg_cb_info* info) {
    std::unique_ptr<ServerRpcState> state(static_cast<ServerRpcState*>(info->arg));
    if (state->handle != HG_HANDLE_NULL) {
        (void) HG_Destroy(state->handle);
    }
    return HG_SUCCESS;
}

hg_return_t MercuryShard::Impl::bulk_pull_cb(const struct hg_cb_info* info) {
    std::unique_ptr<ServerRpcState> state(static_cast<ServerRpcState*>(info->arg));
    auto* impl = state->impl;
    impl->record_bulk_latency(*state);
    if (info->ret == HG_SUCCESS) {
        impl->on_bulk_transfer(static_cast<std::uint64_t>(state->request.size()));
    } else {
        state->request.clear();
        state->response.clear();
        state->output.status = -1;
        impl->on_rpc_error();
        impl->on_bulk_error();
    }
    impl->cleanup_server_bulk(*state);
    if (info->ret == HG_SUCCESS && state->rpc != nullptr && state->rpc->handler) {
        impl->run_server_handler(std::move(state));
    } else {
        impl->respond_server(std::move(state));
    }
    return HG_SUCCESS;
}

hg_return_t MercuryShard::Impl::forward_cb(const struct hg_cb_info* info) {
    std::unique_ptr<ClientRpcState> state(static_cast<ClientRpcState*>(info->arg));
    auto* impl = state->impl;
    state->timeout_timer.cancel();

    const bool already_failed = state->promise_done;
    if (info->ret != HG_SUCCESS) {
        state->drop_peer_on_cleanup = true;
        if (!already_failed) {
            state->promise.set_exception(std::runtime_error(
                std::string("HG_Forward failed: ") + HG_Error_to_string(info->ret)));
            state->promise_done = true;
            impl->on_rpc_error();
        }
    } else if (!already_failed) {
        // Forward succeeded at the transport level — decode the reply.
        const auto ret = HG_Get_output(info->info.forward.handle, &state->output);
        if (ret != HG_SUCCESS) {
            state->promise.set_exception(std::runtime_error(
                std::string("HG_Get_output failed: ") + HG_Error_to_string(ret)));
            state->promise_done = true;
            impl->on_rpc_error();
        } else {
            if (state->output.status != 0) {
                state->promise.set_exception(
                    std::runtime_error("remote Mercury RPC handler failed"));
                impl->on_rpc_error();
            } else {
                const auto* begin = static_cast<const std::uint8_t*>(state->output.payload.data);
                std::vector<std::uint8_t> response;
                if (begin != nullptr && state->output.payload.size > 0) {
                    response.assign(begin,
                        begin + static_cast<std::size_t>(state->output.payload.size));
                }
                state->promise.set_value(std::move(response));
            }
            state->promise_done = true;
            (void) HG_Free_output(info->info.forward.handle, &state->output);
        }
        state->recycle_handle = true;
    } else {
        // Caller already saw a timeout; the Mercury handle is still reusable
        // since the forward itself succeeded.
        state->recycle_handle = true;
    }

    impl->record_forward_latency_if_needed(*state);
    impl->cleanup_client(*state);
    return HG_SUCCESS;
}

hg_return_t MercuryShard::Impl::lookup_cb(const struct hg_cb_info* info) {
    auto* raw = static_cast<LookupState*>(info->arg);
    auto* impl = raw->impl;
    auto lookup = impl->take_pending_lookup(raw->peer);
    if (!lookup) {
        lookup = impl->take_orphaned_lookup(raw);
        if (!lookup) {
            return HG_SUCCESS;
        }
    }
    if (lookup->cancelled) {
        return HG_SUCCESS;
    }

    if (info->ret != HG_SUCCESS) {
        const std::string msg = std::string("HG_Addr_lookup failed: ") + HG_Error_to_string(info->ret);
        for (auto& waiter : lookup->waiters) {
            impl->fail_client(std::move(waiter), msg);
        }
        return HG_SUCCESS;
    }

    // Success: cache the resolved address and fan out to all pending waiters.
    auto& peer = impl->peer_state(lookup->peer);
    if (peer.addr == HG_ADDR_NULL) {
        peer.addr = info->info.lookup.addr;
    } else if (info->info.lookup.addr != peer.addr) {
        (void) HG_Addr_free(impl->hg_class, info->info.lookup.addr);
    }
    for (auto& waiter : lookup->waiters) {
        waiter->addr = peer.addr;
        impl->start_client_forward(std::move(waiter));
    }
    return HG_SUCCESS;
}

hg_return_t MercuryShard::Impl::dispatch_cb(hg_handle_t handle) {
    const auto* info = HG_Get_info(handle);
    if (info == nullptr) {
        HG_Destroy(handle);
        return HG_INVALID_ARG;
    }
    auto* impl = static_cast<Impl*>(HG_Registered_data(info->hg_class, info->id));
    if (impl == nullptr) {
        HG_Destroy(handle);
        return HG_NOENTRY;
    }
    return static_cast<hg_return_t>(impl->handle_rpc(handle));
}

#endif  // MKMQ_HAVE_MERCURY

// ---------------------------------------------------------------------------
// MercuryShard public API — all methods forward to Impl.
// ---------------------------------------------------------------------------

MercuryShard::MercuryShard() : impl_(std::make_unique<Impl>()) {}
MercuryShard::~MercuryShard() = default;

seastar::future<> MercuryShard::start(std::string address, bool listen) {
    auto& impl = *impl_;
    if (impl.running) {
        return seastar::make_ready_future<>();
    }
#if MKMQ_HAVE_MERCURY
    impl.hg_class = HG_Init(address.c_str(), listen ? HG_TRUE : HG_FALSE);
    if (impl.hg_class == nullptr) {
        return seastar::make_exception_future<>(
            std::runtime_error("HG_Init failed for " + address));
    }
    impl.hg_context = HG_Context_create(impl.hg_class);
    if (impl.hg_context == nullptr) {
        HG_Finalize(impl.hg_class);
        impl.hg_class = nullptr;
        return seastar::make_exception_future<>(std::runtime_error("HG_Context_create failed"));
    }
    impl.address = std::move(address);
    impl.running = true;
    // Register any handlers that were added before start().
    for (auto& [_, entry] : impl.rpcs_by_name) {
        if (entry->id == 0) {
            impl.bind_rpc_to_mercury(*entry);
        }
    }
    impl.arm_progress_timer();
#else
    (void) listen;
    impl.address = std::move(address);
    impl.running = true;
#endif
    return seastar::make_ready_future<>();
}

seastar::future<> MercuryShard::stop() {
    auto& impl = *impl_;
    if (!impl.running) {
        return seastar::make_ready_future<>();
    }
    impl.running = false;
#if MKMQ_HAVE_MERCURY
    impl.progress_timer.cancel();
    if (impl.hg_class != nullptr) {
        impl.clear_all_peers();
    }
    if (impl.hg_context != nullptr) {
        HG_Context_destroy(impl.hg_context);
        impl.hg_context = nullptr;
    }
    if (impl.hg_class != nullptr) {
        HG_Finalize(impl.hg_class);
        impl.hg_class = nullptr;
    }
    impl.rpcs_by_id.clear();
    impl.last_rpc_ = nullptr;
    // Leave rpcs_by_name populated so that a subsequent start() re-registers
    // the same handlers; we just clear their Mercury ids.
    for (auto& [_, entry] : impl.rpcs_by_name) {
        entry->id = 0;
    }
#endif
    return seastar::make_ready_future<>();
}

bool MercuryShard::running() const noexcept { return impl_->running; }
std::uint64_t MercuryShard::rpc_forwards() const noexcept { return impl_->rpc_forwards; }
std::uint64_t MercuryShard::rpc_receives() const noexcept { return impl_->rpc_receives; }
std::uint64_t MercuryShard::rpc_errors() const noexcept { return impl_->rpc_errors; }
std::uint64_t MercuryShard::rpc_timeouts() const noexcept { return impl_->rpc_timeouts; }
std::uint64_t MercuryShard::bulk_transfers() const noexcept { return impl_->bulk_transfers; }
std::uint64_t MercuryShard::bulk_bytes() const noexcept { return impl_->bulk_bytes; }
std::uint64_t MercuryShard::bulk_errors() const noexcept { return impl_->bulk_errors; }

seastar::metrics::internal::time_estimated_histogram MercuryShard::rpc_forward_latency() const {
    return impl_->rpc_forward_latency;
}
seastar::metrics::internal::time_estimated_histogram MercuryShard::rpc_handler_latency() const {
    return impl_->rpc_handler_latency;
}
seastar::metrics::internal::time_estimated_histogram MercuryShard::bulk_latency() const {
    return impl_->bulk_latency;
}

void MercuryShard::register_rpc(std::string_view name, RpcHandler handler) {
#if MKMQ_HAVE_MERCURY
    impl_->register_handler(name, std::move(handler));
#else
    impl_->deferred_handlers.emplace(std::string(name), std::move(handler));
#endif
}

seastar::future<std::vector<std::uint8_t>> MercuryShard::forward(
    std::string_view peer,
    std::string_view rpc,
    std::vector<std::uint8_t> payload) {
#if MKMQ_HAVE_MERCURY
    auto& impl = *impl_;

    // Loopback fast-path: invoke the local handler without touching Mercury.
    if (peer == impl.address) {
        auto* entry = impl.find_rpc_by_name(rpc);
        if (entry == nullptr || !entry->handler) {
            return seastar::make_exception_future<std::vector<std::uint8_t>>(
                std::runtime_error("Mercury RPC is not registered: " + std::string(rpc)));
        }
        return entry->handler(std::move(payload));
    }

    if (!impl.running || impl.hg_class == nullptr || impl.hg_context == nullptr) {
        return seastar::make_exception_future<std::vector<std::uint8_t>>(
            std::runtime_error("Mercury shard is not running"));
    }

    auto* entry = impl.find_rpc_by_name(rpc);
    if (entry == nullptr) {
        // An RPC we've never seen on this shard — reserve an id for it so
        // that the dispatch table on the peer side matches.
        impl.register_handler(rpc, RpcHandler{});
        entry = impl.find_rpc_by_name(rpc);
    }
    if (entry->id == 0) {
        impl.bind_rpc_to_mercury(*entry);
    }

    impl.on_rpc_forward();
    auto state = std::make_unique<Impl::ClientRpcState>(&impl);
    state->started = std::chrono::steady_clock::now();
    state->rpc_id = entry->id;
    state->peer = std::string(peer);  // single copy; reused for peer map lookups below
    state->request = std::move(payload);
    if (state->request.size() >= kBulkTransferThreshold) {
        void* buffer = state->request.data();
        const hg_size_t size = static_cast<hg_size_t>(state->request.size());
        const auto bulk_ret = HG_Bulk_create(
            impl.hg_class, 1, &buffer, &size, HG_BULK_READ_ONLY, &state->origin_bulk);
        if (bulk_ret != HG_SUCCESS) {
            impl.on_rpc_error();
            return seastar::make_exception_future<std::vector<std::uint8_t>>(
                std::runtime_error(std::string("HG_Bulk_create failed: ") + HG_Error_to_string(bulk_ret)));
        }
        state->input.mode = static_cast<std::int32_t>(HgTransferMode::Bulk);
        state->input.bulk_size = size;
        state->input.bulk_handle = state->origin_bulk;
    } else {
        state->input.mode = static_cast<std::int32_t>(HgTransferMode::Eager);
        state->input.eager.size = static_cast<hg_size_t>(state->request.size());
        state->input.eager.data = state->request.empty() ? nullptr : state->request.data();
    }
    auto future = state->promise.get_future();

    // Fast path: resolved address already cached.
    auto& peer_entry = impl.peer_state(state->peer);
    if (peer_entry.addr != HG_ADDR_NULL) {
        state->addr = peer_entry.addr;
        impl.start_client_forward(std::move(state));
        return future;
    }

    // Coalesce concurrent lookups onto a single in-flight request.
    if (peer_entry.pending_lookup) {
        peer_entry.pending_lookup->waiters.push_back(std::move(state));
        return future;
    }

    auto lookup = std::make_unique<Impl::LookupState>();
    lookup->impl = &impl;
    lookup->peer = state->peer;
    lookup->waiters.push_back(std::move(state));
    auto* raw_lookup = lookup.get();
    peer_entry.pending_lookup = std::move(lookup);

    const auto ret = HG_Addr_lookup(
        impl.hg_context, &Impl::lookup_cb, raw_lookup,
        raw_lookup->peer.c_str(), HG_OP_ID_IGNORE);
    if (ret != HG_SUCCESS) {
        auto failed = impl.take_pending_lookup(raw_lookup->peer);
        if (failed) {
            const std::string msg = std::string("HG_Addr_lookup failed: ") + HG_Error_to_string(ret);
            for (auto& waiter : failed->waiters) {
                impl.fail_client(std::move(waiter), msg);
            }
        }
    }
    return future;
#else
    (void) peer;
    (void) rpc;
    (void) payload;
    return seastar::make_exception_future<std::vector<std::uint8_t>>(
        std::runtime_error("Mercury support is disabled in this build"));
#endif
}

}  // namespace mkmq

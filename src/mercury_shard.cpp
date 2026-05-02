#include "mkmq/mercury_shard.hpp"

#if MKMQ_HAVE_MERCURY
#include <mercury.h>
#include <mercury_bulk.h>
#include <mercury_proc.h>
#include <mercury_proc_bulk.h>
#endif

#include <seastar/core/future.hh>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace mkmq {
namespace {

#if MKMQ_HAVE_MERCURY

constexpr hg_size_t kBulkTransferThreshold = 64 * 1024;
constexpr std::size_t kHandlePoolLimitPerPeerRpc = 128;
constexpr auto kMercuryIdleProgressInterval = std::chrono::microseconds(5);
constexpr auto kMercuryActiveProgressInterval = std::chrono::microseconds(1);
constexpr unsigned int kMercuryTriggerBatch = 64;
constexpr std::size_t kMercuryProgressRounds = 8;

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

struct ClientRpcState {
    ClientRpcState()
        : timeout_timer([this] {
              if (!promise_done) {
                  promise.set_exception(std::runtime_error("Mercury RPC timed out for peer: " + peer));
                  promise_done = true;
                  if (rpc_errors != nullptr) {
                      ++*rpc_errors;
                  }
                  if (rpc_timeouts != nullptr) {
                      ++*rpc_timeouts;
                  }
                  if (forward_latency != nullptr) {
                      forward_latency->add(std::chrono::steady_clock::now() - started);
                      latency_recorded = true;
                  }
              }
          }) {}

    seastar::promise<std::vector<std::uint8_t>> promise;
    seastar::timer<> timeout_timer;
    std::chrono::steady_clock::time_point started;
    hg_class_t* hg_class{nullptr};
    hg_context_t* hg_context{nullptr};
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
    std::uint64_t* rpc_errors{nullptr};
    std::uint64_t* rpc_timeouts{nullptr};
    seastar::metrics::internal::time_estimated_histogram* forward_latency{nullptr};
    MercuryShard* shard{nullptr};
    std::string handle_pool_key;
    bool recycle_handle{false};
    bool drop_peer_cache{false};
};

struct LookupState {
    MercuryShard* shard{nullptr};
    hg_class_t* hg_class{nullptr};
    hg_context_t* hg_context{nullptr};
    std::string peer;
    std::vector<std::unique_ptr<ClientRpcState>> waiters;
    bool cancelled{false};
};

struct ServerRpcState {
    hg_handle_t handle{HG_HANDLE_NULL};
    MercuryShard::RpcHandler* handler{nullptr};
    HgRequest input;
    bool input_loaded{false};
    hg_bulk_t local_bulk{HG_BULK_NULL};
    std::vector<std::uint8_t> request;
    std::vector<std::uint8_t> response;
    HgReply output;
    std::chrono::steady_clock::time_point started;
    seastar::metrics::internal::time_estimated_histogram* handler_latency{nullptr};
    seastar::metrics::internal::time_estimated_histogram* bulk_latency{nullptr};
    std::uint64_t* rpc_errors{nullptr};
    std::uint64_t* bulk_transfers{nullptr};
    std::uint64_t* bulk_bytes{nullptr};
    std::uint64_t* bulk_errors{nullptr};
};

hg_return_t mercury_respond_cb(const struct hg_cb_info* info);
hg_return_t mercury_forward_cb(const struct hg_cb_info* info);
hg_return_t mercury_lookup_cb(const struct hg_cb_info* info);
void cleanup_client_mercury_resources(ClientRpcState& state);

struct MercuryClientCache {
    std::unordered_map<std::string, hg_addr_t> addrs;
    std::unordered_map<std::string, std::unique_ptr<LookupState>> lookups;
    std::unordered_map<std::string, std::vector<hg_handle_t>> handle_pools;
    std::vector<std::unique_ptr<LookupState>> orphaned_lookups;
};

}  // namespace

void* mercury_client_cache_raw(MercuryShard* shard) noexcept {
    return shard->client_cache_;
}

namespace {

MercuryClientCache& mercury_client_cache(MercuryShard* shard) {
    return *static_cast<MercuryClientCache*>(mercury_client_cache_raw(shard));
}

std::string handle_pool_key(const std::string& peer, hg_id_t rpc_id) {
    std::string key = peer;
    key.push_back('\0');
    key += std::to_string(static_cast<std::uint64_t>(rpc_id));
    return key;
}

hg_addr_t cached_peer_addr(MercuryShard* shard, const std::string& peer) {
    auto& cache = mercury_client_cache(shard);
    const auto it = cache.addrs.find(peer);
    return it == cache.addrs.end() ? HG_ADDR_NULL : it->second;
}

hg_addr_t cache_peer_addr(MercuryShard* shard, hg_class_t* hg_class, const std::string& peer, hg_addr_t addr) {
    auto& cache = mercury_client_cache(shard);
    const auto [it, inserted] = cache.addrs.emplace(peer, addr);
    if (!inserted) {
        if (addr != HG_ADDR_NULL && addr != it->second) {
            (void) HG_Addr_free(hg_class, addr);
        }
        return it->second;
    }
    return addr;
}

std::unique_ptr<LookupState> take_lookup_state(MercuryShard* shard, const std::string& peer) {
    auto& cache = mercury_client_cache(shard);
    auto it = cache.lookups.find(peer);
    if (it == cache.lookups.end()) {
        return nullptr;
    }
    auto state = std::move(it->second);
    cache.lookups.erase(it);
    return state;
}

std::unique_ptr<LookupState> take_orphaned_lookup_state(LookupState* raw_lookup) {
    auto& lookups = mercury_client_cache(raw_lookup->shard).orphaned_lookups;
    const auto it = std::find_if(lookups.begin(), lookups.end(), [raw_lookup](const auto& lookup) {
        return lookup.get() == raw_lookup;
    });
    if (it == lookups.end()) {
        return nullptr;
    }
    auto state = std::move(*it);
    lookups.erase(it);
    return state;
}

hg_handle_t take_cached_handle(MercuryShard* shard, const std::string& key) {
    auto& cache = mercury_client_cache(shard);
    auto it = cache.handle_pools.find(key);
    if (it == cache.handle_pools.end() || it->second.empty()) {
        return HG_HANDLE_NULL;
    }
    auto handle = it->second.back();
    it->second.pop_back();
    return handle;
}

void recycle_cached_handle(MercuryShard* shard, const std::string& key, hg_handle_t handle) {
    auto& pool = mercury_client_cache(shard).handle_pools[key];
    if (pool.size() >= kHandlePoolLimitPerPeerRpc) {
        (void) HG_Destroy(handle);
        return;
    }
    pool.push_back(handle);
}

void drop_peer_cache(MercuryShard* shard, hg_class_t* hg_class, const std::string& peer) {
    auto& cache = mercury_client_cache(shard);
    auto addr_it = cache.addrs.find(peer);
    if (addr_it != cache.addrs.end()) {
        (void) HG_Addr_free(hg_class, addr_it->second);
        cache.addrs.erase(addr_it);
    }

    const std::string prefix = peer + '\0';
    for (auto it = cache.handle_pools.begin(); it != cache.handle_pools.end();) {
        if (it->first.rfind(prefix, 0) != 0) {
            ++it;
            continue;
        }
        for (auto handle : it->second) {
            (void) HG_Destroy(handle);
        }
        it = cache.handle_pools.erase(it);
    }
}

void clear_client_cache(MercuryShard* shard, hg_class_t* hg_class) {
    if (mercury_client_cache_raw(shard) == nullptr) {
        return;
    }
    auto& cache = mercury_client_cache(shard);
    for (auto& [_, lookup] : cache.lookups) {
        for (auto& waiter : lookup->waiters) {
            if (!waiter->promise_done) {
                waiter->promise.set_exception(std::runtime_error("Mercury shard stopped during address lookup"));
                waiter->promise_done = true;
            }
            cleanup_client_mercury_resources(*waiter);
        }
        lookup->waiters.clear();
        lookup->cancelled = true;
        cache.orphaned_lookups.push_back(std::move(lookup));
    }
    cache.lookups.clear();
    for (auto& [_, handles] : cache.handle_pools) {
        for (auto handle : handles) {
            (void) HG_Destroy(handle);
        }
    }
    cache.handle_pools.clear();
    for (auto& [_, addr] : cache.addrs) {
        (void) HG_Addr_free(hg_class, addr);
    }
    cache.addrs.clear();
}

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

void cleanup_client_mercury_resources(ClientRpcState& state) {
    if (state.origin_bulk != HG_BULK_NULL) {
        (void) HG_Bulk_free(state.origin_bulk);
        state.origin_bulk = HG_BULK_NULL;
    }
    if (state.handle != HG_HANDLE_NULL) {
        if (state.recycle_handle && state.shard != nullptr && !state.handle_pool_key.empty()) {
            recycle_cached_handle(state.shard, state.handle_pool_key, state.handle);
        } else {
            (void) HG_Destroy(state.handle);
        }
        state.handle = HG_HANDLE_NULL;
    }
    if (state.drop_peer_cache && state.shard != nullptr) {
        drop_peer_cache(state.shard, state.hg_class, state.peer);
    }
    state.addr = HG_ADDR_NULL;
}

void cleanup_server_bulk_resources(ServerRpcState& state) {
    if (state.local_bulk != HG_BULK_NULL) {
        (void) HG_Bulk_free(state.local_bulk);
        state.local_bulk = HG_BULK_NULL;
    }
    if (state.input_loaded) {
        (void) HG_Free_input(state.handle, &state.input);
        state.input_loaded = false;
    }
}

void respond_server_rpc(std::unique_ptr<ServerRpcState> state) {
    if (state->handler_latency != nullptr) {
        state->handler_latency->add(std::chrono::steady_clock::now() - state->started);
    }
    state->output.payload.size = static_cast<hg_size_t>(state->response.size());
    state->output.payload.data = state->response.empty() ? nullptr : state->response.data();
    const auto respond_ret = HG_Respond(
        state->handle,
        mercury_respond_cb,
        state.get(),
        &state->output);
    if (respond_ret == HG_SUCCESS) {
        (void) state.release();
    } else if (state->handle != HG_HANDLE_NULL) {
        (void) HG_Destroy(state->handle);
    }
}

void run_server_handler(std::unique_ptr<ServerRpcState> state, MercuryShard::RpcHandler* handler) {
    (void) (*handler)(std::move(state->request)).then_wrapped(
        [state = std::move(state)](seastar::future<std::vector<std::uint8_t>> result) mutable {
            try {
                state->response = result.get();
                state->output.status = 0;
            } catch (...) {
                state->response.clear();
                state->output.status = -1;
                if (state->rpc_errors != nullptr) {
                    ++*state->rpc_errors;
                }
            }
            respond_server_rpc(std::move(state));
        }).handle_exception([](std::exception_ptr) {});
}

hg_return_t mercury_bulk_pull_cb(const struct hg_cb_info* info) {
    std::unique_ptr<ServerRpcState> state(static_cast<ServerRpcState*>(info->arg));
    if (state->bulk_latency != nullptr) {
        state->bulk_latency->add(std::chrono::steady_clock::now() - state->started);
    }
    if (info->ret == HG_SUCCESS) {
        if (state->bulk_transfers != nullptr) {
            ++*state->bulk_transfers;
        }
        if (state->bulk_bytes != nullptr) {
            *state->bulk_bytes += static_cast<std::uint64_t>(state->request.size());
        }
    } else {
        state->request.clear();
        state->response.clear();
        state->output.status = -1;
        if (state->rpc_errors != nullptr) {
            ++*state->rpc_errors;
        }
        if (state->bulk_errors != nullptr) {
            ++*state->bulk_errors;
        }
    }
    cleanup_server_bulk_resources(*state);
    auto* handler = state->handler;
    if (info->ret == HG_SUCCESS && handler != nullptr) {
        run_server_handler(std::move(state), handler);
    } else {
        respond_server_rpc(std::move(state));
    }
    return HG_SUCCESS;
}

void fail_client_rpc(std::unique_ptr<ClientRpcState> state, const std::string& message) {
    state->timeout_timer.cancel();
    if (!state->promise_done) {
        state->promise.set_exception(std::runtime_error(message));
        state->promise_done = true;
    }
    if (state->rpc_errors != nullptr) {
        ++*state->rpc_errors;
    }
    if (state->forward_latency != nullptr && !state->latency_recorded) {
        state->forward_latency->add(std::chrono::steady_clock::now() - state->started);
        state->latency_recorded = true;
    }
    cleanup_client_mercury_resources(*state);
}

void start_client_forward(std::unique_ptr<ClientRpcState> state) {
    if (state->promise_done) {
        cleanup_client_mercury_resources(*state);
        return;
    }

    state->handle_pool_key = handle_pool_key(state->peer, state->rpc_id);
    state->handle = take_cached_handle(state->shard, state->handle_pool_key);
    if (state->handle != HG_HANDLE_NULL) {
        const auto reset_ret = HG_Reset(state->handle, state->addr, state->rpc_id);
        if (reset_ret != HG_SUCCESS) {
            (void) HG_Destroy(state->handle);
            state->handle = HG_HANDLE_NULL;
        }
    }
    if (state->handle == HG_HANDLE_NULL) {
        auto ret = HG_Create(state->hg_context, state->addr, state->rpc_id, &state->handle);
        if (ret != HG_SUCCESS) {
            state->drop_peer_cache = true;
            fail_client_rpc(std::move(state), std::string("HG_Create failed: ") + HG_Error_to_string(ret));
            return;
        }
    }

    auto ret = HG_Forward(state->handle, mercury_forward_cb, state.get(), &state->input);
    if (ret != HG_SUCCESS) {
        state->drop_peer_cache = true;
        fail_client_rpc(std::move(state), std::string("HG_Forward failed: ") + HG_Error_to_string(ret));
        return;
    }
    state->timeout_timer.arm(std::chrono::milliseconds(100));
    (void) state.release();
}

hg_return_t mercury_dispatch_cb(hg_handle_t handle) {
    const auto* info = HG_Get_info(handle);
    if (info == nullptr) {
        HG_Destroy(handle);
        return HG_INVALID_ARG;
    }
    auto* shard = static_cast<MercuryShard*>(HG_Registered_data(info->hg_class, info->id));
    if (shard == nullptr) {
        HG_Destroy(handle);
        return HG_NOENTRY;
    }
    return static_cast<hg_return_t>(shard->handle_mercury_rpc(handle));
}

hg_return_t mercury_forward_cb(const struct hg_cb_info* info) {
    std::unique_ptr<ClientRpcState> state(static_cast<ClientRpcState*>(info->arg));
    state->timeout_timer.cancel();
    if (info->ret != HG_SUCCESS) {
        state->drop_peer_cache = true;
    }
    if (state->promise_done) {
        // The Seastar caller already saw a timeout; just release Mercury resources.
    } else if (info->ret != HG_SUCCESS) {
        state->promise.set_exception(std::runtime_error(
            std::string("HG_Forward failed: ") + HG_Error_to_string(info->ret)));
        state->promise_done = true;
        if (state->rpc_errors != nullptr) {
            ++*state->rpc_errors;
        }
    } else {
        const auto ret = HG_Get_output(info->info.forward.handle, &state->output);
        if (ret != HG_SUCCESS) {
            state->promise.set_exception(std::runtime_error(
                std::string("HG_Get_output failed: ") + HG_Error_to_string(ret)));
            state->promise_done = true;
            if (state->rpc_errors != nullptr) {
                ++*state->rpc_errors;
            }
        } else if (state->output.status != 0) {
            state->promise.set_exception(std::runtime_error("remote Mercury RPC handler failed"));
            state->promise_done = true;
            if (state->rpc_errors != nullptr) {
                ++*state->rpc_errors;
            }
        } else {
            const auto* begin = static_cast<const std::uint8_t*>(state->output.payload.data);
            std::vector<std::uint8_t> response;
            if (begin != nullptr && state->output.payload.size > 0) {
                response.assign(begin, begin + static_cast<std::size_t>(state->output.payload.size));
            }
            state->promise.set_value(std::move(response));
            state->promise_done = true;
        }
        if (ret == HG_SUCCESS) {
            (void) HG_Free_output(info->info.forward.handle, &state->output);
        }
        state->recycle_handle = true;
    }
    if (state->promise_done && info->ret == HG_SUCCESS) {
        state->recycle_handle = true;
    }
    if (!state->latency_recorded && state->forward_latency != nullptr) {
        state->forward_latency->add(std::chrono::steady_clock::now() - state->started);
        state->latency_recorded = true;
    }
    cleanup_client_mercury_resources(*state);
    return HG_SUCCESS;
}

hg_return_t mercury_lookup_cb(const struct hg_cb_info* info) {
    auto* raw_lookup = static_cast<LookupState*>(info->arg);
    auto lookup = take_lookup_state(raw_lookup->shard, raw_lookup->peer);
    if (!lookup) {
        lookup = take_orphaned_lookup_state(raw_lookup);
        if (!lookup) {
            return HG_SUCCESS;
        }
    }
    if (lookup->cancelled) {
        return HG_SUCCESS;
    }

    if (info->ret != HG_SUCCESS) {
        for (auto& waiter : lookup->waiters) {
            fail_client_rpc(std::move(waiter), std::string("HG_Addr_lookup failed: ") + HG_Error_to_string(info->ret));
        }
        return HG_SUCCESS;
    }

    const auto addr = cache_peer_addr(lookup->shard, lookup->hg_class, lookup->peer, info->info.lookup.addr);
    for (auto& waiter : lookup->waiters) {
        waiter->addr = addr;
        start_client_forward(std::move(waiter));
    }
    return HG_SUCCESS;
}

hg_return_t mercury_respond_cb(const struct hg_cb_info* info) {
    std::unique_ptr<ServerRpcState> state(static_cast<ServerRpcState*>(info->arg));
    (void) info;
    if (state->handle != HG_HANDLE_NULL) {
        (void) HG_Destroy(state->handle);
    }
    return HG_SUCCESS;
}

#endif

}  // namespace

MercuryShard::MercuryShard()
    : progress_timer_([this] {
          const bool progressed = progress_once();
          arm_progress_timer(progressed);
      })
#if MKMQ_HAVE_MERCURY
    , client_cache_(new MercuryClientCache())
#endif
{}

MercuryShard::~MercuryShard() {
#if MKMQ_HAVE_MERCURY
    delete static_cast<MercuryClientCache*>(client_cache_);
#endif
    client_cache_ = nullptr;
}

seastar::future<> MercuryShard::start(std::string address, bool listen) {
    if (running_) {
        return seastar::make_ready_future<>();
    }
#if MKMQ_HAVE_MERCURY
    hg_class_ = HG_Init(address.c_str(), listen ? HG_TRUE : HG_FALSE);
    if (hg_class_ == nullptr) {
        return seastar::make_exception_future<>(std::runtime_error("HG_Init failed for " + address));
    }
    hg_context_ = HG_Context_create(static_cast<hg_class_t*>(hg_class_));
    if (hg_context_ == nullptr) {
        HG_Finalize(static_cast<hg_class_t*>(hg_class_));
        hg_class_ = nullptr;
        return seastar::make_exception_future<>(std::runtime_error("HG_Context_create failed"));
    }
#else
    (void) address;
    (void) listen;
#endif
    address_ = std::move(address);
    running_ = true;
    for (const auto& [name, _] : handlers_) {
        register_mercury_rpc(name);
    }
    arm_progress_timer();
    return seastar::make_ready_future<>();
}

seastar::future<> MercuryShard::stop() {
    if (!running_) {
        return seastar::make_ready_future<>();
    }
    running_ = false;
    progress_timer_.cancel();
#if MKMQ_HAVE_MERCURY
    if (hg_class_ != nullptr) {
        clear_client_cache(this, static_cast<hg_class_t*>(hg_class_));
    }
    if (hg_context_ != nullptr) {
        HG_Context_destroy(static_cast<hg_context_t*>(hg_context_));
        hg_context_ = nullptr;
    }
    if (hg_class_ != nullptr) {
        HG_Finalize(static_cast<hg_class_t*>(hg_class_));
        hg_class_ = nullptr;
    }
    rpc_ids_.clear();
    rpc_names_.clear();
#endif
    return seastar::make_ready_future<>();
}

bool MercuryShard::running() const noexcept {
    return running_;
}

std::uint64_t MercuryShard::rpc_forwards() const noexcept {
    return rpc_forwards_;
}

std::uint64_t MercuryShard::rpc_receives() const noexcept {
    return rpc_receives_;
}

std::uint64_t MercuryShard::rpc_errors() const noexcept {
    return rpc_errors_;
}

std::uint64_t MercuryShard::rpc_timeouts() const noexcept {
    return rpc_timeouts_;
}

std::uint64_t MercuryShard::bulk_transfers() const noexcept {
    return bulk_transfers_;
}

std::uint64_t MercuryShard::bulk_bytes() const noexcept {
    return bulk_bytes_;
}

std::uint64_t MercuryShard::bulk_errors() const noexcept {
    return bulk_errors_;
}

seastar::metrics::internal::time_estimated_histogram MercuryShard::rpc_forward_latency() const {
    return rpc_forward_latency_;
}

seastar::metrics::internal::time_estimated_histogram MercuryShard::rpc_handler_latency() const {
    return rpc_handler_latency_;
}

seastar::metrics::internal::time_estimated_histogram MercuryShard::bulk_latency() const {
    return bulk_latency_;
}

void MercuryShard::register_rpc(std::string name, RpcHandler handler) {
    auto rpc_name = std::move(name);
    handlers_[rpc_name] = std::move(handler);
    if (running_) {
        register_mercury_rpc(rpc_name);
    }
}

seastar::future<std::vector<std::uint8_t>> MercuryShard::forward(
    std::string peer,
    std::string rpc,
    std::vector<std::uint8_t> payload) {
#if MKMQ_HAVE_MERCURY
    if (peer == address_) {
        const auto it = handlers_.find(rpc);
        if (it == handlers_.end()) {
            return seastar::make_exception_future<std::vector<std::uint8_t>>(
                std::runtime_error("Mercury RPC is not registered: " + rpc));
        }
        return it->second(std::move(payload));
    }

    if (!running_ || hg_class_ == nullptr || hg_context_ == nullptr) {
        return seastar::make_exception_future<std::vector<std::uint8_t>>(
            std::runtime_error("Mercury shard is not running"));
    }

    if (rpc_ids_.find(rpc) == rpc_ids_.end()) {
        register_mercury_rpc(rpc);
    }
    const auto rpc_id = static_cast<hg_id_t>(rpc_ids_.at(rpc));

    ++rpc_forwards_;
    auto state = std::make_unique<ClientRpcState>();
    state->started = std::chrono::steady_clock::now();
    state->hg_class = static_cast<hg_class_t*>(hg_class_);
    state->hg_context = static_cast<hg_context_t*>(hg_context_);
    state->shard = this;
    state->rpc_id = rpc_id;
    state->peer = peer;
    state->request = std::move(payload);
    if (state->request.size() >= kBulkTransferThreshold) {
        void* buffer = state->request.data();
        const hg_size_t size = static_cast<hg_size_t>(state->request.size());
        const auto bulk_ret = HG_Bulk_create(
            state->hg_class,
            1,
            &buffer,
            &size,
            HG_BULK_READ_ONLY,
            &state->origin_bulk);
        if (bulk_ret != HG_SUCCESS) {
            ++rpc_errors_;
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
    state->rpc_errors = &rpc_errors_;
    state->rpc_timeouts = &rpc_timeouts_;
    state->forward_latency = &rpc_forward_latency_;
    auto future = state->promise.get_future();

    if (auto addr = cached_peer_addr(this, state->peer); addr != HG_ADDR_NULL) {
        state->addr = addr;
        start_client_forward(std::move(state));
        return future;
    }

    auto& cache = mercury_client_cache(this);
    auto lookup_it = cache.lookups.find(peer);
    if (lookup_it != cache.lookups.end()) {
        lookup_it->second->waiters.push_back(std::move(state));
        return future;
    }

    auto lookup = std::make_unique<LookupState>();
    lookup->shard = this;
    lookup->hg_class = static_cast<hg_class_t*>(hg_class_);
    lookup->hg_context = static_cast<hg_context_t*>(hg_context_);
    lookup->peer = peer;
    lookup->waiters.push_back(std::move(state));
    auto* raw_lookup = lookup.get();
    cache.lookups.emplace(peer, std::move(lookup));
    auto ret = HG_Addr_lookup(
        static_cast<hg_context_t*>(hg_context_),
        mercury_lookup_cb,
        raw_lookup,
        peer.c_str(),
        HG_OP_ID_IGNORE);
    if (ret != HG_SUCCESS) {
        auto failed_lookup = take_lookup_state(this, peer);
        if (failed_lookup) {
            for (auto& waiter : failed_lookup->waiters) {
                fail_client_rpc(std::move(waiter), std::string("HG_Addr_lookup failed: ") + HG_Error_to_string(ret));
            }
        }
        return future;
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

void MercuryShard::arm_progress_timer(bool recently_active) {
    if (running_) {
        progress_timer_.arm(recently_active ? kMercuryActiveProgressInterval : kMercuryIdleProgressInterval);
    }
}

bool MercuryShard::progress_once() {
#if MKMQ_HAVE_MERCURY
    if (!running_ || hg_context_ == nullptr) {
        return false;
    }
    bool progressed = false;
    unsigned int actual_count = 0;
    for (std::size_t round = 0; round < kMercuryProgressRounds; ++round) {
        bool triggered = false;
        do {
            const auto ret = HG_Trigger(
                static_cast<hg_context_t*>(hg_context_),
                0,
                kMercuryTriggerBatch,
                &actual_count);
            if (ret != HG_SUCCESS) {
                actual_count = 0;
                break;
            }
            if (actual_count > 0) {
                triggered = true;
                progressed = true;
            }
        } while (actual_count > 0);

        const auto progress_ret = HG_Progress(static_cast<hg_context_t*>(hg_context_), 0);
        if (progress_ret == HG_SUCCESS) {
            progressed = true;
            continue;
        }
        if (!triggered) {
            break;
        }
    }
    return progressed;
#else
    return false;
#endif
}

void MercuryShard::register_mercury_rpc(const std::string& name) {
#if MKMQ_HAVE_MERCURY
    if (hg_class_ == nullptr || rpc_ids_.find(name) != rpc_ids_.end()) {
        return;
    }
    const auto id = HG_Register_name(
        static_cast<hg_class_t*>(hg_class_),
        name.c_str(),
        proc_hg_request,
        proc_hg_reply,
        mercury_dispatch_cb);
    if (id == 0) {
        throw std::runtime_error("HG_Register_name failed for " + name);
    }
    const auto ret = HG_Register_data(static_cast<hg_class_t*>(hg_class_), id, this, nullptr);
    if (ret != HG_SUCCESS) {
        throw std::runtime_error(std::string("HG_Register_data failed: ") + HG_Error_to_string(ret));
    }
    rpc_ids_[name] = static_cast<std::uint64_t>(id);
    rpc_names_[static_cast<std::uint64_t>(id)] = name;
#else
    (void) name;
#endif
}

int MercuryShard::handle_mercury_rpc(void* raw_handle) {
#if MKMQ_HAVE_MERCURY
    ++rpc_receives_;
    const auto started = std::chrono::steady_clock::now();
    auto handle = static_cast<hg_handle_t>(raw_handle);
    const auto* info = HG_Get_info(handle);
    if (info == nullptr) {
        ++rpc_errors_;
        (void) HG_Destroy(handle);
        return HG_INVALID_ARG;
    }
    const auto name_it = rpc_names_.find(static_cast<std::uint64_t>(info->id));
    if (name_it == rpc_names_.end()) {
        ++rpc_errors_;
        (void) HG_Destroy(handle);
        return HG_NOENTRY;
    }
    const auto handler_it = handlers_.find(name_it->second);
    if (handler_it == handlers_.end()) {
        ++rpc_errors_;
        (void) HG_Destroy(handle);
        return HG_NOENTRY;
    }

    auto state = std::make_unique<ServerRpcState>();
    state->handle = handle;
    state->handler = &handler_it->second;
    state->started = started;
    state->handler_latency = &rpc_handler_latency_;
    state->bulk_latency = &bulk_latency_;
    state->rpc_errors = &rpc_errors_;
    state->bulk_transfers = &bulk_transfers_;
    state->bulk_bytes = &bulk_bytes_;
    state->bulk_errors = &bulk_errors_;

    auto ret = HG_Get_input(handle, &state->input);
    if (ret != HG_SUCCESS) {
        ++rpc_errors_;
        (void) HG_Destroy(handle);
        return ret;
    }
    state->input_loaded = true;

    if (state->input.mode == static_cast<std::int32_t>(HgTransferMode::Bulk)) {
        if (state->input.bulk_size == 0 || state->input.bulk_handle == HG_BULK_NULL) {
            ++rpc_errors_;
            ++bulk_errors_;
            cleanup_server_bulk_resources(*state);
            state->output.status = -1;
            respond_server_rpc(std::move(state));
            return HG_SUCCESS;
        }
        state->request.resize(static_cast<std::size_t>(state->input.bulk_size));
        void* buffer = state->request.data();
        const hg_size_t size = state->input.bulk_size;
        ret = HG_Bulk_create(
            static_cast<hg_class_t*>(hg_class_),
            1,
            &buffer,
            &size,
            HG_BULK_WRITE_ONLY,
            &state->local_bulk);
        if (ret != HG_SUCCESS) {
            ++rpc_errors_;
            ++bulk_errors_;
            cleanup_server_bulk_resources(*state);
            state->output.status = -1;
            respond_server_rpc(std::move(state));
            return HG_SUCCESS;
        }
        ret = HG_Bulk_transfer(
            static_cast<hg_context_t*>(hg_context_),
            mercury_bulk_pull_cb,
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
            ++rpc_errors_;
            ++bulk_errors_;
            cleanup_server_bulk_resources(*state);
            state->output.status = -1;
            respond_server_rpc(std::move(state));
            return HG_SUCCESS;
        }
        (void) state.release();
        return HG_SUCCESS;
    }

    const auto* bytes = static_cast<const std::uint8_t*>(state->input.eager.data);
    if (bytes != nullptr && state->input.eager.size > 0) {
        state->request.assign(bytes, bytes + static_cast<std::size_t>(state->input.eager.size));
    }
    cleanup_server_bulk_resources(*state);
    run_server_handler(std::move(state), &handler_it->second);
    return HG_SUCCESS;
#else
    (void) raw_handle;
    return 0;
#endif
}

}  // namespace mkmq

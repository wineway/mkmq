#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/internal/estimated_histogram.hh>
#include <seastar/core/timer.hh>

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace mkmq {

class MercuryShard {
public:
    using RpcHandler = std::function<seastar::future<std::vector<std::uint8_t>>(
        std::vector<std::uint8_t>)>;

    MercuryShard();
    ~MercuryShard();

    seastar::future<> start(std::string address, bool listen);
    seastar::future<> stop();
    bool running() const noexcept;

    void register_rpc(std::string name, RpcHandler handler);
    seastar::future<std::vector<std::uint8_t>> forward(
        std::string peer,
        std::string rpc,
        std::vector<std::uint8_t> payload);
    int handle_mercury_rpc(void* handle);
    std::uint64_t rpc_forwards() const noexcept;
    std::uint64_t rpc_receives() const noexcept;
    std::uint64_t rpc_errors() const noexcept;
    std::uint64_t rpc_timeouts() const noexcept;
    std::uint64_t bulk_transfers() const noexcept;
    std::uint64_t bulk_bytes() const noexcept;
    std::uint64_t bulk_errors() const noexcept;
    seastar::metrics::internal::time_estimated_histogram rpc_forward_latency() const;
    seastar::metrics::internal::time_estimated_histogram rpc_handler_latency() const;
    seastar::metrics::internal::time_estimated_histogram bulk_latency() const;

private:
    friend void* mercury_client_cache_raw(MercuryShard* shard) noexcept;

    void arm_progress_timer();
    void progress_once();
    void register_mercury_rpc(const std::string& name);

    bool running_{false};
    std::string address_;
    seastar::timer<> progress_timer_;
    std::unordered_map<std::string, RpcHandler> handlers_;
    std::unordered_map<std::string, std::uint64_t> rpc_ids_;
    std::unordered_map<std::uint64_t, std::string> rpc_names_;
    std::uint64_t rpc_forwards_{0};
    std::uint64_t rpc_receives_{0};
    std::uint64_t rpc_errors_{0};
    std::uint64_t rpc_timeouts_{0};
    std::uint64_t bulk_transfers_{0};
    std::uint64_t bulk_bytes_{0};
    std::uint64_t bulk_errors_{0};
    seastar::metrics::internal::time_estimated_histogram rpc_forward_latency_;
    seastar::metrics::internal::time_estimated_histogram rpc_handler_latency_;
    seastar::metrics::internal::time_estimated_histogram bulk_latency_;
    void* hg_class_{nullptr};
    void* hg_context_{nullptr};
    void* client_cache_{nullptr};
};

}  // namespace mkmq

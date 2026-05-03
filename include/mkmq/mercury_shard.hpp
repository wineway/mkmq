#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/internal/estimated_histogram.hh>

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace mkmq {

class MercuryShard {
public:
    using RpcHandler = std::function<seastar::future<std::vector<std::uint8_t>>(
        std::vector<std::uint8_t>)>;

    MercuryShard();
    ~MercuryShard();

    MercuryShard(const MercuryShard&) = delete;
    MercuryShard& operator=(const MercuryShard&) = delete;
    MercuryShard(MercuryShard&&) = delete;
    MercuryShard& operator=(MercuryShard&&) = delete;

    seastar::future<> start(std::string address, bool listen);
    seastar::future<> stop();
    bool running() const noexcept;

    // Hot-path APIs take string_view so callers with pre-owned std::string or
    // string-literal RPC names never incur a copy across the call boundary.
    void register_rpc(std::string_view name, RpcHandler handler);
    seastar::future<std::vector<std::uint8_t>> forward(
        std::string_view peer,
        std::string_view rpc,
        std::vector<std::uint8_t> payload);

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
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace mkmq

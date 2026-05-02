#include "mkmq/broker_shard.hpp"
#include "mkmq/config.hpp"

#include <seastar/core/app-template.hh>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/prometheus.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/defer.hh>

#include <boost/program_options.hpp>

#include <csignal>
#include <iostream>
#include <optional>
#include <stdexcept>

namespace bpo = boost::program_options;

namespace {

class StopSignal {
public:
    enum class Event {
        Stop,
        Reload,
    };

    StopSignal() {
        seastar::handle_signal(SIGINT, [this] {
            signaled(Event::Stop);
        });
        seastar::handle_signal(SIGTERM, [this] {
            signaled(Event::Stop);
        });
        seastar::handle_signal(SIGHUP, [this] {
            signaled(Event::Reload);
        });
    }

    ~StopSignal() {
        seastar::handle_signal(SIGINT, [] {});
        seastar::handle_signal(SIGTERM, [] {});
        seastar::handle_signal(SIGHUP, [] {});
    }

    seastar::future<Event> wait() {
        return cond_.wait([this] {
            return pending_.has_value();
        }).then([this] {
            auto event = *pending_;
            pending_.reset();
            return event;
        });
    }

private:
    void signaled(Event event) {
        pending_ = event;
        cond_.broadcast();
    }

    std::optional<Event> pending_;
    seastar::condition_variable cond_;
};

}  // namespace

int main(int argc, char** argv) {
    seastar::app_template::config app_config;
    app_config.name = "mkmq_broker";
    app_config.auto_handle_sigint_sigterm = false;
    seastar::app_template app(app_config);
    app.add_options()
        ("config", bpo::value<std::string>(), "Required YAML broker configuration")
        ("prometheus-prefix", bpo::value<std::string>()->default_value("mkmq"), "Prometheus metric prefix");

    return app.run(argc, argv, [&app] {
        const auto& args = app.configuration();
        if (!args.count("config")) {
            return seastar::make_exception_future<>(
                std::runtime_error("--config is required"));
        }

        auto config_path = args["config"].as<std::string>();
        auto config = mkmq::load_config_file(config_path);
        auto prefix = args["prometheus-prefix"].as<std::string>();

        return seastar::async([config_path = std::move(config_path), config = std::move(config), prefix = std::move(prefix)]() mutable {
            StopSignal stop_signal;
            seastar::sharded<mkmq::BrokerShard> shards;
            seastar::httpd::http_server_control prometheus_server;
            bool shards_started = false;
            bool prometheus_started = false;

            auto cleanup = seastar::defer([&] {
                if (shards_started) {
                    shards.stop().get();
                }
                if (prometheus_started) {
                    prometheus_server.stop().get();
                }
            });

            const auto broker = mkmq::find_broker(config, config.node_id);
            if (!broker.has_value()) {
                throw std::runtime_error("configured node_id is missing from brokers");
            }

            shards.start().get();
            shards_started = true;
            shards.invoke_on_all([config](mkmq::BrokerShard& shard) {
                return shard.start(config);
            }).get();
            shards.invoke_on_all([&shards](mkmq::BrokerShard& shard) {
                shard.set_peers(&shards);
            }).get();

            prometheus_server.start("mkmq-prometheus").get();
            prometheus_started = true;
            seastar::prometheus::config prometheus_config;
            prometheus_config.prefix = prefix;
            seastar::prometheus::start(prometheus_server, prometheus_config).get();
            prometheus_server.listen(
                seastar::socket_address{seastar::net::inet_address("0.0.0.0"), config.metrics_port}).get();

            std::cout << "mkmq broker " << config.node_id
                      << " kafka=" << broker->kafka_host << ':' << broker->kafka_port
                      << " metrics=:" << config.metrics_port
                      << " generation=" << config.cluster_generation
                      << std::endl;

            bool stopping = false;
            while (!stopping) {
                switch (stop_signal.wait().get()) {
                    case StopSignal::Event::Stop:
                        stopping = true;
                        break;
                    case StopSignal::Event::Reload: {
                        auto updated = mkmq::load_config_file(config_path);
                        shards.invoke_on_all([updated](mkmq::BrokerShard& shard) {
                            return shard.apply_config(updated);
                        }).get();
                        config = std::move(updated);
                        std::cout << "mkmq reloaded config generation="
                                  << config.cluster_generation << std::endl;
                        break;
                    }
                }
            }
        });
    });
}

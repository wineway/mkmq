# mercury-kafka-mq

C++23 Seastar/Mercury Kafka-compatible message queue prototype for fixed-version
HFT experiments.

The broker uses Seastar's share-nothing TCP stack for Kafka clients. Broker
internode plumbing is Mercury-only; the first transport target is OFI
`tcp_rxm`. The hot path has no blocking sockets, no thread-per-connection
model, and no mutex-backed partition state.

## Build

Debug functional build on the test host:

```sh
cmake -S . -B build-debug \
  -DSeastar_DIR=/seastar/build/debug \
  -DMERCURY_ROOT=/opt/mercury-9c79250 \
  -DMKMQ_ENABLE_MERCURY=ON \
  -DMKMQ_ENABLE_INTEGRATION_TESTS=ON \
  -DCMAKE_BUILD_TYPE=Debug
cmake --build build-debug -j
ctest --test-dir build-debug --output-on-failure
```

Set `MKMQ_ENABLE_MERCURY=OFF` only for local protocol/storage work without a
Mercury install. The test host baseline uses Mercury commit `9c79250` installed
at `/opt/mercury-9c79250`, built with `NA_USE_OFI=ON` for `ofi+tcp;ofi_rxm`.

Release benchmark build:

```sh
cmake -S . -B build-release \
  -DSeastar_DIR=/seastar/build/release \
  -DMKMQ_ENABLE_MERCURY=ON \
  -DMERCURY_ROOT=/path/to/mercury/install \
  -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j
```

`CRC32C` hardware support is mandatory. On x86_64 the CMake check requires
SSE4.2 and adds `-msse4.2`.

## Run

`--config` is required:

```sh
./build-debug/mkmq_broker --config configs/single-broker.yaml --smp 1 --memory 1G --overprovisioned
```

The broker validates static topology at startup. Broker endpoints must be
explicit, and each partition must explicitly set `shard`, `leader`, `replicas`,
`isr`, `durability`, `queue_limit`, `max_frame_bytes`, `fetch_visibility`, and
`allow_follower_fetch`.

Prometheus metrics are served on `metrics_port` from the YAML config at
`/metrics`. Exported broker metrics include Kafka request/error counters,
Produce/Fetch request latency histograms, queue depth, disk write/fsync
counters and histograms, Mercury RPC and bulk-transfer counters and latency
histograms, broker health checks, follower catch-up counters, replica lag,
degraded partitions, and replica error counts.

## Smoke And Benchmark Client

The repository includes a small process-isolated Kafka-frame client:

```sh
./build-debug/mkmq_kafka_bench --host 127.0.0.1 --port 9092 --topic orders --smoke
./build-debug/mkmq_kafka_bench --host 127.0.0.1 --port 9092 --topic orders \
  --messages 10000 --payload-size 128 --acks 1
```

It exercises `ApiVersions v3`, `Metadata v9`, `Produce v9`, `Fetch v12`, and
`ListOffsets v6`. Those fixed-version wire structs and codecs are generated
under `include/mkmq/generated` and `src/generated` by
`tools/generate_kafka_schemas.py`. Benchmark output reports produce round-trip
`p50`, `p99`, and max latency in microseconds.

## SIGHUP Reload

The broker reloads `--config` on `SIGHUP`. Reloads may add topics/partitions and
change leader/read-policy/durability/limit fields. They reject broker endpoint
changes, partition deletion, shard migration, and replica/ISR changes.

```sh
tests/integration_reload.sh build-debug
tests/integration_rf3_degraded.sh build-debug configs/three-broker-rf3.yaml
tests/integration_fetch_wait.sh build-debug configs/single-broker.yaml
tests/integration_fetch_visibility.sh build-debug
tests/integration_unsupported_produce.sh build-debug
tests/integration_acks_zero.sh build-debug
tests/integration_no_auto_create.sh build-debug
tests/integration_segment_rollover.sh build-debug
tests/integration_rf3_mercury.sh build-debug
tests/integration_rf3_catchup.sh build-debug
tests/integration_produce_timeout.sh build-debug
```

## Kafka Scope

Only one flexible version is accepted for each API:

- `ApiVersions v3`
- `Metadata v9`
- `Produce v9`
- `Fetch v12`
- `ListOffsets v6`

For librdkafka/kcat compatibility, the `ApiVersions` response advertises the
minimum Produce/Fetch versions needed for feature negotiation while dispatch
still accepts only the fixed versions above.

Topics are never auto-created. Missing topics and partitions return
`UnknownTopicOrPartition`.

## Runtime Semantics

- `acks=0`: enqueue/write locally and do not return a Kafka response.
- `acks=1`: return after the configured local durability (`write` or `fsync`).
- `acks=-1/all`: waits for configured ISR followers through Mercury
  `replica_append`; large replication payloads move through Mercury bulk pull,
  while unreachable or failing followers fail fast with `NotEnoughReplicas`.
- Produce and fetch fail fast on queue/full-frame limits and unsupported
  leadership/read policy.
- RF>1 `acks=1` schedules asynchronous ISR replication. Remote follower
  replication failure marks the partition degraded and increments replica error
  metrics.
- Followers periodically use Mercury `replica_fetch` to catch up from their
  configured static leader after restart or a missed async replication window.
- Segment writes also append a DMA-written `.index` file with base/last offset
  and segment-position metadata; restart still scans segments to rebuild the
  in-memory fetch index.

## Repository Layout

- `include/mkmq`, `src`: Seastar broker shards, protocol, config, Mercury shard.
- `include/mkmq/generated`, `src/generated`: checked-in fixed Kafka schema
  manifest and generated codec for local Kafka commit `faa8b4870f`.
- `tools/generate_kafka_schemas.py`: generator for schema manifests and fixed
  Kafka codecs.
- `configs`: sample static cluster configs.

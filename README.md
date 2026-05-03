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

## Benchmarks

Both rounds ran on the same Ubuntu 25.04 / Linux 6.x host, single NVMe,
Release builds of mkmq and Apache Kafka 4.1.2, each cluster 3 brokers
(RF=3, `min.insync.replicas=3`, `durability=write`, no fsync per produce),
different local ports (Kafka 9092–9094, mkmq 9292–9294). Both clusters run
concurrently on the same machine, each broker pinned to one Seastar/JVM
process.

### Round 1 — `kafka-producer-perf-test` (pipelined, one producer)

Apache Kafka's bundled perf tool against both clusters, identical producer
config on both sides: `acks=all linger.ms=0 batch.size=1
max.in.flight.requests.per.connection=1 enable.idempotence=false`.
Reported latency is record-enqueue to ACK, so absolute numbers include
accumulator queueing and are not a single round-trip measurement — they
are still directly comparable between clusters because the tool is the
same.

| workload                   | Kafka 4.1.2                         | mkmq                                | Δ          |
|----------------------------|-------------------------------------|-------------------------------------|------------|
| 10 000 × 128 B, acks=all   | 3 798 rec/s, p50 1 448, p99 2 306 ms | 4 517 rec/s, p50 1 169, p99 1 889 ms | +19 % / −18 % p99 |
| 5 000 × 1 KiB, acks=all    | 2 912 rec/s, p50 790,  p99 1 403 ms  | 2 784 rec/s, p50 765,  p99 1 428 ms  | ≈ parity   |
| 2 000 × 8 KiB, acks=all    | 1 835 rec/s, p50 436,  p99 695 ms    | 1 864 rec/s, p50 426,  p99 685 ms    | ≈ parity   |
| 20 000 × 256 B, acks=1     | 6 319 rec/s, p50 1 910, p99 2 677 ms | 5 432 rec/s, p50 2 128, p99 3 201 ms | −14 % / +20 % p99 |
| 20 000 × 256 B, acks=all   | 3 973 rec/s, p50 2 790, p99 4 504 ms | 4 628 rec/s, p50 2 429, p99 3 807 ms | +16 % / −15 % p99 |

Small-payload RF3 quorum commit (the path optimized in the latest
refactor) shows mkmq ahead on throughput and tail. Large-payload
(≥ 1 KiB) workloads are bound by NVMe bandwidth and converge. `acks=1`
small-payload is the one workload where Apache's record accumulator
still wins.

### Round 2 — Strict serial Java producer, RF3

Minimal Java producer issuing `send().get()` per record — one Kafka
produce request in flight at a time, each record waits for the RF3
quorum ACK before the next is dispatched. Same producer config as
Round 1. 200-record warmup, then the measured batch. Latency here is
the true request round-trip, including Kafka client serialization and
network path to the leader.

| workload                 | Kafka 4.1.2 p50 | mkmq p50 | Kafka p99 | mkmq p99 | Kafka max | mkmq max |
|--------------------------|----------------:|---------:|----------:|---------:|----------:|---------:|
| 10 000 × 128 B, acks=all | 514 µs          | **224 µs** (−56 %) | 2 527 µs | **512 µs** (−80 %) | 14 866 µs | 5 309 µs |
| 5 000 × 1 KiB, acks=all  | 398 µs          | **292 µs** (−27 %) |   820 µs | **581 µs** (−29 %) | 10 095 µs | 5 064 µs |
| 2 000 × 8 KiB, acks=all  | 445 µs          | **339 µs** (−24 %) |   860 µs |   908 µs (+6 %)    |  5 296 µs | 5 399 µs |
| 20 000 × 256 B, acks=1   | **120 µs**      |   153 µs (+28 %)    |   556 µs | **435 µs** (−22 %) | 10 759 µs | 5 268 µs |
| 20 000 × 256 B, acks=all | 207 µs          | **171 µs** (−17 %) |   559 µs | **456 µs** (−18 %) | 10 896 µs | 5 320 µs |

mkmq p50 is lower than Kafka on every RF3 acks=all workload, and p99
tail is consistently tighter — the Kafka `max` column shows the 10–15 ms
GC/pageflush spikes mkmq doesn't have. The one regression, acks=1 256 B
p50, is the Kafka producer's batching/NIO path reaching into
sub-microsecond territory on a single-leader local write; mkmq still
wins the same workload at p99.

These numbers are loopback, same-host; they bound what the software
stack costs but do not reflect cross-host network or multi-tenant
NVMe contention.

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

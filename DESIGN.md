# Seastar + Mercury HFT MQ Design

## Architecture

`BrokerShard` is the ownership boundary. Each Seastar shard owns its Kafka
connections, configured local partitions, log/index state, metrics, and one
`MercuryShard`. Cross-shard partition access uses `seastar::sharded`
invocation; no shared mutex store remains.

`MercuryShard` initializes one Mercury context per Seastar shard. Progress is
driven by a Seastar timer calling `HG_Trigger` and zero-timeout `HG_Progress`;
there is no independent Mercury progress thread. Small RPC payloads are encoded
eagerly. Payloads at or above 64KiB are advertised with a Mercury bulk handle,
and the receiver pulls the bytes into shard-local memory before dispatching the
registered handler.

`PartitionLogShard` keeps an in-memory offset index and writes framed Kafka
record batches to aligned segment files with Seastar DMA I/O. Startup scans
existing segment frames to rebuild the in-memory index. Segment size defaults
to `1GiB`; the active segment rolls to a new base-offset file when it reaches
the configured size. Each append also records a fixed-size DMA-written
`.index` entry with base offset, last offset, segment base, segment position,
and record byte size.

## Kafka Protocol

The first release intentionally accepts one flexible version per API:

- `ApiVersions v3`
- `Metadata v9`
- `Produce v9`
- `Fetch v12`
- `ListOffsets v6`

The generated schema manifest and fixed-version codec are checked in under
`include/mkmq/generated` and `src/generated`; they record Kafka source commit
`faa8b4870f` and are rendered by `tools/generate_kafka_schemas.py`. Runtime
dispatch converts generated wire structs into the broker's internal partition
request/result types, so schema-aware encode/decode stays out of the hot
dispatch code. `ApiVersions` advertises broader Produce/Fetch minimums where
librdkafka requires them for feature negotiation, but dispatch still rejects
anything other than the fixed versions listed above.

Unsupported APIs and versions fail before dispatch. Missing topics/partitions
return `UnknownTopicOrPartition`; topic auto-creation is not implemented.
Fetch requests that do not currently have records wait up to the smaller of the
client `max_wait_ms` and the broker `fetch_max_wait_ms_cap`.

## Configuration

`--config` is mandatory. YAML must include:

- `cluster_generation`
- `brokers`
- Kafka and Mercury addresses
- explicit topics and partitions
- per-partition `shard`, `leader`, `replicas`, `isr`, `durability`,
  `queue_limit`, `max_frame_bytes`, `fetch_visibility`,
  `allow_follower_fetch`

SIGHUP reload is implemented for adding topics/partitions and for changing
leader/read-policy/durability/limit fields. Reload rejects broker endpoint
changes, partition deletion, shard migration, migration plan changes, and
replica/ISR changes. Partition YAML accepts a reserved `migration` object with
`generation`, `target_shard`, and `target_leader`; it is parsed and validated
but not executed.

## Durability And Backpressure

Record batches are rewritten with broker-assigned base offsets and CRC32C before
append. CRC32C uses the mandatory hardware/SSE4.2 backend selected by CMake.

`acks=1` returns after local `write` or `fsync`, depending on partition config,
then schedules ISR replication asynchronously through Mercury `replica_append`.
Failed follower replication marks the partition degraded and records replica
error metrics. `acks=0` writes without a response. `acks=-1` waits for
configured ISR replication and fails fast with `NotEnoughReplicas` when an ISR
follower is unreachable or rejects the append.

Each broker also registers `health_ping` and `replica_fetch` Mercury RPCs.
Followers periodically ping peers and fetch records from their configured
static leader starting at the local log end. Returned records are appended
through the same replica append path, which lets restarted followers catch up
without shrinking ISR automatically.

Queue and frame limits are enforced per partition. The broker fails fast rather
than parking Kafka requests indefinitely. Produce waits are capped by the
smaller of the client timeout and broker `produce_wait_ms_cap`; when that cap
expires the response is `RequestTimedOut`, while any already-issued local or
Mercury operation is allowed to finish under the broker gate before shutdown.

## Observability

Seastar metrics are exported through Prometheus `/metrics`. Current counters
cover Kafka requests/errors, produce/fetch errors, replica append/error counts,
degraded partitions, aggregate replica lag, per-shard queue depth, local disk
write/fsync counts, Kafka request latency, Produce/Fetch latency, disk
write/fsync latency histograms, Mercury RPC forward/receive/error/timeout
counters, Mercury bulk transfer/byte/error counters, and Mercury RPC/bulk
latency histograms, broker health-check counters, and follower catch-up
counters.

## FIX
1. standard library file api
2. mercury 
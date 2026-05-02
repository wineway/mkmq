#!/usr/bin/env bash
set -euo pipefail

build_dir=${1:-build-debug}
broker_bin="$build_dir/mkmq_broker"
client_bin="$build_dir/mkmq_kafka_bench"
tmp=${TMPDIR:-/tmp}/mkmq-produce-timeout
config="$tmp/produce-timeout.yaml"
log="$tmp/broker.log"

wait_for_port() {
  for _ in $(seq 1 50); do
    python3 - <<'PY' >/dev/null 2>&1 && return 0
import socket
s = socket.socket()
s.settimeout(0.1)
s.connect(("127.0.0.1", 9392))
s.close()
PY
    sleep 0.1
  done
  return 1
}

rm -rf "$tmp" /tmp/mkmq-produce-timeout-data
mkdir -p "$tmp"
cat >"$config" <<'YAML'
node_id: 0
cluster_generation: 1
data_dir: /tmp/mkmq-produce-timeout-data
metrics_port: 9942
fetch_max_wait_ms_cap: 100
produce_wait_ms_cap: 10
segment_bytes: 1073741824
brokers:
  - id: 0
    kafka_host: 127.0.0.1
    kafka_port: 9392
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3644
  - id: 1
    kafka_host: 127.0.0.1
    kafka_port: 9393
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3645
topics:
  - name: orders
    partitions:
      - id: 0
        shard: 0
        leader: 0
        replicas: [0, 1]
        isr: [0, 1]
        durability: write
        queue_limit: 8192
        max_frame_bytes: 1048576
        fetch_visibility: high_watermark
        allow_follower_fetch: false
YAML

"$broker_bin" --config "$config" --smp 1 --memory 1G --overprovisioned >"$log" 2>&1 &
broker=$!
trap 'kill -TERM "$broker" 2>/dev/null || true; wait "$broker" 2>/dev/null || true' EXIT

wait_for_port
if "$client_bin" --host 127.0.0.1 --port 9392 --topic orders --acks -1 --smoke >"$tmp/client.log" 2>&1; then
  cat "$tmp/client.log"
  exit 1
fi
grep -q "Kafka error 7" "$tmp/client.log"
sleep 0.2
curl -fsS http://127.0.0.1:9942/metrics >"$tmp/metrics.log"
grep -q "mkmq_mkmq_produce_errors.* [1-9]" "$tmp/metrics.log"
grep -q "mkmq_mkmq_mercury_rpc_timeouts.* [1-9]" "$tmp/metrics.log"

#!/usr/bin/env bash
set -euo pipefail

build_dir=${1:-build-debug}
broker_bin="$build_dir/mkmq_broker"
client_bin="$build_dir/mkmq_kafka_bench"
tmp=${TMPDIR:-/tmp}/mkmq-no-auto-create
config="$tmp/single.yaml"
log="$tmp/broker.log"

wait_for_port() {
  for _ in $(seq 1 80); do
    python3 - <<'PY' >/dev/null 2>&1 && return 0
import socket
s = socket.socket()
s.settimeout(0.1)
s.connect(("127.0.0.1", 9892))
s.close()
PY
    sleep 0.1
  done
  return 1
}

rm -rf "$tmp" /tmp/mkmq-no-auto-create-data
mkdir -p "$tmp"
cat >"$config" <<'YAML'
node_id: 0
cluster_generation: 1
data_dir: /tmp/mkmq-no-auto-create-data
metrics_port: 9992
fetch_max_wait_ms_cap: 100
produce_wait_ms_cap: 100
segment_bytes: 1073741824
brokers:
  - id: 0
    kafka_host: 127.0.0.1
    kafka_port: 9892
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:4244
topics:
  - name: orders
    partitions:
      - id: 0
        shard: 0
        leader: 0
        replicas: [0]
        isr: [0]
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
"$client_bin" --host 127.0.0.1 --port 9892 --topic missing --metadata-only --expect-metadata-error 3
"$client_bin" --host 127.0.0.1 --port 9892 --topic missing --acks 1 --smoke \
  --expect-metadata-error 3 \
  --expect-produce-error 3
"$client_bin" --host 127.0.0.1 --port 9892 --topic missing --list-offsets-only --expect-list-offsets-error 3
"$client_bin" --host 127.0.0.1 --port 9892 --topic missing --metadata-only --expect-metadata-error 3
"$client_bin" --host 127.0.0.1 --port 9892 --topic orders --acks 1 --smoke

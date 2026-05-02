#!/usr/bin/env bash
set -euo pipefail

build_dir=${1:-build-debug}
broker_bin="$build_dir/mkmq_broker"
client_bin="$build_dir/mkmq_kafka_bench"
tmp=${TMPDIR:-/tmp}/mkmq-segment-rollover
config="$tmp/single-small-segments.yaml"
log="$tmp/broker.log"

wait_for_port() {
  for _ in $(seq 1 50); do
    python3 - <<'PY' >/dev/null 2>&1 && return 0
import socket
s = socket.socket()
s.settimeout(0.1)
s.connect(("127.0.0.1", 9292))
s.close()
PY
    sleep 0.1
  done
  return 1
}

start_broker() {
  "$broker_bin" --config "$config" --smp 1 --memory 1G --overprovisioned >"$log" 2>&1 &
  broker=$!
  wait_for_port
}

stop_broker() {
  if [ "${broker:-0}" -gt 0 ]; then
    kill -TERM "$broker" 2>/dev/null || true
    wait "$broker" 2>/dev/null || true
    broker=0
  fi
}

rm -rf "$tmp" /tmp/mkmq-segment-rollover-data
mkdir -p "$tmp"
cat >"$config" <<'YAML'
node_id: 0
cluster_generation: 1
data_dir: /tmp/mkmq-segment-rollover-data
metrics_port: 9842
fetch_max_wait_ms_cap: 100
produce_wait_ms_cap: 100
segment_bytes: 4096
brokers:
  - id: 0
    kafka_host: 127.0.0.1
    kafka_port: 9292
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3544
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

broker=0
trap 'stop_broker' EXIT

start_broker
"$client_bin" --host 127.0.0.1 --port 9292 --topic orders --messages 4 --payload-size 128 --acks 1
stop_broker

segments=$(find /tmp/mkmq-segment-rollover-data -name 'orders-0-*.segment' | wc -l)
test "$segments" -ge 2
index_file=/tmp/mkmq-segment-rollover-data/shard-0/orders-0.index
test -s "$index_file"
index_size=$(wc -c <"$index_file")
test "$index_size" -ge 4096

rm -f "$index_file"
start_broker
"$client_bin" --host 127.0.0.1 --port 9292 --topic orders --fetch-only \
  --expect-fetch-min-bytes 1 \
  --expect-fetch-max-bytes 1048576
stop_broker
test -s "$index_file"

start_broker
"$client_bin" --host 127.0.0.1 --port 9292 --topic orders --acks 1 --smoke

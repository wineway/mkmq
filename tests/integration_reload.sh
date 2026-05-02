#!/usr/bin/env bash
set -euo pipefail

build_dir=${1:-build-debug}
broker_bin="$build_dir/mkmq_broker"
client_bin="$build_dir/mkmq_kafka_bench"
config=${TMPDIR:-/tmp}/mkmq-reload-test.yaml
data_dir=${TMPDIR:-/tmp}/mkmq-reload-test-data
log=${TMPDIR:-/tmp}/mkmq-reload-test.log

write_config() {
  local generation=$1
  local include_trades=$2
  cat >"$config" <<YAML
node_id: 0
cluster_generation: $generation
data_dir: $data_dir
metrics_port: 9642
fetch_max_wait_ms_cap: 100
produce_wait_ms_cap: 100
segment_bytes: 1073741824
brokers:
  - id: 0
    kafka_host: 127.0.0.1
    kafka_port: 9092
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3344
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
  if [[ "$include_trades" == "yes" ]]; then
    cat >>"$config" <<YAML
  - name: trades
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
  fi
}

wait_for_port() {
  for _ in $(seq 1 50); do
    python3 - <<'PY' >/dev/null 2>&1 && return 0
import socket
s = socket.socket()
s.settimeout(0.1)
s.connect(("127.0.0.1", 9092))
s.close()
PY
    sleep 0.1
  done
  return 1
}

rm -rf "$data_dir"
write_config 1 no
"$broker_bin" --config "$config" --smp 1 --memory 1G --overprovisioned >"$log" 2>&1 &
broker=$!
trap 'kill -TERM "$broker" 2>/dev/null || true; wait "$broker" 2>/dev/null || true' EXIT

wait_for_port
"$client_bin" --host 127.0.0.1 --port 9092 --topic orders --acks 1 --smoke

write_config 2 yes
kill -HUP "$broker"
sleep 0.5
"$client_bin" --host 127.0.0.1 --port 9092 --topic trades --acks 1 --smoke
grep -q "reloaded config generation=2" "$log"

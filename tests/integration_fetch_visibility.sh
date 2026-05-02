#!/usr/bin/env bash
set -euo pipefail

build_dir=${1:-build-debug}
broker_bin="$build_dir/mkmq_broker"
client_bin="$build_dir/mkmq_kafka_bench"
tmp=${TMPDIR:-/tmp}/mkmq-fetch-visibility
log_dir="$tmp/logs"

wait_for_port() {
  local port=$1
  for _ in $(seq 1 80); do
    python3 - "$port" <<'PY' >/dev/null 2>&1 && return 0
import socket
import sys
port = int(sys.argv[1])
s = socket.socket()
s.settimeout(0.1)
s.connect(("127.0.0.1", port))
s.close()
PY
    sleep 0.1
  done
  return 1
}

write_config() {
  local visibility=$1
  local path=$2
  local data_dir=$3
  cat >"$path" <<YAML
node_id: 0
cluster_generation: 1
data_dir: ${data_dir}
metrics_port: 9952
fetch_max_wait_ms_cap: 100
produce_wait_ms_cap: 100
segment_bytes: 1073741824
brokers:
  - id: 0
    kafka_host: 127.0.0.1
    kafka_port: 9492
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3844
  - id: 1
    kafka_host: 127.0.0.1
    kafka_port: 9493
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3845
  - id: 2
    kafka_host: 127.0.0.1
    kafka_port: 9494
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3846
topics:
  - name: orders
    partitions:
      - id: 0
        shard: 0
        leader: 0
        replicas: [0, 1, 2]
        isr: [0, 1, 2]
        durability: write
        queue_limit: 8192
        max_frame_bytes: 1048576
        fetch_visibility: ${visibility}
        allow_follower_fetch: false
YAML
}

write_cluster_config() {
  local node=$1
  local allow_follower_fetch=$2
  local path=$3
  local data_dir="/tmp/mkmq-fetch-visibility-follower-${allow_follower_fetch}-${node}"
  cat >"$path" <<YAML
node_id: ${node}
cluster_generation: 1
data_dir: ${data_dir}
metrics_port: $((9962 + node))
fetch_max_wait_ms_cap: 100
produce_wait_ms_cap: 100
segment_bytes: 1073741824
brokers:
  - id: 0
    kafka_host: 127.0.0.1
    kafka_port: 9592
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3944
  - id: 1
    kafka_host: 127.0.0.1
    kafka_port: 9593
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3945
  - id: 2
    kafka_host: 127.0.0.1
    kafka_port: 9594
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3946
topics:
  - name: orders
    partitions:
      - id: 0
        shard: 0
        leader: 0
        replicas: [0, 1, 2]
        isr: [0, 1, 2]
        durability: write
        queue_limit: 8192
        max_frame_bytes: 1048576
        fetch_visibility: high_watermark
        allow_follower_fetch: ${allow_follower_fetch}
YAML
}

run_case() {
  local visibility=$1
  local min_bytes=$2
  local max_bytes=$3
  local config="$tmp/${visibility}.yaml"
  local data_dir="/tmp/mkmq-fetch-visibility-${visibility}"
  local log="$log_dir/${visibility}.broker.log"
  local fetch_log="$log_dir/${visibility}.fetch.log"

  rm -rf "$data_dir"
  write_config "$visibility" "$config" "$data_dir"
  "$broker_bin" --config "$config" --smp 1 --memory 1G --overprovisioned >"$log" 2>&1 &
  broker=$!
  trap 'kill -TERM "$broker" 2>/dev/null || true; wait "$broker" 2>/dev/null || true' RETURN

  wait_for_port 9492
  "$client_bin" --host 127.0.0.1 --port 9492 --topic orders --acks 1 --smoke >"$log_dir/${visibility}.produce.log"
  "$client_bin" --host 127.0.0.1 --port 9492 --topic orders --fetch-only \
    --expect-fetch-min-bytes "$min_bytes" \
    --expect-fetch-max-bytes "$max_bytes" >"$fetch_log"
  cat "$fetch_log"

  kill -TERM "$broker" 2>/dev/null || true
  wait "$broker" 2>/dev/null || true
  trap - RETURN
}

run_follower_case() {
  local allow_follower_fetch=$1
  local expect_error=$2
  local label="follower-${allow_follower_fetch}"

  for node in 0 1 2; do
    rm -rf "/tmp/mkmq-fetch-visibility-follower-${allow_follower_fetch}-${node}"
    write_cluster_config "$node" "$allow_follower_fetch" "$tmp/${label}-${node}.yaml"
  done

  "$broker_bin" --config "$tmp/${label}-0.yaml" --smp 1 --memory 1G --overprovisioned >"$log_dir/${label}-0.log" 2>&1 &
  broker0=$!
  "$broker_bin" --config "$tmp/${label}-1.yaml" --smp 1 --memory 1G --overprovisioned >"$log_dir/${label}-1.log" 2>&1 &
  broker1=$!
  "$broker_bin" --config "$tmp/${label}-2.yaml" --smp 1 --memory 1G --overprovisioned >"$log_dir/${label}-2.log" 2>&1 &
  broker2=$!
  trap 'kill -TERM "$broker0" "$broker1" "$broker2" 2>/dev/null || true; wait "$broker0" "$broker1" "$broker2" 2>/dev/null || true' RETURN

  wait_for_port 9592
  wait_for_port 9593
  wait_for_port 9594
  "$client_bin" --host 127.0.0.1 --port 9592 --topic orders --acks -1 --smoke >"$log_dir/${label}.produce.log"

  if [[ "$expect_error" == "0" ]]; then
    "$client_bin" --host 127.0.0.1 --port 9593 --topic orders --fetch-only \
      --expect-fetch-min-bytes 1 \
      --expect-fetch-max-bytes 1048576 >"$log_dir/${label}.fetch.log"
  else
    "$client_bin" --host 127.0.0.1 --port 9593 --topic orders --fetch-only \
      --expect-fetch-error "$expect_error" >"$log_dir/${label}.fetch.log"
  fi
  cat "$log_dir/${label}.fetch.log"

  kill -TERM "$broker0" "$broker1" "$broker2" 2>/dev/null || true
  wait "$broker0" "$broker1" "$broker2" 2>/dev/null || true
  trap - RETURN
}

rm -rf "$tmp"
mkdir -p "$log_dir"

run_case high_watermark 0 0
run_case leader_local_end 1 1048576
run_follower_case false 6
run_follower_case true 0

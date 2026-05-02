#!/usr/bin/env bash
set -euo pipefail

build_dir=${1:-build-debug}
broker_bin="$build_dir/mkmq_broker"
client_bin="$build_dir/mkmq_kafka_bench"
tmp=${TMPDIR:-/tmp}/mkmq-rf3-mercury
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
  local node=$1
  local kafka_port=$2
  local metrics_port=$3
  local mercury_port=$4
  local path="$tmp/broker-${node}.yaml"
  cat >"$path" <<YAML
node_id: ${node}
cluster_generation: 1
data_dir: /tmp/mkmq-rf3-mercury-${node}
metrics_port: ${metrics_port}
fetch_max_wait_ms_cap: 100
produce_wait_ms_cap: 100
segment_bytes: 1073741824
brokers:
  - id: 0
    kafka_host: 127.0.0.1
    kafka_port: 9192
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3444
  - id: 1
    kafka_host: 127.0.0.1
    kafka_port: 9193
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3445
  - id: 2
    kafka_host: 127.0.0.1
    kafka_port: 9194
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3446
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
        allow_follower_fetch: false
YAML
  echo "$path"
}

rm -rf "$tmp" /tmp/mkmq-rf3-mercury-0 /tmp/mkmq-rf3-mercury-1 /tmp/mkmq-rf3-mercury-2
mkdir -p "$log_dir"

config0=$(write_config 0 9192 9742 3444)
config1=$(write_config 1 9193 9743 3445)
config2=$(write_config 2 9194 9744 3446)

"$broker_bin" --config "$config0" --smp 1 --memory 1G --overprovisioned >"$log_dir/broker-0.log" 2>&1 &
broker0=$!
"$broker_bin" --config "$config1" --smp 1 --memory 1G --overprovisioned >"$log_dir/broker-1.log" 2>&1 &
broker1=$!
"$broker_bin" --config "$config2" --smp 1 --memory 1G --overprovisioned >"$log_dir/broker-2.log" 2>&1 &
broker2=$!
trap 'kill -TERM "$broker0" "$broker1" "$broker2" 2>/dev/null || true; wait "$broker0" "$broker1" "$broker2" 2>/dev/null || true' EXIT

wait_for_port 9192
wait_for_port 9193
wait_for_port 9194

"$client_bin" --host 127.0.0.1 --port 9192 --topic orders --acks -1 --smoke >"$log_dir/client.log"
cat "$log_dir/client.log"
"$client_bin" --host 127.0.0.1 --port 9192 --topic orders --acks -1 --messages 2 --payload-size 131072 >"$log_dir/client-bulk.log"
cat "$log_dir/client-bulk.log"

sleep 0.3
curl -fsS http://127.0.0.1:9742/metrics >"$log_dir/leader.metrics"
curl -fsS http://127.0.0.1:9743/metrics >"$log_dir/follower-1.metrics"
curl -fsS http://127.0.0.1:9744/metrics >"$log_dir/follower-2.metrics"
grep -q "mkmq_mkmq_degraded_partitions.* 0" "$log_dir/leader.metrics"
grep -q "mkmq_mkmq_replica_errors.* 0" "$log_dir/leader.metrics"
grep -q "mkmq_mkmq_mercury_rpc_forwards.* [1-9]" "$log_dir/leader.metrics"
grep -q "mkmq_mkmq_mercury_rpc_forward_latency" "$log_dir/leader.metrics"
grep -q "mkmq_mkmq_replica_appends.* [1-9]" "$log_dir/follower-1.metrics"
grep -q "mkmq_mkmq_replica_appends.* [1-9]" "$log_dir/follower-2.metrics"
grep -q "mkmq_mkmq_mercury_rpc_receives.* [1-9]" "$log_dir/follower-1.metrics"
grep -q "mkmq_mkmq_mercury_rpc_handler_latency" "$log_dir/follower-1.metrics"
grep -q "mkmq_mkmq_mercury_bulk_transfers.* [1-9]" "$log_dir/follower-1.metrics"
grep -q "mkmq_mkmq_mercury_bulk_bytes.* [1-9]" "$log_dir/follower-1.metrics"
grep -q "mkmq_mkmq_mercury_bulk_latency" "$log_dir/follower-1.metrics"

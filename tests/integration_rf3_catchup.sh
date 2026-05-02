#!/usr/bin/env bash
set -euo pipefail

build_dir=${1:-build-debug}
broker_bin="$build_dir/mkmq_broker"
client_bin="$build_dir/mkmq_kafka_bench"
tmp=${TMPDIR:-/tmp}/mkmq-rf3-catchup
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
  local path="$tmp/broker-${node}.yaml"
  cat >"$path" <<YAML
node_id: ${node}
cluster_generation: 1
data_dir: /tmp/mkmq-rf3-catchup-${node}
metrics_port: $((9852 + node))
fetch_max_wait_ms_cap: 100
produce_wait_ms_cap: 100
segment_bytes: 1073741824
brokers:
  - id: 0
    kafka_host: 127.0.0.1
    kafka_port: 9392
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3744
  - id: 1
    kafka_host: 127.0.0.1
    kafka_port: 9393
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3745
  - id: 2
    kafka_host: 127.0.0.1
    kafka_port: 9394
    mercury_address: ofi+tcp;ofi_rxm://127.0.0.1:3746
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

rm -rf "$tmp" /tmp/mkmq-rf3-catchup-0 /tmp/mkmq-rf3-catchup-1 /tmp/mkmq-rf3-catchup-2
mkdir -p "$log_dir"

config0=$(write_config 0)
config2=$(write_config 2)

"$broker_bin" --config "$config0" --smp 1 --memory 1G --overprovisioned >"$log_dir/broker-0.log" 2>&1 &
broker0=$!
broker2=0
trap 'kill -TERM "$broker0" "$broker2" 2>/dev/null || true; wait "$broker0" "$broker2" 2>/dev/null || true' EXIT

wait_for_port 9392
"$client_bin" --host 127.0.0.1 --port 9392 --topic orders --acks 1 --messages 5 --payload-size 128 >"$log_dir/client.log"
cat "$log_dir/client.log"

"$broker_bin" --config "$config2" --smp 1 --memory 1G --overprovisioned >"$log_dir/broker-2.log" 2>&1 &
broker2=$!
wait_for_port 9394

for _ in $(seq 1 30); do
  curl -fsS http://127.0.0.1:9854/metrics >"$log_dir/follower-2.metrics"
  if grep -q "mkmq_mkmq_replica_catchup_records.* [1-9]" "$log_dir/follower-2.metrics"; then
    break
  fi
  sleep 0.2
done

grep -q "mkmq_mkmq_replica_catchup_records.* [1-9]" "$log_dir/follower-2.metrics"
grep -q "mkmq_mkmq_replica_appends.* [1-9]" "$log_dir/follower-2.metrics"
grep -q "mkmq_mkmq_broker_health_checks.* [1-9]" "$log_dir/follower-2.metrics"

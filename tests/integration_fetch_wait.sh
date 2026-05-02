#!/usr/bin/env bash
set -euo pipefail

build_dir=${1:-build-debug}
broker_bin="$build_dir/mkmq_broker"
client_bin="$build_dir/mkmq_kafka_bench"
config=${2:-configs/single-broker.yaml}
log=${TMPDIR:-/tmp}/mkmq-fetch-wait.log
client_log=${TMPDIR:-/tmp}/mkmq-fetch-wait-client.log
metrics_log=${TMPDIR:-/tmp}/mkmq-fetch-wait-metrics.log

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

rm -rf /tmp/mkmq-single
"$broker_bin" --config "$config" --smp 1 --memory 1G --overprovisioned >"$log" 2>&1 &
broker=$!
trap 'kill -TERM "$broker" 2>/dev/null || true; wait "$broker" 2>/dev/null || true' EXIT

wait_for_port
"$client_bin" --host 127.0.0.1 --port 9092 --topic orders --acks 1 --smoke --fetch-empty-wait-ms 500 >"$client_log"
cat "$client_log"
awk -F= '/fetch_empty_wait_us=/ { if ($2 >= 70000 && $2 <= 300000) found=1 } END { exit found ? 0 : 1 }' "$client_log"
curl -fsS http://127.0.0.1:9642/metrics >"$metrics_log"
grep -q "mkmq_mkmq_kafka_request_latency" "$metrics_log"
grep -q "mkmq_mkmq_produce_latency" "$metrics_log"
grep -q "mkmq_mkmq_fetch_latency" "$metrics_log"
grep -q "mkmq_mkmq_disk_write_latency" "$metrics_log"
grep -q "mkmq_mkmq_disk_writes.* [1-9]" "$metrics_log"

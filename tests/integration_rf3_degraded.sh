#!/usr/bin/env bash
set -euo pipefail

build_dir=${1:-build-debug}
broker_bin="$build_dir/mkmq_broker"
client_bin="$build_dir/mkmq_kafka_bench"
config=${2:-configs/three-broker-rf3.yaml}
log=${TMPDIR:-/tmp}/mkmq-rf3-degraded.log

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

rm -rf /tmp/mkmq-broker-0
"$broker_bin" --config "$config" --smp 1 --memory 1G --overprovisioned >"$log" 2>&1 &
broker=$!
trap 'kill -TERM "$broker" 2>/dev/null || true; wait "$broker" 2>/dev/null || true' EXIT

wait_for_port

if "$client_bin" --host 127.0.0.1 --port 9092 --topic orders --acks -1 --smoke >"${log}.acks_all" 2>&1; then
  cat "${log}.acks_all"
  exit 1
fi
grep -Eq "Kafka error (7|19)" "${log}.acks_all"

"$client_bin" --host 127.0.0.1 --port 9092 --topic orders --acks 1 --smoke
sleep 0.3
curl -fsS http://127.0.0.1:9642/metrics >"${log}.metrics"
grep -q "mkmq_mkmq_degraded_partitions.* 1" "${log}.metrics"
grep -q "mkmq_mkmq_replica_errors.* [1-9]" "${log}.metrics"

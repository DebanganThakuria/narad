#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
RUN_DIR="${NARAD_LOCAL_SOAK_DIR:-$ROOT_DIR/tmp/local-soak}"
BIN="$RUN_DIR/narad"
LOG_DIR="$RUN_DIR/logs"
PID_FILE="$RUN_DIR/narad.pids"
GO_BIN="${GO:-go}"
DEFAULT_GO_CACHE="${TMP_BASE}/narad-go-cache"

HTTP_PORTS=(18081 18082 18083)
CLUSTER_PORTS=(19081 19082 19083)
PPROF_PORTS=(6061 6062 6063)
NODE_IDS=(narad-1 narad-2 narad-3)
PIDS=()

RETENTION_MS="${NARAD_SOAK_RETENTION_MS:-86400000}"
VISIBILITY_TIMEOUT_MS="${NARAD_SOAK_VISIBILITY_TIMEOUT_MS:-30000}"
MAX_IN_FLIGHT="${NARAD_SOAK_MAX_IN_FLIGHT_PER_PARTITION:-1024}"
MAX_ACKED_AHEAD="${NARAD_SOAK_MAX_ACKED_AHEAD_PER_PARTITION:-1024}"

wait_ready() {
  local port="$1"
  local url="http://127.0.0.1:${port}/readyz"
  for _ in {1..120}; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "node on port ${port} did not become ready" >&2
  return 1
}

pid_alive() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

stop_started() {
  for pid in "${PIDS[@]:-}"; do
    if pid_alive "$pid"; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}

supervise_foreground() {
  trap 'stop_started; exit 0' INT TERM
  echo "foreground supervisor enabled; press Ctrl-C to stop the local soak cluster"
  while true; do
    for pid in "${PIDS[@]:-}"; do
      if ! pid_alive "$pid"; then
        echo "narad pid $pid exited; stopping remaining nodes" >&2
        stop_started
        exit 1
      fi
    done
    sleep 5
  done
}

if [[ -f "$PID_FILE" ]]; then
  while read -r pid; do
    if pid_alive "$pid"; then
      echo "local soak cluster already appears to be running; pid file: $PID_FILE" >&2
      exit 2
    fi
  done <"$PID_FILE"
fi

mkdir -p "$LOG_DIR"

echo "building narad into $BIN"
(
  cd "$ROOT_DIR"
  GOCACHE="${GOCACHE:-$DEFAULT_GO_CACHE}" "$GO_BIN" build -o "$BIN" ./cmd/narad
)

PEERS="narad-1@127.0.0.1:19081,narad-2@127.0.0.1:19082,narad-3@127.0.0.1:19083"

trap 'stop_started' ERR
for i in 0 1 2; do
  node="${NODE_IDS[$i]}"
  http_port="${HTTP_PORTS[$i]}"
  cluster_port="${CLUSTER_PORTS[$i]}"
  pprof_port="${PPROF_PORTS[$i]}"
  data_dir="$RUN_DIR/data/$node"
  log_file="$LOG_DIR/$node.log"

  echo "starting $node http=127.0.0.1:$http_port cluster=127.0.0.1:$cluster_port pprof=127.0.0.1:$pprof_port data=$data_dir"
  NARAD_HTTP_ADDR="127.0.0.1:${http_port}" \
  NARAD_HTTP_PPROF_ADDR="127.0.0.1:${pprof_port}" \
  NARAD_CLUSTER_ADDR="127.0.0.1:${cluster_port}" \
  NARAD_NODE_ID="$node" \
  NARAD_CLUSTER_PEERS="$PEERS" \
  NARAD_DATA_DIR="$data_dir" \
  NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS="$RETENTION_MS" \
  NARAD_TOPIC_DEFAULT_VISIBILITY_TIMEOUT_MS="$VISIBILITY_TIMEOUT_MS" \
  NARAD_TOPIC_DEFAULT_MAX_IN_FLIGHT_PER_PARTITION="$MAX_IN_FLIGHT" \
  NARAD_TOPIC_DEFAULT_MAX_ACKED_AHEAD_PER_PARTITION="$MAX_ACKED_AHEAD" \
  NARAD_HTTP_MAX_CONSUME_WAIT="${NARAD_HTTP_MAX_CONSUME_WAIT:-30s}" \
  NARAD_LOG_FORMAT="${NARAD_LOG_FORMAT:-text}" \
  NARAD_LOG_LEVEL="${NARAD_LOG_LEVEL:-warn}" \
    nohup "$BIN" serve >"$log_file" 2>&1 &
  PIDS+=("$!")
done
trap - ERR

printf "%s\n" "${PIDS[@]}" >"$PID_FILE"

for port in "${HTTP_PORTS[@]}"; do
  wait_ready "$port"
done

cat <<EOF
local soak cluster is ready
nodes: http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083
pprof: http://127.0.0.1:6061/debug/pprof/ http://127.0.0.1:6062/debug/pprof/ http://127.0.0.1:6063/debug/pprof/
data:  $RUN_DIR/data
logs:  $LOG_DIR
pids:  $PID_FILE
stop:  ./scripts/local-soak-stop.sh
EOF

if [[ "${NARAD_SOAK_FOREGROUND:-0}" == "1" ]]; then
  supervise_foreground
fi

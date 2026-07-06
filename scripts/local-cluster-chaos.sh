#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
TMP_DIR="${NARAD_LOCAL_CLUSTER_DIR:-$(mktemp -d "${TMP_BASE}/narad-local-chaos.XXXXXX")}"
BIN="$TMP_DIR/narad"
LOG_DIR="$TMP_DIR/logs"
PID_DIR="$TMP_DIR/pids"
GO_BIN="${GO:-go}"
DEFAULT_GO_CACHE="${TMP_BASE}/narad-go-cache"

HTTP_PORTS=(18281 18282 18283)
CLUSTER_PORTS=(19281 19282 19283)
NODE_IDS=(narad-1 narad-2 narad-3)
RESTARTER_PID=""

cleanup() {
  local status=$?
  if [[ -n "${RESTARTER_PID:-}" ]] && kill -0 "$RESTARTER_PID" 2>/dev/null; then
    kill "$RESTARTER_PID" 2>/dev/null || true
    wait "$RESTARTER_PID" 2>/dev/null || true
  fi
  for i in 0 1 2; do
    stop_node "$i" >/dev/null 2>&1 || true
  done

  if [[ "$status" -eq 0 && "${NARAD_KEEP_CLUSTER_ARTIFACTS:-0}" != "1" ]]; then
    rm -rf "$TMP_DIR"
  else
    echo "cluster artifacts kept at: $TMP_DIR" >&2
    echo "node logs: $LOG_DIR" >&2
  fi
}
trap cleanup EXIT

node_url() {
  local i="$1"
  echo "http://127.0.0.1:${HTTP_PORTS[$i]}"
}

nodes_csv() {
  echo "$(node_url 0),$(node_url 1),$(node_url 2)"
}

pid_file() {
  local i="$1"
  echo "$PID_DIR/${NODE_IDS[$i]}.pid"
}

wait_ready() {
  local i="$1"
  local url
  url="$(node_url "$i")/readyz"
  for _ in {1..80}; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "${NODE_IDS[$i]} did not become ready at $url" >&2
  return 1
}

start_node() {
  local i="$1"
  local node="${NODE_IDS[$i]}"
  local http_port="${HTTP_PORTS[$i]}"
  local cluster_port="${CLUSTER_PORTS[$i]}"
  local data_dir="$TMP_DIR/$node"
  local log_file="$LOG_DIR/$node.log"

  echo "starting $node http=127.0.0.1:$http_port cluster=127.0.0.1:$cluster_port"
  NARAD_HTTP_ADDR="127.0.0.1:${http_port}" \
  NARAD_CLUSTER_ADDR="127.0.0.1:${cluster_port}" \
  NARAD_NODE_ID="$node" \
  NARAD_CLUSTER_PEERS="$PEERS" \
  NARAD_DATA_DIR="$data_dir" \
  NARAD_SECURITY_ENABLED="${NARAD_SECURITY_ENABLED:-false}" \
  NARAD_LOG_FORMAT="${NARAD_LOG_FORMAT:-text}" \
  NARAD_LOG_LEVEL="${NARAD_LOG_LEVEL:-info}" \
    "$BIN" serve >>"$log_file" 2>&1 &
  echo "$!" >"$(pid_file "$i")"
}

stop_node() {
  local i="$1"
  local file pid
  file="$(pid_file "$i")"
  if [[ ! -f "$file" ]]; then
    return 0
  fi
  pid="$(cat "$file")"
  if [[ -z "$pid" ]]; then
    rm -f "$file"
    return 0
  fi
  if kill -0 "$pid" 2>/dev/null; then
    echo "stopping ${NODE_IDS[$i]} pid=$pid"
    kill "$pid" 2>/dev/null || true
    for _ in {1..40}; do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.25
    done
    if kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  fi
  rm -f "$file"
}

chaos_restarts() {
  sleep "${NARAD_CHAOS_INITIAL_DELAY_SECONDS:-2}"
  while true; do
    local node_idx down_for pause_for
    node_idx=$((RANDOM % 3))
    down_for="${NARAD_CHAOS_DOWN_SECONDS:-1}"
    pause_for=$((1 + RANDOM % 3))
    stop_node "$node_idx"
    sleep "$down_for"
    start_node "$node_idx"
    wait_ready "$node_idx"
    sleep "$pause_for"
  done
}

mkdir -p "$LOG_DIR" "$PID_DIR"

echo "building narad into $BIN"
(
  cd "$ROOT_DIR"
  GOCACHE="${GOCACHE:-$DEFAULT_GO_CACHE}" "$GO_BIN" build -o "$BIN" ./cmd/narad
)

PEERS="narad-1@127.0.0.1:19281,narad-2@127.0.0.1:19282,narad-3@127.0.0.1:19283"

for i in 0 1 2; do
  start_node "$i"
done
for i in 0 1 2; do
  wait_ready "$i"
done
echo "all nodes ready"

chaos_restarts &
RESTARTER_PID="$!"

(
  cd "$ROOT_DIR"
  GOCACHE="${GOCACHE:-$DEFAULT_GO_CACHE}" "$GO_BIN" run ./tests/integration \
    --mode chaos \
    --nodes "$(nodes_csv)" \
    --run-id "lcc-$(date +%s%N)" \
    --topics 3 \
    --messages 240 \
    --partitions 6 \
    --produce-concurrency 12 \
    --consume-concurrency 12 \
    --visibility-timeout 3s \
    --timeout 2m \
    --cleanup=false \
    "$@"
)

echo "PASS local cluster chaos"

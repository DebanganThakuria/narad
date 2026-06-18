#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
TMP_DIR="${NARAD_LOCAL_CLUSTER_DIR:-$(mktemp -d "${TMP_BASE}/narad-local-recovery.XXXXXX")}"
BIN="$TMP_DIR/narad"
LOG_DIR="$TMP_DIR/logs"
PLAN_DIR="$TMP_DIR/plans"
GO_BIN="${GO:-go}"
DEFAULT_GO_CACHE="${TMP_BASE}/narad-go-cache"

HTTP_PORTS=(18181 18182 18183)
CLUSTER_PORTS=(19181 19182 19183)
NODE_IDS=(narad-1 narad-2 narad-3)
PIDS=("" "" "")

cleanup() {
  local status=$?
  for i in "${!PIDS[@]}"; do
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
  NARAD_LOG_FORMAT="${NARAD_LOG_FORMAT:-text}" \
  NARAD_LOG_LEVEL="${NARAD_LOG_LEVEL:-info}" \
    "$BIN" serve >>"$log_file" 2>&1 &
  PIDS[$i]="$!"
}

stop_node() {
  local i="$1"
  local pid="${PIDS[$i]:-}"
  if [[ -z "$pid" ]]; then
    return 0
  fi
  if kill -0 "$pid" 2>/dev/null; then
    echo "stopping ${NODE_IDS[$i]} pid=$pid"
    kill "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
  PIDS[$i]=""
}

partition_dir() {
  local node_idx="$1"
  local topic="$2"
  local partition="$3"
  printf "%s/%s/topics/%s/p%05d" "$TMP_DIR" "${NODE_IDS[$node_idx]}" "$topic" "$partition"
}

wipe_owner_segments_keep_hwm() {
  local node_idx="$1"
  local topic="$2"
  local partition="$3"
  local dir
  dir="$(partition_dir "$node_idx" "$topic" "$partition")"
  echo "wiping owner segment files only: $dir"
  if [[ -d "$dir" ]]; then
    find "$dir" -type f -name '*.log' -delete
  fi
}

wipe_partition_dir() {
  local node_idx="$1"
  local topic="$2"
  local partition="$3"
  local dir
  dir="$(partition_dir "$node_idx" "$topic" "$partition")"
  echo "wiping partition dir: $dir"
  rm -rf "$dir"
}

json_field() {
  local file="$1"
  local field="$2"
  python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$file" "$field"
}

run_driver() {
  local mode="$1"
  local plan="$2"
  local run_id="$3"
  (
    cd "$ROOT_DIR"
    GOCACHE="${GOCACHE:-$DEFAULT_GO_CACHE}" "$GO_BIN" run ./tests/integration \
      --mode "$mode" \
      --nodes "$(nodes_csv)" \
      --plan "$plan" \
      --run-id "$run_id" \
      --topics 1 \
      --messages 1 \
      --partitions 3 \
      --replication-factor 2 \
      --timeout 90s \
      --cleanup=false
  )
}

mkdir -p "$LOG_DIR" "$PLAN_DIR"

echo "building narad into $BIN"
(
  cd "$ROOT_DIR"
  GOCACHE="${GOCACHE:-$DEFAULT_GO_CACHE}" "$GO_BIN" build -o "$BIN" ./cmd/narad
)

PEERS="narad-1@127.0.0.1:19181,narad-2@127.0.0.1:19182,narad-3@127.0.0.1:19183"

for i in 0 1 2; do
  start_node "$i"
done
for i in 0 1 2; do
  wait_ready "$i"
done
echo "all nodes ready"

RUN_ID="lcr-$(date +%s%N)"

OWNER_PLAN="$PLAN_DIR/owner-repair.json"
echo "preparing owner-from-follower recovery scenario"
run_driver prepare-owner-repair "$OWNER_PLAN" "$RUN_ID"
owner_idx="$(json_field "$OWNER_PLAN" owner_index)"
owner_topic="$(json_field "$OWNER_PLAN" topic)"
owner_partition="$(json_field "$OWNER_PLAN" partition)"

stop_node "$owner_idx"
wipe_owner_segments_keep_hwm "$owner_idx" "$owner_topic" "$owner_partition"
start_node "$owner_idx"
wait_ready "$owner_idx"
run_driver verify-owner-repair "$OWNER_PLAN" "$RUN_ID"
echo "owner-from-follower recovery passed"

FOLLOWER_PLAN="$PLAN_DIR/follower-repair.json"
echo "preparing follower-from-owner recovery scenario"
run_driver prepare-follower-repair "$FOLLOWER_PLAN" "$RUN_ID"
follower_idx="$(json_field "$FOLLOWER_PLAN" follower_index)"
follower_topic="$(json_field "$FOLLOWER_PLAN" topic)"
follower_partition="$(json_field "$FOLLOWER_PLAN" partition)"
repair_owner_idx="$(json_field "$FOLLOWER_PLAN" owner_index)"

stop_node "$follower_idx"
wipe_partition_dir "$follower_idx" "$follower_topic" "$follower_partition"
start_node "$follower_idx"
wait_ready "$follower_idx"

# StoreRecovery repairs follower replicas from the owner side. Restart the
# owner after the follower is back so the owner-side startup repair runs.
sleep 2
stop_node "$repair_owner_idx"
start_node "$repair_owner_idx"
wait_ready "$repair_owner_idx"
run_driver verify-follower-repair "$FOLLOWER_PLAN" "$RUN_ID"
echo "follower-from-owner recovery passed"

echo "PASS local cluster recovery"

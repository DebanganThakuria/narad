#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
TMP_DIR="${NARAD_LOCAL_CLUSTER_DIR:-$(mktemp -d "${TMP_BASE}/narad-local-cluster.XXXXXX")}"
BIN="$TMP_DIR/narad"
LOG_DIR="$TMP_DIR/logs"
GO_BIN="${GO:-go}"
DEFAULT_GO_CACHE="${TMP_BASE}/narad-go-cache"

HTTP_PORTS=(18081 18082 18083)
CLUSTER_PORTS=(19081 19082 19083)
NODE_IDS=(narad-1 narad-2 narad-3)
PIDS=()

# Run the secured path by default so this harness exercises RBAC +
# cluster-secret auth under load. Override NARAD_SECURITY_ENABLED=false
# to run without auth.
SECURITY_ENABLED="${NARAD_SECURITY_ENABLED:-true}"
CLUSTER_SECRET="${NARAD_CLUSTER_SECRET:-local-cluster-secret}"
ADMIN_PASSWORD="${NARAD_ADMIN_PASSWORD:-local-admin-password}"

cleanup() {
  local status=$?
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  for pid in "${PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done

  if [[ "$status" -eq 0 && "${NARAD_KEEP_CLUSTER_ARTIFACTS:-0}" != "1" ]]; then
    rm -rf "$TMP_DIR"
  else
    echo "cluster artifacts kept at: $TMP_DIR" >&2
    echo "node logs: $LOG_DIR" >&2
  fi
}
trap cleanup EXIT

wait_ready() {
  local port="$1"
  local url="http://127.0.0.1:${port}/readyz"
  for _ in {1..80}; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "node on port ${port} did not become ready" >&2
  return 1
}

# wait_admin_auth blocks until the seeded root admin can authenticate on
# the first node (admin-only /v1/users returns 200).
wait_admin_auth() {
  local url="http://127.0.0.1:${HTTP_PORTS[0]}/v1/users"
  for _ in {1..80}; do
    if curl -fsS -u "admin:${ADMIN_PASSWORD}" "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "root admin was not seeded / could not authenticate" >&2
  return 1
}

mkdir -p "$LOG_DIR"

echo "building narad into $BIN"
(
  cd "$ROOT_DIR"
  GOCACHE="${GOCACHE:-$DEFAULT_GO_CACHE}" "$GO_BIN" build -o "$BIN" ./cmd/narad
)

PEERS="narad-1@127.0.0.1:19081,narad-2@127.0.0.1:19082,narad-3@127.0.0.1:19083"

for i in 0 1 2; do
  node="${NODE_IDS[$i]}"
  http_port="${HTTP_PORTS[$i]}"
  cluster_port="${CLUSTER_PORTS[$i]}"
  data_dir="$TMP_DIR/$node"
  log_file="$LOG_DIR/$node.log"

  echo "starting $node http=127.0.0.1:$http_port cluster=127.0.0.1:$cluster_port"
  NARAD_HTTP_ADDR="127.0.0.1:${http_port}" \
  NARAD_CLUSTER_ADDR="127.0.0.1:${cluster_port}" \
  NARAD_NODE_ID="$node" \
  NARAD_CLUSTER_PEERS="$PEERS" \
  NARAD_DATA_DIR="$data_dir" \
  NARAD_SECURITY_ENABLED="$SECURITY_ENABLED" \
  NARAD_CLUSTER_SECRET="$CLUSTER_SECRET" \
  NARAD_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
  NARAD_LOG_FORMAT="${NARAD_LOG_FORMAT:-text}" \
  NARAD_LOG_LEVEL="${NARAD_LOG_LEVEL:-info}" \
    "$BIN" serve >"$log_file" 2>&1 &
  PIDS+=("$!")
done

for port in "${HTTP_PORTS[@]}"; do
  wait_ready "$port"
done

DRIVER_AUTH=()
if [[ "$SECURITY_ENABLED" == "true" ]]; then
  # The root admin is seeded asynchronously right after leader election;
  # wait until it can authenticate before driving load.
  wait_admin_auth
  DRIVER_AUTH=(--username admin --password "$ADMIN_PASSWORD")
fi

echo "all nodes ready"
(
  cd "$ROOT_DIR"
  "$GO_BIN" run ./tests/integration \
    --nodes "http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083" \
    "${DRIVER_AUTH[@]}" \
    "$@"
)

#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "launchd soak mode is only supported on macOS" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_BASE="${TMPDIR:-/tmp}"
TMP_BASE="${TMP_BASE%/}"
RUN_DIR="${NARAD_LOCAL_SOAK_DIR:-$ROOT_DIR/tmp/local-soak}"
BIN="$RUN_DIR/narad"
TESTER_BIN="$RUN_DIR/narad-tester"
LOG_DIR="$RUN_DIR/logs"
LAUNCHD_DIR="$RUN_DIR/launchd"
GO_BIN="${GO:-go}"
DEFAULT_GO_CACHE="${TMP_BASE}/narad-go-cache"
DOMAIN="gui/$(id -u)"

HTTP_PORTS=(18081 18082 18083)
CLUSTER_PORTS=(19081 19082 19083)
PPROF_PORTS=(6061 6062 6063)
NODE_IDS=(narad-1 narad-2 narad-3)

RETENTION_MS="${NARAD_SOAK_RETENTION_MS:-86400000}"
VISIBILITY_TIMEOUT_MS="${NARAD_SOAK_VISIBILITY_TIMEOUT_MS:-30000}"
MAX_IN_FLIGHT="${NARAD_SOAK_MAX_IN_FLIGHT_PER_PARTITION:-1024}"
MAX_ACKED_AHEAD="${NARAD_SOAK_MAX_ACKED_AHEAD_PER_PARTITION:-1024}"

NODES="${NARAD_NODES:-http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083}"
TESTER_RUN_ID="${NARAD_TESTER_RUN_ID:-month-$(date -u +%Y%m%dT%H%M%SZ)}"
TESTER_METRICS_ADDR="${NARAD_TESTER_METRICS_ADDR:-127.0.0.1:9095}"

plist_escape() {
  local value="$1"
  value="${value//&/&amp;}"
  value="${value//</&lt;}"
  value="${value//>/&gt;}"
  value="${value//\"/&quot;}"
  printf "%s" "$value"
}

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

wait_tester_metrics() {
  local url="http://${TESTER_METRICS_ADDR}/metrics"
  local attempts="${NARAD_TESTER_METRICS_READY_ATTEMPTS:-480}"
  for ((i = 0; i < attempts; i++)); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  echo "tester metrics endpoint ${url} did not become ready" >&2
  return 1
}

write_node_plist() {
  local idx="$1"
  local node="${NODE_IDS[$idx]}"
  local http_port="${HTTP_PORTS[$idx]}"
  local cluster_port="${CLUSTER_PORTS[$idx]}"
  local pprof_port="${PPROF_PORTS[$idx]}"
  local label="com.narad.local-soak.${node}"
  local plist="$LAUNCHD_DIR/${label}.plist"
  local data_dir="$RUN_DIR/data/$node"
  local log_file="$LOG_DIR/$node.log"
  local err_file="$LOG_DIR/$node.err.log"

  cat >"$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>$(plist_escape "$BIN")</string>
    <string>serve</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$(plist_escape "$ROOT_DIR")</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>NARAD_HTTP_ADDR</key>
    <string>127.0.0.1:${http_port}</string>
    <key>NARAD_HTTP_PPROF_ADDR</key>
    <string>127.0.0.1:${pprof_port}</string>
    <key>NARAD_CLUSTER_ADDR</key>
    <string>127.0.0.1:${cluster_port}</string>
    <key>NARAD_NODE_ID</key>
    <string>$node</string>
    <key>NARAD_CLUSTER_PEERS</key>
    <string>narad-1@127.0.0.1:19081,narad-2@127.0.0.1:19082,narad-3@127.0.0.1:19083</string>
    <key>NARAD_DATA_DIR</key>
    <string>$(plist_escape "$data_dir")</string>
    <key>NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS</key>
    <string>$RETENTION_MS</string>
    <key>NARAD_TOPIC_DEFAULT_VISIBILITY_TIMEOUT_MS</key>
    <string>$VISIBILITY_TIMEOUT_MS</string>
    <key>NARAD_TOPIC_DEFAULT_MAX_IN_FLIGHT_PER_PARTITION</key>
    <string>$MAX_IN_FLIGHT</string>
    <key>NARAD_TOPIC_DEFAULT_MAX_ACKED_AHEAD_PER_PARTITION</key>
    <string>$MAX_ACKED_AHEAD</string>
    <key>NARAD_HTTP_MAX_CONSUME_WAIT</key>
    <string>${NARAD_HTTP_MAX_CONSUME_WAIT:-30s}</string>
    <key>NARAD_LOG_FORMAT</key>
    <string>${NARAD_LOG_FORMAT:-text}</string>
    <key>NARAD_LOG_LEVEL</key>
    <string>${NARAD_LOG_LEVEL:-warn}</string>
  </dict>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>$(plist_escape "$log_file")</string>
  <key>StandardErrorPath</key>
  <string>$(plist_escape "$err_file")</string>
</dict>
</plist>
EOF
}

write_tester_plist() {
  local label="com.narad.local-soak.tester"
  local plist="$LAUNCHD_DIR/${label}.plist"
  local log_file="$LOG_DIR/narad-tester.log"
  local err_file="$LOG_DIR/narad-tester.err.log"

  cat >"$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>$(plist_escape "$TESTER_BIN")</string>
    <string>--nodes</string>
    <string>$(plist_escape "$NODES")</string>
    <string>--topics</string>
    <string>${NARAD_TESTER_TOPICS:-10}</string>
    <string>--topic-prefix</string>
    <string>${NARAD_TESTER_TOPIC_PREFIX:-narad-soak}</string>
    <string>--run-id</string>
    <string>$(plist_escape "$TESTER_RUN_ID")</string>
    <string>--messages-per-second</string>
    <string>${NARAD_TESTER_MESSAGES_PER_SECOND:-50}</string>
    <string>--max-messages-per-second</string>
    <string>${NARAD_TESTER_MAX_MESSAGES_PER_SECOND:-100000}</string>
    <string>--rate-ramp-step</string>
    <string>${NARAD_TESTER_RATE_RAMP_STEP:-10}</string>
    <string>--rate-ramp-interval</string>
    <string>${NARAD_TESTER_RATE_RAMP_INTERVAL:-10m}</string>
    <string>--dispatch-interval</string>
    <string>${NARAD_TESTER_DISPATCH_INTERVAL:-1ms}</string>
    <string>--producer-concurrency</string>
    <string>${NARAD_TESTER_PRODUCER_CONCURRENCY:-16}</string>
    <string>--consumer-concurrency</string>
    <string>${NARAD_TESTER_CONSUMER_CONCURRENCY:-32}</string>
    <string>--payload-bytes</string>
    <string>${NARAD_TESTER_PAYLOAD_BYTES:-512}</string>
    <string>--partitions</string>
    <string>${NARAD_TESTER_PARTITIONS:-12}</string>
    <string>--replication-factor</string>
    <string>${NARAD_TESTER_REPLICATION_FACTOR:-2}</string>
    <string>--max-in-flight-per-partition</string>
    <string>${NARAD_TESTER_MAX_IN_FLIGHT_PER_PARTITION:-1024}</string>
    <string>--max-acked-ahead-per-partition</string>
    <string>${NARAD_TESTER_MAX_ACKED_AHEAD_PER_PARTITION:-1024}</string>
    <string>--retention</string>
    <string>${NARAD_TESTER_RETENTION:-24h}</string>
    <string>--visibility-timeout</string>
    <string>${NARAD_TESTER_VISIBILITY_TIMEOUT:-30s}</string>
    <string>--consume-wait</string>
    <string>${NARAD_TESTER_CONSUME_WAIT:-20ms}</string>
    <string>--metrics-addr</string>
    <string>$(plist_escape "$TESTER_METRICS_ADDR")</string>
    <string>--missing-after</string>
    <string>${NARAD_TESTER_MISSING_AFTER:-5m}</string>
    <string>--max-outstanding-messages</string>
    <string>${NARAD_TESTER_MAX_OUTSTANDING_MESSAGES:-1000000}</string>
    <string>--ledger-scan-interval</string>
    <string>${NARAD_TESTER_LEDGER_SCAN_INTERVAL:-10s}</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$(plist_escape "$ROOT_DIR")</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>$(plist_escape "$log_file")</string>
  <key>StandardErrorPath</key>
  <string>$(plist_escape "$err_file")</string>
</dict>
</plist>
EOF
}

bootstrap_plist() {
  local plist="$1"
  launchctl bootstrap "$DOMAIN" "$plist"
}

bootout_plist() {
  local plist="$1"
  if [[ -f "$plist" ]]; then
    launchctl bootout "$DOMAIN" "$plist" >/dev/null 2>&1 || true
  fi
}

mkdir -p "$LOG_DIR" "$LAUNCHD_DIR"

echo "building narad into $BIN"
(
  cd "$ROOT_DIR"
  GOCACHE="${GOCACHE:-$DEFAULT_GO_CACHE}" "$GO_BIN" build -o "$BIN" ./cmd/narad
  GOCACHE="${GOCACHE:-$DEFAULT_GO_CACHE}" "$GO_BIN" build -o "$TESTER_BIN" ./cmd/narad-tester
)

for i in 0 1 2; do
  write_node_plist "$i"
done
write_tester_plist

bootout_plist "$LAUNCHD_DIR/com.narad.local-soak.tester.plist"
for i in 0 1 2; do
  bootout_plist "$LAUNCHD_DIR/com.narad.local-soak.${NODE_IDS[$i]}.plist"
done

for i in 0 1 2; do
  bootstrap_plist "$LAUNCHD_DIR/com.narad.local-soak.${NODE_IDS[$i]}.plist"
done

for port in "${HTTP_PORTS[@]}"; do
  wait_ready "$port"
done
bootstrap_plist "$LAUNCHD_DIR/com.narad.local-soak.tester.plist"
wait_tester_metrics

cat <<EOF
local launchd soak is running
nodes:       $NODES
tester:      http://${TESTER_METRICS_ADDR}/metrics
pprof:       http://127.0.0.1:6061/debug/pprof/ http://127.0.0.1:6062/debug/pprof/ http://127.0.0.1:6063/debug/pprof/
rate:        ${NARAD_TESTER_MESSAGES_PER_SECOND:-50} msg/sec +/-${NARAD_TESTER_RATE_RAMP_STEP:-10} every ${NARAD_TESTER_RATE_RAMP_INTERVAL:-10m}, cap ${NARAD_TESTER_MAX_MESSAGES_PER_SECOND:-100000}, dispatch ${NARAD_TESTER_DISPATCH_INTERVAL:-1ms}
topic caps:  max_in_flight=${NARAD_TESTER_MAX_IN_FLIGHT_PER_PARTITION:-1024} max_acked_ahead=${NARAD_TESTER_MAX_ACKED_AHEAD_PER_PARTITION:-1024}
consume:     concurrency=${NARAD_TESTER_CONSUMER_CONCURRENCY:-32} wait=${NARAD_TESTER_CONSUME_WAIT:-20ms}
run_id:      $TESTER_RUN_ID
ledger:      in-memory, exact consumed sequence tracking, max outstanding ${NARAD_TESTER_MAX_OUTSTANDING_MESSAGES:-1000000}
logs:        $LOG_DIR
launchd:     $LAUNCHD_DIR
stop:        ./scripts/local-soak-launchd-stop.sh
EOF

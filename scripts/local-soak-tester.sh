#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GO_BIN="${GO:-go}"

NODES="${NARAD_NODES:-http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083}"
RUN_ID="${NARAD_TESTER_RUN_ID:-}"

args=(
  "--nodes" "$NODES"
  "--topics" "${NARAD_TESTER_TOPICS:-10}"
  "--topic-prefix" "${NARAD_TESTER_TOPIC_PREFIX:-narad-soak}"
  "--messages-per-second" "${NARAD_TESTER_MESSAGES_PER_SECOND:-50}"
  "--max-messages-per-second" "${NARAD_TESTER_MAX_MESSAGES_PER_SECOND:-100000}"
  "--rate-ramp-step" "${NARAD_TESTER_RATE_RAMP_STEP:-10}"
  "--rate-ramp-interval" "${NARAD_TESTER_RATE_RAMP_INTERVAL:-10m}"
  "--dispatch-interval" "${NARAD_TESTER_DISPATCH_INTERVAL:-1ms}"
  "--producer-concurrency" "${NARAD_TESTER_PRODUCER_CONCURRENCY:-16}"
  "--consumer-concurrency" "${NARAD_TESTER_CONSUMER_CONCURRENCY:-32}"
  "--payload-bytes" "${NARAD_TESTER_PAYLOAD_BYTES:-512}"
  "--partitions" "${NARAD_TESTER_PARTITIONS:-12}"
  "--max-in-flight-per-partition" "${NARAD_TESTER_MAX_IN_FLIGHT_PER_PARTITION:-1024}"
  "--max-acked-ahead-per-partition" "${NARAD_TESTER_MAX_ACKED_AHEAD_PER_PARTITION:-1024}"
  "--retention" "${NARAD_TESTER_RETENTION:-24h}"
  "--visibility-timeout" "${NARAD_TESTER_VISIBILITY_TIMEOUT:-30s}"
  "--consume-wait" "${NARAD_TESTER_CONSUME_WAIT:-20ms}"
  "--metrics-addr" "${NARAD_TESTER_METRICS_ADDR:-127.0.0.1:9095}"
  "--missing-after" "${NARAD_TESTER_MISSING_AFTER:-5m}"
  "--max-outstanding-messages" "${NARAD_TESTER_MAX_OUTSTANDING_MESSAGES:-1000000}"
  "--ledger-scan-interval" "${NARAD_TESTER_LEDGER_SCAN_INTERVAL:-10s}"
  "--max-messages" "${NARAD_TESTER_MAX_MESSAGES:-0}"
  "--drain-timeout" "${NARAD_TESTER_DRAIN_TIMEOUT:-2m}"
)

if [[ -n "$RUN_ID" ]]; then
  args+=("--run-id" "$RUN_ID")
fi

cd "$ROOT_DIR"
exec "$GO_BIN" run ./cmd/narad-tester "${args[@]}" "$@"

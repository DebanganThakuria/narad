#!/usr/bin/env bash
# Same-compute shootout runner. One broker at a time, identical
# resources, identical workload. Usage:
#
#   ./run.sh                 # all systems
#   ./run.sh narad kafka     # a subset
#
# Results append to results.jsonl; a markdown table prints at the end.
set -euo pipefail
cd "$(dirname "$0")"

SYSTEMS=("$@")
if [ ${#SYSTEMS[@]} -eq 0 ]; then
  SYSTEMS=(narad nats rabbitmq redis kafka pulsar)
fi
COUNT="${COUNT:-50000}"
SIZE="${SIZE:-256}"
WORKERS="${WORKERS:-16}"

command -v jq >/dev/null || { echo "jq required"; exit 1; }
(cd driver && go build -o ../driver-bin .)

RESULTS="results.jsonl"

for sys in "${SYSTEMS[@]}"; do
  echo "=== $sys: starting broker (2 CPU / 2 GB) ==="
  docker compose --profile "$sys" up -d --wait --quiet-pull
  # Give single-node metadata/leader election a moment to settle.
  sleep 5
  echo "=== $sys: driving ${COUNT} x ${SIZE}B with ${WORKERS} workers ==="
  if ./driver-bin -system "$sys" -count "$COUNT" -size "$SIZE" -workers "$WORKERS" >> "$RESULTS"; then
    tail -1 "$RESULTS" | jq -c 'del(.durability)'
  else
    echo "!!! $sys run failed (see stderr above)"
  fi
  docker compose --profile "$sys" down -v --remove-orphans > /dev/null 2>&1
done

echo
echo "| system | produce msg/s | p50 | p99 | consume+ack msg/s | durability of the produce ack |"
echo "|---|---|---|---|---|---|"
jq -r '"| \(.system) | \(.produce_msgs_per_sec | floor) | \(.produce_p50_ms)ms | \(.produce_p99_ms)ms | \(.consume_msgs_per_sec | floor) | \(.durability) |"' "$RESULTS"

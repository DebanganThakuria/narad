#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${NARAD_LOCAL_SOAK_DIR:-$ROOT_DIR/tmp/local-soak}"
PID_FILE="$RUN_DIR/narad.pids"

if [[ ! -f "$PID_FILE" ]]; then
  echo "no local soak pid file found at $PID_FILE"
  exit 0
fi

while read -r pid; do
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "stopping narad pid $pid"
    kill "$pid" 2>/dev/null || true
  fi
done <"$PID_FILE"

while read -r pid; do
  if [[ -n "$pid" ]]; then
    wait "$pid" 2>/dev/null || true
  fi
done <"$PID_FILE"

rm -f "$PID_FILE"
echo "local soak cluster stopped; data and logs kept under $RUN_DIR"

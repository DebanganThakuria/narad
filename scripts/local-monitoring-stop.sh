#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${NARAD_LOCAL_MONITORING_DIR:-$ROOT_DIR/tmp/local-monitoring}"
PROM_PID_FILE="$RUN_DIR/prometheus.pid"

if [[ -f "$PROM_PID_FILE" ]]; then
  pid="$(cat "$PROM_PID_FILE")"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "stopping Prometheus pid $pid"
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -f "$PROM_PID_FILE"
fi

if command -v brew >/dev/null 2>&1 && brew list prometheus >/dev/null 2>&1; then
  brew services stop prometheus >/dev/null 2>&1 || true
fi

if [[ "${1:-}" == "--grafana" ]] && command -v brew >/dev/null 2>&1; then
  brew services stop grafana >/dev/null 2>&1 || true
fi

echo "local monitoring stopped; Prometheus data kept under $RUN_DIR"

#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${NARAD_LOCAL_MONITORING_DIR:-$ROOT_DIR/tmp/local-monitoring}"
PROM_PID_FILE="$RUN_DIR/prometheus.pid"
PROM_LOG="$RUN_DIR/prometheus.log"
PROM_DATA="$RUN_DIR/prometheus-data"
PROM_CONFIG="$ROOT_DIR/ops/local-monitoring/prometheus.yml"
GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3000}"
GRAFANA_AUTH="${GRAFANA_AUTH:-admin:admin}"

pid_alive() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

if ! command -v prometheus >/dev/null 2>&1; then
  echo "prometheus binary not found. Install locally with: brew install prometheus" >&2
  exit 2
fi

mkdir -p "$RUN_DIR" "$PROM_DATA"

if command -v brew >/dev/null 2>&1 && brew list prometheus >/dev/null 2>&1; then
  if [[ -f /opt/homebrew/etc/prometheus.yml && ! -f /opt/homebrew/etc/prometheus.yml.narad-backup ]]; then
    cp -p /opt/homebrew/etc/prometheus.yml /opt/homebrew/etc/prometheus.yml.narad-backup
  fi
  cp "$PROM_CONFIG" /opt/homebrew/etc/prometheus.yml
  echo "starting Prometheus via Homebrew services on http://127.0.0.1:9090"
  brew services restart prometheus >/dev/null
elif [[ -f "$PROM_PID_FILE" ]] && pid_alive "$(cat "$PROM_PID_FILE")"; then
  echo "Prometheus is already running with pid $(cat "$PROM_PID_FILE")"
else
  echo "starting Prometheus on http://127.0.0.1:9090"
  prometheus \
    --config.file="$PROM_CONFIG" \
    --storage.tsdb.path="$PROM_DATA" \
    --web.listen-address="127.0.0.1:9090" \
    --web.enable-lifecycle \
    >"$PROM_LOG" 2>&1 &
  echo "$!" >"$PROM_PID_FILE"
fi

if command -v brew >/dev/null 2>&1 && brew list grafana >/dev/null 2>&1; then
  if [[ "${NARAD_START_GRAFANA:-1}" == "1" ]]; then
    brew services start grafana >/dev/null 2>&1 || true
  fi
else
  echo "Grafana not found via Homebrew. Install with: brew install grafana" >&2
fi

for _ in {1..30}; do
  if curl -fsS "$GRAFANA_URL/api/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if curl -fsS "$GRAFANA_URL/api/health" >/dev/null 2>&1; then
  curl -fsS -u "$GRAFANA_AUTH" \
    -H "Content-Type: application/json" \
    -X POST "$GRAFANA_URL/api/datasources" \
    --data '{"name":"Prometheus","type":"prometheus","url":"http://127.0.0.1:9090","access":"proxy","isDefault":true,"uid":"prometheus"}' \
    >/dev/null 2>&1 || true

  if command -v python3 >/dev/null 2>&1; then
    for dashboard in "$ROOT_DIR"/ops/local-monitoring/grafana/dashboards/*.json; do
      [[ -f "$dashboard" ]] || continue
      payload="$RUN_DIR/dashboard-payload-$(basename "$dashboard")"
      python3 "$ROOT_DIR/ops/local-monitoring/wrap-dashboard.py" "$dashboard" "$payload"
      curl -fsS -u "$GRAFANA_AUTH" \
        -H "Content-Type: application/json" \
        -X POST "$GRAFANA_URL/api/dashboards/db" \
        --data @"$payload" \
        >/dev/null 2>&1 || true
    done
  else
    echo "python3 not found; import the dashboard JSON manually from ops/local-monitoring/grafana/dashboards" >&2
  fi
else
  echo "Grafana is not reachable at $GRAFANA_URL; import the dashboard JSON manually after Grafana starts" >&2
fi

cat <<EOF
monitoring started
Prometheus: http://127.0.0.1:9090
Grafana:    $GRAFANA_URL
Prom log:   $PROM_LOG
stop:       ./scripts/local-monitoring-stop.sh
EOF

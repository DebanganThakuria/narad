#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "launchd soak mode is only supported on macOS" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${NARAD_LOCAL_SOAK_DIR:-$ROOT_DIR/tmp/local-soak}"
LAUNCHD_DIR="$RUN_DIR/launchd"
DOMAIN="gui/$(id -u)"
LABELS=(
  com.narad.local-soak.tester
  com.narad.local-soak.narad-1
  com.narad.local-soak.narad-2
  com.narad.local-soak.narad-3
)

for label in "${LABELS[@]}"; do
  plist="$LAUNCHD_DIR/${label}.plist"
  if [[ -f "$plist" ]]; then
    echo "stopping $label"
    launchctl bootout "$DOMAIN" "$plist" >/dev/null 2>&1 || true
  else
    launchctl bootout "$DOMAIN/$label" >/dev/null 2>&1 || true
  fi
done

echo "local launchd soak stopped; data and logs kept under $RUN_DIR"

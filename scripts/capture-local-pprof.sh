#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${NARAD_PPROF_OUT_DIR:-$ROOT_DIR/tmp/pprof}"
BIN="${NARAD_PPROF_BINARY:-$ROOT_DIR/tmp/local-soak/narad}"
PORTS=(${NARAD_PPROF_PORTS:-6061 6062 6063})
CPU_SECONDS="${NARAD_PPROF_CPU_SECONDS:-30}"

mkdir -p "$OUT_DIR"

for i in "${!PORTS[@]}"; do
  node=$((i + 1))
  port="${PORTS[$i]}"
  base="$OUT_DIR/narad-${node}"
  url="http://127.0.0.1:${port}/debug/pprof"

  echo "capturing node ${node} from ${url}"
  curl -fsS -o "${base}.heap.pb.gz" "${url}/heap"
  curl -fsS -o "${base}.allocs.pb.gz" "${url}/allocs"
  curl -fsS -o "${base}.goroutine.txt" "${url}/goroutine?debug=2"
done

if [[ "${NARAD_PPROF_CAPTURE_CPU:-0}" == "1" ]]; then
  echo "capturing ${CPU_SECONDS}s CPU profile from node 1"
  curl -fsS -o "$OUT_DIR/narad-1.cpu.pb.gz" "http://127.0.0.1:${PORTS[0]}/debug/pprof/profile?seconds=${CPU_SECONDS}"
fi

cat <<EOF
pprof profiles written to $OUT_DIR

inspect heap:
  go tool pprof -top "$BIN" "$OUT_DIR/narad-1.heap.pb.gz"
  go tool pprof -http=:0 "$BIN" "$OUT_DIR/narad-1.heap.pb.gz"
EOF

# Local Monitoring

This directory holds the Prometheus scrape config and Grafana dashboards for a
local Narad cluster, plus the source of the devstack `narad-stage` dashboard.

Start three nodes yourself (`make local-cluster-e2e` tears its cluster down
when the driver exits, so it is not what you want here). Each node gets a
dedicated metrics listener on 9101-9103, which is what `prometheus.yml`
scrapes, and a pprof listener on 6061-6063. Auth and cluster TLS are off
here only because every address is 127.0.0.1; do not reuse these flags on
a routable address.

```bash
make build
mkdir -p tmp/local-monitoring/logs
: > tmp/local-monitoring/narad.pids
PEERS="narad-1@127.0.0.1:19081,narad-2@127.0.0.1:19082,narad-3@127.0.0.1:19083"
for i in 1 2 3; do
  NARAD_NODE_ID="narad-$i" \
  NARAD_HTTP_ADDR="127.0.0.1:1808$i" \
  NARAD_HTTP_METRICS_ADDR="127.0.0.1:910$i" \
  NARAD_HTTP_PPROF_ADDR="127.0.0.1:606$i" \
  NARAD_CLUSTER_ADDR="127.0.0.1:1908$i" \
  NARAD_CLUSTER_PEERS="$PEERS" \
  NARAD_DATA_DIR="tmp/local-monitoring/narad-$i" \
  NARAD_SECURITY_ENABLED=false \
  NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT=true \
  NARAD_SECURITY_ALLOW_INSECURE_CLUSTER=true \
    bin/narad serve >"tmp/local-monitoring/logs/narad-$i.log" 2>&1 &
  echo $! >> tmp/local-monitoring/narad.pids
done
```

Then start Prometheus and import the Grafana dashboards:

```bash
make local-monitoring-start
```

Drive load through the same nodes with
`make cluster-load NARAD_NODES='http://127.0.0.1:18081,http://127.0.0.1:18082,http://127.0.0.1:18083'`.

Dashboards:

- `Narad Nodes`: Narad-native API, broker, storage, CPU, memory, runtime, and disk metrics.

Narad exposes process CPU and memory through the Prometheus Go/process collectors. Use `process_resident_memory_bytes{job="narad"}` as the local stand-in for Kubernetes memory usage; it is resident process memory. `go_memstats_heap_alloc_bytes` is only live Go heap and is usually lower than real process memory because it excludes goroutine stacks, mmap/file mappings, allocator overhead, and runtime metadata.

Useful memory queries:

```promql
sum(process_resident_memory_bytes{job="narad"})
process_resident_memory_bytes{job="narad"}
go_memstats_heap_alloc_bytes{job="narad"}
go_memstats_sys_bytes{job="narad"}
```

Narad also exposes `narad_data_dir_size_bytes` and `narad_data_dir_available_bytes` for per-node data directory usage and filesystem headroom.

## pprof

With the nodes started as above (`NARAD_HTTP_PPROF_ADDR`, or `--pprof-addr`, one loopback address per node):

- node 1: `http://127.0.0.1:6061/debug/pprof/`
- node 2: `http://127.0.0.1:6062/debug/pprof/`
- node 3: `http://127.0.0.1:6063/debug/pprof/`

Capture memory and goroutine snapshots:

```bash
mkdir -p tmp/pprof
for node in 1 2 3; do
  port=$((6060 + node))
  curl -fsS -o "tmp/pprof/narad-${node}.heap.pb.gz" "http://127.0.0.1:${port}/debug/pprof/heap"
  curl -fsS -o "tmp/pprof/narad-${node}.allocs.pb.gz" "http://127.0.0.1:${port}/debug/pprof/allocs"
  curl -fsS -o "tmp/pprof/narad-${node}.goroutine.txt" "http://127.0.0.1:${port}/debug/pprof/goroutine?debug=2"
done
```

Inspect a heap profile (the binary must be the one the nodes are running;
`scripts/capture-local-pprof.sh` takes it as `NARAD_PPROF_BINARY`):

```bash
go tool pprof -top bin/narad tmp/pprof/narad-1.heap.pb.gz
go tool pprof -http=:0 bin/narad tmp/pprof/narad-1.heap.pb.gz
```

Capture a 30-second CPU profile from node 1:

```bash
curl -fsS -o tmp/pprof/narad-1.cpu.pb.gz "http://127.0.0.1:6061/debug/pprof/profile?seconds=30"
go tool pprof -top bin/narad tmp/pprof/narad-1.cpu.pb.gz
```

Stop Prometheus, then the nodes:

```bash
make local-monitoring-stop
kill $(cat tmp/local-monitoring/narad.pids)
```

## Devstack dashboard (Narad Stage)

`grafana/dashboards/narad-stage-dashboard.json` is the source of the org Grafana
dashboard `narad-stage`. The devstack pods are scraped every 60s through pod
annotations, and the datasource does not declare that interval, so
`$__rate_interval` would be sized for 15s scrapes and a 60s burst would be
averaged away. The dashboard therefore uses an explicit `$window` rate window
(default 2m, two scrapes), a 1m minimum step, and a row of run totals and peak
rates (`Produced in range`, `Acked in range`, `Peak produce/s`, `Peak ack/s`)
that are independent of the window. `fix-stage-dashboard.py` applies the same
treatment to a dashboard exported from Grafana.

To upload a change, wrap it and POST it with a Grafana session:

```bash
python3 ops/monitoring/wrap-dashboard.py ops/monitoring/grafana/dashboards/narad-stage-dashboard.json /tmp/narad-stage-payload.json
curl -s -H "Cookie: grafana_session=<session>" -H "Content-Type: application/json" \
  -X POST https://grafana.np.razorpay.in/api/dashboards/db --data-binary @/tmp/narad-stage-payload.json
```

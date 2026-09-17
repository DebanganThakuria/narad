# Local Monitoring

This directory holds the Prometheus scrape config and Grafana dashboards for a
local Narad cluster, plus the source of the devstack `narad-stage` dashboard.

Start a local three-node cluster with `make local-cluster-e2e` (or run the
nodes yourself on ports 18081-18083, which is what `prometheus.yml` scrapes),
then start Prometheus and import the Grafana dashboards:

```bash
make local-monitoring-start
```

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

With `narad.pprof.enabled: true` and one loopback pprof address per node, for example:

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

Inspect a heap profile:

```bash
go tool pprof -top bin/narad tmp/pprof/narad-1.heap.pb.gz
go tool pprof -http=:0 bin/narad tmp/pprof/narad-1.heap.pb.gz
```

Capture a 30-second CPU profile from node 1:

```bash
curl -fsS -o tmp/pprof/narad-1.cpu.pb.gz "http://127.0.0.1:6061/debug/pprof/profile?seconds=30"
go tool pprof -top bin/narad tmp/pprof/narad-1.cpu.pb.gz
```

Stop Prometheus:

```bash
make local-monitoring-stop
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

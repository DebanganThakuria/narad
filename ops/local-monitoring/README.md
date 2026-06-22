# Local Soak Monitoring

This directory contains the Prometheus scrape config and Grafana dashboard for a long-running local Narad soak.

Start the three-node cluster:

```bash
make local-soak-cluster
```

Start Prometheus and import the Grafana dashboard:

```bash
make local-monitoring-start
```

Start the tester:

```bash
make local-soak-tester
```

By default the local tester starts at `50 msg/sec`, adds `10 msg/sec` every
`10m`, caps at `100000 msg/sec`, then holds that rate. With the default step,
reaching the cap from `50 msg/sec` takes about 69 days.

Useful overrides:

```bash
NARAD_TESTER_MESSAGES_PER_SECOND=100 NARAD_TESTER_MAX_MESSAGES_PER_SECOND=500 make local-soak-tester
NARAD_TESTER_RATE_RAMP_STEP=1000 NARAD_TESTER_RATE_RAMP_INTERVAL=1m make local-soak-tester
NARAD_TESTER_RATE_RAMP_STEP=0 make local-soak-tester
NARAD_TESTER_TOPICS=20 NARAD_TESTER_PARTITIONS=24 make local-soak-tester
NARAD_TESTER_RUN_ID=month-1 NARAD_TESTER_MAX_OUTSTANDING_MESSAGES=1000000 make local-soak-tester
```

The tester exposes metrics on `127.0.0.1:9095` and keeps live correctness state in memory. Produced messages stay in the outstanding set until first valid consumption. Consumed sequences are tracked exactly for duplicate classification, so the duplicate and unknown counters are not affected by a recent-message cache cap. New produces are throttled when outstanding messages reach `NARAD_TESTER_MAX_OUTSTANDING_MESSAGES`.

Dashboards:

- `Narad Local Soak`: tester plus Narad end-to-end soak view.
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

The local soak scripts expose pprof on one loopback port per Narad node:

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
go tool pprof -top tmp/local-soak/narad tmp/pprof/narad-1.heap.pb.gz
go tool pprof -http=:0 tmp/local-soak/narad tmp/pprof/narad-1.heap.pb.gz
```

Capture a 30-second CPU profile from node 1:

```bash
curl -fsS -o tmp/pprof/narad-1.cpu.pb.gz "http://127.0.0.1:6061/debug/pprof/profile?seconds=30"
go tool pprof -top tmp/local-soak/narad tmp/pprof/narad-1.cpu.pb.gz
```

Stop local processes:

```bash
make local-soak-stop
make local-monitoring-stop
```

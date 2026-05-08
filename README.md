# Narad

A lightweight, durable, queue-first event streaming system. Producers push
JSON messages to topics; consumers pull (with optional long-polling) and
acknowledge to advance their offset. Replay is supported via explicit
offsets.

> **Status:** single-node, production-shaped surface. The HTTP API,
> append-only segmented log, SQLite metastore, JSON-Schema validation,
> per-topic retention, partitioning, Prometheus metrics, and a debug
> pprof listener are all functional. Multi-node leader/follower
> replication, cross-node partition rebalancing, message-visibility
> timeouts (SQS-style in-flight tracking), and schema-update with
> backwards-compatibility checks are the next big chunks.

## Architecture (current)

```
                         ┌─────────────┐
HTTP Producers ─────────▶│             │
  (port 7942)            │   Broker    │── append ──▶ Partition Log (disk)
HTTP Consumers ◀── pull ─┤             │                 ▲
  (long-poll)            └──────┬──────┘                 │
                                │                        │  notify
                                │  metadata               on append
                                ▼
                          SQLite Metastore
                       (topics, schemas, offsets,
                        in-process LRU cache)
```

Cluster traffic (replication, follower fetch, membership) will run on
port 7943 once `narad worker` lands real replication.

A separate, opt-in pprof listener (`--pprof-addr 127.0.0.1:6060`) and
a `/metrics` Prometheus endpoint on the public port round out the
operator surface.

## Layout

```
cmd/
  narad/                single binary with subcommands (serve/worker/client/version)
internal/
  config/               defaults + JSON file + env + flag layering
  observability/
    logger/             thin log/slog wrapper
    metrics/            Prometheus collectors, HTTP middleware, lag poller,
                          pprof-safe /metrics endpoint
  httpserver/           http.Server, router, middleware (Recover, RequestID,
                          metrics middleware, AccessLog), pprof listener
    handlers/           request handlers
  broker/               orchestrator: produce / consume / ack / topic CRUD,
                          plus Snapshot used by the metrics poller
  storage/              append-only Log: in-memory buffer + async flusher
                          per partition, zstd-compressed batched frames,
                          per-frame CRC32C, segment rotation, retention
                          reaper, skip-and-continue corruption recovery
  metastore/            SQLite-backed metadata store (gorm + glebarez/sqlite,
                          pure-Go) with byte-bounded LRU read cache
  topic/                shared value types (Topic, Retention, Message,
                          Details, PartitionStats)
  partition/            partition selection (FNV hash + round-robin)
  replication/          Replicator interface + single-node Local stub
  schema/               SchemaRegistry interface, JSON-Schema impl
                          (santhosh-tekuri/jsonschema), AlwaysValid stub
  consumer/             OffsetTracker interface + metastore-backed impl
                          with batched flushes and per-topic eviction
tests/
  e2e/                  HTTP-level end-to-end tests (httptest + real
                          broker), split by feature surface
.github/
  workflows/ci.yml      build / unit-tests / e2e-tests jobs (race-enabled)
```

## Quickstart

```sh
make build         # produces bin/narad
bin/narad serve    # listens on :7942
```

## Developer setup (one-time)

```sh
make tools-install   # gofumpt + goimports into $(go env GOPATH)/bin
```

`make fmt` auto-formats the tree with both tools; `make check` runs
`fmt-check + vet + test` for a strict no-write pass.

In another terminal, the easiest way to drive the server is the
built-in `narad client` subcommand (HTTP under the hood):

```sh
narad client topics create orders
narad client topics list
narad client topics get orders
echo '{"id":1,"amount":1500}' | narad client produce --key c1 orders
narad client consume --wait 5s orders
narad client ack --partition 0 --offset 0 orders
narad client topics alter --partitions 16 orders
narad client topics delete orders
```

Or hit the HTTP API directly (all data routes live under `/v1`):

```sh
# Create with explicit retention.
curl -X POST localhost:7942/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","partitions":8,"replication_factor":2,
       "retention":{"max_age_ms":3600000,"max_bytes":1073741824}}'

# Produce.
curl -X POST localhost:7942/v1/topics/orders/produce \
  -H 'Content-Type: application/json' \
  -d '{"key":"customer-42","message":{"id":1,"amount":1500}}'

# Consume with long-poll.
curl 'localhost:7942/v1/topics/orders/consume?wait=5s'

# Ack.
curl -X POST localhost:7942/v1/topics/orders/ack \
  -H 'Content-Type: application/json' \
  -d '{"partition":0,"offset":0}'

# Update retention without restart.
curl -X PATCH localhost:7942/v1/topics/orders \
  -H 'Content-Type: application/json' \
  -d '{"retention":{"max_age_ms":86400000,"max_bytes":0}}'

# List with pagination.
curl 'localhost:7942/v1/topics?limit=50'
curl 'localhost:7942/v1/topics?limit=50&page_token=<from previous response>'

# Scrape metrics.
curl localhost:7942/metrics
```

API routes:

```
POST    /v1/topics                          create (accepts retention overrides)
GET     /v1/topics?limit=&page_token=       list (keyset pagination by name)
GET     /v1/topics/{topic}                  get single + per-partition stats
PATCH   /v1/topics/{topic}                  alter partitions OR retention
                                              (exactly one per request)
DELETE  /v1/topics/{topic}                  delete topic and all data
POST    /v1/topics/{topic}/produce
GET     /v1/topics/{topic}/consume
POST    /v1/topics/{topic}/ack
GET     /healthz                            liveness
GET     /readyz                             readiness (broker.Ready)
GET     /metrics                            Prometheus exposition
```

## CLI surface

```
narad serve     run the HTTP API server (default port 7942)
narad worker    run the cluster worker (default port 7943)
narad client    interact with a running narad serve over HTTP
narad version   print build version
narad --help    top-level help
```

Run `narad <subcommand> --help` for the full flag list.

Common `narad serve` flags:

```
--port 7942                  override the API listen port
--addr 0.0.0.0:7942          override the API listen address (host+port)
--cluster-port 7943          override the cluster listen port
--config narad.json          path to JSON config file (optional)
--data-dir ./data            storage directory
--log-level info             debug | info | warn | error
--log-format json            json | text
--pprof-addr 127.0.0.1:6060  enable pprof on this address; empty disables
```

## Configuration

Narad reads configuration from up to four sources. Higher numbers win:

```
1. defaults (built into the binary)
2. JSON config file        (--config narad.json)
3. environment variables   (NARAD_*)
4. CLI flags               (--port, --data-dir, ...)
```

A starting template lives in [`narad.example.json`](./narad.example.json):

```jsonc
{
  "http":    { "addr": ":7942", "read_timeout": "10s", "max_consume_wait": "30s" },
  "cluster": { "addr": ":7943" },
  "storage": {
    "data_dir": "data",
    "fsync": "per_write",
    "codec": "zstd",
    "compression_level": "best",
    "flush_bytes": 1048576,
    "flush_records": 1000,
    "flush_interval_ms": 100,
    "segment_bytes": 67108864,
    "retention_check_interval_ms": 60000
  },
  "topic": {
    "default_partitions": 8,
    "max_partitions": 1024,
    "default_replication_factor": 2,
    "default_retention_age_ms": 604800000,
    "default_retention_bytes": 0
  },
  "log":     { "level": "info", "format": "json" },
  "worker":  { "enabled": false },
  "debug":   { "pprof_addr": "" }
}
```

Useful environment overrides:

| Variable | Effect |
|---|---|
| `NARAD_HTTP_ADDR` | API listen address |
| `NARAD_CLUSTER_ADDR` | Cluster listen address |
| `NARAD_DATA_DIR` | Storage directory |
| `NARAD_LOG_LEVEL` | `debug` / `info` / `warn` / `error` |
| `NARAD_LOG_FORMAT` | `json` / `text` |
| `NARAD_FSYNC` | `per_write` / `batched` |
| `NARAD_STORAGE_CODEC` | `zstd` / `none` |
| `NARAD_STORAGE_COMPRESSION_LEVEL` | `fastest` / `default` / `better` / `best` |
| `NARAD_STORAGE_FLUSH_BYTES` | flush when buffer ≥ N bytes |
| `NARAD_STORAGE_FLUSH_RECORDS` | flush when buffer ≥ N records |
| `NARAD_STORAGE_FLUSH_INTERVAL_MS` | flush at least every N ms |
| `NARAD_STORAGE_SEGMENT_BYTES` | roll the active segment past N bytes |
| `NARAD_STORAGE_RETENTION_CHECK_INTERVAL_MS` | retention reaper sweep period |
| `NARAD_TOPIC_DEFAULT_PARTITIONS` | default partition count when omitted from CreateTopic |
| `NARAD_TOPIC_MAX_PARTITIONS` | upper bound for partition count |
| `NARAD_TOPIC_DEFAULT_REPLICATION_FACTOR` | default replication factor when omitted (must be ≥ 2) |
| `NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS` | default retention age (default: 7 days) |
| `NARAD_TOPIC_DEFAULT_RETENTION_BYTES` | default retention size cap (default: 0 = disabled) |
| `NARAD_DEBUG_PPROF_ADDR` | pprof listener address; empty disables |
| `NARAD_ADDR` | (client only) base URL for `narad client` |

See `internal/config/config.go` for the full list and the matching JSON keys.

## Storage layer

A partition log is a *directory* of segment files. The active segment
receives writes; older segments are sealed (read-only). Each segment
is an append-only file made of CRC-checked, optionally zstd-compressed
*frames*. Each frame holds 1..N records that share a contiguous offset
range:

```
┌─ frame ────────────────────────────────────────────────────────────┐
│ magic (2B) │ flags (1B) │ recordCount (4B) │ baseOffset (8B)        │
│ uncompressed (4B) │ compressed (4B) │ crc32c (4B)                   │
│ payload (compressed): [length:4][bytes] xN                         │
└────────────────────────────────────────────────────────────────────┘
```

**Produce path.** `Append` pushes the record into a per-partition
in-memory buffer and returns the assigned offset immediately — no
fsync, no disk I/O on the hot path. A single flusher goroutine drains
the buffer to disk when one of `flush_bytes`, `flush_records`, or
`flush_interval_ms` is crossed, and one final time on graceful
shutdown. Multiple producer goroutines may call `Append` concurrently;
the buffer is internally synchronized.

**Concurrency model.** *Many writers, one flusher per partition file.*
Producer goroutines contend only on the buffer's lightweight mutex;
exactly one goroutine — the flusher — touches the file. Throughput
scales by adding more partitions, not more writers per file.

**Durability.** Records produced before a flush are durable. Records
produced in the open flush window can be lost on a hard crash; this
is bounded by `flush_interval_ms` and the byte/record thresholds, and
graceful shutdown (SIGTERM) always does a final flush. Future
multi-node replication will close the crash window — peers ACK from
their own in-memory buffers before the producer is acknowledged.

**Recovery.** On startup the log file is scanned frame by frame:

* A frame whose magic, header, CRC, or inner record stream is corrupt
  is *skipped*, not removed; the scanner resyncs on the next valid
  magic and continues. The bad frame's offsets become permanent gaps
  that read as `ErrOffsetNotFound`.
* A torn tail (an interrupted write at EOF) is truncated to the last
  valid frame boundary so future appends start clean.

**Compression.** `zstd` at `SpeedBestCompression` is the default. The
flusher pays the encoder cost off the produce hot path; zstd's
decompression speed is independent of the encoder level, so reads are
unaffected by the choice. A single-slot decoded-frame cache on each
log makes sequential consumer reads inside a batch O(1) after the
first decompression.

**Segments.** Each partition lives under a directory; the flusher
rolls a new segment when the active one crosses `storage.segment_bytes`
(default 64 MiB):

```
data/topics/orders/p00007/
  00000000000000000000.log    (sealed, base offset 0)
  00000000000000010000.log    (sealed, base offset 10000)
  00000000000000020000.log    (active, base offset 20000)
```

Segment filenames encode the segment's first offset (20-digit
zero-padded). Sealed segments are kept open for reads but never
written to.

## Retention

A per-partition reaper deletes sealed segments past the topic's
retention bounds. Defaults: 7 days, no size cap. Both bounds are
configurable globally (`topic.default_retention_age_ms`,
`topic.default_retention_bytes`) and per-topic via:

* the `retention` field on `POST /v1/topics`, and
* `PATCH /v1/topics/{name}` with a `retention` body — the broker
  closes cached partition logs so the next access reopens with the
  new bounds (storage folds retention in at log-open time).

Zero in either field inherits the policy default; negatives are
rejected. The active segment is never deleted, regardless of its age.
Records in deleted segments become permanent gaps; reads return
`404`/`ErrOffsetNotFound`.

The reaper sweeps every `storage.retention_check_interval_ms` (default
1 minute). Age is measured by segment-file mtime — a sealed segment's
mtime stops advancing, so it's a stable proxy for "time of last write
to that segment".

## Topics

Topics are created via `POST /v1/topics`. `partitions`,
`replication_factor`, and `retention` are optional; omitting them (or
sending zero) applies the configured defaults.

```jsonc
{
  "name": "orders",
  "partitions": 8,                    // 0 = use default
  "replication_factor": 2,            // 0 = use default; must be >= 2
  "retention": {                      // omit to use defaults
    "max_age_ms": 86400000,           // 0 = use default
    "max_bytes":  1073741824          // 0 = use default
  }
}
```

`PATCH /v1/topics/{name}` accepts **exactly one** of `partitions` or
`retention` per request:

```sh
# Increase partitions (increase-only; decrease/equal returns 400).
curl -X PATCH localhost:7942/v1/topics/orders \
  -H 'Content-Type: application/json' -d '{"partitions": 32}'

# Update retention (no restart needed; cached logs reopen on next access).
curl -X PATCH localhost:7942/v1/topics/orders \
  -H 'Content-Type: application/json' \
  -d '{"retention":{"max_age_ms":3600000}}'
```

Existing records are not moved when partitions increase — they stay in
the partitions they were originally written to. Future records'
partition assignment uses `hash(key) % newCount`, so an existing key
may now hash to a different partition than its prior records.
Consumers that depend on per-key ordering should be aware. Decreasing
the partition count is not supported (offsets are immutable).

`DELETE /v1/topics/{name}` removes a topic, its on-disk segments, and
its consumer offsets. Irreversible. The topic name can be reused
immediately afterwards — a fresh produce starts at offset 0 again.

`GET /v1/topics/{name}` returns the topic record plus per-partition
runtime stats (segment count, oldest/next offset, bytes, oldest
segment mtime).

`GET /v1/topics` returns topics in lexicographic order by name.
Pagination is keyset by name (cursor in `next_page_token`), robust
against inserts and deletes between pages. `limit` defaults to 100,
caps at 1000. `limit=0` is rejected at the HTTP layer; the
broker-internal "no limit" path is reserved for the metrics poller.

## Observability

**`/metrics` (Prometheus exposition).** Mounted on the public API
listener. Strict cardinality budget — labels are limited to
`{topic}`, `{topic, partition}`, and `{route, method, status}` (where
`route` is the matched ServeMux pattern, never the literal path).
Highlights:

* `narad_http_*` — requests by route/method/status, duration histogram,
  bytes in/out, in-flight gauge.
* `narad_messages_{produced,consumed}_total{topic,partition}` and the
  matching `bytes_*_total` counters.
* `narad_consume_wait_seconds{topic,outcome}` and
  `narad_consume_empty_total{topic}` for tuning long-poll behaviour.
* `narad_consumer_lag_messages{topic,partition}` and
  `narad_oldest_unconsumed_message_age_seconds{topic,partition}` for
  autoscaling consumer worker counts. The age metric uses the segment
  mtime containing the committed offset — Narad's on-disk frame
  doesn't carry per-message timestamps, so this is documented as an
  upper bound on "time since the consumer's next message was last
  touched", not exact produce time.
* `narad_consumer_dropped_messages{topic,partition}` — count of
  unacknowledged messages already deleted by retention. Non-zero
  means data was lost before the consumer caught up.
* `narad_storage_{flush,fsync,retention_run}_duration_seconds`,
  `narad_storage_segments_rolled_total`,
  `narad_storage_retention_{deletions,bytes_deleted,messages_deleted}_total`.
* Inventory gauges: `narad_topics_total`, `narad_partitions_total`,
  `narad_topic_bytes{topic}`, `narad_segments{topic,partition}`.
* Boot: `narad_boot_duration_seconds`,
  `narad_storage_segments_scanned_at_boot_total{topic,partition}`.

A 5-second background poller refreshes the gauge-style metrics
(inventory + lag) by calling `Broker.Snapshot`. Counters and
histograms update inline at each call site. Series for deleted topics
are pruned on the next tick so `DeleteTopic` doesn't leak series.

**pprof.** Enable with `--pprof-addr 127.0.0.1:6060` (or
`NARAD_DEBUG_PPROF_ADDR=...`). Disabled by default. Handlers are
registered explicitly on a private mux (no `DefaultServeMux` leak),
and only `ReadHeaderTimeout` is set so `/debug/pprof/profile?seconds=N`
isn't truncated. Bind to loopback in production — pprof exposes
goroutine and heap details. The listener logs a warning if it sees a
non-loopback address.

```sh
go tool pprof http://127.0.0.1:6060/debug/pprof/heap
```

**Healthz / readyz.** `GET /healthz` is a fixed-200 liveness probe.
`GET /readyz` returns 200 if `broker.Ready` succeeds, 503 otherwise —
intended for Kubernetes-style traffic gating.

## Testing

```sh
make test                          # full suite, race detector on
go test ./internal/...             # unit tests only
go test ./tests/e2e/... -race      # HTTP-level e2e against a real broker
go test ./tests/e2e/... -run TestConsume   # one feature
```

Unit tests live next to the code they cover. End-to-end HTTP tests
under `tests/e2e/` are split by feature surface — one file per
endpoint plus `lifecycle_test.go` for cross-cutting flows and
`metrics_test.go` for the observability layer. The e2e harness
(`helpers_test.go`) builds a real broker (SQLite metastore + temp
partition logs) per test and exposes it via `httptest`. `envOpts`
let individual tests override the policy, long-poll cap, or enable
`/metrics` without bloating a fat constructor.

## CI

`.github/workflows/ci.yml` runs three jobs in parallel on every push
to master and every PR:

* **build** — `go vet ./...` + `go build ./...`
* **unit-tests** — `go test -race -count=1` for everything except `tests/e2e`
* **e2e-tests** — `go test -race -count=1 ./tests/e2e/...`

Go version is pinned via `go-version-file: go.mod` so bumping the
toolchain happens in one place. Each job has a 10-minute timeout
(generous for the current ~30s local runtime).

## Design decisions

* **Pure-Go dependencies.** SQLite is pulled in via
  `glebarez/sqlite` (modernc/sqlite under the hood — no CGO).
  Compression is `klauspost/compress`. JSON-Schema validation is
  `santhosh-tekuri/jsonschema`. Metrics use `prometheus/client_golang`.
  All four are pure Go; the binary builds and tests on any
  CGO-disabled environment that supports `go test -race`.
* **Single binary with subcommands.** `narad serve|worker|client|version`
  follows the kubectl/etcd/consul convention and keeps the install
  story to one drop-in binary.
* **Operator endpoints separated from data endpoints.** `/metrics`
  shares the public listener (standard Prometheus convention), but
  pprof is a separate, opt-in listener so its leak surface
  (goroutine stacks, heap layout, profile DoS) is firewall-isolated
  by default.

## Roadmap

* Real leader/follower replication (`internal/replication`).
* Cross-node partition assignment & rebalancing.
* Message-visibility timeouts (SQS-style in-flight tracking) so
  multiple consumers can pull from one topic without redelivery
  during processing.
* Schema-update with backwards-compatibility checking
  (`PATCH /v1/topics/{name}` extension).
* HTTP endpoints for schema registration (currently broker-internal
  via the `schema.Registry` interface).
* Auth, rate limiting.

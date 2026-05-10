# Narad

A lightweight, durable, queue-first event streaming system. Producers push
JSON messages to topics; consumers pull (with optional long-polling) and
acknowledge to advance their offset. Replay is supported via explicit
offsets.

> **Status:** single-node, production-shaped surface. The HTTP API,
> append-only segmented log, SQLite metastore, JSON-Schema validation,
> per-topic retention, partitioning, **SQS-style in-flight tracking
> with gap-skipping reservations + out-of-order acks + HMAC-signed
> receipt handles**, Prometheus metrics, and a debug pprof listener
> are all functional. Multi-node leader/follower replication,
> cross-node partition rebalancing, and schema-update with
> backwards-compatibility checks are the next big chunks.

### Breaking changes (pre-1.0)

The consumer wire contract changed when in-flight tracking landed:

- `POST /v1/topics/{topic}/ack` body went from `{"partition", "offset"}`
  to `{"receipt_handle"}`. The handle is the opaque, HMAC-signed token
  returned in the consume response. Old clients sending
  `{partition, offset}` get **400**.
- `POST /v1/topics` and `PATCH /v1/topics/{topic}` use flat scalar
  fields (`retention_ms`, `visibility_timeout_ms`,
  `max_in_flight_per_partition`, `max_acked_ahead_per_partition`)
  rather than the previous nested `retention: { max_age_ms, max_bytes }`
  object. `max_bytes`-based retention has been removed; size-cap
  retention is on the roadmap if anyone needs it.
- `topic.Message.timestamp` and `topic.Topic.created_at` are now Unix
  seconds (`int64`) rather than RFC-3339 strings. Wire format is
  timezone-independent.
- `narad client ack` now takes `--handle` (or reads the handle from
  stdin); `--partition` / `--offset` are gone. Pipe receipt handles in:
  `consume | jq -r .receipt_handle | narad client ack <topic>`.

The SQLite metastore schema is auto-migrated, but the new columns
(`retention_ms`, `visibility_timeout_ms`, `max_in_flight_per_partition`,
`max_acked_ahead_per_partition`) replace `max_age_ms` / `max_bytes`.
For local dev databases the simplest path is to wipe the data dir.

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

The repo is grouped into category folders (domain / persistence /
transport / platform) under `internal/`. Each leaf is one Go package;
broker and HTTP handlers fan out into per-domain subpackages.

```
cmd/
  narad/                              single binary: main, serve, worker, client, version
internal/
  domain/
    topic/                            value types (Topic, Message, PartitionStats)
  persistence/
    storage/                          append-only segmented log (16 files; see doc.go)
    metastore/                        SQLite metadata store: topics, schemas, offsets
  consumer/                           offset tracker, in-flight reservations,
                                      HMAC receipt-handle codec
  broker/                             orchestrator facade (impl.go embeds the managers)
    errs/                             shared error sentinels
    runtime/                          *Logs (lazy partition-log map),
                                      *Snapshotter, *Lifecycle
    topics/                           CreateTopic / Update* / Delete / Get / List
    messaging/                        Produce / Consume / Ack
  transport/
    httpserver/                       http.Server, router, middleware
      handlers/                       *Set + shared helpers (WriteJSON, DecodeJSON, ...)
        topics/                       /v1/topics CRUD endpoints
        messaging/                    produce / consume / ack endpoints
        health/                       /healthz, /readyz
  platform/
    config/                           defaults + JSON file + env + flag layering
    schema/                           JSON-Schema registry (santhosh-tekuri)
    partition/                        FNV hash + round-robin partition picker
    replication/                      Replicator interface + single-node Local stub
    observability/
      logger/                         thin log/slog wrapper
      metrics/                        Prometheus collectors, HTTP middleware, lag poller
tests/
  e2e/                                HTTP-level end-to-end tests (httptest + real
                                      broker), split by feature surface
.github/
  workflows/ci.yml                    build / unit-tests / e2e-tests (race-enabled)
```

Most multi-file packages have a `doc.go` with a per-file map for
quick navigation (notably `internal/persistence/storage/`,
`internal/persistence/metastore/`, and `internal/platform/config/`).
The broker subpackages are split by operation: `topics/` has
`create.go`, `update.go`, `delete.go`, `query.go`; `messaging/` has
`produce.go`, `consume.go`, `ack.go`; `runtime/` has `logs.go`,
`snapshot.go`, `lifecycle.go`.

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
msg=$(narad client consume --wait 5s orders)
echo "$msg" | jq -r .receipt_handle | narad client ack orders
narad client topics alter --partitions 16 orders
narad client topics delete orders

# Create with explicit retention + visibility + per-partition caps.
narad client topics create \
  --partitions 8 --replication-factor 2 \
  --retention-ms 3600000 --visibility-timeout-ms 30000 \
  --max-in-flight-per-partition 64 --max-acked-ahead-per-partition 256 \
  orders

# Update retention without restart.
narad client topics alter --retention-ms 86400000 orders

# Adjust the consumer-parallelism caps.
narad client topics alter --max-in-flight-per-partition 128 orders

# Register a JSON Schema (file or "-" for stdin).
narad client topics alter --schema-file orders.schema.json orders
cat orders.schema.json | narad client topics alter --schema-file - orders

# Paginated listing (limit defaults to 100, caps at 1000).
narad client topics list --limit 50
narad client topics list --limit 50 --page-token "<next_page_token from previous response>"
```

Or hit the HTTP API directly (all data routes live under `/v1`):

```sh
# Create with explicit retention + visibility + caps.
curl -X POST localhost:7942/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","partitions":8,"replication_factor":2,
       "retention_ms":3600000,"visibility_timeout_ms":30000,
       "max_in_flight_per_partition":64,"max_acked_ahead_per_partition":256}'

# Produce.
curl -X POST localhost:7942/v1/topics/orders/produce \
  -H 'Content-Type: application/json' \
  -d '{"key":"customer-42","message":{"id":1,"amount":1500}}'

# Consume with long-poll. Response includes a receipt_handle.
curl 'localhost:7942/v1/topics/orders/consume?wait=5s'

# Ack — handle is the opaque token returned by Consume.
curl -X POST localhost:7942/v1/topics/orders/ack \
  -H 'Content-Type: application/json' \
  -d '{"receipt_handle":"<token from consume response>"}'

# Update retention without restart.
curl -X PATCH localhost:7942/v1/topics/orders \
  -H 'Content-Type: application/json' \
  -d '{"retention_ms": 86400000}'

# List with pagination.
curl 'localhost:7942/v1/topics?limit=50'
curl 'localhost:7942/v1/topics?limit=50&page_token=<from previous response>'

# Scrape metrics.
curl localhost:7942/metrics
```

API routes:

```
POST    /v1/topics                          create (retention/visibility/caps optional)
GET     /v1/topics?limit=&page_token=       list (keyset pagination by name)
GET     /v1/topics/{topic}                  get single + per-partition stats
PATCH   /v1/topics/{topic}                  alter any combination of:
                                              partitions, retention_ms,
                                              visibility_timeout_ms,
                                              max_in_flight_per_partition,
                                              max_acked_ahead_per_partition,
                                              schema
DELETE  /v1/topics/{topic}                  delete topic and all data
POST    /v1/topics/{topic}/produce
GET     /v1/topics/{topic}/consume          response carries receipt_handle
POST    /v1/topics/{topic}/ack              body: {"receipt_handle": "..."}
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

Topics are created via `POST /v1/topics`. All policy fields are
optional; omitting them (or sending zero) applies the configured
defaults.

```jsonc
{
  "name": "orders",
  "partitions": 8,                              // 0 = use default
  "replication_factor": 2,                      // 0 = use default; must be >= 2
  "retention_ms": 86400000,                     // 0 = use default
  "visibility_timeout_ms": 30000,               // 0 = use default
  "max_in_flight_per_partition": 1024,          // per-partition reservation cap
  "max_acked_ahead_per_partition": 1024         // per-partition out-of-order ack cap
}
```

`PATCH /v1/topics/{name}` accepts any combination of fields; each is
applied in turn:

```sh
# Increase partitions (increase-only; decrease/equal returns 400).
curl -X PATCH localhost:7942/v1/topics/orders \
  -H 'Content-Type: application/json' -d '{"partitions": 32}'

# Update retention (no restart needed; cached logs reopen on next access).
curl -X PATCH localhost:7942/v1/topics/orders \
  -H 'Content-Type: application/json' \
  -d '{"retention_ms": 3600000}'

# Adjust the consumer-parallelism caps.
curl -X PATCH localhost:7942/v1/topics/orders \
  -H 'Content-Type: application/json' \
  -d '{"max_in_flight_per_partition": 64, "max_acked_ahead_per_partition": 256}'
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

## Parallel consumers (single partition)

Narad's consume path supports SQS-style **gap-skipping reservation +
out-of-order acknowledgement**, so a single partition can feed many
concurrent consumer threads / pods without redelivery.

**The model:**

* `Consume` reserves the partition's lowest reachable offset that is
  neither already in flight nor sitting in the partition's "acked
  ahead" set, marks it invisible for `visibility_timeout_ms`, and
  returns the message together with an opaque, HMAC-signed
  `receipt_handle`.
* `Ack` decodes the handle, verifies it cryptographically, and only
  commits if it still matches an active reservation. Acks for already-
  committed offsets, expired reservations, or re-issued offsets are
  rejected.
* When an ack arrives in offset order it advances the partition's
  committed offset and walks forward through any contiguous run of
  previously out-of-order acks. Out-of-order acks for higher offsets
  sit in a sparse `ackedAhead` set per partition until the head
  catches up.

**Per-partition caps** bound the in-memory book-keeping:

| Cap | Default | Effect when reached |
|---|---|---|
| `max_in_flight_per_partition` | 1024 | `Consume` returns 204 (no message); existing reservations must ack or expire |
| `max_acked_ahead_per_partition` | 1024 | `Ack` of an out-of-order offset returns **503**, signaling the partition's head is genuinely stuck |

**Ack error codes:**

| Status | Meaning |
|---|---|
| 204 | Acked. |
| 400 | Malformed handle, missing handle, or topic in path doesn't match the topic encoded in the handle. |
| 401 | HMAC verification failed — handle was forged or signed with a different broker secret. |
| 410 | Handle no longer matches an active reservation: already committed, visibility timeout expired, or broker restarted (handles are signed with a process-local secret and do not survive restart). |
| 503 | Out-of-order ack rejected because `max_acked_ahead_per_partition` is full — head of queue is stuck; consumer should back off. |

**Consumer pattern (CLI):**

```sh
# Consumer worker loop. The receipt handle round-trips through stdin.
while true; do
  msg=$(narad client consume --wait 5s --partition 0 orders) || break
  [ -z "$msg" ] && continue   # 204 = nothing to do
  echo "$msg" | jq -r .receipt_handle | narad client ack orders
done
```

**Known limitation (v1):** when an in-flight cap slot frees via an ack,
long-poll consumers blocked on the cap will not wake until either a
new produce notifies the partition or their long-poll deadline
expires. Acceptable for typical workloads; a "shard-freed" notify
signal is on the roadmap.

**Operational note:** receipt handles are signed with a per-process
random key generated at broker startup. Restarting the broker
invalidates every outstanding handle — clients see 410 on subsequent
acks, the corresponding reservations expire via visibility timeout,
and the messages are redelivered. This is intentional: the in-flight
set is in-memory only.

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

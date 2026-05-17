# Narad

[![CI](https://github.com/DebanganThakuria/narad/actions/workflows/ci.yml/badge.svg)](https://github.com/DebanganThakuria/narad/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/DebanganThakuria/narad)](./go.mod)

Narad is a lightweight, durable, queue-first event streaming system built
in Go. Producers push JSON messages to topics; consumers pull with
optional long-polling, acknowledge with receipt handles, and can replay
from explicit offsets.

## Why Narad

Narad is aimed at teams that want a small, understandable event system
with queue semantics, explicit operational tradeoffs, and a single Go
binary that is easy to run locally and in Kubernetes.

It focuses on:

- durable append-only segmented storage
- queue-style consume/ack semantics with replay support
- Raft-backed control-plane metadata
- any-pod routing to the owning partition
- JSON-Schema validation and per-topic tuning
- Prometheus metrics and operational debugging hooks

## Project status

Narad is pre-1.0 and under active development.

Today, the control plane, HTTP API, CLI surface, topic CRUD, consumer
flows, and core storage paths are covered by unit and end-to-end tests.
The main roadmap item still in progress is fully replicated data-plane
durability.

## Community and project docs

- [Contributing guide](./CONTRIBUTING.md)
- [Code of conduct](./CODE_OF_CONDUCT.md)
- [Security policy](./SECURITY.md)
- [License](./LICENSE)

> **Current implementation status:** the control-plane architecture is in
> place. The HTTP API, append-only segmented log, **Raft+bbolt
> metastore** (topics, schemas, partition assignments, cluster
> membership), **cluster controller** (partition assignment, heartbeat
> monitoring, leader election), **any-pod proxy routing**,
> JSON-Schema validation, per-topic retention, partitioning,
> **SQS-style in-flight tracking with gap-skipping reservations,
> out-of-order acks, and nonce-verified receipt handles**, Prometheus
> metrics, and a debug pprof listener are functional. Data-plane
> replication (leader → follower log sync, `.offsets` files, HWM/LEO)
> is the next milestone.

### Breaking changes (pre-1.0)

> **Status:** control-plane architecture complete. The HTTP API,
> append-only segmented log, **Raft+bbolt metastore** (topics, schemas,
> partition assignments, cluster membership), **cluster controller**
> (partition assignment, heartbeat monitoring, leader election),
> **any-pod proxy routing**, JSON-Schema validation, per-topic retention,
> partitioning, **SQS-style in-flight tracking with gap-skipping
> reservations + out-of-order acks + nonce-verified receipt handles**,
> Prometheus metrics, and a debug pprof listener are all functional.
> Data-plane replication (leader → follower log sync, `.offsets` files,
> HWM/LEO) is the next milestone.

### Breaking changes (pre-1.0)

- `POST /v1/topics/{topic}/ack` body is `{"receipt_handle": "<token>"}`.
  The handle is a `base64url(json({t,p,o,n}))` token returned by
  Consume — no longer HMAC-signed. Tampering returns **400**; a stale
  or already-committed handle returns **410** (not 401).
- `POST /v1/topics` and `PATCH /v1/topics/{topic}` use flat scalar
  fields (`retention_ms`, `visibility_timeout_ms`,
  `max_in_flight_per_partition`, `max_acked_ahead_per_partition`).
- `topic.Message.timestamp` and `topic.Topic.created_at` are Unix
  seconds (`int64`). Wire format is timezone-independent.
- `narad client ack` takes `--handle` (or reads the handle from stdin).
  Pipe receipt handles: `consume | jq -r .receipt_handle | narad client ack <topic>`.
- The SQLite metastore has been replaced by a Raft+bbolt store. Wipe
  the `data/` directory when upgrading from older builds.

## Architecture

```
                                ┌─────────────────────────┐
                                │   Raft control plane     │
                                │  (hashicorp/raft + bbolt)│
                                │                          │
                                │  topics · schemas        │
                                │  assignments · members   │
                                └────────────┬────────────┘
                                             │ read (local bbolt replica)
                         ┌───────────────────┼───────────────────┐
                         │                   │                   │
HTTP Producers ─────────▶│     narad-0       │     narad-1       │  ...
  (port 7942)            │     (broker)      │     (broker)      │
HTTP Consumers ◀── pull ─│                   │                   │
                         │  partition log    │  partition log    │
                         │  (disk, PV)       │  (disk, PV)       │
                         └───────────────────┴───────────────────┘
                                  ↑ any pod proxies to owner via
                                    cluster port (7943)
```

**Control plane.** Three Raft voters (e.g. `narad-0..2`) form a consensus
cluster; remaining pods are non-voters that receive the full replicated
state. The Raft leader acts as the **controller** — it assigns partitions
when topics are created and marks pods dead when their heartbeats time out.
Every pod's local bbolt replica serves metadata reads (topic config,
partition assignment, member address) without a network round-trip.

**Data plane.** Each partition is owned by one pod. That pod holds the
segment files on its persistent volume and manages the in-flight
reservation table in memory. Requests that arrive at the wrong pod are
transparently proxied to the owner via the internal cluster port.

**Replication.** A `replication.Local` stub is wired today (single-copy
durability, bounded by async flush + PV durability). Leader→follower
log replication and `.offsets` files are the next milestone.

## Layout

```
cmd/
  narad/                    single binary: main, serve, worker, client, version
internal/
  errs/                     all 15 sentinel errors in one place (ErrTopicNotFound, ...)
  domain/
    topic/                  value types (Topic, Message, PartitionStats)
  persistence/
    storage/                append-only segmented log (see doc.go for file map)
      codec/                Codec interface + zstd / noop implementations
    metastore/              Raft+bbolt metadata store: topics, schemas,
                            partition assignments, cluster members
  consumer/                 in-flight reservations, nonce-based receipt handles,
                            min-heap expiry, out-of-order ack tracking
  cluster/
    controller/             partition assignment algorithm, heartbeat monitor,
                            OnLeadershipAcquired reconciliation, Heartbeater
    router.go               any-pod proxy routing (RouteProduce/Consume/Ack)
  broker/                   orchestrator facade (impl.go embeds the managers)
    runtime/                *Logs (lazy partition-log map), *Snapshotter, *Lifecycle
    topics/                 CreateTopic / Update* / Delete / Get / List
    messaging/              Produce / Consume / Ack
  transport/
    httpserver/             http.Server, router, middleware
      handlers/             *Set + shared helpers (WriteJSON, DecodeJSON, ...)
        topics/             /v1/topics CRUD endpoints
        messaging/          produce / consume / ack endpoints
        health/             /healthz, /readyz
  platform/
    config/                 defaults + JSON file + env + flag layering
    schema/                 JSON-Schema registry (santhosh-tekuri)
    partition/              FNV hash + round-robin partition picker
    replication/            Replicator interface + single-node Local stub
    observability/
      logger/               thin log/slog wrapper
      metrics/              Prometheus collectors, HTTP middleware, lag poller
tests/
  e2e/                      HTTP-level end-to-end tests (httptest + real broker)
.github/
  workflows/ci.yml          build / unit-tests / e2e-tests (race-enabled)
```

## Quickstart

```sh
make build         # produces bin/narad
bin/narad serve    # listens on :7942 (API) and :7943 (cluster/Raft)
```

## Developer setup (one-time)

```sh
make tools-install   # gofumpt + goimports into $(go env GOPATH)/bin
```

`make fmt` auto-formats the tree; `make check` runs `fmt-check + vet + test`.

In another terminal, the easiest way to drive the server is the
built-in `narad client` subcommand:

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

# Paginated listing (limit defaults to 100, caps at 1000).
narad client topics list --limit 50
narad client topics list --limit 50 --page-token "<next_page_token from previous response>"
```

Or hit the HTTP API directly (all data routes live under `/v1`):

```sh
# Create topic.
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

# Ack — handle is the token returned by Consume.
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
PATCH   /v1/topics/{topic}                  alter: partitions, retention_ms,
                                             visibility_timeout_ms,
                                             max_in_flight_per_partition,
                                             max_acked_ahead_per_partition, schema
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
    "default_retention_age_ms": 604800000
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
| `NARAD_STORAGE_CODEC` | `zstd` / `none` |
| `NARAD_STORAGE_COMPRESSION_LEVEL` | `fastest` / `default` / `better` / `best` |
| `NARAD_STORAGE_FLUSH_BYTES` | flush when buffer ≥ N bytes |
| `NARAD_STORAGE_FLUSH_RECORDS` | flush when buffer ≥ N records |
| `NARAD_STORAGE_FLUSH_INTERVAL_MS` | flush at least every N ms |
| `NARAD_STORAGE_SEGMENT_BYTES` | roll the active segment past N bytes |
| `NARAD_STORAGE_RETENTION_CHECK_INTERVAL_MS` | retention reaper sweep period |
| `NARAD_TOPIC_DEFAULT_PARTITIONS` | default partition count when omitted from CreateTopic |
| `NARAD_TOPIC_MAX_PARTITIONS` | upper bound for partition count |
| `NARAD_TOPIC_DEFAULT_REPLICATION_FACTOR` | default replication factor (must be ≥ 2) |
| `NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS` | default retention age (default: 7 days) |
| `NARAD_ADDR` | (client only) base URL for `narad client` |

## Storage layer

A partition log is a *directory* of segment files. The active segment
receives writes; older segments are sealed (read-only). Each segment
is an append-only file made of CRC-checked, optionally zstd-compressed
*frames*. Each frame holds 1..N records that share a contiguous offset range:

```
┌─ frame ────────────────────────────────────────────────────────────┐
│ magic (2B) │ flags (1B) │ recordCount (4B) │ baseOffset (8B)       │
│ uncompressed (4B) │ compressed (4B) │ crc32c (4B)                  │
│ payload (compressed): [length:4][bytes] xN                         │
└────────────────────────────────────────────────────────────────────┘
```

**Produce path.** `Append` pushes the record into a per-partition
in-memory buffer and returns the assigned offset immediately. A single
flusher goroutine drains the buffer to disk when `flush_bytes`,
`flush_records`, or `flush_interval_ms` is crossed, and one final time
on graceful shutdown.

**Concurrency model.** Many writers, one flusher per partition file.
Producer goroutines contend only on the buffer's lightweight mutex.

**Durability.** Records produced before a flush are durable. Records
in the open flush window can be lost on a hard crash; this window is
bounded by the flush thresholds. Graceful shutdown always does a final
flush. Future data-plane replication will close the crash window by
requiring follower acknowledgement before the producer is notified.

**Recovery.** On startup the log file is scanned frame by frame. Corrupt
frames are skipped (scanner resyncs on the next valid magic). A torn
tail (interrupted write at EOF) is truncated to the last valid frame.

**Segments.**

```
data/topics/orders/p00007/
  00000000000000000000.log    (sealed, base offset 0)
  00000000000000010000.log    (sealed, base offset 10000)
  00000000000000020000.log    (active, base offset 20000)
```

## Metadata store (Raft + bbolt)

Topic configs, schemas, partition assignments, and cluster membership
are stored in a Raft-replicated bbolt database. Three pods act as Raft
voters; the rest are non-voters that receive the full replicated state.

- **Reads** are served from each pod's local bbolt replica — no network
  round-trip, ms-fresh staleness.
- **Writes** go through `raft.Apply` and require quorum. Control-plane
  write rate is very low (topic CRUD, partition assignments, heartbeats)
  so Raft overhead is negligible.
- **Peer discovery.** Pods register themselves via `bootstrap_peers`
  config (static list of voter addresses). On K8s, use a headless
  Service so pod DNS names are predictable:
  `narad-0.narad.default.svc.cluster.local:7943`.
- **Snapshots and log compaction** are handled automatically by the Raft
  library (`FileSnapshotStore`). The FSM snapshot is a full bbolt
  database copy.

## Cluster controller

The **controller** is always the Raft leader — no separate election. On
leadership takeover, `OnLeadershipAcquired` reconciles the FSM state:
finds topics with unassigned partitions and pods with missed heartbeats.

**Partition assignment.** When a topic is created, the controller assigns
each partition to the least-loaded active pod (sort by current partition
count, round-robin for ties). Assignments are **sticky** — without
data-plane replication, data lives only on the owning pod's disk, so
partitions cannot be moved automatically.

**Heartbeats.** Each pod calls `RegisterMember` (an upsert) every 5
seconds. If a pod's heartbeat is older than `DeadTimeout` (default 30s),
the controller marks it dead. Partitions owned by dead pods return 503
until the pod restarts and its persistent volume reattaches.

**Graceful shutdown.** If the leader calls `raft.LeadershipTransfer()`
before shutting down, a follower elects itself immediately (~150ms)
instead of waiting for the full heartbeat timeout + election window
(~300–600ms).

## Routing

Requests arrive at any pod via the load balancer. Each pod reads the
partition assignment from its local bbolt replica and either handles
the request locally (if it owns the partition) or proxies it to the
owner via the cluster port.

- **Produce**: key is hashed to a partition; request proxied if needed.
- **Consume**: a partition is chosen (random by default, lag-aware in
  v1 via peer `/metrics` scraping); request proxied to its owner with
  the partition pinned.
- **Ack**: partition is decoded from the receipt handle; request proxied
  if needed.

## Parallel consumers (single partition)

Narad's consume path supports SQS-style **gap-skipping reservation +
out-of-order acknowledgement**, so a single partition can feed many
concurrent consumer threads / pods without redelivery.

**The model:**

* `Consume` reserves the partition's lowest reachable offset that is
  neither already in flight nor sitting in the partition's "acked ahead"
  set, marks it invisible for `visibility_timeout_ms`, and returns the
  message with a `receipt_handle`.
* `Ack` decodes the handle, verifies the nonce against the active
  reservation, and commits. Acks for already-committed offsets, expired
  reservations, or re-reserved offsets return **410**.
* When an ack arrives in offset order it advances the partition's
  committed offset and walks forward through any contiguous run of
  previously out-of-order acks. Out-of-order acks sit in a sparse
  `ackedAhead` set per partition until the head catches up.

**Receipt handle format.** Handles are `base64url(json({t,p,o,n}))` where
`t`=topic, `p`=partition, `o`=offset, `n`=nonce. The nonce is generated
per-reservation — it proves the consumer received this specific instance
of the message. A forged handle with a wrong nonce returns **410**, not
**401** (there is no shared secret). Any pod can decode the partition
from the handle for routing without any shared key.

**Per-partition caps:**

| Cap | Default | Effect when reached |
|---|---|---|
| `max_in_flight_per_partition` | 1024 | `Consume` returns 204 (no message) |
| `max_acked_ahead_per_partition` | 1024 | `Ack` of out-of-order offset returns **503** |

**Ack error codes:**

| Status | Meaning |
|---|---|
| 204 | Acked. |
| 400 | Malformed handle, missing handle, or topic mismatch. |
| 410 | Handle no longer matches an active reservation: already committed, visibility timeout expired, or broker restarted. |
| 503 | Out-of-order ack rejected — `max_acked_ahead_per_partition` is full; head of queue is stuck. |

**Consumer pattern (CLI):**

```sh
while true; do
  msg=$(narad client consume --wait 5s orders) || break
  [ -z "$msg" ] && continue
  echo "$msg" | jq -r .receipt_handle | narad client ack orders
done
```

**Operational note.** The in-flight set is in-memory only. Restarting
the broker clears all reservations — clients see 410 on subsequent acks,
and the messages are redelivered after the visibility timeout. A
background purger goroutine sweeps expired reservations every second so
the in-flight cap and consumer lag metrics stay accurate between consume
calls.

## Observability

**`/metrics` (Prometheus).** Highlights:

* `narad_http_*` — requests by route/method/status, duration, bytes, in-flight.
* `narad_messages_{produced,consumed}_total{topic,partition}` and `bytes_*_total`.
* `narad_consume_wait_seconds{topic,outcome}` and `narad_consume_empty_total{topic}`.
* `narad_consumer_lag_messages{topic,partition}` and
  `narad_oldest_unconsumed_message_age_seconds{topic,partition}`.
* `narad_consumer_dropped_messages{topic,partition}` — unacknowledged
  messages deleted by retention (data loss indicator).
* `narad_storage_*` — flush/fsync/retention durations, segments rolled,
  bytes deleted.
* Inventory gauges: `narad_topics_total`, `narad_partitions_total`,
  `narad_topic_bytes{topic}`, `narad_segments{topic,partition}`.
* `narad_partition_available_v1{topic,partition}` — messages ready to
  deliver. **This metric name is load-bearing** — the cluster router
  scrapes peer `/metrics` endpoints to make lag-aware consume routing
  decisions. Do not rename it.

A 5-second background poller refreshes gauge-style metrics. Series for
deleted topics are pruned on the next tick.

**pprof.** Enable with `--pprof-addr 127.0.0.1:6060`. Bind to loopback
in production.

**Healthz / readyz.** `GET /healthz` is a fixed-200 liveness probe.
`GET /readyz` returns 200 if `broker.Ready` succeeds, 503 otherwise.

## Testing

```sh
make test                          # full suite, race detector on
go test ./cmd/narad                # CLI parity tests against a real in-process HTTP server
go test ./internal/...             # unit tests only
go test ./tests/e2e/... -race      # HTTP-level e2e against a real broker
go test ./tests/e2e/... -run TestConsume   # one feature
```

The CLI suite in `cmd/narad/client_test.go` exercises `narad client`
commands against a real in-process HTTP server and asserts stdout/stderr
behavior for topic CRUD plus produce/consume/ack flows.

The e2e harness (`helpers_test.go`) builds a real broker with a
single-node Raft metastore (waits for leader election) and temp partition
logs per test, exposed via `httptest`. Tests are split by feature surface.

## CI

`.github/workflows/ci.yml` runs three jobs in parallel on every push to
master and every PR:

For public-release readiness, `.github/settings.yml` also declares the
intended repository defaults:

- keep the repo private until validation is complete
- require PR reviews on `master`
- require the three CI checks to pass before merge
- allow squash merge
- disable merge commits and rebase merges
- delete branches after merge
- enable security scanning and Dependabot updates

That file is declarative repo configuration for settings-sync tools; it
makes the target public-repo posture reviewable without actually changing
repository visibility yet.


* **build** — `go vet ./...` + `go build ./...`
* **unit-tests** — `go test -race -count=1` for everything except `tests/e2e`
* **e2e-tests** — `go test -race -count=1 ./tests/e2e/...`

## Third-party libraries

Narad is built on top of a small set of well-known Go libraries:

- [`github.com/hashicorp/raft`](https://github.com/hashicorp/raft) — Raft consensus and cluster coordination
- [`github.com/hashicorp/raft-boltdb/v2`](https://github.com/hashicorp/raft-boltdb) — BoltDB-backed Raft log and stable store
- [`go.etcd.io/bbolt`](https://github.com/etcd-io/bbolt) — embedded metadata storage
- [`github.com/klauspost/compress`](https://github.com/klauspost/compress) — zstd compression for log segments
- [`github.com/santhosh-tekuri/jsonschema/v6`](https://github.com/santhosh-tekuri/jsonschema) — JSON-Schema validation
- [`github.com/prometheus/client_golang`](https://github.com/prometheus/client_golang) — Prometheus metrics and instrumentation

These libraries make it possible to keep Narad as a pure-Go project with
no CGO requirement.

## Design decisions

* **Pure-Go dependencies.** `hashicorp/raft` + `raft-boltdb` for
  consensus, `go.etcd.io/bbolt` for the FSM state, `klauspost/compress`
  for zstd, `santhosh-tekuri/jsonschema` for JSON-Schema validation,
  `prometheus/client_golang` for metrics. All pure Go; no CGO required.
* **Single binary with subcommands.** `narad serve|worker|client|version`
  follows the kubectl/etcd/consul convention.
* **Controller = Raft leader.** No separate controller election process —
  Raft's built-in leader election provides split-brain prevention for free.
* **Lazy expiry + background purger.** In-flight reservations expire via
  a min-heap; a background goroutine sweeps all shards every second.
  Lazy expiry during `ReserveNext` fixes the cap-blocking edge case
  immediately; the purger keeps metrics accurate between consume calls.
* **Operator endpoints separated from data endpoints.** `/metrics` shares
  the public listener (standard Prometheus convention); pprof is a
  separate, opt-in listener.

## Roadmap

* **Data-plane replication.** Leader→follower log sync (`sync to follower
  page cache, async fsync on both`), LEO/HWM distinction, log truncation
  on leader change. `replication.Local` stub is wired today.
* **`.offsets` log files.** Per-partition committed offset persistence
  (Kafka `__consumer_offsets` style): append-only log, replicated in the
  same stream as data, aggressively compacted to keep only the latest.
  `onCommit` callback in `InFlight` is ready to wire this up.
* **Cross-AZ replication (RF=3).** `replication_factor` is already in
  the topic API; RF=3 support requires the replication protocol above.
* **Consume routing v1.** Lag-aware weighted-random partition pick via
  peer `/metrics` scraping (infrastructure is built; wiring pending).
* **Auth, rate limiting.**
* **Schema evolution** — backwards-compatibility checking on
  `PATCH /v1/topics/{name}` with schema field.

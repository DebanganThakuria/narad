# Narad

[![CI](https://github.com/DebanganThakuria/narad/actions/workflows/ci.yml/badge.svg)](https://github.com/DebanganThakuria/narad/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/DebanganThakuria/narad)](./go.mod)

<p align="center">
  <img src="./assets/narad.png" alt="Narad logo: durable messages, timeless connections" width="420">
</p>

Narad is a lightweight, queue-first event streaming system built in Go.
It provides durable append-only storage, HTTP produce/consume/ack APIs,
Raft-backed metadata, Prometheus metrics, and a single static binary that
is straightforward to run locally or in Kubernetes.

Narad is currently **pre-1.0**. It is suitable for local evaluation,
experimentation, and early integration work. It should not be exposed
directly to untrusted networks or used as a production dependency until
the production-readiness gates documented below are complete.

## Overview

Narad is designed for teams that want a small, understandable event
system with queue semantics and explicit operational tradeoffs. Producers
send raw message bodies to topics; consumers pull with optional
long-polling, acknowledge with receipt handles, and can replay from
explicit offsets. Topics can opt into JSON-Schema validation when they
need JSON enforcement.

Narad's core vision is to provide a simple queue-like interface backed by
durable logs: applications get familiar produce, consume, and ack
workflows, while operators retain replayability and log-based recovery
semantics.

Core capabilities:

- durable append-only segmented storage
- queue-style consume/ack semantics with replay support
- parent → child topic fan-out with per-key ordering
- Raft-backed control-plane metadata
- any-pod produce ingress with async owner dispatch
- JSON-Schema validation and per-topic tuning
- Prometheus metrics and operational debugging hooks

## Project status

Narad is under active development. The current public release line starts
at `v0.1.0-alpha.1`.

| Area | Status |
|---|---|
| API and CLI | Functional, pre-1.0 compatibility |
| Storage engine | WAL-first produce path and segmented partition logs |
| Cluster metadata | Raft+bbolt control plane |
| Delivery model | At-least-once; consumers must be idempotent |
| Security | Basic auth + RBAC + cluster-secret auth (secure by default); TLS terminates at the ingress; no rate limiting yet |
| Production use | Blocked on the readiness gates below |

CI runs race-enabled unit and end-to-end tests, plus local 3-node
integration and chaos smoke tests.

## Observed benchmark

One Kubernetes benchmark run on a 3-node Narad cluster sustained about
**50,000 logical messages per second** through the full
produce -> consume -> ack loop. Each Narad pod used roughly **500 MiB**
of memory and was capped around **5 vCPUs**.

This is an observed benchmark, not a committed SLO. Results depend on
payload shape, topic and partition layout, client concurrency, storage
class, kernel and container limits, and garbage collector settings.

Benchmark environment highlights:

| Setting | Value |
|---|---|
| Cluster size | 3 Narad pods |
| Per-pod CPU runtime | `GOMAXPROCS=5` |
| Per-pod memory target | `GOMEMLIMIT=1GiB` |
| GC target | `GOGC=400` |
| Default partitions | `NARAD_TOPIC_DEFAULT_PARTITIONS=3` |
| Max partitions | `NARAD_TOPIC_MAX_PARTITIONS=108` |
| In-flight cap | `NARAD_TOPIC_DEFAULT_MAX_IN_FLIGHT_PER_PARTITION=1024` |
| Acked-ahead cap | `NARAD_TOPIC_DEFAULT_MAX_ACKED_AHEAD_PER_PARTITION=1024` |
| Visibility timeout | `NARAD_TOPIC_DEFAULT_VISIBILITY_TIMEOUT_MS=30000` |
| Retention age | `NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS=43200000` |
| Log format | `NARAD_LOG_FORMAT=json`, `NARAD_LOG_LEVEL=info` |

## Community and project docs

- [Contributing guide](./CONTRIBUTING.md)
- [Code of conduct](./CODE_OF_CONDUCT.md)
- [Security policy](./SECURITY.md)
- [Support policy](./SUPPORT.md)
- [PCA flow diagrams](./PCA_FLOWS.md)
- [License](./LICENSE)

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
                         │  ingress WAL      │  ingress WAL      │
                         │  (disk, PV)       │  (disk, PV)       │
                         └───────────────────┴───────────────────┘
                                  ↑ accepted records dispatch to owner
                                    over the cluster port (7943)
```

**Control plane.** Three Raft voters (e.g. `narad-0..2`) form a consensus
cluster; remaining pods are non-voters that receive the full replicated
state. The Raft leader acts as the **controller** — it assigns partitions
when topics are created and marks pods dead when their heartbeats time out.
Every pod's local bbolt replica serves metadata reads (topic config,
partition assignment, member address) without a network round-trip.

**Data plane.** Each partition is owned by one pod. That pod holds the
segment files on its persistent volume and manages the in-flight
reservation table in memory. Produce can hit any pod: the ingress pod
validates the request, writes it to its single local ingress WAL, and
returns `202 Accepted` with no response body. A background dispatcher
moves accepted records to the selected owner partition, where they
become visible to consumers.
Consume and ack still route/probe owners through the internal cluster
port when the local pod cannot serve the request.

**Durability (no replication).** Narad does not replicate partition
data: each partition has a single owner whose durable log is the sole
copy. Durability comes from two synchronous fsync points. (1) Produce
durability means the record exists in the receiving pod's ingress WAL on
a durable volume, fsynced before `202 Accepted`. (2) When the background
dispatcher copies a WAL record to the owner partition log, it
synchronously fsyncs the segment and reads the record back to validate
its frame CRC *before* the record is made visible (high-watermark
advanced) and *before* the WAL is allowed to compact past it — so a
record is never lost or served corrupt as long as the owner's volume
survives. If a pod dies but its volume survives, both logs replay on
restart.

## Layout

```
cmd/
  narad/                    single binary: main, serve, client, version
internal/
  errs/                     sentinel errors in one place (ErrTopicNotFound, ...)
  domain/
    topic/                  value types (Topic, Message, PartitionStats)
  persistence/
    storage/                append-only segmented log (see doc.go for file map)
      codec/                Codec interface + zstd / noop implementations
    metastore/              Raft+bbolt metadata store: topics, schemas,
                            partition assignments, cluster members
  consumer/                 in-flight reservations, nonce-based receipt handles,
                            min-heap expiry, out-of-order ack tracking
  broker/                   orchestrator facade (impl.go embeds the managers)
    ingress/                produce ingress WAL (fsynced accept; dispatch source)
    runtime/                *Logs (lazy partition-log map), *Snapshotter,
                            *Lifecycle, startup orphan sweep
    topics/                 CreateTopic / Update* / Delete / Get / List
    messaging/              Produce / Consume / Ack
  cluster/                  any-pod routing + node-to-node RPC
    router.go               RouteProduce/Consume/Ack, BroadcastDeleteTopic
    produce_dispatcher.go   drains the ingress WAL to partition owners
    fanout_runner.go        parent→child fan-out: cursor reconciler
    fanout_cursor.go        per-(child, parentPartition) fan-out cursor loop
    rpc_server.go           node-RPC server (handles forwarded ops)
    rpc_client.go           peer client (forwards ops over QUIC)
    controller/             partition assignment, heartbeat monitor,
                            OnLeadershipAcquired reconciliation, Heartbeater
  transport/
    httpserver/             http.Server, router, middleware
      handlers/             *Set + shared helpers (WriteJSON, DecodeJSON, ...)
        topics/             /v1/topics CRUD + fan-out children endpoints
        messaging/          produce / consume / ack endpoints
        health/             /healthz, /readyz
  protocol/
    clusterwire/            cluster stream-framing protocol (node-RPC over QUIC)
    node/                   node-RPC operations + request/response codecs
  platform/
    clusterrpc/             QUIC transport for the cluster (node-to-node)
    config/                 defaults + JSON file + env + flag layering
    schema/                 JSON-Schema registry (santhosh-tekuri)
    partition/              FNV hash + round-robin partition picker
    httpclient/             shared HTTP client
    netaddr/                address parsing / normalization helpers
    observability/
      logger/               thin log/slog wrapper
      metrics/              Prometheus collectors, HTTP middleware, lag poller
tests/
  e2e/                      HTTP-level end-to-end tests (httptest + real broker)
.github/
  workflows/ci.yml          build · unit · e2e · cluster integration · chaos
  workflows/container.yml   multi-arch GHCR image publish
```

## Quickstart

Install the current alpha with Go:

```sh
go install github.com/debanganthakuria/narad/cmd/narad@v0.1.0-alpha.1
narad serve
```

Or build from a source checkout:

```sh
make build         # produces bin/narad
bin/narad serve    # listens on :7942 (API) and :7943 (cluster/Raft)
```

Security is on by default: the server seeds a root `admin` user at first
start (a random password is logged once, or set `NARAD_ADMIN_PASSWORD`)
and every API call requires HTTP Basic auth. For local experimentation
you can disable it with `NARAD_SECURITY_ENABLED=false`. TLS is expected to
terminate at an ingress in front of Narad; bind the plaintext listener
only to loopback or a trusted network.

### Container Image

Narad ships as a single static binary inside a small non-root container
image. Published images are available from GHCR:

```sh
docker pull ghcr.io/debanganthakuria/narad:v0.1.0-alpha.1
docker run --rm -p 7942:7942 -p 7943:7943 ghcr.io/debanganthakuria/narad:v0.1.0-alpha.1
```

For local image development:

```sh
make docker-build
make docker-push
```

The CI workflow in `.github/workflows/container.yml` publishes multi-arch
images to `ghcr.io/debanganthakuria/narad` on `master`, version tags, and
manual workflow dispatches. The default branch also gets the `latest` tag.

The image starts `narad serve` by default, listens on `7942` for the public API
and `7943` for cluster traffic, and uses `/var/lib/narad` as the data
directory. Mount the StatefulSet PVC at that path. In Kubernetes, run the pod
with `securityContext.fsGroup: 10001` so the non-root `narad` user can write to
the mounted volume.

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
  --partitions 8 \
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
  -d '{"name":"orders","partitions":8,
       "retention_ms":3600000,"visibility_timeout_ms":30000,
       "max_in_flight_per_partition":64,"max_acked_ahead_per_partition":256}'

# Produce. The body is the message; key and partition are optional query params.
curl -X POST 'localhost:7942/v1/topics/orders/produce?key=customer-42' \
  -H 'Content-Type: application/octet-stream' \
  --data-binary '{"id":1,"amount":1500}'

# Response: 202 Accepted with an empty body.

# Consume with long-poll. Response includes a receipt_handle.
curl 'localhost:7942/v1/topics/orders/consume?wait=5s'

# Ack — handle is the token returned by Consume.
curl -X POST 'localhost:7942/v1/topics/orders/ack?receipt_handle=<token from consume response>'

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
POST    /v1/topics/{topic}/produce?key=&partition=
                                             raw body; 202 Accepted with an empty body
GET     /v1/topics/{topic}/consume          response carries receipt_handle
POST    /v1/topics/{topic}/ack?receipt_handle=...
GET     /healthz                            liveness
GET     /readyz                             readiness (broker.Ready)
GET     /metrics                            Prometheus exposition
```

## CLI surface

```
narad serve     run the HTTP API server (default port 7942)
narad client    interact with a running narad serve over HTTP
narad version   print build version
narad --help    top-level help
```

Common `narad serve` flags:

```
--port 7942                  override the API listen port
--addr 0.0.0.0:7942          override the API listen address (host+port)
--cluster-port 7943          override the cluster listen port
--node-id narad-0            stable Raft node ID (required for local multi-node runs)
--config narad.json          path to JSON config file (optional)
--data-dir ./data            storage directory
--log-level info             debug | info | warn | error
--log-format json            json | text
--pprof-addr 127.0.0.1:6060  enable pprof on this address; empty disables
```

For a real 3-node local cluster, run each process with a unique API port, cluster port, data dir, and `--node-id`, and give all three the same 3-voter peer list via `NARAD_CLUSTER_PEERS`.

```sh
NARAD_CLUSTER_PEERS='narad-0@127.0.0.1:9101,narad-1@127.0.0.1:9102,narad-2@127.0.0.1:9103' ./bin/narad serve --addr 127.0.0.1:7942 --cluster-port 9101 --node-id narad-0 --data-dir ./data/narad-0
NARAD_CLUSTER_PEERS='narad-0@127.0.0.1:9101,narad-1@127.0.0.1:9102,narad-2@127.0.0.1:9103' ./bin/narad serve --addr 127.0.0.1:7944 --cluster-port 9102 --node-id narad-1 --data-dir ./data/narad-1
NARAD_CLUSTER_PEERS='narad-0@127.0.0.1:9101,narad-1@127.0.0.1:9102,narad-2@127.0.0.1:9103' ./bin/narad serve --addr 127.0.0.1:7945 --cluster-port 9103 --node-id narad-2 --data-dir ./data/narad-2
```

Then point the client at any node:

```sh
NARAD_ADDR=http://127.0.0.1:7942 narad client topics create orders
NARAD_ADDR=http://127.0.0.1:7944 narad client topics get orders
```

`NARAD_CLUSTER_PEERS` and `cluster.peers` must contain exactly three `id@host:port` voters including the local node, and `cluster.node_id` / `--node-id` must be set whenever peers are configured.

If you leave peers unset, Narad keeps the single-node bootstrap path.

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
  "http":    { "addr": ":7942", "read_timeout": "10s", "max_consume_wait": "10s" },
  "cluster": {
    "addr": "127.0.0.1:9101",
    "node_id": "narad-0",
    "peers": [
      { "id": "narad-0", "addr": "127.0.0.1:9101" },
      { "id": "narad-1", "addr": "127.0.0.1:9102" },
      { "id": "narad-2", "addr": "127.0.0.1:9103" }
    ]
  },
  "storage": { "data_dir": "data" },
  "topic": {
    "default_partitions": 3,
    "max_partitions": 108,
    "default_retention_age_ms": 604800000
  },
  "log":     { "level": "info", "format": "json" }
}
```

Useful environment overrides:

| Variable | Effect |
|---|---|
| `NARAD_HTTP_ADDR` | API listen address |
| `NARAD_HTTP_PPROF_ADDR` | optional pprof listen address |
| `NARAD_HTTP_MAX_CONSUME_WAIT` | server-side cap for long-poll consume wait |
| `NARAD_CLUSTER_ADDR` | Cluster listen address |
| `NARAD_NODE_ID` | stable local Raft node ID |
| `NARAD_CLUSTER_PEERS` | comma-separated 3-voter list as `id@host:port` |
| `NARAD_DATA_DIR` | Storage directory |
| `NARAD_LOG_LEVEL` | `debug` / `info` / `warn` / `error` |
| `NARAD_LOG_FORMAT` | `json` / `text` |
| `NARAD_TOPIC_DEFAULT_PARTITIONS` | default partition count when omitted from CreateTopic |
| `NARAD_TOPIC_MAX_PARTITIONS` | upper bound for partition count |
| `NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS` | default retention age (default: 7 days; every topic has a 1-hour minimum, 0 = keep forever) |
| `NARAD_TOPIC_DEFAULT_VISIBILITY_TIMEOUT_MS` | default queue visibility timeout |
| `NARAD_TOPIC_DEFAULT_MAX_IN_FLIGHT_PER_PARTITION` | default reservation cap per partition |
| `NARAD_TOPIC_DEFAULT_MAX_ACKED_AHEAD_PER_PARTITION` | default out-of-order ack cap per partition |
| `NARAD_FANOUT_MAX_BATCH_RECORDS` | records per fan-out batch (default 4096) |
| `NARAD_FANOUT_MAX_BATCH_BYTES` | payload bytes per fan-out batch (default 4 MiB) |
| `NARAD_FANOUT_LINGER_MS` | fan-out batch linger before a partial batch commits (default 25) |
| `NARAD_ADDR` | (client only) base URL for `narad client` |

The JSON config intentionally exposes only `storage.data_dir`,
`storage.codec`, and `storage.compression_level` from the storage layer.
Environment variables expose only `NARAD_DATA_DIR` for storage. Flush,
fsync, WAL sync, and segment-layout tuning are engine internals with
production defaults; tuning them per deployment tends to create
hard-to-debug behavior differences.

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

**Produce ingress path.** HTTP produce reads the request body as the raw
message payload. Optional metadata (`key`, `partition`) comes from query
params. For topics without a schema, Narad does not parse or validate the
payload as JSON; it appends the bytes to the receiving pod's ingress WAL
and returns `202 Accepted` with an empty body. Schema-enabled topics
validate the same raw body before acceptance. A background dispatcher
replays durable WAL records and commits them to the selected owner
partition. The WAL record ID is internal checkpointing only; clients do
not receive a message ID, partition, or offset from produce.

**Partition append path.** Owner-side append pushes the record into a
per-partition in-memory buffer and returns the assigned offset to the
dispatcher. A single flusher goroutine drains the buffer to disk when
Narad's built-in batch thresholds are crossed, and one final time on
graceful shutdown.

**Concurrency model.** Many writers, one flusher per partition file.
Producer goroutines contend only on the buffer's lightweight mutex.

**Durability.** Produce durability is provided by the ingress WAL on the
receiving pod's persistent volume. Partition-log durability is a second
stage: once the dispatcher commits a record to the owner partition, the
partition flusher writes and syncs frames in batches. Records accepted
to WAL but not yet visible are replayed after process restart as long as
the ingress pod's volume survives. Graceful shutdown always does a final
partition-log flush and sync.

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
- **Peer discovery.** Each voter starts with a static 3-node peer list via
  `cluster.peers` or `NARAD_CLUSTER_PEERS`, using `id@host:port` entries.
  On K8s, use stable pod IDs and a headless Service so pod DNS names are
  predictable, for example
  `narad-0@narad-0.narad.default.svc.cluster.local:7943`.
- **Snapshots and log compaction** are handled automatically by the Raft
  library (`FileSnapshotStore`). The FSM snapshot is a full bbolt
  database copy.

## Cluster controller

The **controller** is always the Raft leader — no separate election. On
leadership takeover, `OnLeadershipAcquired` reconciles the FSM state:
finds topics with unassigned partitions and pods with missed heartbeats.

**Partition assignment.** When a topic is created, the controller assigns
each partition to an active pod by round-robin over the (ID-sorted)
active members. Each partition has a single owner — there is no follower.
Assignments are **sticky**: a partition whose owner is currently dead is
**not** reassigned, because the partition's data lives only on that
owner's disk; it waits for the owner to restart with its volume
reattached. Accepted messages live first in the ingress pod's WAL, then
in the owner partition log after dispatch.

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

- **Produce**: keyed records are hashed to a partition; unkeyed records
  rotate across partitions. The receiving pod accepts the request into
  its ingress WAL, then the dispatcher forwards it to the owner when
  needed.
- **Consume**: a live-owner partition is chosen by round-robin rotation —
  partitions whose owner is currently dead are skipped — and the request
  is proxied to that owner with the partition pinned. (Lag-aware
  selection is future work; see Production readiness.)
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

**Receipt handle format.** Handles are `partition:offset:nonce`. The
topic comes from the ack request path. The nonce is generated
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
| 400 | Malformed or missing handle. |
| 404 | Topic does not exist. |
| 410 | Handle no longer matches an active reservation: already committed, nacked, visibility timeout expired, or broker restarted. |
| 421 | This node does not own the handle's partition (ownership changed mid-flight); retry. |
| 503 | Out-of-order ack rejected — `max_acked_ahead_per_partition` is full; head of queue is stuck. |

**Lease operations on the ack endpoint.** The optional `extend` query
parameter turns an ack into a lease operation on the same receipt
handle, with identical validation (a lapsed handle gets **410**):

| Request | Effect |
|---|---|
| `POST .../ack?receipt_handle=H` | Ack: commit the message. |
| `POST .../ack?receipt_handle=H&extend=true` | Extend: renew the visibility window to a full fresh window (`now + visibility_timeout_ms`). The handle stays valid — heartbeat this while processing runs long, then ack as usual. |
| `POST .../ack?receipt_handle=H&extend=0` | Nack: release the reservation immediately (SQS-style visibility zero). The message is redeliverable right away under a new handle; parked long-pollers are woken. |

CLI: `narad client ack --extend <topic>` / `narad client ack --nack
<topic>`. Extensions and nacks are counted on
`narad_ack_extended_total` and `narad_nack_total`.

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

## Schema validation & evolution

A topic can carry an optional JSON Schema, set at create time or via
`PATCH /v1/topics/{topic}` with a `schema` field. When present, every
produced message body must be valid JSON and match the schema; invalid
payloads are rejected with **400**. Topics without a schema treat
produced bodies as opaque bytes.

Schema changes are **additive-only and backwards-compatible**, enforced
at registration. You may add optional properties; but removing a
property, changing the type of an existing property, dropping a
previously-required field, or adding a new required field is rejected
with `ErrSchemaIncompatible`. The contract is "extend only, never break —
once a field exists, it stays."

## Fan-out (parent → child topics)

Fan-out lets a **parent** topic replicate every message it receives into
one or more **child** topics. Producing to a parent behaves exactly like
producing to a normal topic; in addition, each attached child
independently receives a copy, re-keyed with the child's own
partitioner so **per-key ordering is preserved within each child**.
This is not consumer groups — children are independent topics with
their own partitions, offsets, retention, and consumers.

```
POST   /v1/topics/{parent}/children            attach: {"child": "name", "delay_ms": 0}
GET    /v1/topics/{parent}/children            list children + delay + per-child lag
DELETE /v1/topics/{parent}/children/{child}    detach

narad client topics attach [--delay-ms N] <parent> <child>
narad client topics children <parent>
narad client topics detach <parent> <child>
```

Attach/detach are admin-or-owner operations on the parent. Roles are
exclusive and flat: a topic is `standalone`, `parent`, or `child`
(reported by topic describe); fan-out is depth 1, a child has exactly
one parent, and a parent holds at most 108 children. All link
invariants are enforced atomically in the Raft metastore, so no
sequence of attach/detach/delete can ever observe a half-linked pair.

**How it works.** Fan-out materializes the parent and then tails its
committed log ("model C"): a produce to a parent is byte-for-byte a
normal produce, so the hot path pays nothing. One **cursor** exists per
(child, parent-partition); it runs on the node that owns the parent
partition, reads large slabs of committed records locally
(fill-or-linger batching), re-keys each record with the child's
partitioner, and commits one batch per touched child partition through
the same local/remote commit paths every produce dispatch uses. The
cursor's offset is persisted in the parent partition's directory and
advances only **after** the child batch is durably committed
(commit-before-advance), which is what makes delivery at-least-once.
Each attach stamps the link with a fresh epoch that scopes all cursor
state, so a detach followed by a re-attach can never resume — and
replay — a dead cursor. Cursors spread across the cluster with the
parent partition assignments, so the write amplification divides by
cluster size.

Semantics worth knowing:

* **At-least-once, no backfill.** A child receives messages produced
  from the moment its cursors anchor at the parent's tail — within
  about a second of the attach; `lag_complete: true` in the
  list-children response signals the anchor. Delivery is
  at-least-once — a crash
  mid-batch re-delivers, never loses. Detach stops the flow and keeps
  everything already delivered; a later re-attach starts fresh at the
  parent's tail (no replay of the detached window). Deleting a parent
  detaches its children; they live on standalone.
* **The parent's retained log is the fan-out buffer.** A slow or dead
  child lags without affecting the parent or its siblings. If it falls
  behind the parent's retention, the cursor **drops behind** to the
  oldest retained record and the loss is counted on
  `narad_fanout_child_dropped_messages` — alert on any non-zero rate.
  To bound that failure mode, **every topic has a minimum effective
  retention of one hour**.
* **Schemas are inherited.** At attach, the child's schema must be
  absent (it adopts the parent's, full history) or byte-identical to
  the parent's; anything else is rejected with **409**. While attached,
  the child's schema is parent-managed: parent schema evolution
  propagates atomically to every child, and direct schema changes on
  the child are rejected. On detach the child keeps its schema.
* **No fan-out RBAC gate.** Fan-out is the topic's configured behavior:
  an attached child receives messages regardless of the producing
  user's grants. Producing to the parent and consuming from a child are
  still governed by normal RBAC.
* **Delay children.** Attaching with a positive `delay_ms` makes the
  child a **delay topic**: every record is delivered only once
  `parentCommitTime + delay_ms` has passed — retry-backoff tiers,
  scheduled reprocessing, and "give the auditor the 1h view" fall out
  of one flag. Because commit times are monotonic per partition, the
  cursor's due check is a head-peek-and-sleep — O(1) while idle no
  matter how much is pending, and the pending backlog costs disk, not
  memory. The parent's retained log is the delay buffer, so the parent
  must retain `delay_ms + 1h` (enforced at attach AND when shrinking
  the parent's retention). Direct produce to a delayed child returns
  **409** — the delay is a guarantee of the topic, not of one write
  path. The delay is immutable while attached (detach and re-attach to
  change it) and is per-topic, not per-message. Note that a delay
  child's offset lag is permanently non-zero by design; alert on
  `narad_fanout_due_lag_seconds` (how far behind the DUE frontier the
  cursor runs) instead.
* **Capacity.** A parent sustaining `R` msg/s with `C` children costs
  roughly `R × (C + 1)` commits/s across the cluster: size the cluster
  to the fanned-out rate. Fan-out batches large slabs per child
  partition (one fsync per batch; `fanout.max_batch_records`,
  `fanout.max_batch_bytes`, `fanout.linger_ms` tune the
  latency/throughput trade), and cursors spread across the cluster with
  the parent partition assignments.

Health signals: `narad_fanout_lag_messages{parent,child,partition}` is
the primary one for immediate children (also surfaced as
`lag_messages` in the list-children API),
`narad_fanout_due_lag_seconds` is the one for delay children, plus
`narad_fanout_committed_total` and the batch-size histograms.

## Security (authentication & RBAC)

Narad ships **secure by default**: every HTTP API call requires HTTP
Basic authentication, requests are authorized against per-user grants,
and node-to-node cluster RPC requires a shared secret. Disable it for
local development with `security.enabled=false` (`NARAD_SECURITY_ENABLED`).

**TLS.** Narad speaks plain HTTP and expects TLS to terminate at an
ingress or gateway in front of it. Because Basic credentials travel in
cleartext from that terminator to Narad, the hop between them must be a
trusted network path.

**Users.** A user is a username plus a bcrypt-hashed password and a list
of grants, replicated through the Raft metastore. Manage them via the
admin-only `/v1/users` API:

* `POST /v1/users` — create `{username, password, grants}`
* `GET /v1/users`, `GET /v1/users/{username}` — list / describe (hashes
  are never returned)
* `DELETE /v1/users/{username}`
* `PUT /v1/users/{username}/grants` — replace grants
* `PUT /v1/users/{username}/password` — change password (self-service
  with `current_password`, or admin reset)

**Grants.** Each grant is an action over topic name patterns (a literal
name or a single trailing `*` wildcard, e.g. `orders-*`):

* `produce` — produce to matching topics
* `consume` — consume from and ack matching topics
* `create` — create topics whose name matches; the creator becomes the
  topic **owner**
* `admin` — full access, including user management and altering/deleting
  any topic (carries no patterns)

Topic **alter/delete** require the topic's owner or an admin.

**Root admin.** On first boot with no users, one node seeds a root
`admin` account from `NARAD_ADMIN_PASSWORD`; if unset, it generates a
random password and logs it **once** (`component=audit`). The root
account is undeletable and its grants are immutable — only its password
can change. No account may grant a permission it does not hold, edit its
own grants, or delete itself, and only root may confer the `admin`
action (so admin rights cannot proliferate through delegation).

**Hot path.** Password verification (bcrypt, deliberately slow) is cached
per node and keyed by the users-domain version, so the produce/consume
path never re-hashes. Grant changes take effect on the next request;
password changes cut access immediately. A negative cache, a decaying
per-user failed-attempt throttle, and request de-duplication bound the
cost of wrong or repeated credentials.

**Cluster port.** When security is on and peers are configured,
`NARAD_CLUSTER_SECRET` is required. Each QUIC stream proves knowledge of
the secret (an HMAC, never the raw secret) before the server serves any
request, closing the "reach the port, skip RBAC" bypass. Mutual TLS
between nodes is planned; today the secret rides inside the transport's
existing TLS. Every user mutation and rejected auth emits a structured
`component=audit` log line.

## Observability

**`/metrics` (Prometheus).** Highlights:

* `narad_http_*` — requests by route/method/status, duration, bytes, in-flight.
* `narad_messages_{produced,consumed}_total{topic,partition}` and `bytes_*_total`.
* `narad_consume_wait_seconds{topic,outcome}` and `narad_consume_empty_total{topic}`.
* `narad_consumer_lag_messages{topic,partition}` and
  `narad_oldest_unconsumed_message_age_seconds{topic,partition}`.
* `narad_consumer_dropped_messages{topic,partition}` — unacknowledged
  messages deleted by retention (data loss indicator).
* `narad_storage_*` — flush/fsync/high-watermark persistence/retention
  durations, bytes and messages deleted by retention.
* Inventory gauges: `narad_topics_total`, `narad_partitions_total`,
  `narad_topic_bytes{topic}`, `narad_partition_size_bytes{topic,partition}`,
  `narad_segments{topic,partition}`.

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

`.github/workflows/ci.yml` runs five jobs in parallel on every push to
`master` and every PR (all race-enabled):

* **Build & vet** — `go vet ./...` + `go build ./...`
* **Unit tests** — `go test -race -count=1` for everything except `tests/e2e`
* **E2E tests** — `go test -race -count=1 ./tests/e2e/...`
* **Local cluster integration** — boots a real 3-node cluster and exercises
  topic CRUD + produce/consume/ack across nodes
* **Local cluster chaos** — 3-node smoke test under induced node failures

`.github/workflows/container.yml` publishes the multi-arch GHCR image on
`master`, version tags, and manual dispatch.

`.github/settings.yml` documents the intended public repository defaults:
PR review, required checks on `master`, squash-only merges,
delete-branch-on-merge, security scanning, and Dependabot.

## Third-party libraries

Narad is built on top of a small set of well-known Go libraries:

- [`github.com/hashicorp/raft`](https://github.com/hashicorp/raft) — Raft consensus and cluster coordination
- [`github.com/hashicorp/raft-boltdb/v2`](https://github.com/hashicorp/raft-boltdb) — BoltDB-backed Raft log and stable store
- [`go.etcd.io/bbolt`](https://github.com/etcd-io/bbolt) — embedded metadata storage
- [`github.com/quic-go/quic-go`](https://github.com/quic-go/quic-go) — QUIC transport for node-to-node cluster RPC
- [`github.com/klauspost/compress`](https://github.com/klauspost/compress) — zstd compression for log segments
- [`github.com/santhosh-tekuri/jsonschema/v6`](https://github.com/santhosh-tekuri/jsonschema) — JSON-Schema validation
- [`github.com/prometheus/client_golang`](https://github.com/prometheus/client_golang) — Prometheus metrics and instrumentation

These libraries make it possible to keep Narad as a pure-Go project with
no CGO requirement.

## Design decisions

* **Pure-Go dependencies.** `hashicorp/raft` + `raft-boltdb` for
  consensus, `go.etcd.io/bbolt` for the FSM state, `quic-go` for the
  node-to-node cluster RPC transport, `klauspost/compress` for zstd,
  `santhosh-tekuri/jsonschema` for JSON-Schema validation,
  `prometheus/client_golang` for metrics. All pure Go; no CGO required.
* **Single binary with subcommands.** `narad serve|client|version`
  keeps the local and deployment surface small.
* **Controller = Raft leader.** No separate controller election process —
  Raft's built-in leader election provides split-brain prevention for free.
* **Lazy expiry + background purger.** In-flight reservations expire via
  a min-heap; a background goroutine sweeps all shards every second.
  Lazy expiry during `ReserveNext` fixes the cap-blocking edge case
  immediately; the purger keeps metrics accurate between consume calls.
* **Fan-out pays its amplification off the hot path.** Producing to a
  fan-out parent is a normal produce; children tail the parent's
  committed log with per-(child, partition) cursors on the parent
  partition owners. The parent's retained log doubles as the fan-out
  buffer (bounded by retention — hence the uniform 1-hour retention
  floor), a lagging child stalls only itself, and cursor offsets
  advance only after the child batch is durably committed.
* **Operator endpoints separated from data endpoints.** `/metrics` shares
  the public listener (standard Prometheus convention); pprof is a
  separate, opt-in listener.

## Brand assets

The project logo and identity sheet live under [`assets/`](./assets/).

<p align="center">
  <img src="./assets/narad-design.png" alt="Narad visual identity sheet with logo variants, icons, and design notes" width="720">
</p>

## Production readiness

Narad is pre-1.0. The code has race-enabled unit and end-to-end coverage,
plus 3-node integration and chaos smoke tests in CI, but the following
work must land before a production or externally exposed rollout. The
first is a **hard gate**; until it ships, run Narad only behind a
trusted boundary.

1. **TLS & rate limiting (hard gate).** Authentication (Basic + RBAC)
   and cluster-secret auth now ship (see [Security](#security-authentication--rbac)),
   but Narad still serves plain HTTP: TLS must terminate at an ingress in
   front of it, and that ingress→Narad hop must be a trusted path. Native
   TLS, mutual TLS between cluster nodes, and per-user/IP request rate
   limiting remain future work.
2. **Durability / DR contract (hard gate).** With no data replication
   (one owner per partition; its volume is the only copy), we need a
   *tested* backup/restore runbook for every loss scenario — metastore
   quorum loss and voter replacement, partition-log/WAL volume snapshot +
   restore-by-reattach, and the "volume lost = bounded, documented data
   loss" procedure — with stated RPO (≈ fsync cadence) and RTO (≈ restart
   + recovery scan).
3. **Liveness-aware routing (finish the HA model).** The baseline already
   works: consume rotates only over live owners and skips dead-owner
   partitions, partition assignment is sticky (a dead owner's partition
   waits for it to return rather than being reassigned), and the produce
   dispatcher treats a dead owner as unavailable. Remaining: lag-aware
   partition selection (today it is plain rotation), return a retryable
   **503** for a pinned / dead-owner consume + ack (today that path can
   surface **421**), and fix cursor advance on empty polls so a busy
   partition cannot starve others. (Keyed produce to a dead partition is
   inherently unavailable — to be documented.)
4. **Partition rebalance / scale-out contract.** New Narad pods can join
   the cluster, but existing partition ownership is sticky today; new
   capacity does not automatically absorb existing partitions. Before
   production scale-out, add an explicit rebalance protocol: choose
   candidate partitions, drain or pause writes safely, move/copy the
   partition log plus consumer offset state, verify high-watermark and
   frame CRCs on the destination, atomically update ownership, and roll
   back cleanly on failure. This needs load-aware placement, operator
   controls, metrics, and tests for adding/removing pods under traffic.
5. **Soak, SLOs & capacity.** Define and prove the numbers over time:
   produce-accept p99, produce→visible p99, consume p99, max sustainable
   throughput, max lag, recovery time. Quantify the synchronous
   fsync + CRC-readback cost on the commit path. Multi-hour soak under
   fault injection against the SLOs, wired to Grafana + alerts.
6. **Upgrade/rollback contract.** Current pre-1.0 internal builds use
   current-only on-disk and node-RPC formats; incompatible internal
   changes may require wiping development data. Before 1.0, add explicit
   version headers + migrations for durable formats and version-negotiate
   cluster RPC / QUIC ALPN so N and N+1 coexist during rolling upgrades.
   Then freeze the HTTP API + receipt-handle + wire contracts and cut
   **1.0**.

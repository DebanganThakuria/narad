# Narad

> 📚 **Documentation:** [debanganthakuria.github.io/narad](https://debanganthakuria.github.io/narad/) — Client Guide and Internals, with diagrams.

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

Narad is at **[v1.0.0](https://github.com/DebanganThakuria/narad/releases/tag/v1.0.0)** —
graduated after a chaos matrix, multi-day soak windows (300M+ messages at
1,000 msg/s with zero loss), a 50,000 msg/s full-flow bench, and live
backup/restore, mixed-version-upgrade, and offset-replay drills. TLS is
expected to terminate at an ingress in front of Narad; keep the
ingress→Narad hop on a trusted network.

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
- parent → child topic fan-out with per-key partition affinity
- opt-in async replication: create a child with `parent` and its
  partitions are placed on different nodes than the parent's
- Raft-backed control-plane metadata
- any-pod produce ingress with async owner dispatch
- JSON-Schema validation and per-topic tuning
- Prometheus metrics and operational debugging hooks

## Project status

The current release is [`v1.0.0`](https://github.com/DebanganThakuria/narad/releases/tag/v1.0.0).

| Area | Status |
|---|---|
| API and CLI | Stable `/v1` HTTP API |
| Storage engine | WAL-first produce path and segmented partition logs |
| Cluster metadata | Raft+bbolt control plane |
| Delivery model | At-least-once; consumers must be idempotent |
| Security | Basic auth + RBAC + cluster-secret auth (secure by default); TLS terminates at the ingress; no rate limiting yet |
| Production use | Ready — evidence in the [v1.0.0 release notes](https://github.com/DebanganThakuria/narad/releases/tag/v1.0.0); post-1.0 roadmap below |

CI runs race-enabled unit and end-to-end tests, plus local 3-node
integration and chaos smoke tests.

## Documentation

Everything below is the developer's quick reference. The real documentation — a client guide, a full
deployment/configuration/monitoring handbook, and code-level internals with diagrams — lives at:

**https://debanganthakuria.github.io/narad/**

| You want | Go to |
|---|---|
| Use Narad (produce/consume/fan-out/delay/RBAC) | [Client Guide](https://debanganthakuria.github.io/narad/client/) |
| Deploy and run it (Helm, env vars, metrics, scaling) | [Operate](https://debanganthakuria.github.io/narad/operate/) |
| Understand it (every subsystem, real function names) | [Internals](https://debanganthakuria.github.io/narad/internals/) |

## Observed benchmark

Bench run, 3-node cluster: **50,000 msg/s sustained through the full
produce → consume → ack flow** — the load generator saturated before the
broker did, so treat that as a floor, not a ceiling. Details:
[Scaling & Recovery](https://debanganthakuria.github.io/narad/operate/scaling-and-recovery/).

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
restart. Topics that need to survive volume loss opt into the replica
pattern: a fan-out child created with `parent` is an asynchronous full
copy whose partitions are deliberately anti-affine to the parent's —
see [the docs](https://debanganthakuria.github.io/narad/client/fanout-and-delay/#replication-when-you-ask-for-it).

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

Install with Homebrew or Go:

```sh
brew install debanganthakuria/narad/narad
# or: go install github.com/debanganthakuria/narad/cmd/narad@latest

narad server start --dev   # local playground: loopback, auth off
```

Then, from other terminals:

```sh
narad topic add demo
narad sub demo --peek                                  # live, read-only tail
narad pub demo '{"hello":"narad"}' --count 100 --rate 20
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
docker pull ghcr.io/debanganthakuria/narad:v1.0.0
docker run --rm -p 7942:7942 -p 7943:7943 ghcr.io/debanganthakuria/narad:v1.0.0
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

In another terminal, drive the server with the CLI:

```sh
narad topic add orders --partitions 8 --retention 1h --visibility 30s
narad topic ls
narad topic info orders
narad pub orders '{"id":1,"amount":1500}' --key c1
narad sub orders            # consume + ack; ctrl-c to stop
narad sub orders --peek     # read-only live tail
narad topic edit orders --retention 24h
narad topic rm orders
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
POST    /v1/topics                          create (retention/visibility/caps optional;
                                             parent= creates an anti-affine replica child)
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
narad server start|report    run a broker · cluster overview table
narad topic  add|ls|info|edit|rm|attach|detach|children
narad pub · sub [--peek] · replay · bench
narad user   add|grant|ls|rm
narad ctx    add|select|ls|rm    named server+credential contexts
narad serve · client · version   original commands, unchanged
```

Full walkthrough: [The CLI](https://debanganthakuria.github.io/narad/client/cli/).

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
NARAD_ADDR=http://127.0.0.1:7942 narad topic add orders
NARAD_ADDR=http://127.0.0.1:7944 narad topic info orders
```

`NARAD_CLUSTER_PEERS` and `cluster.peers` must contain exactly three `id@host:port` voters including the local node, and `cluster.node_id` / `--node-id` must be set whenever peers are configured.

If you leave peers unset, Narad keeps the single-node bootstrap path.

## Schema validation & evolution

A topic can carry an optional JSON Schema, set at create time or via
`PATCH /v1/topics/{topic}` with a `schema` field. When present, every
produced message body must be valid JSON and match the schema; invalid
payloads are rejected with **400**. Topics without a schema treat
produced bodies as opaque bytes — JSON, text, or binary all round-trip:
consume returns JSON verbatim, text as a JSON string, and binary
base64-encoded with `"payload_encoding":"base64"` flagged alongside
([details](https://debanganthakuria.github.io/narad/client/consuming/#the-payload-comes-back-the-way-you-sent-it)).

Schema changes are **additive-only and backwards-compatible**, enforced
at registration. You may add optional properties; but removing a
property, changing the type of an existing property, dropping a
previously-required field, or adding a new required field is rejected
with `ErrSchemaIncompatible`. The contract is "extend only, never break —
once a field exists, it stays."

## Testing

```sh
make test                          # full suite, race detector on
go test ./cmd/narad                # CLI parity tests against a real in-process HTTP server
go test ./internal/...             # unit tests only
go test ./tests/e2e/... -race      # HTTP-level e2e against a real broker
go test ./tests/e2e/... -run TestConsume   # one feature
```

The CLI suite in `cmd/narad/cli_v2_test.go` exercises the CLI commands
against a real in-process HTTP server: contexts, topic CRUD with human
units, pub, and the read-only peek path.

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

## What 1.0 proved

Every gate below was exercised against a live 5-node Kubernetes cluster
before the tag was cut; details and numbers live in the
[v1.0.0 release notes](https://github.com/DebanganThakuria/narad/releases/tag/v1.0.0)
and the [Operate handbook](https://debanganthakuria.github.io/narad/operate/).

- **Soak** — multi-day windows at 1,000 msg/s across the full
  produce→consume→ack flow (parent + fan-out child + 60s-delay child):
  300M+ messages in aggregate, zero lost, zero early delay fires.
- **Chaos** — `kill -9` of partition owners, Raft leaders, and joining
  nodes; restarts mid-produce; infrastructure node churn. All zero-loss.
- **Capacity** — 50,000 msg/s sustained through the full
  produce → consume → ack flow on a 3-node cluster; the load generator
  saturated before the broker did, so that's a floor.
- **Operational drills** — backup/restore with demonstrated RPO,
  mixed-version rolling upgrade, live scale-out 3→5 under load, and an
  offset-replay hammer with no impact on live consumers.

## Post-1.0 roadmap

Known limits, in the open. Run Narad behind a trusted ingress and these
are workable today; they are the next engineering items, roughly in order.

1. **Native TLS & rate limiting.** Narad serves plain HTTP; TLS must
   terminate at an ingress and the ingress→Narad hop must be trusted.
   Native TLS, mutual TLS between cluster nodes, and per-user/IP rate
   limiting remain future work.
2. **Routing polish.** Lag-aware partition selection (today: rotation),
   a retryable 503 for pinned dead-owner consume/ack (today that path
   can surface 421), and cursor advance on empty polls.

Shipped since 1.0: **partition rebalance + node decommission** — a new
node's arrival auto-rebalances existing partitions onto it (verbatim
copy, last-moment cutover, no record loss), and `narad cluster
decommission` drains a node off before removal. See
[Rebalance & Decommission](https://debanganthakuria.github.io/narad/internals/rebalance/).

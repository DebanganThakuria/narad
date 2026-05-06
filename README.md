# Narad

A lightweight, durable, queue-first event streaming system. Producers push
JSON messages to topics; consumers pull (with optional long-polling) and
acknowledge to advance their offset. Replay is supported via explicit
offsets. See [`requirements.md`](./requirements.md) for the full PRD.

> **Status:** early — initial wiring only. The append-only log, HTTP API,
> single-binary CLI, and metadata store are functional; replication, JSON
> Schema validation, partitioning across multiple node members, and
> segment rotation are stubbed behind interfaces and will land in
> follow-up work.

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
                       JSON-on-disk Metastore
                       (topics, schemas, offsets)
```

Cluster traffic (replication, follower fetch, membership) will run on
port 7943 once `narad worker` lands real replication.

## Layout

```
cmd/
  narad/             single binary with subcommands (serve/worker/cli/version)
internal/
  config/             defaults + JSON file + env + flag layering
  observability/
    logger/           thin log/slog wrapper
  httpserver/         http.Server, router, middleware
    handlers/         request handlers
  broker/             orchestrator over the ports below
  storage/            append-only Log primitive (one writer per partition)
  metastore/          Metastore interface + JSON-on-disk implementation
  topic/              shared value types (Topic, Partition, Message)
  partition/          partition selection (FNV hash + round-robin)
  replication/        Replicator interface + single-node Local stub
  schema/             SchemaRegistry interface + AlwaysValid stub
  consumer/           OffsetTracker interface + metastore-backed impl
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

In another terminal:

```sh
# create a topic
curl -X POST localhost:7942/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","partitions":4,"replication_factor":1}'

# produce
curl -X POST localhost:7942/topics/orders/produce \
  -H 'Content-Type: application/json' \
  -d '{"key":"customer-42","message":{"id":1,"amount":1500}}'

# consume (queue-style, long-poll up to 5s)
curl 'localhost:7942/topics/orders/consume?wait=5s'

# ack
curl -X POST localhost:7942/topics/orders/ack \
  -H 'Content-Type: application/json' \
  -d '{"partition":0,"offset":0}'
```

## CLI surface

```
narad serve     run the HTTP API server (default port 7942)
narad worker    run the cluster worker (default port 7943)
narad cli       interactive REPL over a single append-only log file
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
  "storage": { "data_dir": "data", "fsync": "per_write" },
  "log":     { "level": "info", "format": "json" },
  "worker":  { "enabled": false }
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

See `internal/config/config.go` for the full list and the matching JSON keys.

## Design decisions

* **Zero third-party Go dependencies.** Everything is built on the
  standard library. We hand-roll any tooling we'd otherwise pull in.
* **JSON-on-disk metastore instead of SQLite.** The PRD calls for SQLite
  but every Go SQLite driver is third-party. The `Metastore` interface
  keeps SQLite a viable future swap.
* **HTTP pull / long-polling instead of WebSockets.** Simpler ops, no
  custom framing, and no third-party `websocket` package. Trade-off:
  pull latency floor is one round-trip; we accept that.
* **One writer per partition.** Enforced by per-partition mutexes inside
  the broker; aligns with the PRD's "log is the source of truth".
* **Single binary with subcommands.** `narad serve|worker|cli|version`
  follows the kubectl/etcd/consul convention and keeps the install
  story to one drop-in binary.
* **JSON config, not YAML.** YAML would cost us a third-party dependency
  for a small ergonomic win.

## Roadmap (deferred from V1 wiring)

* Real leader/follower replication (`internal/replication`).
* Hand-rolled JSON Schema (draft-07 subset) validator.
* Multi-segment partition logs with rotation.
* Cross-node partition assignment & rebalancing.
* Observability metrics (`/metrics`).
* Auth, rate limiting.

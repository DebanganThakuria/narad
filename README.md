# Narad

A lightweight, durable, queue-first event streaming system. Producers push
JSON messages to topics; consumers pull (with optional long-polling) and
acknowledge to advance their offset. Replay is supported via explicit
offsets.

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
  storage/            append-only Log: in-memory buffer + async flusher
                       per partition, zstd-compressed batched frames,
                       per-frame CRC32C, skip-and-continue corruption recovery
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

Or hit the HTTP API directly (all routes live under `/v1`):

```sh
curl -X POST localhost:7942/v1/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","partitions":8}'

curl -X POST localhost:7942/v1/topics/orders/produce \
  -H 'Content-Type: application/json' \
  -d '{"key":"customer-42","message":{"id":1,"amount":1500}}'

curl 'localhost:7942/v1/topics/orders/consume?wait=5s'

curl -X POST localhost:7942/v1/topics/orders/ack \
  -H 'Content-Type: application/json' \
  -d '{"partition":0,"offset":0}'
```

API routes:

```
POST    /v1/topics                          create
GET     /v1/topics                          list
GET     /v1/topics/{topic}                  get single + per-partition stats
PATCH   /v1/topics/{topic}                  alter (increase partitions)
DELETE  /v1/topics/{topic}                  delete topic and all data
POST    /v1/topics/{topic}/produce
GET     /v1/topics/{topic}/consume
POST    /v1/topics/{topic}/ack
GET     /healthz
GET     /readyz
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
    "default_replication_factor": 1,
    "default_retention_age_ms": 604800000,
    "default_retention_bytes": 0
  },
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
| `NARAD_STORAGE_CODEC` | `zstd` / `none` |
| `NARAD_STORAGE_COMPRESSION_LEVEL` | `fastest` / `default` / `better` / `best` |
| `NARAD_STORAGE_FLUSH_BYTES` | flush when buffer ≥ N bytes |
| `NARAD_STORAGE_FLUSH_RECORDS` | flush when buffer ≥ N records |
| `NARAD_STORAGE_FLUSH_INTERVAL_MS` | flush at least every N ms |
| `NARAD_STORAGE_SEGMENT_BYTES` | roll the active segment past N bytes |
| `NARAD_STORAGE_RETENTION_CHECK_INTERVAL_MS` | retention reaper sweep period |
| `NARAD_TOPIC_DEFAULT_PARTITIONS` | default partition count when omitted from CreateTopic |
| `NARAD_TOPIC_MAX_PARTITIONS` | upper bound for partition count |
| `NARAD_TOPIC_DEFAULT_REPLICATION_FACTOR` | default replication factor when omitted |
| `NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS` | default retention age (default: 7 days) |
| `NARAD_TOPIC_DEFAULT_RETENTION_BYTES` | default retention size cap (default: 0 = disabled) |
| `NARAD_ADDR` | (client only) base URL for `narad client` |

See `internal/config/config.go` for the full list and the matching JSON keys.

## Design decisions

* **Single third-party Go dependency.** Everything is built on the
  standard library except for `github.com/klauspost/compress` (pure Go,
  used for zstd compression of partition log frames). All other tooling
  is hand-rolled.
* **Single binary with subcommands.** `narad serve|worker|cli|version`
  follows the kubectl/etcd/consul convention and keeps the install
  story to one drop-in binary.

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
written to. A future retention/compaction pass can delete or rewrite
sealed segments without affecting the active write path.

## Retention

A per-partition reaper deletes sealed segments past the topic's
retention bounds. Defaults: 7 days, no size cap. Both bounds are
configurable globally (`topic.default_retention_age_ms`,
`topic.default_retention_bytes`) and per-topic via the topic record
(zero = disabled). The active segment is never deleted, regardless of
its age. Records in deleted segments become permanent gaps; reads
return `404`/`ErrOffsetNotFound`.

The reaper sweeps every `storage.retention_check_interval_ms` (default
1 minute). Age is measured by segment-file mtime, which is a proxy for
"time of last write" — a sealed segment's mtime stops advancing.

## Topics

Topics are created via `POST /v1/topics`. `partitions` and
`replication_factor` are optional; omitting them (or sending `0`)
applies the configured defaults.

`PATCH /v1/topics/{name}` raises the partition count of an existing
topic:

```sh
curl -X PATCH localhost:7942/v1/topics/orders \
  -H 'Content-Type: application/json' \
  -d '{"partitions": 32}'
```

This is *increase-only*; requests at or below the current count return
400. Existing records are not moved — they stay in the partitions they
were originally written to. Future records' partition assignment uses
`hash(key) % newCount`, so an existing key may now hash to a different
partition than its prior records. Consumers that depend on per-key
ordering should be aware. Decreasing the partition count is not
supported (offsets are immutable).

`DELETE /v1/topics/{name}` removes a topic, its on-disk segments, and
its consumer offsets. Irreversible.

`GET /v1/topics/{name}` returns the topic record plus per-partition
runtime stats (segment count, oldest/next offset, bytes, oldest
segment mtime).

## Roadmap (deferred from V1 wiring)

* Real leader/follower replication (`internal/replication`).
* Hand-rolled JSON Schema (draft-07 subset) validator.
* Multi-segment partition logs with rotation.
* Cross-node partition assignment & rebalancing.
* Observability metrics (`/metrics`).
* Auth, rate limiting.

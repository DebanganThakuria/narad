---
description: "Look up every setting narad serve reads: its environment variable, its config file key, its default, and the values it accepts."
search:
  boost: 2
---

# Configuration reference

Look up every setting `narad serve` reads: its environment variable, its config file key, its default, and the values it accepts.

```json title="narad.json"
{
  "storage": {
    "codec": "zstd",
    "fsync": "per_write"
  }
}
```

```sh title="Command"
narad serve --config narad.json
```

```text title="Output"
narad: config: config: load file: storage.fsync is an internal setting and cannot be configured
```

The node refuses to start: `storage.fsync` is not a setting you can change, and every setting is checked before the node opens any data. Remove the key and it starts with zstd compression on.

Under Kubernetes the Helm chart sets most of these for you; which chart value sets which variable is in [Helm values reference](helm-values.md#env-mapping).

## Precedence {#precedence}

A setting can come from four layers. Each overrides the one before it:

1. the built-in default;
2. the JSON config file named by `--config`;
3. environment variables;
4. command-line flags of `narad serve`.

After the four layers are applied, the whole configuration is checked. Any problem stops the node before it starts, with a message that names the setting, such as `http.max_consume_wait (20s) must be <= http.shutdown_grace (10s)`.

`narad serve` takes these flags:

| Flag | Sets |
|---|---|
| `--config <path>` | the config file to read |
| `--addr <host:port>` | `http.addr` |
| `--port <n>` | `http.addr`, as `:<n>` |
| `--cluster-port <n>` | `cluster.addr`, as `:<n>` |
| `--node-id <id>` | `cluster.node_id` |
| `--data-dir <path>` | `storage.data_dir` |
| `--log-level <level>` | `log.level` |
| `--log-format <format>` | `log.format` |
| `--pprof-addr <host:port>` | `http.pprof_addr` |

`narad server start --dev` is a different entry point for a laptop; it is described in [CLI command reference](cli.md#server).

## HTTP {#http}

| Environment variable | Config file key | Default | Notes |
|---|---|---|---|
| `NARAD_HTTP_ADDR` | `http.addr` | `:7942` | The client API. Nodes also talk to each other on the same port number over UDP (QUIC). Must differ from `cluster.addr`. |
| `NARAD_HTTP_READ_TIMEOUT` | `http.read_timeout` | `10s` | |
| `NARAD_HTTP_WRITE_TIMEOUT` | `http.write_timeout` | `30s` | Must be longer than `http.max_consume_wait`, or long polls are cut off mid-wait. |
| `NARAD_HTTP_IDLE_TIMEOUT` | `http.idle_timeout` | `60s` | |
| `NARAD_HTTP_SHUTDOWN_GRACE` | `http.shutdown_grace` | `10s` | How long a stopping node lets requests in flight finish. Must be at least `http.max_consume_wait`. |
| `NARAD_HTTP_MAX_CONSUME_WAIT` | `http.max_consume_wait` | `10s` | The longest `wait` a consume may ask for; longer ones are cut to it. Raising it past `10s` means raising `http.shutdown_grace` too. Set a positive value: `0` passes the checks but falls back to a 30 s ceiling. |
| `NARAD_HTTP_MAX_HEADER_BYTES` | `http.max_header_bytes` | `65536` | Largest request header block; larger gets `431`. At least `4096`. |
| `NARAD_HTTP_MAX_CONNECTIONS` | `http.max_connections` | `4096` | Open client connections per node; more wait in the listen backlog. `0` removes the cap. |
| `NARAD_HTTP_MAX_CONSUME_IN_FLIGHT_PER_IDENTITY` | `http.max_consume_in_flight_per_identity` | `1024` | Concurrent consumes per user, or per client IP with security off, per node; more get `429`. A batch consume counts as its `max`. `0` removes the cap. |
| `NARAD_HTTP_MAX_PRODUCE_IN_FLIGHT_PER_IDENTITY` (unreleased) | `http.max_produce_in_flight_per_identity` (unreleased) | `0` (off) | Concurrent produces per user, or per client IP with security off, per node; more get `429`. A batch produce counts as its message count, and as one while its body is read. v3.0.1 refuses to start with the file key. |
| `NARAD_HTTP_METRICS_ADDR` | `http.metrics_addr` | empty | When set, `/metrics`, `/healthz` and `/readyz` are served on this address without credentials, and `/metrics` leaves the API port. Keep it inside the cluster. May equal `http.pprof_addr`. |
| `NARAD_HTTP_METRICS_UNAUTHENTICATED` | `http.metrics_unauthenticated` | `false` | Serve `/metrics` on the API port without credentials. Its series name every topic. Ignored when `http.metrics_addr` is set. |
| `NARAD_HTTP_PPROF_ADDR` | `http.pprof_addr` | empty | Serves Go's `net/http/pprof` on this address, without credentials. Keep it inside the cluster. |

## Cluster {#cluster}

| Environment variable | Config file key | Default | Notes |
|---|---|---|---|
| `NARAD_NODE_ID` | `cluster.node_id` | the host name | The node's identity in the cluster. Keep it stable across restarts. |
| `NARAD_CLUSTER_ADDR` | `cluster.addr` | `:7943` | The Raft transport (TCP). |
| `NARAD_CLUSTER_PEERS` | `cluster.peers` | none | The voters that bootstrap the cluster, the same list on every node. In the environment, `id@host:7943,id@host:7943,...`; in the file, a list of `{"id": ..., "addr": ...}`. When set, it lists at least 3 voters. A joining node walks it to find the leader. |
| `NARAD_CLUSTER_ADVERTISE_ADDR` | `cluster.advertise_addr` | empty | The `host:port` other nodes dial for this node's Raft transport. Required when the node is not in the peer list; otherwise the node takes the host from its own peer entry. |
| `NARAD_CLUSTER_INITIAL_MEMBERS` | `cluster.initial_members` | empty | Comma-separated IDs of the nodes that may bootstrap a new cluster; every other node joins the existing one. Empty lets every node bootstrap. Never change it after the cluster exists. |
| `NARAD_CLUSTER_RAFT_SNAPSHOT_THRESHOLD` | `cluster.raft_snapshot_threshold` | `8192` | Metadata log entries applied since the last snapshot before the next one. Greater than 0. |
| `NARAD_CLUSTER_RAFT_SNAPSHOT_INTERVAL` | `cluster.raft_snapshot_interval` | `120s` | How often the threshold is checked, with up to 2x jitter. At least `5ms`. |
| `NARAD_CLUSTER_RAFT_TRAILING_LOGS` | `cluster.raft_trailing_logs` | `10240` | Log entries kept behind a snapshot. A restarting node further behind than this gets the whole snapshot. |

The three Raft settings are the defaults of the Raft library Narad uses. Leave them alone in production.

## Storage {#storage}

| Environment variable | Config file key | Default | Notes |
|---|---|---|---|
| `NARAD_DATA_DIR` | `storage.data_dir` | `data` | Everything the node stores: `topics/`, `ingress/` and `metastore/`. |
| none | `storage.codec` | `none` | `none` or `zstd`. Compression is off by default. zstd shrinks JSON-like payloads a lot, and more under load, when frames hold more records. |
| none | `storage.compression_level` | `fastest` | zstd level: `fastest`, `default`, `better` or `best`. Decompression speed does not depend on it. |
| none | `storage.idle_log_eviction_ms` | `1800000` (30 min) | Close a partition log nothing has touched for this long. `0` turns it off; otherwise at least `60000`. See [Idle partitions](#idle-partitions). |
| none | `storage.cold_retention_walk_ms` | `300000` (5 min) | How often closed partitions are checked for expired data. `0` turns it off; otherwise at least `60000`. |
| none | `storage.consumer_offset_commit_interval_ms` (unreleased) | `1000` | How long an acked position may wait for a sync to disk. `10` to `60000`. v3.0.1 refuses to start with this key. See [Consumer offset commit interval](#consumer-offset-commit-interval). |
| none | `storage.ingress_wal_prealloc` (unreleased) | `false` | Prepare ingress WAL segments ahead of use. v3.0.1 refuses to start with this key, `true` or `false`. See [Ingress WAL segment preparation](#ingress-wal-segment-preparation). |

The storage keys in this table are the only ones the config file accepts. The engine's fsync mode, flush and sync cadence and segment size are internal settings with fixed production values, and a config file that sets one is refused (`storage.<key> is an internal setting and cannot be configured`). What a `202` promises about the disk does not depend on any setting; it is in the [delivery contract](../understand/delivery-contract.md#what-202-means).

### Idle partitions {#idle-partitions}

An open partition log holds goroutines, file descriptors and buffers. A node closes any log untouched for `storage.idle_log_eviction_ms` and reopens it on the next produce, consume or replay.

- Creating a topic opens nothing; it is only a metadata entry until a partition is used.
- Metrics reads never keep a log open, and neither does an attached fan-out child that receives nothing.
- A log is closed only after retention has finished deleting its expired segments.
- Retention deletes data only in open logs. Every `storage.cold_retention_walk_ms`, the node opens each closed partition that holds an expired segment, deletes it, and closes the partition again; `narad_cold_retention_swept_total` counts these.

Watch `narad_open_partition_logs` and `narad_idle_logs_evicted_total` ([Metrics reference](metrics.md#storage-housekeeping)). An abandoned topic still keeps its metadata and its last segment on disk until it is deleted.

### Consumer offset commit interval {#consumer-offset-commit-interval}

**Unreleased:** in master, not in v3.0.1.

`storage.consumer_offset_commit_interval_ms` (default `1000`, `10` to `60000`) sets how long a partition's acked position, and the acks it holds above an unacked message, may wait before they are synced to disk. The node writes them at two cadences:

- Every 100 ms, or every interval when that is shorter, each partition acked since the last tick is written to the operating system's page cache, without a sync. A crash of the broker process loses nothing in the page cache, so it redelivers about the last 100 ms of acks, whatever the interval.
- Once per interval, each partition written since is synced to disk: `fdatasync` on Linux, and on macOS `fsync` followed by one `F_FULLFSYNC` per device for all partitions of the tick. A power loss, a kernel crash or the loss of the machine therefore redelivers up to about the interval plus 100 ms and the sync time: about 1.1 s at the default.

A redelivery is a duplicate, never a loss, and a graceful stop redelivers nothing. v3.0.1 synced every changed partition every 100 ms; set the key to `100` to keep that window, at the cost of more syncs.

The node logs a warning, at most once a minute, when it cannot keep to either cadence: `consumer offset commits cannot keep to their interval` when one tick takes longer than the tick interval, and `consumer offsets wait longer than their durability interval for a device flush` when a partition's writes have waited more than twice the interval. Fewer partitions per node, or a faster disk, helps with either.

### Ingress WAL segment preparation {#ingress-wal-segment-preparation}

**Unreleased:** in master, not in v3.0.1.

`storage.ingress_wal_prealloc` (default `false`) makes the [ingress WAL](glossary.md#ingress-wal) create and zero-fill its next 64 MiB segment in the background. Group commits then overwrite blocks that already exist, and their `fdatasync` does not also have to commit the file's metadata through the file system journal. The gain was measured on ext4 only; APFS showed none. Measure on your own volumes before you rely on it.

- **Disk:** up to two extra 64 MiB segments per node (the prepared active segment and a spare, `next-segment.prep` in `<data_dir>/ingress/produce`), and one segment of background zero-filling per segment. When the disk is full, preparation fails and the next segment is created the plain way.
- **Crash recovery:** a torn write inside a prepared segment is truncated rather than refused. The details are in [Produce path](../understand/produce-path.md).
- **Rollback:** turn the setting off and restart each node once on this release before moving to v3.0.1 or earlier. The steps are in [Upgrade Narad](../operate/upgrade.md#version-notes).

## Topic defaults {#topic-defaults}

These apply when a topic is created without the field, or with `0`. Existing topics keep their values.

| Environment variable | Config file key | Default | Notes |
|---|---|---|---|
| `NARAD_TOPIC_DEFAULT_PARTITIONS` | `topic.default_partitions` | `3` | At least 3, and at most `topic.max_partitions`. |
| `NARAD_TOPIC_MAX_PARTITIONS` | `topic.max_partitions` | `108` | The most partitions a topic may have, at create or later. |
| `NARAD_TOPIC_DEFAULT_RETENTION_AGE_MS` | `topic.default_retention_age_ms` | `604800000` (7 days) | `0` keeps messages forever; any other value is at least `3600000` (1 hour). The Helm chart sets 12 hours. |
| `NARAD_TOPIC_DEFAULT_VISIBILITY_TIMEOUT_MS` | `topic.default_visibility_timeout_ms` | `30000` | Greater than 0, and no longer than the default retention when that is not 0. |
| `NARAD_TOPIC_DEFAULT_MAX_IN_FLIGHT_PER_PARTITION` | `topic.default_max_in_flight_per_partition` | `1024` | Greater than 0. |
| `NARAD_TOPIC_DEFAULT_MAX_ACKED_AHEAD_PER_PARTITION` | `topic.default_max_acked_ahead_per_partition` | `1024` | Greater than 0. |

What each topic field does is in [Create a topic](http-api.md#create-topic).

## Fan-out {#fan-out}

| Environment variable | Config file key | Default | Notes |
|---|---|---|---|
| `NARAD_FANOUT_MAX_BATCH_RECORDS` | `fanout.max_batch_records` | `4096` | Most records copied to a child in one batch. |
| `NARAD_FANOUT_MAX_BATCH_BYTES` | `fanout.max_batch_bytes` | `4194304` (4 MiB) | Most payload bytes in one batch. |
| `NARAD_FANOUT_LINGER_MS` | `fanout.linger_ms` | `25` | How long a partly filled batch waits for more records. |

Larger batches mean fewer syncs on the child and more delay for each record. How the copy works is in [Fan-out engine](../understand/fanout-engine.md).

## Logging and security {#logging-and-security}

| Environment variable | Config file key | Default | Notes |
|---|---|---|---|
| `NARAD_LOG_LEVEL` | `log.level` | `info` | `debug`, `info`, `warn` or `error`. |
| `NARAD_LOG_FORMAT` | `log.format` | `json` | `json` or `text`. |
| `NARAD_SECURITY_ENABLED` | `security.enabled` | `true` | HTTP Basic authentication and grants on the API, and the shared secret between nodes. |
| `NARAD_ADMIN_PASSWORD` | none | generated | The root admin's password, used when a secured cluster first starts with no users. Unset, the node that creates the root admin generates a password and logs it once. |
| `NARAD_CLUSTER_SECRET` | none | none | The shared secret every node proves to the others on the node-to-node port. Required when security is on and `cluster.peers` is set. |
| `NARAD_CLUSTER_TLS_CERT_FILE`, `NARAD_CLUSTER_TLS_KEY_FILE`, `NARAD_CLUSTER_TLS_CA_FILE` | `security.cluster_tls_cert_file`, `security.cluster_tls_key_file`, `security.cluster_tls_ca_file` | empty | Mutual TLS for Raft: all three or none. Read once at startup; see [Raft TLS certificates](../operate/raft-tls.md). |
| `NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT` | `security.allow_plaintext_raft` | `false` | With security on and `cluster.peers` set, a node refuses to start without the Raft TLS files unless this says the Raft port is fenced some other way, such as by a NetworkPolicy. |
| `NARAD_SECURITY_ALLOW_INSECURE_CLUSTER` | `security.allow_insecure_cluster` | `false` | Required to run several nodes with security off, which leaves the API, the node-to-node port and Raft open. One node needs nothing. |
| `NARAD_SECURITY_ALLOW_LEGACY_CLUSTER_AUTH` | `security.allow_legacy_cluster_auth` | `false` | Also accept the older node-to-node authentication, for a rolling upgrade from a release that used it. Turn it off once every node has rolled. See [Upgrade Narad](../operate/upgrade.md#version-notes). |

The two secrets can only be set in the environment, so config files and ConfigMaps never hold them. Why each setting exists is in [Networking and security](../understand/networking-and-security.md), and what to set before going live is in the [Production checklist](../operate/production-checklist.md).

## Config file {#config-file}

The file named by `--config` is JSON. It is strict:

- A key the loader does not know, at any level, stops the node from starting. That includes a key from a newer release, which matters when rolling back: remove `storage.consumer_offset_commit_interval_ms`, `storage.ingress_wal_prealloc` and `http.max_produce_in_flight_per_identity` from the file before a node runs v3.0.1 or earlier ([Upgrade Narad](../operate/upgrade.md#roll-back)).
- Durations are strings with a unit, such as `"10s"` or `"500ms"`. A bare number is refused.
- JSON has no comments.

A file that sets a few common values:

```json title="narad.json"
{
  "http": {
    "addr": ":7942",
    "max_consume_wait": "10s",
    "metrics_addr": ":9100"
  },
  "storage": {
    "data_dir": "/var/lib/narad",
    "codec": "zstd",
    "compression_level": "fastest"
  },
  "topic": {
    "default_retention_age_ms": 43200000
  },
  "log": {
    "level": "info",
    "format": "json"
  }
}
```

The Helm chart writes `narad.config` from its values into this file ([Helm values reference](helm-values.md#values)).

## Go runtime {#go-runtime}

**Unreleased:** in master, not in v3.0.1.

On Linux, when `GOMEMLIMIT` is unset, `narad serve` sets Go's soft memory limit to 90% of the process's cgroup memory limit, so the garbage collector works harder near the limit instead of letting a burst get the process killed. It logs `go memory limit set from the cgroup memory limit (set GOMEMLIMIT to override)` once at startup. Any non-empty `GOMEMLIMIT`, `off` included, wins; an empty one counts as unset. Without a cgroup memory limit, nothing is set. The binary never changes `GOGC`.

## Tuning {#tuning}

| You want | Change |
|---|---|
| Less disk | `storage.codec: zstd` |
| Longer long polls | `http.max_consume_wait`, with `http.shutdown_grace` at least as long and `http.write_timeout` longer |
| Larger fan-out batches on slow disks | a higher `fanout.linger_ms` |
| Fewer offset syncs under heavy ack traffic (unreleased) | a higher `storage.consumer_offset_commit_interval_ms`; a power loss then redelivers more acked messages |
| Fewer redeliveries after a power loss (unreleased) | a lower `storage.consumer_offset_commit_interval_ms`; `100` matches v3.0.1 |
| A ceiling on one user's concurrent produces (unreleased) | `http.max_produce_in_flight_per_identity` |

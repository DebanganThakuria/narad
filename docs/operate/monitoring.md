# Monitoring

`GET /metrics` on any node serves Prometheus metrics. The exposition names every topic with its partition count, lag, throughput and fan-out graph, the same inventory topic listing only shows to grant holders, so it is not served to anyone who can reach the API port: by default it lives on its **own listener** (`NARAD_HTTP_METRICS_ADDR`, `:9100` in the chart, which is what the `ServiceMonitor` scrapes, credential-free and cluster-internal) and is a 404 on the API port; with no metrics listener it is served on the API port **behind the same Basic auth as the API** (Prometheus supports `basic_auth` per scrape job), unless `NARAD_HTTP_METRICS_UNAUTHENTICATED=true` restores the old open behaviour. Namespace prefix: `narad_`.

**Don't build a dashboard; import ours.** The repo ships a ready-to-go Grafana dashboard at
[`ops/monitoring/grafana/dashboards/narad-node-dashboard.json`](https://github.com/DebanganThakuria/narad/blob/master/ops/monitoring/grafana/dashboards/narad-node-dashboard.json):
14 panels covering throughput, consumer backlog, errors & rejections, disk, storage fsync
latency, and process health. It's the exact dashboard the 47-hour, 170M-message 1.0 soak
was judged on. Some of its panels read differently since the batch forms and
the `hwm` change: the HTTP panels count requests, which stop tracking messages
once clients batch (see Traffic below; Message Throughput plots the message
rates), and the Storage Latency panel still plots
`narad_storage_high_watermark_persist_duration_seconds`, which no longer
measures a commit (see Storage engine below).

## The five alerts that matter

If you configure nothing else, configure these. Each one is a symptom that pages *you* before your users do:

| Alert | Expression sketch | It means |
|---|---|---|
| **Fan-out data loss** | `rate(narad_fanout_child_dropped_messages[5m]) > 0` | A child fell behind the parent's retention and lost records. Never fires in a sane config, which is exactly why it must page |
| **Delay child behind** | `narad_fanout_due_lag_seconds > 60` | Due messages aren't being delivered. The *only* honest lag signal for delay children (offset lag is always ≈ rate×delay by design) |
| **Consumer-side loss** | `rate(narad_consumer_corrupt_skipped_total[5m]) > 0` or `consumer_dropped_messages` | A permanently unreadable record was skipped: bounded, logged, and should be investigated |
| **Disk runway** | `narad_data_dir_available_bytes` trending toward 0 | Retention math vs reality. See [Scaling & Recovery](scaling-and-recovery.md) for the sizing formula |
| **Produce latched off** | `narad_ingress_wal_failed == 1` | A write or sync of this node's ingress WAL failed, and the node answers every produce with `500` until it restarts. Consume keeps working and `/readyz` stays up, so nothing else takes the node out of rotation. Fix the disk, then restart the node; everything it acked is replayed ([why it latches](../internals/produce-path.md#when-the-wals-disk-fails)) |

Honorable mention: `rate(narad_errors_total[5m])` by `component`/`kind` as a catch-all, and no-leader detection via your Raft port health if you want belt and suspenders.

## Full metric reference

### Traffic

| Metric | Type | Labels |
|---|---|---|
| `narad_messages_produced_total` / `_consumed_total` | counter | topic |
| `narad_bytes_produced_total` / `_consumed_total` | counter | topic |
| `narad_produce_rejections_total` | counter | topic, reason (`schema`, `delayed_child`, …) |
| `narad_consume_wait_seconds` | histogram | long-poll latency shape |
| `narad_consume_empty_total` | counter | 204s: idle consumers polling |
| `narad_http_requests_total`, `_request_duration_seconds`, `_requests_in_flight`, `_request_bytes_in_total`, `_response_bytes_out_total` | (various) | the usual HTTP suspects: route (the matched pattern), method and status, except in-flight (unlabeled) and the byte counters (route only) |

The HTTP series count requests, not messages. A batch produce has its own route, `POST /v1/topics/{topic}/produce/batch`; a batch consume and a batch ack share the single-message routes (`GET /v1/topics/{topic}/consume`, `POST /v1/topics/{topic}/ack`), and a batch ack is answered `200` whatever its handles' outcomes, where a single ack is answered `204`. One batch request carries up to 100 messages, so once clients batch, take message rates from `narad_messages_produced_total` and `_consumed_total`. A panel that selects produces with `route=~".*/produce"` misses the batch route, and one that counts acks as `status="204"` misses batch acks.

### Queue health

| Metric | Meaning |
|---|---|
| `narad_consumer_lag_messages` | Committed high watermark minus committed frontier, per partition: what a consumer can still read (buffered and hidden-tail records are not counted). Series for a partition that moved to another node are dropped on the next 5 s tick, so `sum by (topic, partition)` across nodes does not double count. A partition this node owns but whose log is closed (idle-evicted, or not opened since a restart) keeps its series, read from its files and refreshed at most every 30 s; one whose consumer state is not loaded reports the persisted frontier, not 0 |
| `narad_oldest_unconsumed_message_age_seconds` | Upper bound on how stale the next message is |
| `narad_inflight_size` / `narad_acked_ahead_size` | Lease table pressure vs the topic caps |
| `narad_ack_rejected_total` | 410s: consumers losing races (normal in small doses) |
| `narad_acked_ahead_size` at the topic cap | the head of that partition is stuck (a consumer holding it died or never acked): consume returns 204 for fresh offsets until the head redelivers and is acked. Acks are never rejected for it |
| `narad_ack_extended_total` / `narad_nack_total` | Lease heartbeats and hand-backs. The nacks include records a [batch consume](../client/consuming.md#consuming-in-batches) reserved but its 8 MiB response bound left out, which only text inflated by JSON escaping reaches; `narad_messages_consumed_total` counts those again when they are redelivered |

The per-partition series (lag, oldest unconsumed age, in-flight and acked-ahead sizes, `narad_partition_size_bytes`, `narad_segments`) are exported for every partition the node owns, open log or not, as long as its files hold a boundary: a closed partition whose `hwm` file is empty (as a crash leaves it, until the log is opened and closed again) has no series. Before, a partition whose log was idle-evicted, or not yet opened since a restart, dropped out of them, which hid exactly the untouched backlog they exist to show. For such a partition the file-derived values (size, segments, the high watermark) are cached for up to 30 s, so a cold-retention sweep between two polls can leave them stale for that long; the frontier and the in-flight and acked-ahead sizes are always current. A deleted topic's series stay pruned even while one of its logs is still open on a node.

Three more series are built from the same set of partitions and changed with it. `narad_partitions_total` and `narad_topic_bytes` now count owned partitions whose log is closed, where they used to count only open ones, so both step up after the upgrade on a node with idle-evicted partitions or partitions not reopened since a restart; that is a change in what they count, not growth. `narad_consumer_dropped_messages` is exported for those partitions too, and a partition whose consumer state is not loaded is measured from its persisted frontier: it used to count as dropped every offset below the log start.

### Fan-out

| Metric | Meaning |
|---|---|
| `narad_fanout_lag_messages` | Parent HWM − cursor, per (parent, child, partition). The health signal for *normal* children |
| `narad_fanout_due_lag_seconds` | Seconds behind the due frontier. The health signal for *delay* children |
| `narad_fanout_committed_total` | Records delivered into children |
| `narad_fanout_child_dropped_messages` | **Data loss counter.** Alert on any movement |
| `narad_fanout_batch_records` / `_batch_bytes` | Batch effectiveness histograms |

### Storage engine

| Metric | Meaning |
|---|---|
| `narad_storage_fsync_duration_seconds` | Your disk's honesty meter |
| `narad_storage_flush_duration_seconds` / `_flush_bytes_total` | Flusher throughput |
| `narad_storage_high_watermark_persist_duration_seconds` | Cost of writing the `hwm` file: at most once per log open (the first commit after the open empties it) and at most once per close (the exact boundary, skipped when the file already holds it). Commits no longer write it, so its rate no longer tracks commits, and a dashboard or alert that read it as a per-commit cost no longer measures one |
| `narad_storage_retention_bytes_deleted_total` / `_messages_deleted_total` | Reaper activity, labeled by reason |
| `narad_data_dir_size_bytes` / `_available_bytes`, `narad_topic_bytes`, `narad_partition_size_bytes`, `narad_segments` | Disk accounting at every zoom level |

### Storage housekeeping

| Metric | Type | Meaning |
|---|---|---|
| `narad_cold_retention_swept_total` | counter | Closed partitions the cold-retention walk opened, reaped and closed again because a segment had expired. Rises only on idle topics with expired data. |
| `narad_reaper_restarts` | gauge | Times the shared retention loop was replaced because it stopped ticking. Any value above zero deserves a look at the log. |
| `narad_ingress_wal_failed` | gauge | 1 once a write or sync of the ingress WAL has failed (latched until restart; produce answers `500` meanwhile), else 0. Alert on 1. |
| `narad_ingress_dispatch_backlog_records` | gauge | Records in this node's ingress WAL that a restart would replay: the WAL's durable next sequence minus the dispatch checkpoint last stored, refreshed on every 5 s poller tick. Small on a healthy node (produce raises it, and it falls as the dispatcher commits records to their partition owners and stores its checkpoint); a value that stays above 0 while producers are idle means records are not reaching their owners. It is what to watch before a rollback: pause producers and [roll a node back](helm-chart.md#rolling-back-to-an-earlier-release) only once it reads 0 on every node, in a sample taken after the pause. |

### Cluster & misc

| Metric | Type | Meaning |
|---|---|---|
| `narad_cluster_rpc_requests_total` | counter | Peer RPCs this node issued, by `op` and `outcome` (`ok`, `rejected` for a 4xx, `failed` for a 5xx, `timeout`, `error`) |
| `narad_cluster_rpc_request_seconds` | histogram | Their round-trip time, by `op` |

Both count RPCs, not records. Forwarded acks, extends and nacks to one owner travel together under heavy load, and a client batch ack's handles for one owner always do when there are two or more, as a single `op="ack_batch"` RPC; those are not counted under `op="ack"`, `op="extend_ack"` or `op="nack"`, so add `ack_batch` to a panel that reads those as the forwarded-ack rate or latency. A node on an older release never sends it.

`narad_topics_total`, `narad_partitions_total` (the owned partitions that have per-partition series, open log or closed, as described under Queue health), `narad_open_partition_logs` (refreshed every poller tick, eviction on or off), `narad_errors_total{component,kind}`, `narad_boot_duration_seconds`.

## Reading the dashboards under failure

What healthy failure handling looks like, so you don't page yourself for the system working:

- **Node killed** → `consumer_lag` and `fanout_lag` spike on its partitions, drain within ~a minute of its return; `due_lag` spikes then returns to 0; duplicates tick up (at-least-once seams). All expected.
- **`fanout_due_lag_seconds` plateaus above 0** → *not* expected. That's the frozen-loss signature; go read the [Cluster Lifecycle war stories](../internals/cluster-lifecycle.md) and check the cursor logs.
- **Readiness down, liveness up on one pod** → it's catching up, waiting for admission, or has lost sight of the Raft leader (`/readyz` says which in its body). Leave it alone; it knows what it's doing. A decommissioned pod that has been Raft-removed stays in this state until you scale it away; that is expected.
- **Readiness down on every pod** → no Raft leader: quorum is lost. Bring the missing voters back; don't restart the survivors.

## pprof

`narad.pprof.enabled: true` serves the full `net/http/pprof` suite on `:6060`. We keep it on in staging; CPU profiles during soak tests are how the produce hot path stayed honest. Like the metrics listener it is unauthenticated and must stay loopback or cluster-internal (the ingress never routes to it; a NetworkPolicy keeps other namespaces out). The two may share one address.

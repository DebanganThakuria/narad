# Monitoring

`GET /metrics` on any node serves Prometheus metrics (unauthenticated by design — it's a scrape target, not a secret). The chart ships a `ServiceMonitor` when `metrics.enabled: true`. Namespace prefix: `narad_`.

**Don't build a dashboard — import ours.** The repo ships a ready-to-go Grafana dashboard at
[`ops/monitoring/grafana/dashboards/narad-node-dashboard.json`](https://github.com/DebanganThakuria/narad/blob/master/ops/monitoring/grafana/dashboards/narad-node-dashboard.json):
14 panels covering throughput, consumer backlog, errors & rejections, disk, storage fsync
latency, and process health. It's the exact dashboard the 47-hour, 170M-message 1.0 soak
was judged on.

## The four alerts that matter

If you configure nothing else, configure these. Each one is a symptom that pages *you* before your users do:

| Alert | Expression sketch | It means |
|---|---|---|
| **Fan-out data loss** | `rate(narad_fanout_child_dropped_messages[5m]) > 0` | A child fell behind the parent's retention and lost records. Never fires in a sane config — which is exactly why it must page |
| **Delay child behind** | `narad_fanout_due_lag_seconds > 60` | Due messages aren't being delivered. The *only* honest lag signal for delay children (offset lag is always ≈ rate×delay by design) |
| **Consumer-side loss** | `rate(narad_consumer_corrupt_skipped_total[5m]) > 0` or `consumer_dropped_messages` | A permanently unreadable record was skipped — bounded, logged, and should be investigated |
| **Disk runway** | `narad_data_dir_available_bytes` trending toward 0 | Retention math vs reality. See [Scaling & Recovery](scaling-and-recovery.md) for the sizing formula |

Honorable mention: `rate(narad_errors_total[5m])` by `component`/`kind` as a catch-all, and no-leader detection via your Raft port health if you want belt and suspenders.

## Full metric reference

### Traffic

| Metric | Type | Labels |
|---|---|---|
| `narad_messages_produced_total` / `_consumed_total` | counter | topic |
| `narad_bytes_produced_total` / `_consumed_total` | counter | topic |
| `narad_produce_rejections_total` | counter | topic, reason (`schema`, `delayed_child`, …) |
| `narad_consume_wait_seconds` | histogram | long-poll latency shape |
| `narad_consume_empty_total` | counter | 204s — idle consumers polling |
| `narad_http_requests_total`, `_request_duration_seconds`, `_requests_in_flight`, `_request_bytes_in_total`, `_response_bytes_out_total` | — | the usual HTTP suspects |

### Queue health

| Metric | Meaning |
|---|---|
| `narad_consumer_lag_messages` | HWM minus committed frontier, per partition |
| `narad_oldest_unconsumed_message_age_seconds` | Upper bound on how stale the next message is |
| `narad_inflight_size` / `narad_acked_ahead_size` | Lease table pressure vs the topic caps |
| `narad_ack_rejected_total` | 410s — consumers losing races (normal in small doses) |
| ack `503`s in `narad_http_requests_total` | acked-ahead set full — consumers not retrying acks; the broker throttles fresh deliveries until the frontier unsticks |
| `narad_ack_extended_total` / `narad_nack_total` | Lease heartbeats and hand-backs |

### Fan-out

| Metric | Meaning |
|---|---|
| `narad_fanout_lag_messages` | Parent HWM − cursor, per (parent, child, partition). The health signal for *normal* children |
| `narad_fanout_due_lag_seconds` | Seconds behind the due frontier — the health signal for *delay* children |
| `narad_fanout_committed_total` | Records delivered into children |
| `narad_fanout_child_dropped_messages` | **Data loss counter.** Alert on any movement |
| `narad_fanout_batch_records` / `_batch_bytes` | Batch effectiveness histograms |

### Storage engine

| Metric | Meaning |
|---|---|
| `narad_storage_fsync_duration_seconds` | Your disk's honesty meter |
| `narad_storage_flush_duration_seconds` / `_flush_bytes_total` | Flusher throughput |
| `narad_storage_high_watermark_persist_duration_seconds` | HWM fsync cost (bounded by design) |
| `narad_storage_retention_bytes_deleted_total` / `_messages_deleted_total` | Reaper activity, labeled by reason |
| `narad_data_dir_size_bytes` / `_available_bytes`, `narad_topic_bytes`, `narad_partition_size_bytes`, `narad_segments` | Disk accounting at every zoom level |

### Cluster & misc

`narad_topics_total`, `narad_partitions_total`, `narad_errors_total{component,kind}`, `narad_boot_duration_seconds`.

## Reading the dashboards under failure

What healthy failure handling looks like, so you don't page yourself for the system working:

- **Node killed** → `consumer_lag` and `fanout_lag` spike on its partitions, drain within ~a minute of its return; `due_lag` spikes then returns to 0; duplicates tick up (at-least-once seams). All expected.
- **`fanout_due_lag_seconds` plateaus above 0** → *not* expected. That's the frozen-loss signature; go read the [Cluster Lifecycle war stories](../internals/cluster-lifecycle.md) and check the cursor logs.
- **Readiness down, liveness up on one pod** → it's catching up or waiting for admission. Leave it alone; it knows what it's doing.

## pprof

`narad.pprof.enabled: true` serves the full `net/http/pprof` suite on `:6060`. We keep it on in staging — CPU profiles during soak tests are how the produce hot path stayed honest.

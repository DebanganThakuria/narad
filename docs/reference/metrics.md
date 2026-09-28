# Metrics reference

Look up every Prometheus series a Narad node exports: its type, its labels and what it measures.

```sh title="Request"
curl -sS -u "$AUTH" "$NARAD/metrics" \
  | grep '^narad_consumer_lag_messages{partition="1",topic="orders"}'
```

```text title="Output"
narad_consumer_lag_messages{partition="1",topic="orders"} 1
```

- `$NARAD` is the base URL of one node, for example `http://127.0.0.1:7942`. Metrics are per node: scrape every node, not the load balancer.
- `$AUTH` is `username:password` of any user. On a node with a metrics listener, scrape that listener instead, without credentials.

Every series name starts with `narad_`, except the standard `go_*` and `process_*` series of the Prometheus Go client. Which alerts to set, and what to watch during an incident, is in [Monitor and alert](../operate/monitoring.md).

The tables below group the series by area; each table heading is a link target.
{: #full-metric-reference }

## Where metrics are served {#metrics-listener}

| `http.metrics_addr` | `/metrics` is served | Credentials |
|---|---|---|
| empty (the binary's default) | on the API port | the API's, unless `http.metrics_unauthenticated` is `true` |
| set (the Helm chart sets `:9100`) | on that address only | none |

A metrics listener also serves `/healthz` and `/readyz`. Keep it inside the cluster: the series name every topic, with its traffic and its fan-out links. Details are in the [Configuration reference](configuration.md#http) and [Helm values reference](helm-values.md#metrics).

Gauges that describe partitions (lag, sizes, segments) are refreshed by a poller every 5 seconds. A partition that moved to another node loses its series on this node at the next refresh, so summing across nodes by `topic` and `partition` does not count it twice.

## Traffic {#traffic}

| Series | Type | Labels | Meaning |
|---|---|---|---|
| `narad_messages_produced_total` | counter | `topic`, `partition` | Messages appended to a partition log, counted on the node that owns the partition. |
| `narad_bytes_produced_total` | counter | `topic`, `partition` | Payload bytes appended. |
| `narad_messages_consumed_total` | counter | `topic`, `partition` | Messages handed to queue consumers. Replays are not counted. A message delivered again is counted again. |
| `narad_bytes_consumed_total` | counter | `topic`, `partition` | Payload bytes handed to queue consumers. |
| `narad_produce_rejections_total` | counter | `topic`, `reason` | Produces refused before they were stored: `schema` (the schema refused the payload) or `delayed_child` (a produce to a delay child). |
| `narad_consume_wait_seconds` | histogram | `topic`, `outcome` | Time consumes spent waiting, by `outcome`: `hit`, `timeout`, `cancelled` or `no_wait`. |
| `narad_consume_empty_total` | counter | `topic` | Consumes that returned no message: idle consumers polling. |
| `narad_http_requests_total` | counter | `route`, `method`, `status` | HTTP requests. `route` is the matched route pattern, such as `GET /v1/topics/{topic}`; a request that matches no route is `unmatched`. |
| `narad_http_request_duration_seconds` | histogram | `route`, `method`, `status` | HTTP request duration. |
| `narad_http_request_bytes_in_total` | counter | `route` | Request bytes, from `Content-Length`. |
| `narad_http_response_bytes_out_total` | counter | `route` | Response bytes. |
| `narad_http_requests_in_flight` | gauge | none | HTTP requests being served. |

The HTTP series count requests, not messages. A batch produce (**Unreleased**) has its own route, `POST /v1/topics/{topic}/produce/batch`. A batch consume and a batch ack use the single-message routes, and a batch ack answers `200` where a single ack answers `204`. One batch carries up to 100 messages, so take message rates from `narad_messages_produced_total` and `narad_messages_consumed_total`. A panel that selects `route=~".*/produce"` misses batch produces, and one that counts acks as `status="204"` misses batch acks.

## Queue health {#queue-health}

| Series | Type | Labels | Meaning |
|---|---|---|---|
| `narad_consumer_lag_messages` | gauge | `topic`, `partition` | Messages consumers can still take: the partition's high watermark minus its [committed frontier](glossary.md#committed-frontier). |
| `narad_oldest_unconsumed_message_age_seconds` | gauge | `topic`, `partition` | Age of the oldest message not yet acked; 0 when caught up. |
| `narad_consumer_dropped_messages` | gauge | `topic`, `partition` | Messages deleted by retention before anyone acked them: the start of the log minus the committed frontier, or 0. Any value above 0 is data consumers never saw. |
| `narad_inflight_size` | gauge | `topic`, `partition` | Messages leased right now. Compare with the topic's `max_in_flight_per_partition`. |
| `narad_acked_ahead_size` | gauge | `topic`, `partition` | Acks held above the oldest unacked message. At the topic's `max_acked_ahead_per_partition`, the partition delivers nothing new until that message is acked: a consumer holding it died or never acked. Acks are never refused for it. |
| `narad_ack_extended_total` | counter | `topic` | Leases extended (`extend=true`). |
| `narad_nack_total` | counter | `topic` | Leases handed back early (`extend=0`), including messages a batch consume took but left out of its answer to stay under 8 MiB. |
| `narad_consumer_corrupt_skipped_total` | counter | `topic`, `partition` | Messages consume skipped because they could not be read from disk. Each is a lost message; alert on any increase. |
| `narad_ack_rejected_total` | counter | `reason` | Registered but never incremented. Count `410` answers from `narad_http_requests_total{route="POST /v1/topics/{topic}/ack",status="410"}` instead. |

**Unreleased:** in master, the per-partition gauges on this page (queue health, and the partition size and segment counts) are also exported for partitions whose log is closed, read from disk at most every 30 seconds. A partition with no consumer state loaded reports its stored frontier rather than 0. `narad_partitions_total` and `narad_topic_bytes` count those partitions too, so both can step up after an upgrade to master without any growth.

## Fan-out {#fan-out}

| Series | Type | Labels | Meaning |
|---|---|---|---|
| `narad_fanout_lag_messages` | gauge | `parent`, `child`, `partition` | Parent messages not yet copied to the child: the parent's high watermark minus the child's cursor. The health signal for an ordinary child. |
| `narad_fanout_due_lag_seconds` | gauge | `parent`, `child`, `partition` | How far a delay child's cursor runs behind the messages that are due; 0 while the next one is not due yet. The health signal for a delay child. |
| `narad_fanout_committed_total` | counter | `parent`, `child` | Messages copied into children. |
| `narad_fanout_child_dropped_messages` | counter | `parent`, `child` | Parent messages a child never got: they aged out of the parent's retention before the child's cursor reached them, or could not be read. Each is data loss for that child; alert on any increase. |
| `narad_fanout_batch_records` | histogram | none | Messages per copied batch. |
| `narad_fanout_batch_bytes` | histogram | none | Payload bytes per copied batch. |

## Storage engine {#storage-engine}

| Series | Type | Labels | Meaning |
|---|---|---|---|
| `narad_storage_fsync_duration_seconds` | histogram | `topic`, `partition` | Time spent syncing partition files to disk. |
| `narad_storage_flush_duration_seconds` | histogram | `topic`, `partition` | Time spent writing buffered records to segment files. |
| `narad_storage_flush_bytes_total` | counter | `topic`, `partition` | Bytes written to segment files. |
| `narad_storage_high_watermark_persist_duration_seconds` | histogram | `topic`, `partition`, `outcome` | Time spent writing a partition's `hwm` file (`outcome` `ok` or `error`). In master this happens at most once per log open and once per close, not once per commit, so its rate does not follow commits. |
| `narad_storage_retention_bytes_deleted_total` | counter | `topic`, `partition`, `reason` | Bytes deleted by retention (`reason="age"`). |
| `narad_storage_retention_messages_deleted_total` | counter | `topic`, `partition`, `reason` | Messages deleted by retention. |
| `narad_storage_retention_run_duration_seconds` | histogram | `topic`, `partition` | Time one retention pass took. |
| `narad_data_dir_size_bytes` | gauge | none | Bytes used under the node's data directory. |
| `narad_data_dir_available_bytes` | gauge | none | Bytes free on the data directory's file system. |
| `narad_topic_bytes` | gauge | `topic` | Bytes a topic uses on this node. |
| `narad_partition_size_bytes` | gauge | `topic`, `partition` | Bytes a partition uses. |
| `narad_segments` | gauge | `topic`, `partition` | Segment files of a partition. |

## Storage housekeeping {#storage-housekeeping}

| Series | Type | Labels | Meaning |
|---|---|---|---|
| `narad_open_partition_logs` | gauge | none | Partition logs open on this node. |
| `narad_idle_logs_evicted_total` | counter | none | Logs closed because nothing touched them for `storage.idle_log_eviction_ms`. |
| `narad_cold_retention_swept_total` | counter | none | Closed partitions opened to delete expired data, then closed again. |
| `narad_reaper_restarts` | gauge | none | Times the retention loop was replaced because it stopped. Any value above 0 is worth a look at the logs. |
| `narad_ingress_wal_failed` (unreleased) | gauge | none | `1` once a write or sync of the node's [ingress WAL](glossary.md#ingress-wal) failed, else `0`. While it is `1`, every produce to the node gets `500` until the node restarts; consume and `/readyz` are not affected, so alert on this gauge. |
| `narad_ingress_dispatch_backlog_records` (unreleased) | gauge | none | Records in the ingress WAL that a restart would replay: accepted, but not yet confirmed at their partition owner. Small on a healthy node; a value that stays above 0 while producers are idle means records are not reaching their owners. Wait for 0 on every node before a rollback ([Upgrade Narad](../operate/upgrade.md#roll-back)). |

## Cluster and other series {#cluster-misc}

| Series | Type | Labels | Meaning |
|---|---|---|---|
| `narad_cluster_rpc_requests_total` | counter | `op`, `outcome` | Requests this node sent to other nodes, by operation and `outcome`: `ok`, `rejected` (a 4xx), `failed` (a 5xx), `timeout` or `error`. |
| `narad_cluster_rpc_request_seconds` | histogram | `op` | Their round-trip time. |
| `narad_moves_inflight` | gauge | none | Partition moves this node is running as the destination. |
| `narad_moves_total` | counter | `outcome` | Finished moves: `completed`, or `force_promoted` when the source died and the copy took over. |
| `narad_moves_duration_seconds` | histogram | none | Time from a move starting to the ownership change. |
| `narad_moves_bytes_total` | counter | none | Bytes copied by finished moves. |
| `narad_topics_total` | gauge | none | Topics in the cluster. |
| `narad_partitions_total` | gauge | none | Partitions this node owns. |
| `narad_errors_total` | counter | `component`, `kind` | Errors by where they happened, for example `http`/`5xx`, `storage`/`fsync_poisoned` or `storage`/`retention_unlink`. |
| `narad_boot_duration_seconds` | gauge | none | Time from process start to the API listening, set once. |

The RPC series count requests, not messages. Under heavy load, forwarded acks, extends and nacks to one owner travel together as one `op="ack_batch"` request (always, for a batch ack with two or more handles for one owner), which `op="ack"`, `op="extend_ack"` and `op="nack"` do not count. Add `ack_batch` to a panel that reads those as the forwarded-ack rate.

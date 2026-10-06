---
description: "Look up every Prometheus series a Narad node exports: its type, its labels and what it measures."
---

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

The tables below group the series by area; each table heading is a link target. Each Series cell holds the series name, then its type and labels.
{: #full-metric-reference }

## Where metrics are served {#metrics-listener}

| `http.metrics_addr` | `/metrics` is served | Credentials |
|---|---|---|
| empty (the binary's default) | on the API port | the API's, unless `http.metrics_unauthenticated` is `true` |
| set (the Helm chart sets `:9100`) | on that address only | none |

A metrics listener also serves `/healthz` and `/readyz`. Keep it inside the cluster: the series name every topic, with its traffic and its fan-out links. Details are in the [Configuration reference](configuration.md#http) and [Helm values reference](helm-values.md#metrics).

Gauges that describe partitions (lag, sizes, segments) are refreshed by a poller every 5 seconds. A partition that moved to another node loses its series on this node at the next refresh, so summing across nodes by `topic` and `partition` does not count it twice.

**New in v3.1.0:** the poller runs two loops, both every 5 seconds. The vitals loop refreshes `narad_ingress_wal_failed`, `narad_ingress_dispatch_backlog_records`, `narad_open_partition_logs`, `narad_reaper_restarts` and `narad_data_dir_available_bytes`, each with a 2-second limit, so a slow broker snapshot or a hung volume does not freeze the others. The inventory loop refreshes everything that comes from the broker snapshot and the data-directory walk. Each loop's last finished pass is in `narad_poller_last_success_timestamp_seconds` ([below](#cluster-misc)).

## Traffic {#traffic}

| Series | Meaning |
|---|---|
| `narad_messages_produced_total`<br>counter; labels `topic`, `partition` | Messages appended to a partition log, counted on the node that owns the partition. |
| `narad_bytes_produced_total`<br>counter; labels `topic`, `partition` | Payload bytes appended. |
| `narad_messages_consumed_total`<br>counter; labels `topic`, `partition` | Messages handed to queue consumers. Replays are not counted. A message delivered again is counted again. |
| `narad_bytes_consumed_total`<br>counter; labels `topic`, `partition` | Payload bytes handed to queue consumers. |
| `narad_produce_rejections_total`<br>counter; labels `topic`, `reason` | Produces refused before they were stored: `schema` (the schema refused the payload), `delayed_child` (a produce to a delay child) or (unreleased) `remote_child` (a produce to a remote child's stub). |
| `narad_consume_wait_seconds`<br>histogram; labels `topic`, `outcome` | Time consumes spent waiting, by `outcome`: `hit`, `timeout`, `cancelled` or `no_wait`. |
| `narad_consume_empty_total`<br>counter; labels `topic` | Consumes that returned no message: idle consumers polling. |
| `narad_http_requests_total`<br>counter; labels `route`, `method`, `status` | HTTP requests. `route` is the matched route pattern, such as `GET /v1/topics/{topic}`; a request that matches no route is `unmatched`. |
| `narad_http_request_duration_seconds`<br>histogram; labels `route`, `method`, `status` | HTTP request duration. |
| `narad_http_request_bytes_in_total`<br>counter; labels `route` | Request bytes, from `Content-Length`. |
| `narad_http_response_bytes_out_total`<br>counter; labels `route` | Response bytes. |
| `narad_http_requests_in_flight`<br>gauge; no labels | HTTP requests being served. |
| `narad_http_batch_body_budget_rejections_total` (unreleased)<br>counter; no labels | Batch produce bodies over 1 MiB answered `503` because the node's budget for them (`http.max_batch_body_bytes_in_flight`) was full. |

The HTTP series count requests, not messages. A batch produce (**v3.1.0**) has its own route, `POST /v1/topics/{topic}/produce/batch`. A batch consume and a batch ack use the single-message routes, and a batch ack answers `200` where a single ack answers `204`. One batch carries up to 100 messages (a batch produce up to 1,000, unreleased), so take message rates from `narad_messages_produced_total` and `narad_messages_consumed_total`. A panel that selects `route=~".*/produce"` misses batch produces, and one that counts acks as `status="204"` misses batch acks.

## Schema validation {#schema-validation}

**New in v3.1.0.** These series are process-wide on each node, with no `topic` label.

| Series | Meaning |
|---|---|
| `narad_schema_validation_seconds`<br>histogram; no labels | Time of each schema validation that ran under the node's [validation capacity](schema-rules.md#validation-capacity) bound (payloads above 16 KiB, and payloads on schemas flagged as costly), decode included, waiting for a slot excluded. Small payloads on ordinary schemas are not timed. |
| `narad_schema_validations_in_flight`<br>gauge; no labels | Validations running under that bound now. It sits at the node's CPU count while the bound is full; produces that then wait 5 seconds get `503`. |
| `narad_schema_rejections_total`<br>counter; labels `reason` | Payloads and schema documents refused by schema checks, by `reason`: `depth` (a payload nested deeper than 256 levels), `malformed` (not valid UTF-8, or not one JSON text), `invalid` (the schema refused the payload), `busy` (no validation slot within 5 seconds), `canceled` (the request ended while it waited for a slot), `definition_paths` (a schema refused at registration for reaching a subschema through more than 64 paths) and `definition_pattern` (a schema refused for a costly pattern). |

`narad_produce_rejections_total{reason="schema"}` counts every produce refused by schema validation, `busy` and `canceled` ones included, per topic; these series say why.

## Queue health {#queue-health}

<figure class="nr-dia nr-dia--doc" id="fig-metrics-queue-gauges">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--sky">
--8<-- "diagrams/metrics-queue-gauges.html"
</div>
<figcaption>One partition's log, not to scale. <code>narad_consumer_lag_messages</code> counts from the oldest unacked message (lime) up to the high watermark, so leased and acked-ahead messages count toward lag and the written but hidden tail never does.</figcaption>
</figure>

| Series | Meaning |
|---|---|
| `narad_consumer_lag_messages`<br>gauge; labels `topic`, `partition` | Messages consumers can still take: the partition's high watermark minus its [committed frontier](glossary.md#committed-frontier). |
| `narad_oldest_unconsumed_message_age_seconds`<br>gauge; labels `topic`, `partition` | Age of the oldest message not yet acked; 0 when caught up. |
| `narad_consumer_dropped_messages`<br>gauge; labels `topic`, `partition` | Messages deleted by retention before anyone acked them: the start of the log minus the committed frontier, or 0. Any value above 0 is data consumers never saw. |
| `narad_inflight_size`<br>gauge; labels `topic`, `partition` | Messages leased right now. Compare with the topic's `max_in_flight_per_partition`. |
| `narad_acked_ahead_size`<br>gauge; labels `topic`, `partition` | Acks held above the oldest unacked message. At the topic's `max_acked_ahead_per_partition`, the partition delivers nothing new until that message is acked: a consumer holding it died or never acked. Acks are never refused for it. |
| `narad_ack_extended_total`<br>counter; labels `topic` | Leases extended (`extend=true`). |
| `narad_nack_total`<br>counter; labels `topic` | Leases handed back early (`extend=0`), including messages a batch consume took but left out of its answer to stay under 8 MiB. |
| `narad_consumer_corrupt_skipped_total`<br>counter; labels `topic`, `partition` | Messages consume skipped because they could not be read from disk. Each is a lost message; alert on any increase. |
| `narad_ack_rejected_total`<br>counter; labels `reason` | Registered but never incremented. Count `410` answers to acks from `narad_http_requests_total` instead, as shown below the table. |

The ack answers that found no lease (`410`):

```promql
narad_http_requests_total{route="POST /v1/topics/{topic}/ack",status="410"}
```

**New in v3.1.0:** the per-partition gauges on this page (queue health, and the partition size and segment counts) are also exported for partitions whose log is closed, read from disk at most every 30 seconds. A partition with no consumer state loaded reports its stored frontier rather than 0. `narad_partitions_total` and `narad_topic_bytes` count those partitions too, so both can step up after an upgrade to v3.1.0 without any growth.

## Fan-out {#fan-out}

| Series | Meaning |
|---|---|
| `narad_fanout_lag_messages`<br>gauge; labels `parent`, `child`, `partition` | Parent messages not yet copied to the child: the parent's high watermark minus the child's cursor. The health signal for an ordinary child. |
| `narad_fanout_due_lag_seconds`<br>gauge; labels `parent`, `child`, `partition` | How far a delay child's cursor runs behind the messages that are due; 0 while the next one is not due yet. The health signal for a delay child. |
| `narad_fanout_committed_total`<br>counter; labels `parent`, `child` | Messages copied into children. |
| `narad_fanout_child_dropped_messages`<br>counter; labels `parent`, `child` | Parent messages a child never got: they aged out of the parent's retention before the child's cursor reached them, or could not be read. Each is data loss for that child; alert on any increase. |
| `narad_fanout_batch_records`<br>histogram; no labels | Messages per copied batch. |
| `narad_fanout_batch_bytes`<br>histogram; no labels | Payload bytes per copied batch. |

## Remote replication {#remote-replication}

**Unreleased:** in master, not in v3.1.0.

A [remote child](glossary.md#remote-child) exports its link per parent partition from the node that runs the cursor, which is the owner of that parent partition; a cursor that stops on a node (its partition moved, the link was deleted) removes its per-partition series there. Each remote exports its transport, credential and key state per node. No series carries a URL, a username, a key version or a ciphertext. `narad_fanout_lag_messages`, `narad_fanout_committed_total` and `narad_fanout_child_dropped_messages` above count remote children too. Alerts are in [Monitor and alert](../operate/monitoring.md#remote-alerts).

| Series | Meaning |
|---|---|
| `narad_fanout_remote_state`<br>gauge; labels `parent`, `child`, `partition`, `state` | 1 for the cursor's current [link state](remote-children.md#link-states). A state change removes the series of the state before, so each partition has one series. |
| `narad_fanout_remote_lag_seconds`<br>gauge; labels `parent`, `child`, `partition` | Age of the oldest parent record the remote has not accepted yet: the live recovery point. Kept fresh every 5 seconds while a slab does not ship. |
| `narad_fanout_remote_retention_headroom_seconds`<br>gauge; labels `parent`, `child`, `partition` | The parent's retention minus that age: the time left before drop-behind. |
| `narad_fanout_remote_last_success_timestamp_seconds`<br>gauge; labels `parent`, `child` | Unix time of the last request the remote accepted. |
| `narad_fanout_remote_check_failures_total`<br>counter; labels `parent`, `child` | Target checks while the link runs that errored. A check that errors never stops sending. |
| `narad_fanout_remote_skipped_records_total`<br>counter; labels `parent`, `child` | Parent records dropped because an admin skipped them: each is a record not copied. |
| `narad_remote_requests_total`<br>counter; labels `remote`, `code` | Requests this node sent to a remote, by status code (`error` for a transport failure). |
| `narad_remote_request_seconds`<br>histogram; labels `remote` | Round-trip time of those requests. |
| `narad_remote_rtt_seconds`<br>gauge; labels `remote` | TCP connect time to the remote, as the last check measured it. |
| `narad_remote_errors_total`<br>counter; labels `remote`, `class` | Failed requests, by class: a link state, `edge` (something in front of the target answered) or `encoding` (the target could not decode a compressed request). |
| `narad_remote_resent_records_total`<br>counter; labels `remote` | Records sent again after a failure that left it unknown whether they landed: the duplicate volume on the remote. |
| `narad_remote_gate_backoff_seconds`<br>gauge; labels `remote` | The remote's current backoff on this node; 0 while healthy. |
| `narad_remote_held_bytes`<br>gauge; no labels | Bytes of records held in memory across a failure, against `remotes.max_held_bytes`. |
| `narad_remote_inflight_wait_seconds`<br>histogram; labels `remote` | Time a request waited for one of the remote's `max_in_flight` slots. |
| `narad_remote_chunk_bytes_limit`<br>gauge; labels `remote` | The remote's adaptive request size cap on this node: 960 KiB when healthy, down to 64 KiB after timeouts. |
| `narad_remote_wire_bytes_total`, `narad_remote_body_bytes_total`<br>counter; labels `remote` | Request body bytes sent to the remote, after and before compression. |
| `narad_remote_credential_state`<br>gauge; labels `remote`, `state` | 1 for this node's [credential cache state](remote-children.md#cache-states) of the remote: `ready`, `stale`, `credential_unreadable` or `node_insecure`. |
| `narad_remote_credential_decrypts_total`<br>counter; labels `remote` | Password decryptions by this node's cache. It moves once per credential version; a rise without a remote change is a bug. |
| `narad_remote_credential_age_seconds`<br>gauge; labels `remote` | Seconds since the remote's password was last set. |
| `narad_remote_credential_key_current`<br>gauge; labels `remote` | 1 when the remote's password is sealed under the current key; 0 means a cluster secret rotation was not finished with a re-encrypt. |
| `narad_remote_key_seals`<br>gauge; labels `key` | Seals counted under a key (`current` or `previous`), a lower bound. A key seals at most 2^30. |
| `narad_remote_key_age_seconds`<br>gauge; labels `key` | Seconds since the current key first sealed a password. |
| `narad_remote_seals_total`<br>counter; no labels | Passwords this node sealed. |
| `narad_remote_reseal_opens_total`<br>counter; no labels | Passwords this node opened to re-encrypt them, as leader. |
| `narad_remote_destination_refused_total`<br>counter; labels `remote`, `reason` | Dials and redirects the address guard, the port list or the host allowlist refused. |
| `narad_remotes_allowlist_configured`<br>gauge; no labels | 1 when `remotes.allowed_hosts` is set on this node. |
| `narad_remotes_plaintext_raft`<br>gauge; no labels | 1 when this node holds remotes and its Raft transport runs without TLS. |

## Storage engine {#storage-engine}

| Series | Meaning |
|---|---|
| `narad_storage_fsync_duration_seconds`<br>histogram; labels `topic`, `partition` | Time spent syncing partition files to disk. |
| `narad_storage_flush_duration_seconds`<br>histogram; labels `topic`, `partition` | Time spent writing buffered records to segment files. |
| `narad_storage_flush_bytes_total`<br>counter; labels `topic`, `partition` | Bytes written to segment files. |
| `narad_storage_high_watermark_persist_duration_seconds`<br>histogram; labels `topic`, `partition`, `outcome` | Time spent writing a partition's `hwm` file (`outcome` `ok` or `error`). From v3.1.0 this happens at most once per log open and once per close, not once per commit, so its rate does not follow commits. |
| `narad_storage_retention_bytes_deleted_total`<br>counter; labels `topic`, `partition`, `reason` | Bytes deleted by retention (`reason="age"`). |
| `narad_storage_retention_messages_deleted_total`<br>counter; labels `topic`, `partition`, `reason` | Messages deleted by retention. |
| `narad_storage_retention_run_duration_seconds`<br>histogram; labels `topic`, `partition` | Time one retention pass took. |
| `narad_data_dir_size_bytes`<br>gauge; no labels | Bytes used under the node's data directory. |
| `narad_data_dir_available_bytes`<br>gauge; no labels | Bytes free on the data directory's file system. |
| `narad_topic_bytes`<br>gauge; labels `topic` | Bytes a topic uses on this node. |
| `narad_partition_size_bytes`<br>gauge; labels `topic`, `partition` | Bytes a partition uses. |
| `narad_segments`<br>gauge; labels `topic`, `partition` | Segment files of a partition. |

## Storage housekeeping {#storage-housekeeping}

| Series | Meaning |
|---|---|
| `narad_open_partition_logs`<br>gauge; no labels | Partition logs open on this node. |
| `narad_idle_logs_evicted_total`<br>counter; no labels | Logs closed because nothing touched them for `storage.idle_log_eviction_ms`. |
| `narad_cold_retention_swept_total`<br>counter; no labels | Closed partitions opened to delete expired data, then closed again. |
| `narad_cold_retention_panics_total`<br>counter; no labels | Closed partitions whose open, sweep or close panicked during the cold retention walk. Each panic was contained: logged at error with the partition and the stack, the partition left alone for 30 minutes, the walk carried on. Any value above 0 is worth a look at the logs. |
| `narad_reaper_restarts`<br>gauge; no labels | Times the retention loop was replaced because it stopped. Any value above 0 is worth a look at the logs. |
| `narad_ingress_wal_failed` (v3.1.0)<br>gauge; no labels | `1` once a write or sync of the node's [ingress WAL](glossary.md#ingress-wal) failed, else `0`. While it is `1`, every produce to the node gets `500` until the node restarts; consume and `/readyz` are not affected, so alert on this gauge. |
| `narad_ingress_dispatch_backlog_records` (v3.1.0)<br>gauge; no labels | Records in the ingress WAL that a restart would replay: accepted, but not yet confirmed at their partition owner. Small on a healthy node; a value that stays above 0 while producers are idle means records are not reaching their owners. Wait for 0 on every node before a rollback ([Upgrade Narad](../operate/upgrade.md#roll-back)). |
| `narad_quarantined_copies` (v3.1.0)<br>gauge; no labels | Partition copies this node set aside instead of deleting: a stale copy or an earlier copy a move found that the new owner cannot vouch for (`topics/<topic>/p<N>.quarantine*`), a set-aside move staging copy (`.moves/<topic>-<N>.quarantine*`), and a deleted topic incarnation's directory (`topics/<topic>.stale-<id>*`). Any of them may hold the only instance of some records, and Narad never removes one on its own except a deleted incarnation's directory, once the leader confirms the incarnation gone. Refreshed every stale-copy sweep (about 30 s) and at startup, never on a scrape; absent until the first inventory. Alert on a value above 0 ([Troubleshooting](../operate/troubleshooting.md#quarantined-copies)). |
| `narad_quarantined_bytes` (v3.1.0)<br>gauge; no labels | Bytes those copies hold. |
| `narad_orphan_topic_dirs` (v3.1.0)<br>gauge; no labels | Topic directories of topics this node's replica no longer knows (a deleted topic whose purge never reached this node) that the last sweep left in place: directories without an incarnation marker, which only a restart removes, and directories the leader has not yet confirmed gone. A value that stays above 0 needs a look ([Troubleshooting](../operate/troubleshooting.md#orphan-topic-directories)). |

## Metastore and Raft {#metastore-raft}

Every node holds a full replica of the [metastore](glossary.md#metastore), kept in step by Raft ([Metastore and Raft](../understand/metastore-and-raft.md)). These series are read when Prometheus scrapes, without waiting on Raft, so they still answer while the node's Raft is stuck. All of them are new in v3.1.0; v3.0.1 exports none.

| Series | Meaning |
|---|---|
| `narad_raft_state`<br>gauge; labels `state` | `1` for this node's Raft state (`follower`, `candidate`, `leader` or `shutdown`), `0` for the other three. Exactly one node of a healthy cluster reports `leader`; `shutdown` means the node left Raft, for example because its metastore stopped applying. |
| `narad_raft_term`<br>gauge; no labels | Current Raft term. It moves on every election, so a term that keeps climbing means leaders keep changing. |
| `narad_raft_last_log_index`<br>gauge; no labels | Index of the last entry in this node's Raft log. |
| `narad_raft_commit_index`<br>gauge; no labels | Raft commit index as this node knows it. |
| `narad_raft_applied_index`<br>gauge; no labels | Last Raft index handed to this node's state machine. |
| `narad_raft_fsm_pending`<br>gauge; no labels | Batches of committed entries waiting for this node's state machine. Above 0 for long means it applies slower than entries commit. |
| `narad_raft_has_leader`<br>gauge; no labels | `1` while this node knows a Raft leader (itself included), else `0`. Without a leader no metadata write (create a topic, register a member) succeeds. |
| `narad_raft_last_contact_seconds`<br>gauge; no labels | Seconds since this node last heard from the leader: `0` on the leader, and the time since the node started on one that has never heard from a leader. |
| `narad_raft_voters`<br>gauge; no labels | Voters in the latest Raft configuration this node knows. |
| `narad_raft_nonvoters`<br>gauge; no labels | Non-voting servers in that configuration: nodes that joined and are not yet [promoted to voter](../understand/cluster-lifecycle.md#join-promotion). Above 0 for more than a minute after a scale-out means a promotion is deferred ([Troubleshooting](../operate/troubleshooting.md#nonvoters-stay)). |
| `narad_metastore_fsm_bytes`<br>gauge; no labels | Size of the metadata database file (`fsm.db`). The file never shrinks, and every Raft snapshot first copies it beside itself, so the volume needs this much free space on top. |
| `narad_metastore_applied_index`<br>gauge; no labels | Highest Raft index whose effects are in this node's metadata database. It equals `narad_raft_applied_index` right after a metadata change, and otherwise trails it by the entries Raft applies without touching `fsm.db`: the no-op each new leader writes, barriers (the controller's before each planning pass, among others) and configuration changes (joins, promotions, removals). So a steady gap is normal, even on a quiet cluster; compare the two only for growth. |
| `narad_metastore_apply_errors_total`<br>counter; labels `kind` | Raft entries the node could not apply as proposed, by `kind`: `storage` (its disk refused the write; retried for up to 30 s, then the node stops), `unknown_entry_type` (a newer release proposed it; the node stops), `undecodable` (skipped on every node alike) and `newer_database` (a snapshot from a newer release; the node stops). |
| `narad_metastore_apply_stalled`<br>gauge; no labels | `1` while the node retries a metadata write its disk refused, else `0`. |
| `narad_metastore_apply_stopped`<br>gauge; no labels | `1` once the node has stopped applying Raft entries, else `0`. It then leaves Raft and exits; see [When a node stops applying](../understand/metastore-and-raft.md#fail-stop). |
| `narad_metastore_snapshot_bytes`<br>gauge; no labels | Size of the last Raft snapshot this node wrote (`0` before the first). |
| `narad_metastore_snapshot_duration_seconds`<br>gauge; no labels | How long that snapshot took, from the copy of `fsm.db` to the snapshot file's close. |
| `narad_metastore_snapshot_failures_total`<br>counter; no labels | Raft snapshots that failed on this node, for example for lack of disk space for the copy. Raft tries again at its next interval, and its log grows until one succeeds. |
| `narad_raft_tls_cert_not_after_seconds`<br>gauge; labels `kind` | When this node's Raft TLS certificate (`kind="leaf"`) and the earliest-expiring CA in its Raft CA bundle (`kind="ca"`) expire, in Unix seconds. Absent when the Raft transport runs without TLS. The node reads the files only at startup, so a renewed certificate shows here after the restart that loads it. Alert on `narad_raft_tls_cert_not_after_seconds - time() < 7 * 86400` ([Raft TLS certificates](../operate/raft-tls.md#expiry)). |

## Authentication {#authentication}

| Series | Meaning |
|---|---|
| `narad_auth_verify_queued`<br>gauge; no labels | Password checks (bcrypt) admitted and not yet finished, waiting for one of the node's 4 verification slots or running (from v3.1.0). It includes checks whose clients have already gone: they still run. A sustained value above 0 with a rising rate of `401` and `429` answers is a failed-login flood ([Failed-login throttle](../understand/networking-and-security.md#auth-throttle)). |

## Cluster and other series {#cluster-misc}

| Series | Meaning |
|---|---|
| `narad_cluster_rpc_requests_total`<br>counter; labels `op`, `outcome` | Requests this node sent to other nodes, by operation and `outcome`: `ok`, `rejected` (a 4xx), `failed` (a 5xx), `timeout` or `error`. |
| `narad_cluster_rpc_request_seconds`<br>histogram; labels `op` | Their round-trip time. |
| `narad_moves_inflight`<br>gauge; no labels | Partition moves this node is running as the destination. |
| `narad_moves_total`<br>counter; labels `outcome` | Finished moves: `completed`, or `force_promoted` when the source died and the copy took over. |
| `narad_moves_duration_seconds`<br>histogram; no labels | Time from a move starting to the ownership change. |
| `narad_moves_bytes_total`<br>counter; no labels | Bytes copied by finished moves. |
| `narad_moves_blocked` (v3.1.0)<br>gauge; labels `reason` | Moves that cannot finish on their own. On a move's destination: `copy_unverifiable` (the staged copy failed verification twice, the second time after a fresh copy, so the node stopped freezing the source; or a dead source's copy fails it) and `source_dead_copy_behind` (the source is dead and the copy is behind its last high watermark, so it cannot be force-promoted). On the leader only: `source_dead` and `target_dead`, the in-flight moves whose source or destination member is dead. Every reason is exported at 0. Alert on a value above 0 ([Troubleshooting](../operate/troubleshooting.md#moves-blocked)). |
| `narad_topics_total`<br>gauge; no labels | Topics in the cluster. |
| `narad_partitions_total`<br>gauge; no labels | Partitions this node owns. |
| `narad_errors_total`<br>counter; labels `component`, `kind` | Errors by where they happened, for example `http`/`5xx`, `storage`/`fsync_poisoned` or `storage`/`retention_unlink`. |
| `narad_boot_duration_seconds`<br>gauge; no labels | Time from process start to the API listening, set once. |
| `narad_poller_last_success_timestamp_seconds` (v3.1.0)<br>gauge; labels `loop` | When the metrics poller's `vitals` or `inventory` loop last finished a pass, in Unix seconds; until the first, when the poller started. Both loops run every 5 seconds, so more than 30 seconds old means the gauges that loop feeds are frozen. The vitals loop does not count a pass in which a source failed or did not answer within 2 seconds; `narad_errors_total{component="metrics"}` says which source (kind `<source>_timeout`, `<source>_panic` or `<source>`). |
| `narad_member_heartbeat_failures` (v3.1.0)<br>gauge; no labels | This node's consecutive failed member heartbeats to the Raft leader, `0` after a success. Heartbeats run every 5 seconds, and the leader marks a member dead after 30 seconds without one. |
| `narad_member_heartbeat_last_success_timestamp_seconds` (v3.1.0)<br>gauge; no labels | When this node's last member heartbeat succeeded, in Unix seconds; `0` until the first. |

The RPC series count requests, not messages. Under heavy load, forwarded acks, extends and nacks to one owner travel together as one `op="ack_batch"` request (always, for a batch ack with two or more handles for one owner), which `op="ack"`, `op="extend_ack"` and `op="nack"` do not count. Add `ack_batch` to a panel that reads those as the forwarded-ack rate.

### Cluster controller {#cluster-controller}

**New in v3.1.0.** The controller runs on the Raft leader only, so these series hold a value only there: every other node, and a node that lost leadership, reports 0 or no series.

| Series | Meaning |
|---|---|
| `narad_decommission_blocked`<br>gauge; labels `node`, `reason` | 1 for each reason a draining node's decommission cannot progress: `below_min_voters`, `no_healthy_majority`, `owner_dead`, `no_receivers`, `node_status_unavailable` (each needs you), or `dispatch_backlog`, `move_target`, `move_budget_full`, `leader_transfer` (each clears on its own; `move_target` needs you when the move's source is dead, and is then logged at error). A series goes away when its reason does. Alert on any series that stays ([Troubleshooting](../operate/troubleshooting.md#decommission-blocked)). |
| `narad_dead_marking_refused`<br>gauge; no labels | 1 while the leader refuses a dead verdict that would leave fewer alive Raft voters than a quorum, else 0. A leader that holds its lease cannot have lost most voters, so its node RPC plane is the likelier fault ([Troubleshooting](../operate/troubleshooting.md#log-dead-marking-refused)). |
| `narad_colocated_child_partitions`<br>gauge; no labels | Fan-out child partitions owned by the same node as their parent's same-index partition, so both copies sit on one disk. Placement avoids it when it can; nothing moves a partition to fix it ([Back up and replicate topics](../operate/backups.md)). |

---
description: "Scrape Narad's Prometheus metrics, graph the series that matter, and set up the seven alerts that catch real trouble."
---

# Monitor and alert

Scrape Narad's Prometheus metrics, graph the series that matter, and set up the seven alerts that catch real trouble.

Before you start: a cluster installed with the Helm chart, and a Prometheus that can reach its pods.

## Scrape the metrics {#scrape}

With the chart's default `metrics.enabled: true`, every pod serves `/metrics` on its own listener on port 9100, without credentials. Check one pod:

```bash
kubectl port-forward -n narad pod/narad-0 9100:9100
```

In a second terminal:

```bash
curl -s http://127.0.0.1:9100/metrics \
  | grep -E '^narad_(topics_total|data_dir_available_bytes) '
```

```text title="Output"
narad_data_dir_available_bytes 1.78317099008e+11
narad_topics_total 0
```

Every metric name starts with `narad_`. Point Prometheus at port 9100 in one of two ways:

- **Pod annotations.** Each pod carries `prometheus.io/scrape: "true"`, `prometheus.io/port: "9100"` and `prometheus.io/path: /metrics`, which an annotation-based scrape config picks up.
- **Prometheus Operator.** Set `serviceMonitor.enabled=true` and the chart creates a ServiceMonitor for the `metrics` port of its Service.

Port 9100 is unauthenticated and names every topic, so keep it inside the cluster ([Production checklist](production-checklist.md#metrics-exposure)). The same listener answers `/healthz` and `/readyz` for the kubelet's probes ([Helm values reference](../reference/helm-values.md#ports-and-probes)). Every metric is described in the [Metrics reference](../reference/metrics.md).

## Graph the metrics {#dashboard}

Narad does not ship a dashboard. These queries cover what a dashboard per node needs; add a `job` or `instance` selector to match your scrape config.

| Panel | Queries |
|---|---|
| Message throughput | `sum(rate(narad_messages_produced_total[1m]))`, `sum(rate(narad_messages_consumed_total[1m]))`, and empty consumes, `sum(rate(narad_consume_empty_total[1m]))` |
| HTTP requests | `sum by (route, status) (rate(narad_http_requests_total[1m]))` |
| HTTP latency | `histogram_quantile(0.99, sum by (le, route) (rate(narad_http_request_duration_seconds_bucket[5m])))` |
| Errors and rejections | `sum by (route, status) (rate(narad_http_requests_total{status!~"2.."}[1m]))`, `sum by (component, kind) (rate(narad_errors_total[1m]))`, `sum by (reason) (rate(narad_produce_rejections_total[1m]))` |
| Consumer backlog | `sum(narad_consumer_lag_messages)`, `sum(narad_inflight_size)`, `sum(narad_acked_ahead_size)` |
| Consumer age and wait | `max(narad_oldest_unconsumed_message_age_seconds)`, `histogram_quantile(0.95, sum by (le, outcome) (rate(narad_consume_wait_seconds_bucket[5m])))` |
| Disk | `narad_data_dir_size_bytes`, `narad_data_dir_available_bytes`, and the largest topics, `topk(10, sum by (topic) (narad_topic_bytes))` |
| Storage latency | `histogram_quantile(0.95, sum by (le) (rate(narad_storage_flush_duration_seconds_bucket[5m])))`, and the same over `narad_storage_fsync_duration_seconds_bucket` |
| Storage throughput | `sum(rate(narad_storage_flush_bytes_total[1m]))`, `sum(rate(narad_storage_retention_bytes_deleted_total[1m]))`, `sum(rate(narad_bytes_produced_total[1m]))` |
| Process | `rate(process_cpu_seconds_total[1m])`, `process_resident_memory_bytes`, `go_memstats_heap_alloc_bytes`, `go_goroutines`, `process_open_fds` |
| Inventory | `narad_topics_total`, `narad_partitions_total`, `narad_boot_duration_seconds` |

From v3.1.0, two series read differently from their names:

- `narad_http_requests_total` counts requests, not messages. One batch request carries up to 100 messages, so once clients batch, read throughput from `narad_messages_produced_total` and `narad_messages_consumed_total` instead.
- `narad_storage_high_watermark_persist_duration_seconds` no longer measures a commit, so leave it off a storage latency panel.

## Set up the alerts {#alerts}

If you configure nothing else, configure these seven. Each one fires on a condition that needs a person. The first six read Narad's metrics; the seventh reads a Kubernetes metric from kube-state-metrics, because Narad has no metric for a node that is down:

| Alert | Expression | What it means |
|---|---|---|
| Fan-out data loss | `rate(narad_fanout_child_dropped_messages[5m]) > 0` | A [fan-out child](../reference/glossary.md#fan-out-child) fell behind its parent's retention, or hit an unreadable record, and lost records. |
| Delay child behind | `narad_fanout_due_lag_seconds > 60` | Due messages are not reaching a [delay child](../reference/glossary.md#delay-child). This is the only lag signal for delay children: their offset lag is always about rate times delay. |
| Consumer-side loss | `rate(narad_consumer_corrupt_skipped_total[5m]) > 0` or `narad_consumer_dropped_messages > 0` | A consumer skipped a permanently unreadable record, or [retention](../reference/glossary.md#retention) deleted messages nobody had acked. |
| Disk runway | `predict_linear(narad_data_dir_available_bytes[6h], 24 * 3600) < 0` | At the rate of the last six hours, the data volume fills within a day. |
| Produce latched off (v3.1.0) | `narad_ingress_wal_failed == 1` | A write or sync of the node's [ingress WAL](../reference/glossary.md#ingress-wal) failed. The node answers every produce with `500` until it restarts, while consume and `/readyz` keep working. |
| Quarantined copies (v3.1.0) | `narad_quarantined_copies > 0` | The node set a partition copy aside instead of deleting it, because the copy may hold the only instance of some records. Narad never serves it and never removes it on its own, so it needs a person to look at it. |
| Pod not ready (Kubernetes metric) | `kube_pod_status_ready{namespace="narad", condition="true"} == 0`, held for 2 minutes (`for: 2m`) | A Narad pod has not been ready for 2 minutes. The messages stored on it wait until it is back. |

On v3.0.1, which has no `narad_ingress_wal_failed`, watch for the same failure with `rate(narad_errors_total{component="http", kind="5xx"}[5m]) > 0`, which also catches other server errors.

### Metastore and Raft alerts (v3.1.0) {#metastore-alerts}

From v3.1.0, nodes export the [metastore and Raft series](../reference/metrics.md#metastore-raft). Add these alongside the six:

| Alert | Expression | What it means |
|---|---|---|
| Metastore stopped | `narad_metastore_apply_stopped == 1` | The node stopped applying Raft entries: its disk kept refusing a metadata write for about 26 s of retries, or it met an entry or a snapshot from a newer release. It leaves Raft and exits at once, so a scrape often misses this gauge; the restart loop that follows shows as Pod not ready, and the node's log says why (`metastore: stopped applying raft entries`). |
| Metastore write stalled | `narad_metastore_apply_stalled == 1` | The node's disk is refusing a metadata write. It retries for up to 30 s and then stops as above, so this is the warning that comes first. |
| No Raft leader | `narad_raft_has_leader == 0`, held for 1 minute (`for: 1m`) | The node has known no Raft leader for a minute, so no metadata write (create a topic, register a member) succeeds through it. On every node at once, the cluster has lost its quorum. |
| Metastore growth | `delta(narad_metastore_fsm_bytes[1d]) > 64 * 1024 * 1024` | The metadata database grew by more than 64 MiB in a day, which normal topic and user churn does not do: look for something creating topics, schema versions or users in a loop. Every Raft snapshot copies the file beside itself first, so keep that much free space on the volume as well. |

What to do about a stopped metastore is on the Troubleshooting page under [metastore: stopped applying raft entries](troubleshooting.md#log-metastore-stopped).

### Certificate, poller and heartbeat alerts (v3.1.0) {#node-health-alerts}

From v3.1.0, nodes also export when the Raft TLS certificate expires, when the metrics poller last finished a pass, and whether this node's member heartbeats are failing. A node on v3.0.1 exports none of these series, so these alerts see nothing for it.

| Alert | Expression | What it means |
|---|---|---|
| Raft certificate expiring | `narad_raft_tls_cert_not_after_seconds - time() < 7 * 86400` | The node's Raft TLS certificate (`kind="leaf"`) or the earliest-expiring CA in its bundle (`kind="ca"`) expires within 7 days. One certificate usually serves every node, so once it expires peers refuse every new Raft connection. Narad reads the files only at startup: renew them, then restart the pods one at a time ([Renew node certificates](raft-tls.md#renew)). |
| Metrics poller frozen | `time() - narad_poller_last_success_timestamp_seconds > 30` | A poller loop has not finished a pass for 30 s, so the gauges it feeds show old values: with `loop="vitals"` the WAL health and backlog, open logs, reaper restarts and free space, with `loop="inventory"` the per-partition gauges and topic counts. The node's log and `narad_errors_total{component="metrics"}` say which source failed or hung. |
| Member heartbeats failing | `narad_member_heartbeat_failures > 0`, held for 1 minute (`for: 1m`) | The node has not heartbeated its membership to the Raft leader for a minute, and the leader marks a member dead after 30 s without one. The node logs `member heartbeat failing` with the last error ([Troubleshooting](troubleshooting.md#log-member-heartbeat-failing)). |

### Decommission and move alerts (v3.1.0) {#move-alerts}

The leader exports why a decommission or a move cannot progress ([Cluster controller metrics](../reference/metrics.md#cluster-controller)). Add these too:

| Alert | Expression | What it means |
|---|---|---|
| Decommission blocked | `max by (node, reason) (narad_decommission_blocked) == 1`, held for 10 minutes (`for: 10m`) | A draining node's decommission has not progressed for 10 minutes, for the reason in the label. Some reasons clear on their own within minutes (`dispatch_backlog`, `move_target` unless the move's source is dead, `move_budget_full`, `leader_transfer`); the others need a person ([Troubleshooting](troubleshooting.md#decommission-blocked)). |
| Moves blocked | `sum by (reason) (narad_moves_blocked) > 0`, held for 10 minutes (`for: 10m`) | A partition move cannot finish on its own, and holds one of the 8 move slots until it does or is aborted ([Troubleshooting](troubleshooting.md#moves-blocked)). |
| Dead marking refused | `max(narad_dead_marking_refused) == 1`, held for 5 minutes (`for: 5m`) | The leader is not hearing heartbeats from most voters although Raft still reaches them: its node RPC plane is likely broken ([Troubleshooting](troubleshooting.md#log-dead-marking-refused)). |

### Remote replication alerts (v3.2.0) {#remote-alerts}

A cluster with [remote children](../reference/glossary.md#remote-child) adds these, on the [remote replication series](../reference/metrics.md#remote-replication). The link series come from the node that owns each parent partition, so evaluate them across every node; for a disaster-recovery link, evaluate them outside the source cluster's region ([Watch the link](playbooks/disaster-recovery.md#watch)).

| Alert | Expression | What it means |
|---|---|---|
| Link stalled | `max by (parent, child, state) (narad_fanout_remote_state{state=~NEEDS_FIX}) == 1`, with `NEEDS_FIX` below | The link holds in a state only a person fixes. Page at once ([Troubleshooting](troubleshooting.md#remote-link-stalled)). |
| Link not running | `max by (parent, child, state) (narad_fanout_remote_state{state!~HEALTHY}) == 1`, held for 5 minutes (`for: 5m`) | Any other state (`unavailable`, `throttled`, `unknown`) that has not cleared on its own. |
| Recovery point | `max by (parent, child) (narad_fanout_remote_lag_seconds) > <objective>` | The oldest record not yet on the remote is older than your recovery point objective. |
| Headroom | `min by (parent, child) (narad_fanout_remote_retention_headroom_seconds) < 4 * 3600` (warn below 12 hours) | The oldest unshipped record ages out of the parent within 4 hours: drop-behind comes next ([Troubleshooting](troubleshooting.md#remote-headroom-low)). |
| No progress | `(time() - narad_fanout_remote_last_success_timestamp_seconds > 300) and on (instance, parent, child) (sum by (instance, parent, child) (narad_fanout_lag_messages) > 0)` | Nothing from a node reached the remote for 5 minutes although records wait on that node. |
| Records skipped | `increase(narad_fanout_remote_skipped_records_total[5m]) > 0` | An admin's skip dropped a record: one record not copied. |
| Credential unreadable | `narad_remote_credential_state{state="credential_unreadable"} == 1` or the same with `node_insecure` | A node cannot use a remote's password ([Troubleshooting](troubleshooting.md#remote-credential-unreadable)). |
| Credential stale | `max by (remote) (narad_remote_credential_version) != min by (remote) (narad_remote_credential_version)` held for 1 minute | A node still holds an older password than its peers: its Raft replica is behind ([Troubleshooting](troubleshooting.md#remote-credential-unreadable)). |
| Destination refused | `increase(narad_remote_destination_refused_total[5m]) > 0` | The address guard refused a dial or a redirect: DNS or a URL points where it must not. |
| Held memory | `narad_remote_held_bytes > 0.8 * <remotes.max_held_bytes>` | Cursors hold most of the node's budget for waiting records; past it they read again from disk. |
| Password age | `narad_remote_credential_age_seconds > 80 * 86400` | A remote's password is due for [rotation](remotes.md#rotate-password). |
| Rotation unfinished | `narad_remote_credential_key_current == 0`, held for 1 day | A cluster secret rotation was not finished with `narad remote reencrypt`. |
| Key age | `narad_remote_key_age_seconds{key="current"} > <rotation period> - 10 * 86400` | The cluster secret is due for rotation within 10 days ([operating condition 3](remotes.md#operating-conditions)). |
| Key seals | `narad_remote_key_seals{key="current"} > 2^29` | The key is halfway to the 2^30 seals it allows. |
| Allowlist unset, plaintext Raft | `narad_remotes_allowlist_configured == 0` or `narad_remotes_plaintext_raft == 1` in production | [Operating condition 4](remotes.md#operating-conditions), and Raft TLS on a cluster that holds remotes. |
| Batch body budget | `rate(narad_http_batch_body_budget_rejections_total[5m]) > 0`, sustained | Large batch produces are turned away with `503`: raise `http.max_batch_body_bytes_in_flight` or send smaller batches. |

The two state selectors, as PromQL regular expressions:

```text
NEEDS_FIX = "remote_missing|credential_unreadable|node_insecure|destination_refused|auth_failed|forbidden|target_replaced|target_has_remote_children|target_missing|no_batch_produce|redirect_refused|tls_failed|rejected_record|record_too_large"
HEALTHY   = "running|paused"
```

`narad_fanout_child_dropped_messages`, in the first table, counts a remote child's drop-behind too. A dashboard of `narad_remote_resent_records_total` (duplicates on the remote), `narad_remote_inflight_wait_seconds` (lanes waiting for a slot: raise `max_in_flight`) and `narad_remote_chunk_bytes_limit` (at the 64 KiB floor for 10 minutes, the path is cutting uploads short) explains most slow links.

What to do when one fires is on the [Troubleshooting](troubleshooting.md) page: [produce latched off](troubleshooting.md#produce-500), [delay child behind](troubleshooting.md#due-lag-stuck), [messages lost to retention](troubleshooting.md#log-frontier-behind-retention), [quarantined copies](troubleshooting.md#quarantined-copies), [pod not ready](troubleshooting.md#node-down). For disk runway, check the retention of the largest topics against [Capacity and disk sizing](../reference/capacity.md#disk-sizing).

`rate(narad_errors_total[5m])`, split by its `component` and `kind` labels, makes a useful catch-all panel beside these alerts.

## Read the audit log {#audit-log}

Changes to users, topics and cluster membership are logged as audit lines: message `audit`, attribute `component=audit`, with `event`, `actor` (the authenticated user, empty with security off) and `target`. Route `component=audit` to its own sink if you keep an audit trail. A line is written on the node the client called, also when that node forwarded the change to the Raft leader.

**New in v3.1.0:** topic changes are audited too, once their body passed validation: `topic.create`, `topic.alter` (with `fields`, the retention, cap and partition fields it set), `topic.schema`, `topic.delete` (with `incarnation` when the node that answered ran the delete), `topic.attach` and `topic.detach` (with `child`). These lines also carry `status`, the HTTP status the client got, and `outcome`, which says what happened to the change the line names:

| `outcome` | Meaning |
|---|---|
| `ok` | Applied. |
| `denied` | Refused with `403`; logged at warning level. |
| `rejected` | Refused with another `4xx` by the answering node or the leader. Nothing the line names changed. |
| `failed` | A `5xx` decided by the answering node or the leader. |
| `unknown` | The request ended without a decision the answering node knows: a forward to the leader whose answer never came back (`503`), a `503` from a leader that lost its leadership while committing the change (a later leader may still commit it), or a client that went away mid-change (`499`). The change may have been applied; read the topic to find out. |

`unknown` is never logged as `rejected` or `failed`: a search for the changes that may have happened must include it along with `ok`.

**New in v3.2.0:** remotes and remote children are audited too: `remote.create`, `remote.update`, `remote.delete`, `remote.test`, `remote.reencrypt`, `remote.list` and `remote.get`, and `remote_child.create`, `.pause`, `.resume`, `.accept_target`, `.skip` and `.delete`, refusals included. Each request carries a `request_id`, and the leader writes a second line for every write it proposes, with the same `request_id` and `outcome` `committed` or `refused`, so the two can be joined. No line carries a password, a URL or a username ([Audit lines](remotes.md#audit)). A remote write the leader committed that the answering node could not confirm it applied before answering is answered `503` with `Retry-After`, and its line on that node says `outcome=ok status=503 settled=false`: the change was made.

A `PATCH` that sets several kinds of field applies them one at a time (retention, caps, partitions, then schema) and stops at the first failure, so one that fails part way has already changed the fields before it. Its lines say so: on the node that applied the `PATCH`, the fields applied before the failure are `ok`, and on a node that forwarded it to the leader, which cannot tell how far the leader got, every kind of field before the last is `unknown`. Those lines still carry the failure's `status`, and fields whose outcomes differ go on separate `topic.alter` lines. For example, `{"retention_ms":7200000,"schema":{...}}` with a schema the leader refuses logs `topic.alter fields=retention_ms outcome=ok status=400` and `topic.schema outcome=rejected status=400` when the client called the leader, and the same lines with `outcome=unknown` for `retention_ms` when it called another node.

## Profile with pprof {#pprof}

`narad.pprof.enabled: true` serves Go's `net/http/pprof` endpoints on port 6060. They are off by default and have no authentication, so keep the port inside the cluster; the chart never routes it through the Service or an ingress. With pprof on, take a 30-second CPU profile of one pod:

```bash
kubectl port-forward -n narad pod/narad-0 6060:6060
```

```bash
go tool pprof 'http://127.0.0.1:6060/debug/pprof/profile?seconds=30'
```

Outside the chart, set `NARAD_HTTP_PPROF_ADDR` (for example `127.0.0.1:6060`). It may share an address with `NARAD_HTTP_METRICS_ADDR`.

## Next steps

- [Troubleshooting](troubleshooting.md): what each alert and symptom means and how to fix it.
- [Metrics reference](../reference/metrics.md): every metric, its type and labels.
- [Scale out and in](scaling.md): add nodes when the dashboards show you need them.

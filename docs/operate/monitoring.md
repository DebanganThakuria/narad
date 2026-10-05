---
description: "Scrape Narad's Prometheus metrics, import the ready-made Grafana dashboard, and set up the six alerts that catch real trouble."
---

# Monitor and alert

Scrape Narad's Prometheus metrics, import the ready-made Grafana dashboard, and set up the six alerts that catch real trouble.

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

## Import the dashboard {#dashboard}

The repository ships a Grafana dashboard, [`ops/monitoring/grafana/dashboards/narad-node-dashboard.json`](https://github.com/DebanganThakuria/narad/blob/master/ops/monitoring/grafana/dashboards/narad-node-dashboard.json). Import the JSON file into Grafana. Its 14 panels cover message throughput, HTTP requests and latency, errors and rejections, consumer backlog and age, disk usage and the largest topics, storage latency and throughput, process CPU, memory and runtime counts, and an inventory of topics and partitions.

The panels select `job="narad"`. Name your scrape job `narad`, or change that selector after the import.

Two panels read differently from their titles on builds after v3.0.1 (unreleased):

- **HTTP Requests** counts requests, not messages. One batch request carries up to 100 messages, so once clients batch, read **Message Throughput** instead.
- **Storage Latency** still plots `narad_storage_high_watermark_persist_duration_seconds`, which no longer measures a commit.

## Set up the alerts {#alerts}

If you configure nothing else, configure these six. Each one fires on a condition that needs a person. The first five read Narad's metrics; the sixth reads a Kubernetes metric from kube-state-metrics, because Narad has no metric for a node that is down:

| Alert | Expression | What it means |
|---|---|---|
| Fan-out data loss | `rate(narad_fanout_child_dropped_messages[5m]) > 0` | A [fan-out child](../reference/glossary.md#fan-out-child) fell behind its parent's retention, or hit an unreadable record, and lost records. |
| Delay child behind | `narad_fanout_due_lag_seconds > 60` | Due messages are not reaching a [delay child](../reference/glossary.md#delay-child). This is the only lag signal for delay children: their offset lag is always about rate times delay. |
| Consumer-side loss | `rate(narad_consumer_corrupt_skipped_total[5m]) > 0` or `narad_consumer_dropped_messages > 0` | A consumer skipped a permanently unreadable record, or [retention](../reference/glossary.md#retention) deleted messages nobody had acked. |
| Disk runway | `predict_linear(narad_data_dir_available_bytes[6h], 24 * 3600) < 0` | At the rate of the last six hours, the data volume fills within a day. |
| Produce latched off (unreleased) | `narad_ingress_wal_failed == 1` | A write or sync of the node's [ingress WAL](../reference/glossary.md#ingress-wal) failed. The node answers every produce with `500` until it restarts, while consume and `/readyz` keep working. |
| Pod not ready (Kubernetes metric) | `kube_pod_status_ready{namespace="narad", condition="true"} == 0`, held for 2 minutes (`for: 2m`) | A Narad pod has not been ready for 2 minutes. The messages stored on it wait until it is back. |

On v3.0.1, which has no `narad_ingress_wal_failed`, watch for the same failure with `rate(narad_errors_total{component="http", kind="5xx"}[5m]) > 0`, which also catches other server errors.

### Metastore and Raft alerts (unreleased) {#metastore-alerts}

Releases after v3.0.1 export the [metastore and Raft series](../reference/metrics.md#metastore-raft). Add these alongside the six:

| Alert | Expression | What it means |
|---|---|---|
| Metastore stopped | `narad_metastore_apply_stopped == 1` | The node stopped applying Raft entries: its disk refused a metadata write for 30 s, or it met an entry or a snapshot from a newer release. It leaves Raft and exits at once, so a scrape often misses this gauge; the restart loop that follows shows as Pod not ready, and the node's log says why (`metastore: stopped applying raft entries`). |
| Metastore write stalled | `narad_metastore_apply_stalled == 1` | The node's disk is refusing a metadata write. It retries for up to 30 s and then stops as above, so this is the warning that comes first. |
| No Raft leader | `narad_raft_has_leader == 0`, held for 1 minute (`for: 1m`) | The node has known no Raft leader for a minute, so no metadata write (create a topic, register a member) succeeds through it. On every node at once, the cluster has lost its quorum. |
| Metastore growth | `delta(narad_metastore_fsm_bytes[1d]) > 64 * 1024 * 1024` | The metadata database grew by more than 64 MiB in a day, which normal topic and user churn does not do: look for something creating topics, schema versions or users in a loop. Every Raft snapshot copies the file beside itself first, so keep that much free space on the volume as well. |

What to do about a stopped metastore is on the Troubleshooting page under [metastore: stopped applying raft entries](troubleshooting.md#log-metastore-stopped).

What to do when one fires is on the [Troubleshooting](troubleshooting.md) page: [produce latched off](troubleshooting.md#produce-500), [delay child behind](troubleshooting.md#due-lag-stuck), [messages lost to retention](troubleshooting.md#log-frontier-behind-retention), [pod not ready](troubleshooting.md#node-down). For disk runway, check the retention of the largest topics against [Capacity and disk sizing](../reference/capacity.md#disk-sizing).

`rate(narad_errors_total[5m])`, split by its `component` and `kind` labels, makes a useful catch-all panel beside these alerts.

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

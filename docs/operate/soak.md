# Running the soak

The soak is a long-running workload shaped like a company's production
traffic, meant to sit on a cluster for weeks and answer one question
without anybody watching it: did this cluster lose or duplicate a message
today. It is `--mode soak` in the integration driver, published as
`ghcr.io/debanganthakuria/narad-driver`.

It is not a benchmark. A benchmark drives one topic shape as hard as it
will go, which is the one traffic pattern nothing in production has: it
never idles long enough for retention to matter, never acks out of order,
and never leaves a partition cold.

## What it models

Seven topics, about a thousand messages a second per process:

| profile | partitions | rate/s | payload | handlers | what it reaches |
|---|---|---|---|---|---|
| `payments-authorized` | 12 | 400 | 420 B | 8 fast | the firehose case |
| `payments-captured` | 12 | 250 | 420 B | 6 fast | |
| `webhooks-outbound` | 6 | 200 | 1.1 KB | 16 slow, uneven | out-of-order acks, stranded leases (3% nack, 0.2% never ack) |
| `refunds-initiated` | 3 | 40 | 640 B | 8 slow | long handler latency |
| `settlements-batch` | 3 | burst/5min | 8.2 KB | 2 slow | fat payloads, long visibility |
| `audit-events` | 6 | 100 | 300 B | 4 periodic | a backlog that drains in bursts |
| `config-changes` | 3 | 1 per 9 min | 220 B | 1 | partitions going cold, so only the cold-retention walk can expire them |

Every request goes through the load balancer, so consumers routinely land
on a node that owns nothing they asked for: the cross-node consume path
is exercised continuously rather than by a special test.

Each process owns its own copy of the topics, suffixed with the pod
ordinal. That is what makes verification exact: narad's queue semantics
mean workers sharing a topic are one consumer group, so a message one
process produced can be acked by another, and then nobody can say whether
it was acked or lost. Within a process the group is still many workers on
one topic, which is what makes acks land out of order.

## Deploying it

```bash
kubectl -n <namespace> apply -f ops/monitoring/narad-soak.yaml
```

The manifest pins the driver image, points `NARAD_ENDPOINT` at the
cluster's public address, and splits the modelled rate across pods with
`RATE_SCALE` (three pods at `0.334` reproduce the company once). Nothing
is copied into the pods: the driver ships in its own image so a pod
rescheduled onto a fresh node comes back on its own.

## What to watch

Two metrics decide whether the cluster is healthy. Both should stay at
zero for as long as the soak runs:

```promql
sum(increase(narad_soak_dup_after_ack_total[1h]))
sum(increase(narad_soak_never_delivered_total[1h]))
```

`dup_after_ack` is a message delivered again after this process acked it.
Outside a broker restart that is an exactly-once violation.
`never_delivered` is a message that was accepted and then never arrived
within the loss deadline, on a topic whose consumers are keeping up.

Health, rather than correctness:

```promql
sum(rate(narad_soak_produced_total[5m]))   # should sit near the modelled rate
sum(rate(narad_soak_acked_total[5m]))      # should track produced
sum(narad_soak_outstanding)                # produced and not yet acked; should stay small and flat
histogram_quantile(0.99, sum by (le) (rate(narad_soak_end_to_end_seconds_bucket[5m])))
```

A rising `outstanding` means consumers are falling behind, which on these
profiles means the cluster is slower than it was, not that the harness
changed.

Counters that are expected to be non-zero, and are not faults:

- `nacked_total` and `poisoned_total`: the injected handler failures.
  Poison is what strands leases on purpose.
- `retention_reclaimed_total`: messages that reached their topic's
  retention before a consumer got to them, on a topic configured to allow
  that.
- `backlog_aged_total`: the same, on the profile whose backlog is the
  point.
- `unknown_sequence_total`: a delivery for a sequence the process does
  not remember, which an ambiguous produce or a very late redelivery both
  produce. A steady low rate is normal; a step change is worth a look.

## Disk

Retention is hours rather than days on purpose. At these rates the steady
state is a few gigabytes across the cluster, and every extra hour on a
firehose topic costs about half a gigabyte. Check before running it
somewhere small:

```promql
sum by (k8s_pod) (narad_data_dir_size_bytes)
```

If the topics already exist from an earlier run they keep their original
retention: creating a topic that exists is a no-op, so change it
explicitly.

```bash
curl -X PATCH -H 'Content-Type: application/json' \
  -d '{"retention_ms": 7200000}' "$NARAD/v1/topics/soak-payments-authorized-0"
```

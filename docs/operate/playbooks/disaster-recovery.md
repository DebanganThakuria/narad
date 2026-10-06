---
description: "Keep a live copy of a topic on a recovery cluster, usually in another region, and know how much you would lose if the first cluster were gone."
---

# Set up disaster recovery

Keep a live copy of a topic on a recovery cluster, usually in another region, and know how much you would lose if the first cluster were gone.

**Unreleased:** in master, not in v3.1.0.

Before you start: every item of [Before you start](../remotes.md#before-you-start) on both clusters, in both directions; the CLI with a context for each (`a` for the cluster in use, `b` for the recovery cluster); and a metrics store outside `a`'s region.

A [remote child](../../reference/glossary.md#remote-child) of the topic on `a` sends every record to the same topic on `b`, at least once. `b` then holds everything `a`'s consumers have not processed, up to the link's lag. When `a`'s region is lost, you [fail over](failover.md) to `b`; when it is back, you [fail back](failback.md). The link is an HTTPS client of `b`'s public API, so it assumes no shared network: everything below holds between regions as within one.

## Set up in peacetime {#setup}

1. **Create the topic on `b`** with `a`'s partition count and schema, under the same name, so that clients switch by URL alone. Size its retention as in [Retention](#retention).

2. **Check the bounds in both directions.** `b`'s host and port must be inside `remotes.allowed_hosts` and `remotes.allowed_ports` on every node of `a`, and `a`'s inside them on every node of `b`, since widening them is a config change and a restart. Every node needs egress to the other cluster's ingress. Name each cluster by a hostname pinned to it and its region, never a global failover name: failing over is your decision, not DNS's.

3. **Provision both directions now,** so nobody creates a credential during an incident:

    - the remote `b` on `a`, with the user `repl-from-a-7f3k9q` on `b`;
    - the remote `a` on `b`, with the user `repl-from-b-2m8x4d` on `a`.

    Each cluster keeps its remotes in its own metadata, encrypted under its own cluster secret, so failing back never depends on `a`'s region. **The two clusters never share a cluster secret** ([operating condition 5](../remotes.md#operating-conditions)). A remote with no link sends nothing; rotate the idle pair on the same schedule as the live one.

4. **Attach the link:**

    ```bash
    narad --ctx a topic attach orders orders-dr --remote b \
      --from unconsumed --lanes 2
    ```

    `unconsumed` is enough for a queue: after a failover, `b`'s consumers need only what `a` has not processed. `earliest` sends `a`'s whole retained log at once (at 5 MB/s and 72 hours, 1.3 TB of egress, then a catch-up that competes with live traffic); use it only when `b` must hold the history. Choose `lanes` as in [Throughput](#throughput).

5. **Deploy `b`'s consumers at zero replicas**, ready to scale.

6. **Set up the alerts** in [Watch the link](#watch), evaluated outside `a`'s region.

7. **Rehearse once.** Pause the link for an hour (`narad --ctx a topic pause orders orders-dr --reason drill`), watch the headroom fall, resume it, and time the catch-up.

## Retention {#retention}

- **The source.** `a`'s log is the only buffer while the link is down. A link down for `D` with lag `L` when it stopped loses nothing while `a`'s retention exceeds `L + D` plus the time to notice and fix whatever stopped it. Set at least 72 hours on a topic with a cross-region remote child: regional incidents have lasted most of a day, and 72 hours covers one that runs into a weekend, plus the catch-up. The floor is 24 hours, and the attach warns below 72. During an outage, raising the retention works at once while headroom is above 0.
- **Catching up.** With the link's capacity `C` and the produce rate `P`, the oldest unshipped record gets younger by `C/P - 1` seconds every second once the link is back, so catching up takes `D × P / (C - P)`: as long as the outage at `C = 2P`, five times as long at `C = 1.2P`. With `C ≤ P` the link falls behind with no outage at all; the headroom alert fires before records age out.
- **What drop-behind leaves.** If the oldest unshipped records age out of `a` anyway, the cursor skips them and counts them in `narad_fanout_child_dropped_messages` on `a`. `b` gets no marker, and nothing can fill the gap later.
- **The recovery cluster.** After a failover, `b`'s consumers start at `b`'s oldest retained record, because nothing was ever acked there. So `b`'s retention must exceed the age of `a`'s oldest unprocessed record at the moment of failure, plus the time to fail over. `b` counts retention from each record's arrival on `b`, so it keeps a record up to the link's lag longer than `a` does; where data has a mandated maximum retention, set `b`'s short enough to meet it at the largest lag you tolerate.

## Throughput {#throughput}

A lane sends one request at a time and waits for the answer, so its rate is bounded by the round trip: a request carries up to 1,000 records to a target on this release (100 to an older one), and up to 960 KiB. A key always travels on one lane, so one hot key can never go faster than one lane. More lanes per parent partition (up to 8) add parallel streams; the remote's `max_in_flight` limit (16 per node by default) caps them all. With `remotes.allowed_hosts` set, [`narad remote test`](../remotes.md#test) reports each node's connect time and an estimate of one lane's records per second at that round trip. Size the lanes so the link's capacity is at least twice the produce rate.

The link sends JSON payloads as they are and anything else as base64. Between regions, egress is billed per GB by the source region, plus any NAT gateway on the path; prefer a private interconnect (peering, a transit gateway, a VPN), which narrows who can connect but does not replace TLS or the credential. `compression: zstd` on the remote ([Change a remote](../remotes.md#change)) compresses each request that shrinks by at least 10%, against a target on this release; leave it off for a topic whose records must not leak to each other through compressed lengths.

## Watch the link {#watch}

At the moment `a`'s region fails, the data at risk is the link's lag (records committed on `a` that `b` has not answered `202` for) plus `a`'s ingress dispatch backlog of `orders` (records `a` answered `202` for and had not committed yet, which the lag cannot see). No metric counts that backlog for one topic: `narad_ingress_dispatch_backlog_records` counts every topic the node serves, so it is an upper bound. Nothing on `b`'s side is at risk: a `202` from `b` is durable on `b`.

| Signal | Why | Alert |
|---|---|---|
| `max(narad_fanout_remote_lag_seconds{child="orders-dr"})` | The recovery point | Page above your objective |
| `narad_ingress_dispatch_backlog_records` on `a` | An upper bound on the part of the recovery point the lag cannot see; node-wide, across every topic | Warn when it keeps rising for 10 minutes: records are not reaching their owners. A node that takes produce is rarely at 0, so do not alert on above 0 |
| `sum(rate(narad_fanout_committed_total{parent="orders",child="orders-dr"}[5m])) / sum(rate(narad_messages_produced_total{topic="orders"}[5m]))` | Below 1, the link is falling behind | Warn below 1 for 15 minutes |
| `narad_fanout_remote_retention_headroom_seconds` | Time left before drop-behind | Warn below 12 hours, page below 4 |
| `narad_fanout_remote_state` | Why the link stopped | As in [Monitor and alert](../monitoring.md#remote-alerts) |
| `narad_remote_chunk_bytes_limit` | Below 960 KiB, the path is cutting uploads short | Warn at the 64 KiB floor for 10 minutes |

- **Watch `a` from outside `a`'s region.** Send `a`'s metrics, over an authenticated connection, to a store in `b`'s region or a global one. Do not open `a`'s metrics listener to another region: it serves without credentials, for scrapes inside the cluster. After a failure, the data at risk is then the last stored `lag_seconds` plus one scrape interval, plus at most the last dispatch backlog.
- **Optionally, watch from `b`'s side too.** Give a small topic on `a` its own remote child to `b`, produce `{"ts": <a's Unix ms>}` to it every 5 seconds, and page from `b` when the newest one is more than 30 seconds old. Give the job and the monitor their own users, never the replicator's. The signal outlives `a`'s monitoring; its error is the clock offset between the regions.

## Clocks {#clocks}

No time arithmetic crosses the clusters in the data path: `lag_seconds` compares `a`'s commit time with the clock of the same `a` node. A message's `timestamp` on `b` is when it arrived on `b`, so after an outage hours of records arrive within minutes of timestamps, and a delay child on `b` delays from arrival. Certificate validity is checked against `a`'s clock. Keep both clusters on NTP or the cloud's time service.

## Next steps

- [Fail over to the recovery cluster](failover.md): when `a`'s region is lost.
- [Fail back to the original cluster](failback.md): when it is back.
- [Manage remotes](../remotes.md): credentials, rotation and the security model.

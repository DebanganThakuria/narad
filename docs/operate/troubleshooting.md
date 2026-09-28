---
description: "Match what you see, a status code, a readiness answer, a metric or a log line, to its cause, a check and a fix."
search:
  boost: 2
---

# Troubleshooting

Match what you see, a status code, a readiness answer, a metric or a log line, to its cause, a check and a fix.

Before you start: `kubectl` access to the pods, and the metrics from [Monitor and alert](monitoring.md). Status codes that point at a client mistake (`400`, `404`, `409`, `410`, `413`, `415`) are explained on [Status codes and errors](../reference/status-codes.md), not here.

## Start with readiness {#check-readiness}

Every node explains in its `/readyz` answer why it is not ready. Forward one pod's API port and ask it:

```bash
kubectl port-forward -n narad pod/narad-2 7942:7942
```

In a second terminal:

```bash
curl -s http://127.0.0.1:7942/readyz
```

```text title="Output"
{"error":"metastore: node is not ready: no raft leader known"}
```

| Answer | Meaning |
|---|---|
| `{"status":"ready"}` | The node serves traffic. |
| `not ready` | The node has not finished starting. It waits to be admitted (a new node) and to catch up with the Raft leader, so a node that cannot reach the leader stays here. |
| `no raft leader known` | The node sees no Raft leader: quorum is lost, or the node is cut off from the others. |
| `no contact with the raft leader yet` | The node knows a leader but has not heard from it since it started. |
| `last raft leader contact <duration> ago` | The node has not heard from the leader for more than 5 seconds. |
| `replica has not caught up with the leader since start` | The node's copy of the metadata is still behind the leader's. |

The last four answers start with `metastore: node is not ready:`, as in the output above. The sections below say what to do.

## Status codes

### `500` on every produce {#produce-500}

Every produce to one node answers `500` with `{"error":"produce failed"}`, and the node logs `http server error` with `op=produce`.

**Cause.** A write or sync of the node's [ingress WAL](../reference/glossary.md#ingress-wal) failed, usually because the disk is full (`ENOSPC`) or failing (`EIO`). The node then refuses every later produce, because it cannot tell what the failed write left on disk. Consume and `/readyz` keep working, so the node stays in rotation.

**Check.** `narad_ingress_wal_failed` is `1` on that node (unreleased; on v3.0.1, watch `narad_errors_total{component="http", kind="5xx"}`). Check the volume:

```bash
kubectl exec -n narad narad-0 -- df -h /var/lib/narad
```

**Fix.** Free space or replace the disk, then restart the pod (`kubectl delete pod narad-0 -n narad`). The refusal clears only on a restart. Everything the node acked before the failure is replayed and delivered after it. Why it latches: [Produce path](../understand/produce-path.md#wal-disk-failure).

### `500` authentication unavailable {#auth-500}

Requests to one node answer `500` with `{"error":"authentication unavailable"}`, and the node logs `authentication store failure` with an `err` field.

**Cause.** The node could not read the user record from its local copy of the cluster metadata.

**Check.** The `err` field of the log line, and the node's disk.

**Fix.** Fix what `err` names, then restart the pod. Other nodes authenticate from their own copies, so clients can use them meanwhile.

### `503` on consume or ack {#status-503}

A consume, ack, extend or nack answers `503`.

**Cause.** The body says which:

- `partition owner is down; retry later`: the node that owns the partition is down. A partition has one owner and no replica, so it waits for that node.
- `control plane temporarily unavailable`: the node has no Raft leader, or has not caught up with it yet.

**Check.** `narad cluster members` shows the owner as `dead`, and `/readyz` shows the leader state ([Start with readiness](#check-readiness)).

**Fix.** Bring the node back. Clients retry with backoff; the partition serves again as soon as its owner is back. What each failure means for delivery: [failure matrix](../understand/delivery-contract.md#failure-matrix).

### `502` on ack, extend or nack {#status-502}

An ack, extend or nack answers `502`, with the error of a failed call between nodes as the body.

**Cause.** The node you called forwarded the request to the partition's owner and got no usable answer, often because the owner was restarting or overloaded. The ack may or may not have been applied.

**Check.** `narad_cluster_rpc_requests_total` with `outcome="timeout"` or `outcome="error"` rises on the forwarding node.

**Fix.** Retry the ack. If it answers `410`, the first attempt was applied or the lease had already lapsed; the message may be delivered again either way. Retry rules: [Status codes and errors](../reference/status-codes.md#status-502).

### `429` too many in-flight requests {#status-429}

Consumes answer `429` with `too many in-flight consume requests for this identity (limit 1024 per node)`.

**Cause.** One user holds more concurrent consumes on this node than the cap allows. Every waiting long poll counts, and a batch consume counts as its `max`. With security off, the cap counts per client IP. Produces have the same kind of cap, off by default (unreleased).

**Check.** Count the long polls your consumers keep open against one node, per user.

**Fix.** Keep fewer long polls open per user, give each service its own user, or raise `NARAD_HTTP_MAX_CONSUME_IN_FLIGHT_PER_IDENTITY` ([Configuration reference](../reference/configuration.md#http)).

### `429` too many failed authentication attempts {#auth-429}

Requests with one username answer `429` with `{"error":"too many failed authentication attempts"}`, and the node logs `authentication throttled` with that `username`, at most once every 12 seconds.

**Cause.** Each node allows each username 5 failed password checks, then one more every 12 seconds. A client with a wrong password, or someone guessing, has used them up. While they are used up, even a correct password that the node has not yet accepted is refused.

**Check.** The `username` in the log line, and which clients use it.

**Fix.** Correct the client's password. The node accepts the correct one again after at most 12 seconds without failures. How the throttle works: [Networking and security](../understand/networking-and-security.md#auth-throttle).

### `421` or `500` on topic details {#status-421}

`GET /v1/topics/{topic}` answers `421` with `this node does not own the requested partition`, from every node, or `500` with `get topic failed`.

**Cause.** Topic details gather partition statistics from every partition owner. While one owner is unreachable, the call answers `500`; once that owner has been marked dead, after about 30 seconds without a heartbeat, it answers `421`. A node whose Raft certificate its peers do not trust also causes the `500` ([below](#raft-cert-untrusted)).

**Check.** `narad cluster members` shows the owner as `dead`, or `/readyz` on it is not ready.

**Fix.** Bring the owner back. The topic list, produce, and consume of the other partitions keep working meanwhile.

### `204` gaps after a node returns {#quiet-after-outage}

After a node comes back, some partitions deliver in bursts with quiet gaps between them. Consumers get `204` during the gaps and the consumer lag stops falling. It looks like a stuck broker and is not one.

**Cause.** The outage stranded [leases](../reference/glossary.md#lease): a consumer held a message when its node died and never acked it. Messages after it were acked, but a partition's [committed frontier](../reference/glossary.md#committed-frontier) cannot pass an unacked message, so the partition has nothing new to hand out. When the stranded lease reaches the topic's visibility timeout (30 seconds by default), the message is delivered again, acked, and the whole acked run behind it clears at once. That is the burst.

**Check.** `narad_acked_ahead_size` is above 0 for the quiet partition, and the gaps last about one visibility timeout.

**Fix.** None is needed, and nothing is lost. Give a consumer fleet draining a backlog after an outage more than one visibility timeout before you conclude it stopped. To shorten the gaps, lower the topic's `visibility_timeout_ms` to what your slowest handler needs: the gaps shrink one for one. The mechanism: [Consume path](../understand/consume-path.md#after-an-outage).

## Readiness

### Ready on some pods, not on one {#not-ready-one-pod}

One pod is not ready while its liveness probe passes.

**Cause.** The pod is catching up with the leader, waiting to be admitted, or has lost sight of the leader. A decommissioned pod stays not ready after its removal from Raft until you scale it away. A pod with an untrusted Raft certificate also stays here.

**Check.** Its `/readyz` answer ([Start with readiness](#check-readiness)) and its log.

**Fix.** Usually none: a catching-up pod turns ready by itself. For a decommissioned pod, finish the scale-in ([Scale out and in](scaling.md#decommission)). For a certificate problem, see [below](#raft-cert-untrusted).

### Not ready on every pod {#not-ready-all-pods}

Every pod reports `no raft leader known`.

**Cause.** Raft quorum is lost: more than half the voters are down or cut off. Topic, user and schema changes wait for quorum. Each node still accepts produces, but the Service has no ready pods, so clients that connect through it cannot reach any node.

**Check.** `kubectl get pods -n narad` shows which pods are down or restarting.

**Fix.** Bring the missing voters back. Do not restart the survivors: they hold the state the returning voters need.

## Metrics

### `narad_fanout_due_lag_seconds` stays above 0 {#due-lag-stuck}

The due lag of a delay child climbs or stays flat above 0 instead of returning to 0.

**Cause.** The delay child's cursor is not moving. A cursor waits, without rerouting, while the node that owns one of the child's partitions is down, and it retries a failed commit until it succeeds.

**Check.** `narad cluster members` for a `dead` node, and the logs of the node that owns the parent partition for `fanout: child batch commit failed`, `fanout: persist cursor` or `fanout: read parent slab`.

**Fix.** Bring the child partition's owner back, or fix what the log's `err` names. The cursor resumes where it stopped. How cursors behave: [Fan-out engine](../understand/fanout-engine.md).

## Log lines

### `storage: fsync failed; log poisoned until reopened` {#log-fsync-poisoned}

Logged at error level with `dir`, `durable_tail` and `err`.

**Cause.** A disk sync of one partition failed. The node refuses every later write to that partition, because the data it tried to sync may already be gone, but it still serves the records committed before. New records wait in the ingress WAL, which moves them to other partitions of the topic.

**Check.** `err` and `dir` in the log line, `narad_errors_total{component="storage", kind="fsync_poisoned"}`, and the health of the disk.

**Fix.** Fix the disk, then restart the pod: the partition is reopened and checked, and the records waiting for it are committed. Why a failed sync is final: [Storage engine](../understand/storage-engine.md#fsync-failure).

### `no raft leader for a while; running the cluster join loop` {#log-no-raft-leader}

Logged at warning level after a node has had no Raft leader for 15 seconds.

**Cause.** The node lost its leader: its peers are down, the network between them is cut, or the node was removed from the voters. It now asks its peers to admit it again every 2 seconds.

**Check.** `/readyz` on every pod, and whether the node was decommissioned (`narad cluster members`).

**Fix.** Restore the network or the missing pods. A decommissioned node is refused ([next section](#log-join-rejected)).

### `cluster join refused: this node was decommissioned` {#log-join-rejected}

The full line is `cluster join refused: this node was decommissioned and removed; it will not rejoin with its old data directory. Scale it away, or delete its volume to rejoin as a new node`.

**Cause.** A node that was decommissioned restarted with its old volume. The leader refuses it, so it cannot undo its own decommission.

**Fix.** Scale it away. To use the name again, delete its PersistentVolumeClaim so it starts empty ([Reuse a decommissioned name](scaling.md#reuse-name)).

### `cluster join rejected` {#log-join-rejected-status}

Logged at warning level with `peer` and `status`.

**Cause.** The leader could not admit the node. A `503` status means the leader failed to read the membership or to add the node to Raft. A `400` status means the leader could not read the request, or it lacked the node's ID or Raft address.

**Fix.** For `503`, the node retries every 2 seconds; check the leader's log for `join cluster: add voter` or `join cluster: readmit member`. For `400`, compare the node's `NARAD_NODE_ID` and `NARAD_CLUSTER_ADVERTISE_ADDR` with a working node's.

### `consumer frontier fell behind retention` {#log-frontier-behind-retention}

The full line is `consumer frontier fell behind retention; skipped to oldest retained offset`, at warning level, with `topic`, `partition`, `from`, `to` and `skipped`.

**Cause.** [Retention](../reference/glossary.md#retention) deleted messages that no consumer had acked. Consumers were too slow, stopped, or kept failing on the message at the head. The `skipped` messages are gone.

**Check.** `narad_consumer_dropped_messages` and `narad_oldest_unconsumed_message_age_seconds` against the topic's `retention_ms`.

**Fix.** Give the topic a longer retention, add consumers, or fix the handler that keeps failing. What retention promises: [Delivery contract](../understand/delivery-contract.md#retention).

### `consumer offset commits cannot keep to their interval` {#log-offset-commit-slow}

**Unreleased:** in master, not in v3.0.1.

Logged at warning level, at most once a minute, with `partitions`, `flush_took` and `interval`. A related line, `consumer offsets wait longer than their durability interval for a device flush`, carries `partitions`, `oldest` and `durability_interval`.

**Cause.** Writing acked consumer positions to disk takes longer than its schedule allows. The first line means a crash of the process would deliver again about one write's duration of acks; the second means a power loss would deliver again more acks than `storage.consumer_offset_commit_interval_ms` promises.

**Fix.** Fewer partitions per node, or a faster disk. Both lines are about duplicates after a crash, never loss. The setting: [Configuration reference](../reference/configuration.md#storage).

### `move: set aside stale incarnation directory` {#log-stale-incarnation}

Logged at warning level with `topic` and `err`.

**Cause.** The node holds a directory left by a deleted and recreated topic of the same name (an older [incarnation](../reference/glossary.md#incarnation)), and it failed to rename it aside. Narad never serves such a directory; it renames it to `topics/<name>.stale-<id>` and removes it once the leader confirms that incarnation is gone.

**Check.** The `err` field, usually a permission or disk problem in the data directory.

**Fix.** Fix what `err` names. The node retries on its next pass.

### `reclaim: local partition copy is AHEAD` {#log-partition-quarantined}

The full line is `reclaim: local partition copy is AHEAD of the position it was promoted at elsewhere; quarantined instead of deleted, records after the promoted hwm exist only here`, at error level, with `topic`, `partition`, `owner`, `local_next_offset`, `promoted_hwm` and `quarantine_dir`.

**Cause.** A partition moved away from this node while the node was cut off, and the destination [force-promoted](../reference/glossary.md#force-promote) its copy. This node kept taking records meanwhile, so its copy holds records the new owner never received. Instead of deleting it, the node renamed it to `quarantine_dir`.

**Fix.** Narad never serves or deletes that directory; it goes only when the topic is deleted. The records from `promoted_hwm` onwards exist only there, and Narad has no tool to merge them back. Decide whether they matter, copy the directory off if they do, and delete it when you are done. The move protocol: [Rebalance and decommission](../understand/rebalance.md).

### `x509: certificate signed by unknown authority` {#raft-cert-untrusted}

The Raft leader logs `raft: failed to heartbeat to: peer=<addr>` with `error="tls: failed to verify certificate: x509: certificate signed by unknown authority"`, and the node it names logs `failed to decode incoming command: error="remote error: tls: bad certificate"`.

**Cause.** The node's Raft certificate is signed by a CA its peers do not trust. It cannot join and stays not ready, and its partitions are unavailable.

**Fix.** Give the node a certificate from the trusted CA and restart it. To change the CA without this, follow the three rolls in [Rotate the CA](raft-tls.md#rotate-ca). Details: [Untrusted certificate](raft-tls.md#untrusted-cert).

## Kubernetes

### Helm conflict on `.spec.replicas` {#helm-field-manager-conflict}

`helm upgrade` fails with a conflict on the StatefulSet's `.spec.replicas`.

**Cause.** Someone changed the replica count outside Helm, for example with `kubectl scale`. Helm 4 applies manifests server-side, and refuses to take over a field another client last set.

**Fix.** Check that `replicaCount` in your values is the count you want, then let Helm take the field over:

```bash
helm upgrade narad ./charts/narad -n narad --reuse-values \
  --force-conflicts
```

Scale with `replicaCount` from then on ([Scale out and in](scaling.md)).

## Next steps

- [Status codes and errors](../reference/status-codes.md): every code and whether to retry it.
- [Delivery contract](../understand/delivery-contract.md#failure-matrix): what each failure can lose, in one table.
- [Monitor and alert](monitoring.md): catch these before your users do.

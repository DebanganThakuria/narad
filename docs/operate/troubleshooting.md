---
description: "Match what you see, a status code, a readiness answer, a metric or a log line, to its cause, a check and a fix."
search:
  boost: 2
---

# Troubleshooting

Match what you see, a status code, a readiness answer, a metric or a log line, to its cause, a check and a fix.

Before you start: `kubectl` access to the pods, the metrics from [Monitor and alert](monitoring.md), and the `narad` CLI with admin credentials ([CLI command reference](../reference/cli.md#cluster)). Status codes that point at a client mistake (`400`, `404`, `409`, `410`, `413`, `415`) are explained on [Status codes and errors](../reference/status-codes.md), not here.

## Start with readiness {#check-readiness}

Every node explains in its `/readyz` answer why it is not ready. Forward one pod's API port to a free local port, 7952 here, so it does not clash with a port-forward to the Service on 7942:

```bash
kubectl port-forward -n narad pod/narad-2 7952:7942
```

In a second terminal:

```bash
curl -s http://127.0.0.1:7952/readyz
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

<figure class="nr-dia nr-dia--doc" id="fig-readiness-gates">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/readiness-gates.html"
</div>
<figcaption>A pod answers with the first gate it fails, so <code>narad-2</code> above, answering <code>no raft leader known</code>, has started but sees no Raft leader. The gates are checked on every probe: a ready pod that loses the leader turns not ready again.</figcaption>
</figure>

The last four answers start with `metastore: node is not ready:`, as in the output above. The sections below say what to do.

## Node failures

### A node is down {#node-down}

A pod is not running or restarts in a loop, or `narad cluster members` shows a node with `"status": "dead"`.

**What clients see.** Produces still get `202`, and after about 3 seconds the messages meant for that node's partitions go to other partitions of the topic. Messages already stored on that node wait for it. Acks of those messages, and consumes pinned to one of its partitions with `partition=N`, get `502`, then `503` (`partition owner is down; retry later`) once the node is marked dead after about 30 seconds without a heartbeat. Consumes without `partition` keep being served from the other partitions.

<figure class="nr-dia nr-dia--doc" id="fig-node-down-timeline">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--lilac">
--8<-- "diagrams/node-down-timeline.html"
</div>
<figcaption>Match what your clients see to the clock, which is not to scale here: produces keep getting <code>202</code>, acks and pinned consumes get <code>502</code> and then <code>503</code> once <code>narad-2</code> is marked dead, and messages stored on it, like <code>ord_123</code>, wait until it returns.</figcaption>
</figure>

**Check.** Find the pod and why it stopped:

```bash
kubectl get pods -n narad
kubectl describe pod narad-2 -n narad
kubectl logs narad-2 -n narad --previous
narad cluster members
```

`describe` lists the pod's events, such as a volume that will not attach, a failed probe or an out-of-memory kill. `logs --previous` prints the last lines of the container that stopped.

**Fix.** Get the pod running again on its own volume. A partition has one owner and no replica, so the messages stored on that node come back only with it. If the volume is lost, the node rejoins empty: [restore a snapshot](backups.md#restore), or move consumers to the topic's [replica child](backups.md#replica-children). After the node returns, expect [quiet gaps](#quiet-after-outage) of about one visibility timeout.

## Status codes

### `503` on produce {#produce-503}

A produce answers `503`, often from every node at once.

**Cause.** Narad itself answers a produce with `503` in one case only (**Unreleased**): a topic with a schema, and every schema validation slot on the node stayed busy for 5 seconds; the body then reads `schema: validation capacity busy, retry` ([Validation capacity](../reference/schema-rules.md#validation-capacity)). Any other `503` comes from the proxy in front of it, the load balancer or ingress, when no pod is ready.

**Check.** For the validation case, `narad_schema_validations_in_flight` on the node sits at its CPU count and `narad_schema_rejections_total{reason="busy"}` rises; look for producers sending large payloads, or a schema that `narad_schema_validation_seconds` shows to be slow. Otherwise, `kubectl get pods -n narad`, then `/readyz` on each pod ([Start with readiness](#check-readiness)).

**Fix.** For the validation case, retry with backoff, through another node if you can, and spread large payloads out or simplify the slow schema. When every pod answers `no raft leader known`, follow [Not ready on every pod](#not-ready-all-pods). When pods are down, see [A node is down](#node-down).

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

### `503` on ack, or on a consume pinned to a partition {#status-503}

An ack, extend or nack, or a consume with `partition=N`, answers `503`.

**Cause.** The body says which:

- `partition owner is down; retry later`: the node that owns the partition is down. A partition has one owner and no replica, so it waits for that node.
- `control plane temporarily unavailable`: the node has no Raft leader, or has not caught up with it yet.

**Check.** `narad cluster members` shows the owner as `dead`, and `/readyz` shows the leader state ([Start with readiness](#check-readiness)).

**Fix.** Bring the node back ([A node is down](#node-down)). Clients retry with backoff; the partition serves again as soon as its owner is back. What each failure means for delivery: [failure matrix](../understand/delivery-contract.md#failure-matrix).

### `502` on ack, extend or nack {#status-502}

An ack, extend or nack answers `502`, with the error of a failed call between nodes as the body.

**Cause.** The node you called forwarded the request to the partition's owner and got no usable answer, often because the owner was restarting or overloaded. The ack may or may not have been applied. When the `502`s last, the owner may be down ([A node is down](#node-down)).

**Check.** `narad_cluster_rpc_requests_total` with `outcome="timeout"` or `outcome="error"` rises on the forwarding node.

**Fix.** Retry the ack. If it answers `410`, the first attempt was applied or the lease had already lapsed; the message may be delivered again either way. Retry rules: [Status codes and errors](../reference/status-codes.md#status-502).

### `429` too many in-flight requests {#status-429}

Consumes answer `429` with `too many in-flight consume requests for this identity (limit 1024 per node)`.

**Cause.** One user holds more concurrent consumes on this node than the cap allows. Every waiting long poll counts, and a batch consume counts as its `max`, clamped to the cap. With security off, the cap counts per client IP. Produces have the same kind of cap, off by default (unreleased).

**Check.** Count the long polls your consumers keep open against one node, per user.

**Fix.** Keep fewer long polls open per user, give each service its own user, or raise `NARAD_HTTP_MAX_CONSUME_IN_FLIGHT_PER_IDENTITY` ([Configuration reference](../reference/configuration.md#http)).

### `429` too many failed authentication attempts {#auth-429}

Requests with one username answer `429` with `{"error":"too many failed authentication attempts"}`, and the node logs `authentication throttled` with that `username`, at most once every 12 seconds.

**Cause.** Each node allows each username 5 failed password checks, then one more every 12 seconds. A client with a wrong password, or someone guessing, has used them up. While they are used up, even a correct password that the node has not yet accepted is refused.

**Check.** The `username` in the log line, and which clients use it.

**Fix.** Correct the client's password. The node accepts the correct one again after at most 12 seconds without failures. How the throttle works: [Networking and security](../understand/networking-and-security.md#auth-throttle).

### `421`, `500` or `partial` on topic details {#status-421}

`GET /v1/topics/{topic}` answers `200` with `"partial": true`, and some entries of `partition_stats` have `"status": "owner_unavailable"`, zero statistics and an `owner_liveness`. `narad server report` marks the topic `[k of n partitions unavailable]`. A v3.0.1 node answers `421` with `this node does not own the requested partition` instead, from every node, or `500` with `get topic failed`.

**Unreleased:** the partial answer is in master, not in v3.0.1.

**Cause.** Topic details gather partition statistics from every partition owner, and one owner could not report. `owner_liveness` says why: `dead` (marked dead, after about 30 seconds without a heartbeat), `unreachable` (alive, but its statistics did not come back within 2 seconds, which a node whose Raft certificate its peers do not trust also causes, [below](#raft-cert-untrusted)), `unknown` (no member record with an address) or `unassigned` (no owner yet, for example right after a partition increase). On v3.0.1 the same causes fail the whole call: `500` while the owner is unreachable, `421` once it has been marked dead.

**Check.** `narad cluster members` shows the owner named in `owner_node` as `dead`, or `/readyz` on it is not ready.

**Fix.** Bring the owner back. The topic list, produce, and consume of the other partitions keep working meanwhile. Until then, leave the unavailable partitions out of any total: their zeros are placeholders, not an empty partition. `narad replay` and `narad peek` refuse such a partition rather than start it at offset 0.

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

### `cluster stream rejected: invalid auth` {#log-stream-invalid-auth}

Logged at warning level (`component=audit`) by a node that refused a node RPC stream whose peer could not prove the node's cluster secret.

**Cause.** A peer with another secret, or none, reached this node's node-to-node port (7942/udp). One common case (unreleased): a node with security on was started alone, without `NARAD_CLUSTER_SECRET`, and nodes are now joining it. Such a node generates a secret of its own for the life of the process, so a joiner carrying the shared secret cannot authenticate to it: the joiner's attempts fail (`cluster join attempt failed` at debug level, with `cluster rpc: read server auth proof: ...`) and it never joins.

**Fix.** Set the same `NARAD_CLUSTER_SECRET` on every node, the first one included, and restart the first node before the others join. The first node also needs a `cluster.addr` the others can reach, and either the Raft TLS files on every node or `security.allow_plaintext_raft` with 7943/tcp fenced: with no peers configured it never runs the join loop, so if the others cannot reach its Raft it stays cut off from their Raft. A first node whose Raft first started on a loopback `cluster.addr` cannot be grown by rebinding it: start a new cluster whose first node starts on an address the others can reach, and move the workload to it ([below](#log-raft-address-recorded)). Any other source of these lines is a process that should not be talking to the port: fence 7942/udp ([Networking and security](../understand/networking-and-security.md#ports)).

### `raft configuration records this node at an address other than the one it advertises` {#log-raft-address-recorded}

**Unreleased:** in master, not in v3.0.1.

The full line is `raft configuration records this node at an address other than the one it advertises: other nodes dial the recorded address, and a later cluster.addr does not change it (a node with cluster.peers set re-registers its advertised address through its join loop after about 15 s without a leader), so if the recorded address does not reach this node they cannot reach its raft once it is not the leader; operator action required`, logged once at startup at error level with `node`, `recorded_addr`, `advertise_addr` and `other_servers` (how many other nodes the Raft configuration lists). A node alone in the Raft configuration that now advertises a loopback address logs `raft configuration records this node at an address other than the one it advertises; harmless while no other node is in the configuration` at info instead: it takes no peers, so no node dials either address.

**Cause.** The address a node's Raft first starts on is recorded in the Raft configuration: at bootstrap on the node that seeds the cluster, by the leader when a node joins. The other nodes dial that recorded address. Restarting the node on a new `cluster.addr` or `cluster.advertise_addr` changes where it listens and what it advertises, not the recorded address. The usual case is a node first started alone on a loopback `cluster.addr` such as `127.0.0.1:7943`, then rebound to an address the others can reach so it could take peers. If nodes join it, then once it is not the leader they cannot reach its Raft: it stays leaderless and not ready, and a node whose `cluster.addr` is port-only (`:7943`) that dials the loopback address reaches its own Raft and steps down, so metadata writes can stall across the cluster. With `other_servers=0`, no other node is in the configuration yet, so nothing has gone wrong yet.

**Fix.** If `recorded_addr` is another way of writing an address that reaches this node, nothing. If it is an address the other nodes can reach, restart the node on it. A node first started on a loopback address cannot be fixed by rebinding, because no other node can reach a loopback address: do not let any node join it, and to grow it, start a new cluster whose first node starts on an address the others can reach and move the workload to it (recreate topics, users and grants there, point producers at it, and retire this node once its consumers have drained it). If nodes have already joined it, move the workload the same way while it is still the Raft leader. A node with `cluster.peers` set re-registers the address it advertises: about 15 s after it finds no leader it runs the [join loop](../understand/cluster-lifecycle.md), and the leader's `AddVoter` updates its recorded address, so for such a node (one re-addressed host of a cluster, say) the line clears once the leader can reach the new address. Only a node with no peers configured keeps its recorded address for good.

### `node RPC plane is unauthenticated` {#log-node-rpc-unauthenticated}

**Unreleased:** in master, not in v3.0.1.

The full line is `node RPC plane is unauthenticated: security is disabled and no cluster secret is set, so anything that can send UDP to the API port can create users and topics and produce, consume and ack without credentials`, logged once at startup at warning level with `component=audit` and `addr`.

**Cause.** The node runs with `security.enabled=false` and no `NARAD_CLUSTER_SECRET`. That mode leaves the API open too; this line names the node-to-node port, which listens even on a single node.

**Fix.** Fence 7942/udp so only the cluster's own nodes reach it, or set `NARAD_CLUSTER_SECRET` on every node, which authenticates the port even with security off. With security on, a node never serves the port without a secret ([Networking and security](../understand/networking-and-security.md#cluster-secret)).

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

### `purge deferred: the local metastore still shows the topic incarnation` {#log-purge-deferred}

**Unreleased:** in master, not in v3.0.1.

The full line is `purge deferred: the local metastore still shows the topic incarnation; the leader may ask again, and the startup orphan sweep is the backstop`, at warning level, with `topic` and `incarnation`.

**Cause.** A topic was deleted, and the leader asked this node to remove its files for the deleted [incarnation](../reference/glossary.md#incarnation), but after 5 seconds this node's metadata replica still showed it. Removing the files first could let a request that still sees the topic reopen them, so the node kept them and answered the leader that the purge was deferred. The leader asks again, up to three times in all.

**Check.** Whether this node's replica is behind: `narad cluster members` and the node's Raft metrics.

**Fix.** None, if the error line [below](#log-purge-unfinished) does not follow. If it does, see there.

### `topic purge unfinished on some members` {#log-purge-unfinished}

**Unreleased:** in master, not in v3.0.1.

The full line is `topic purge unfinished on some members; their copies stay until their startup orphan sweep reclaims them`, at error level on the node that ran the delete (the Raft leader), with `topic`, `incarnation`, `members` and `err`.

**Cause.** After a topic delete, the leader asks every live member to remove its files of the deleted incarnation, detached from the client's request and under its own time budget of about 17 seconds. The members listed did not: one could not be reached, its replica did not apply the delete in time (each such member is asked up to three times), or it refused. The delete itself stands; the topic is gone for every client.

**Check.** `err` names each member and why. `narad cluster members` for the members' state.

**Fix.** The copies take disk space but are never served: a recreated topic of the same name is a different incarnation. Each listed member removes them at its next start (the startup orphan sweep). To reclaim the space sooner, restart the listed members one at a time.

### `move: set aside stale incarnation directory` {#log-stale-incarnation}

Logged at warning level with `topic` and `err`.

**Cause.** The node holds a directory left by a deleted and recreated topic of the same name (an older [incarnation](../reference/glossary.md#incarnation)), and it failed to rename it aside. Narad never serves such a directory; it renames it to `topics/<name>.stale-<id>` and removes it once the leader confirms that incarnation is gone.

**Check.** The `err` field, usually a permission or disk problem in the data directory.

**Fix.** Fix what `err` names. The node retries on its next pass.

### `reclaim: local partition copy is AHEAD` {#log-partition-quarantined}

The full line is `reclaim: local partition copy is AHEAD of the position it was promoted at elsewhere; quarantined instead of deleted, records after the promoted hwm exist only here`, at error level, with `topic`, `partition`, `owner`, `local_next_offset`, `promoted_hwm` and `quarantine_dir`.

**Cause.** A partition moved away from this node while the node was cut off, and the destination [force-promoted](../reference/glossary.md#force-promote) its copy. This node kept taking records meanwhile, so its copy holds records the new owner never received. Instead of deleting it, the node renamed it to `quarantine_dir`.

**Fix.** Narad never serves or deletes that directory; it goes only when the topic is deleted. The records from `promoted_hwm` onwards exist only there, and Narad has no tool to merge them back. Decide whether they matter, copy the directory off if they do, and delete it when you are done. The move protocol: [Rebalance and decommission](../understand/rebalance.md).

### `reclaim: the new owner cannot vouch for the local partition copy` {#log-partition-set-aside}

**Unreleased:** in master, not in v3.0.1.

The full line is `reclaim: the new owner cannot vouch for the local partition copy; quarantined instead of deleted, its records may exist only here`, at error level, with `topic`, `partition`, `owner`, `reason` and `quarantine_dir`. The sweep that triggered it logs `move: stale partition copy QUARANTINED, not deleted: the new owner cannot vouch for it; operator action required` next to it.

**Cause.** A partition moved away from this node, and when this node went to delete its old copy, the new owner held less than the move gave it. `reason` says which: the new owner lists no records (it came back on an empty volume, or rolled its install back), it holds records without a move marker (so they did not come from this copy), its move marker records a move from another node (the partition moved on again before this node's sweep ran, so the marker vouches for that node's records, not this copy's), or it lacks a segment, or holds one shorter, below the position it vouches for (a segment lost before it reached its disk). A node holding an install that never flipped (copied from the owner, see [Rebalance](../understand/rebalance.md#target-failure)) sets it aside the same way: the owner's move marker names how the owner got the partition, not this copy, so the owner cannot vouch for it. Its records are usually also on the owner; check before deleting it. The records of this copy may exist nowhere else, so the node renamed it to `quarantine_dir` instead of deleting it.

**Check.** The new owner's partition directory (`topics/<topic>/p<NNNNN>` under its data directory: the partition number zero-padded to 5 digits, such as `p00003` for partition 3) and whether it has a `move.marker`; `narad server report` for its high watermark.

**Fix.** Copy the quarantined directory off before anything else: its records may be the only ones left. Narad never serves or deletes it; it goes only when the topic is deleted. Narad has no tool to merge it back; decide whether its records matter, re-produce them from the copy if they do, and delete it when you are done. The sweep's rules: [Rebalance and decommission](../understand/rebalance.md#what-if-the-source-dies-mid-move).

### `move: the partition's path held an earlier copy with unexpired records` {#log-move-install-set-aside}

**Unreleased:** in master, not in v3.0.1.

The full line is `move: the partition's path held an earlier copy with unexpired records; quarantined instead of replaced, since it may hold records the incoming copy lacks. Operator action required`, at error level, with `topic`, `partition` and `quarantine_dir`.

**Cause.** A partition moved onto this node while its path (`topics/<topic>/p<NNNNN>`, the partition number zero-padded to 5 digits) still held an older copy, usually this node's own copy from when it owned the partition, which its stale-copy sweep had not judged yet, and that copy held unexpired records. The install never deletes such a copy, because it cannot prove the incoming copy holds the same records: this node may have kept committing past a force-promote while it was cut off, or the new owner may have lost records since. Often the incoming copy does hold them all; the node set the old copy aside anyway, renamed to `quarantine_dir`, and installed the incoming one. The earlier copy can also be an earlier attempt's install of the same move, left when the node restarted or its worker was cancelled with the flip pending; it holds the source's records, so check the owner before treating them as the only copy.

**Fix.** As for [the sweep's set-aside](#log-partition-set-aside): copy the quarantined directory off first, since its records may be the only ones left. Narad never serves or deletes it; it goes only when the topic is deleted. Decide whether its records matter, re-produce them from the copy if they do, and delete it when you are done.

### `move: set aside the staging copy of a partition this node owns` {#log-move-keeping-staging}

**Unreleased:** in master, not in v3.0.1.

The full line is `move: set aside the staging copy of a partition this node owns; the partition's records may not all be under its path. Operator action required`, at error level, with `topic`, `partition`, `quarantine_dir`, `partition_dir`, `moved_back` and `partition_dir_has_records`. A move that took its installed copy back off the partition's path and then cannot read the partition's owner logs `move: set aside the staging copy this move moved back, since the partition's owner cannot be read; it may hold the partition's records. Operator action required` the same way, with `quarantine_dir`, `partition_dir` and `err`.

**Cause.** A move to this node ended without seeing its own flip commit, yet this node owns the partition: a flip committed after all. The move's copy was in `dataDir/.moves/<topic>-<partition>` (the partition number not padded, such as `.moves/orders-3`); the node renamed it aside to `quarantine_dir` (such as `.moves/orders-3.quarantine`), where no later move of the partition onto this node clears it, and `partition_dir` is the partition's path (`topics/<topic>/p<NNNNN>`, the partition number zero-padded to 5 digits, such as `topics/orders/p00003`).

- `moved_back=true`: the leader read the flip as not committed, the move took its installed copy off the partition's path, and the flip committed anyway. `quarantine_dir` holds the partition's records as of the flip; `partition_dir` holds only what this node wrote since.
- `moved_back=false`: no copy installed from the move's source is under the partition's path, so `quarantine_dir` may hold records the path lacks.

When the path does hold a copy installed from the move's source and the move moved nothing back, the flip was an earlier attempt's (a restart cancelled that worker with its flip pending) and `staging` only holds a later attempt's re-copy: the node removes it and logs `move: the partition flipped to this node under an earlier attempt's install; removing this attempt's staging copy` at info instead.

**Check.** `partition_dir_has_records`, and the segment files in both directories (each file is named for the offset it starts at).

**Fix.** Stop the node and copy both directories off first. If `partition_dir_has_records=false`, move the set-aside directory into place as `partition_dir` (`.moves/orders-3.quarantine` goes to `topics/orders/p00003`) and start the node. If it is `true`, both copies can hold records the other lacks, at overlapping offsets: do not replace the live partition with the set-aside copy; compare the two, start the node, and re-produce from the set-aside copy the records you decide matter. Narad does not move or delete either directory on its own.

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

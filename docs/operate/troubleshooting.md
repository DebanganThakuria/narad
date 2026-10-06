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
| `{"status":"ready","degraded":[...]}` | **New in v3.1.0.** The node serves traffic, but its Raft TLS certificate (`raft_tls_certificate_expired`) or every CA in its bundle (`raft_tls_ca_expired`) has expired ([below](#log-raft-tls-expired)). |
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

**Cause.** Narad itself answers a produce with `503` in two cases only, both new in v3.1.0: a node being decommissioned answers `this node is being decommissioned and takes no new produce; send it to another node`, with `Retry-After: 1`; and a topic with a schema whose every validation slot on the node stayed busy for 5 seconds answers `schema: validation capacity busy, retry` ([Validation capacity](../reference/schema-rules.md#validation-capacity)). Nothing was stored in either case. v3.0.1 never answers a produce with `503`. Any other `503` comes from the proxy in front of Narad, the load balancer or ingress, when no pod is ready.

**Check.** For the decommission body, `narad cluster members` shows the node `draining`. For the validation case, `narad_schema_validations_in_flight` on the node sits at its CPU count and `narad_schema_rejections_total{reason="busy"}` rises; look for producers sending large payloads, or a schema that `narad_schema_validation_seconds` shows to be slow. Otherwise, `kubectl get pods -n narad`, then `/readyz` on each pod ([Start with readiness](#check-readiness)).

**Fix.** For a draining node, send produce to the other nodes: the Go SDK retries on another node by default. Cancel the decommission to have the node take produce again. For the validation case, retry with backoff, through another node if you can, and spread large payloads out or simplify the slow schema. When every pod answers `no raft leader known`, follow [Not ready on every pod](#not-ready-all-pods). When pods are down, see [A node is down](#node-down).

### `500` on every produce {#produce-500}

Every produce to one node answers `500` with `{"error":"produce failed"}`, and the node logs `http server error` with `op=produce`.

**Cause.** A write or sync of the node's [ingress WAL](../reference/glossary.md#ingress-wal) failed, usually because the disk is full (`ENOSPC`) or failing (`EIO`). The node then refuses every later produce, because it cannot tell what the failed write left on disk. Consume and `/readyz` keep working, so the node stays in rotation.

**Check.** `narad_ingress_wal_failed` is `1` on that node (from v3.1.0; on v3.0.1, watch `narad_errors_total{component="http", kind="5xx"}`). Check the volume:

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

**Cause.** One user holds more concurrent consumes on this node than the cap allows. Every waiting long poll counts, and a batch consume counts as its `max`, clamped to the cap. With security off, the cap counts per client IP. From v3.1.0, produces have the same kind of cap, off by default.

**Check.** Count the long polls your consumers keep open against one node, per user.

**Fix.** Keep fewer long polls open per user, give each service its own user, or raise `NARAD_HTTP_MAX_CONSUME_IN_FLIGHT_PER_IDENTITY` ([Configuration reference](../reference/configuration.md#http)).

### `429` too many failed authentication attempts {#auth-429}

Requests with one username answer `429` with `{"error":"too many failed authentication attempts"}`, and the node logs `authentication throttled` with that `username`, at most once every 12 seconds.

**Cause.** Each node allows each username 5 failed password checks, then one more every 12 seconds. A client with a wrong password, or someone guessing, has used them up. While they are used up, even a correct password that the node has not yet accepted is refused.

Or the node's failure budget is empty (from v3.1.0): repeated wrong passwords across many usernames have used up the 32 checks the node allows for usernames with recent failures, refilled at 4 a second. Until it refills, any username whose own bucket is not full again (about 12 seconds per recent failure) is refused at once, and the node logs `authentication failure budget exhausted` at most once every 10 seconds. Usernames with no recent failures are not affected.

**Check.** The `username` in the log line, and which clients use it. For the budget, look for `authentication failure budget exhausted` and a rising `narad_auth_verify_queued`.

**Fix.** Correct the client's password. The node accepts the correct one again after at most 12 seconds without failures. How the throttle works: [Networking and security](../understand/networking-and-security.md#auth-throttle).

### `421`, `500` or `partial` on topic details {#status-421}

`GET /v1/topics/{topic}` answers `200` with `"partial": true`, and some entries of `partition_stats` have `"status": "owner_unavailable"`, zero statistics and an `owner_liveness`. `narad server report` marks the topic `[k of n partitions unavailable]`. A v3.0.1 node answers `421` with `this node does not own the requested partition` instead, from every node, or `500` with `get topic failed`.

The partial answer is **new in v3.1.0**.

**Cause.** Topic details gather partition statistics from every partition owner, and one owner could not report. `owner_liveness` says why: `dead` (marked dead, after about 30 seconds without a heartbeat), `unreachable` (alive, but its statistics did not come back within 2 seconds, which a node whose Raft certificate its peers do not trust also causes, [below](#raft-cert-untrusted)), `unknown` (no member record with an address) or `unassigned` (no owner yet, for example right after a partition increase). On v3.0.1 the same causes fail the whole call: `500` while the owner is unreachable, `421` once it has been marked dead.

**Check.** `narad cluster members` shows the owner named in `owner_node` as `dead`, or `/readyz` on it is not ready.

**Fix.** Bring the owner back. The topic list, produce, and consume of the other partitions keep working meanwhile. Until then, leave the unavailable partitions out of any total: their zeros are placeholders, not an empty partition. `narad replay` and `narad sub --peek` refuse such a partition rather than start it at offset 0.

### `204` gaps after a node returns {#quiet-after-outage}

After a node comes back, some partitions deliver in bursts with quiet gaps between them. Consumers get `204` during the gaps and the consumer lag stops falling. It looks like a stuck broker and is not one.

**Cause.** The outage stranded [leases](../reference/glossary.md#lease): a consumer held a message when its node died and never acked it. Messages after it were acked, but a partition's [committed frontier](../reference/glossary.md#committed-frontier) cannot pass an unacked message, so the partition has nothing new to hand out. When the stranded lease reaches the topic's visibility timeout (30 seconds by default), the message is delivered again, acked, and the whole acked run behind it clears at once. That is the burst.

**Check.** `narad_acked_ahead_size` is above 0 for the quiet partition, and the gaps last about one visibility timeout.

**Fix.** None is needed, and nothing is lost. Give a consumer fleet draining a backlog after an outage more than one visibility timeout before you conclude it stopped. To shorten the gaps, lower the topic's `visibility_timeout_ms` to what your slowest handler needs: the gaps shrink one for one. The mechanism: [Consume path](../understand/consume-path.md#after-an-outage).

### `412` on a remote write or a remote child {#status-412}

**Unreleased.**

A remotes request, or a remote child's attach, pause, resume or skip, answers `412` and writes nothing.

**Cause.** The message, and the `members` field of the body, name the precondition:

- `not every cluster member runs a release that applies the remote Raft entry types; upgrade or remove the member named here: ...`: a member, possibly a dead one or a Raft server without a member record, still reports an older release.
- `a cluster member's security posture forbids remotes (security off or legacy cluster auth on)`, or `every cluster member must answer the remotes posture check`: a member runs with security off or `security.allow_legacy_cluster_auth`, or did not answer.
- `remote writes carry a password, so this node needs an encrypted API hop ...`: the node that took a create or a password change does not set `remotes.api_hop_encrypted`.
- `the cluster secret (NARAD_CLUSTER_SECRET) must be at least 32 random bytes as ...`, or `no cluster secret`: nothing can be sealed under this secret.
- `remote check failed: stale`, `unreachable`, `old_release` or `target_disagreement`: a member had not applied the latest change to the remote yet, did not answer the checks, runs an older release, or saw a different target.
- `the cluster leader runs an older release ...`: the leader has not been upgraded.

**Check.** `narad cluster members` for dead members and Raft servers without a record; the image each pod runs; `narad remote ls` for the posture each node reports.

**Fix.** Finish the upgrade, and decommission or [forget](../reference/cli.md#cluster) a member that will not come back; turn legacy cluster authentication off; set `remotes.api_hop_encrypted` once the hop is really encrypted ([Before you start](remotes.md#before-you-start)); replace a weak cluster secret ([Rotate the cluster secret](remotes.md#rotate-cluster-secret)); retry a `stale` check after a few seconds.

### `409` has unshipped records on a detach or delete {#remote-unshipped}

**Unreleased.**

`narad topic detach <parent> <child>`, or a delete of a remote child's stub or of its parent, answers `409` `has unshipped records`, with `lag_messages`, `lag_complete` and `dispatch_backlog` in the body.

**Cause.** Some record of the parent is not on the remote yet: a cursor has lag, a partition's owner did not report (`lag_complete` false, or `not_answering`), or a node still holds records of the parent it answered `202` for and has not committed (`dispatch_backlog`, by node). The leader refuses rather than abandon them.

**Fix.** Stop the producers, then `narad topic wait <parent> <child> --lag-zero --stable 60s` and detach again. If the link is stalled, fix that first ([below](#remote-link-stalled)). To abandon the records on purpose, `narad topic detach <parent> <child> --force`; `narad topic rm --force` does not abandon anything. A second attempt within 10 seconds answers `429`: wait for `Retry-After`.

**A node in `not_answering`.** The check asks every member, dead ones included, and a node that does not answer may hold records it answered `202` for: waiting for lag 0 does not help, and every detach without `--force` answers `409` until it answers. Bring the node back: it dispatches its ingress WAL, and the detach goes through once that backlog is shipped. A dead node cannot be decommissioned either until it comes back ([`node_status_unavailable`](#decommission-blocked)). `--force` abandons whatever that node's ingress WAL still holds; use it only for a node that is gone for good with its disk, whose records are lost with it anyway. A node in `backlog_over_scan_limit` could not read its backlog to the end for the check (over 1,000,000 records, or 15 seconds): let it drain, then detach again.

### `409` lives on remote {#remote-stub}

**Unreleased.**

A produce, consume or ack answers `409` `remote child "<name>" lives on remote <remote>; consume it there`.

**Cause.** The topic is a [remote child](../reference/glossary.md#remote-child)'s stub, which has no partitions: its messages are on the remote.

**Fix.** Produce to the parent, and consume the copy on the remote's topic (`remote.topic` in `narad topic info <name>`).

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

### `narad_quarantined_copies` above 0 {#quarantined-copies}

The node holds partition copies it set aside instead of deleting. At startup it lists each one at error level, `quarantined partition copy on this node: it was set aside instead of deleted and may hold the only instance of some of its records; inspect it before removing it (troubleshooting: quarantined copies)`, with `kind`, `topic`, `partition`, `dir`, `bytes` and `mod_time` (twenty lines at most), then `this node holds quarantined partition copies; none is removed automatically (troubleshooting: quarantined copies)` with the totals in `copies` and `bytes`. The gauge and `narad_quarantined_bytes` are refreshed at startup and on every stale-copy sweep (about every 30 s).

**Cause.** `kind` says where the copy came from:

- `partition` (`topics/<topic>/p<NNNNN>.quarantine`, or with a timestamp suffix when that name was taken): a stale copy the new owner could not vouch for, or one ahead of the position it was promoted at, set aside by the [stale-copy sweep](#log-partition-set-aside), or an earlier copy a move's [install](#log-move-install-set-aside) found at the partition's path.
- `staging` (`.moves/<topic>-<N>.quarantine`): a move's staging copy set aside because this node owns the partition by now and the copy may hold records its path lacks ([set-aside staging](#log-move-keeping-staging)), or because the move ended while the partition's owner was dead ([dead source](#log-move-dead-source-staging)).
- `topic_incarnation` (`topics/<topic>.stale-<id>`): the directory of a deleted incarnation of a topic recreated under the same name. Narad removes it on its own once the leader confirms that incarnation is gone; one that stays means the leader cannot be asked or still lists the incarnation.

**Check.** The error line logged when the copy was set aside names why; search the node's logs for its `dir`.

**Fix.** Copy a `partition` or `staging` copy off before anything else: its records may be the only ones left. Narad never serves one and never deletes one on its own; a `partition` copy goes only when its topic is deleted. Decide whether its records matter, re-produce them from the copy if they do, and delete the directory when you are done. The gauge drops on the next sweep.

### `narad_orphan_topic_dirs` stays above 0 {#orphan-topic-directories}

The node holds directories of topics its replica no longer knows: a deleted topic whose purge never reached this node (it was down, marked dead or lagging when the delete committed).

**Cause.** The stale-copy sweep removes such a directory once the leader confirms its [incarnation](../reference/glossary.md#incarnation) is gone (the name is absent, or live as another incarnation), at most 16 a pass. Two kinds stay:

- A directory without an incarnation marker (`topics/<name>/incarnation`), such as one of a topic whose record has no incarnation id. While the node runs, Narad cannot tell it from a directory a concurrent open is making, so only the startup sweep removes it, which runs before the node accepts topic creates.
- A directory the leader has not confirmed gone: the leader cannot be reached, or it still lists the incarnation because this node's replica is behind a create.

**Check.** `ls dataDir/topics` for names `narad topic list` does not show, and the node's logs for `leader confirm:` warnings.

**Fix.** For a leader that cannot be asked, restore the cluster's leader; the next sweep removes the directory. For an unmarked directory, restart the node, or remove the directory by hand once you are sure no topic of that name exists.

### `narad_decommission_blocked` above 0 {#decommission-blocked}

**New in v3.1.0.**

A draining node's decommission cannot progress. The leader exports one series per node and `reason` while it holds, logs each reason once when it appears (`controller: decommission blocked` at error for a reason that needs you, `controller: decommission waiting` at warn for one that clears on its own, both with `node`, `reason` and `detail`), and `controller: decommission no longer blocked` at info when the node is free again. `narad cluster members` shows the reasons the cluster metadata holds under `decommission_blocked`.

**Cause and fix**, by `reason`:

- `below_min_voters` (error): removing the voter would leave fewer than three voters. Add a node, or cancel the decommission. A new decommission like this is refused up front.
- `no_healthy_majority` (error): the voters left alive after the removal would not be a majority, because other voters are dead. Bring the dead voters back, or decommission them first.
- `owner_dead` (error): the node is dead and owns partitions whose data is only on its disk. Bring it back so they can move off, or cancel the decommission.
- `no_receivers` (error): the node owns partitions and no alive node that is not draining can take them. Add a node, or cancel another decommission.
- `node_status_unavailable` (error): the node owns nothing, but its dispatch backlog cannot be read: it is dead or unreachable on the node RPC port. Its ingress WAL may hold messages only it has, so the leader keeps it in Raft. Bring it back; it hands its WAL off and is then removed. Or cancel the decommission.
- `dispatch_backlog` (warn): the node's ingress WAL still holds accepted messages not yet handed to their owners, or it may still take some: it does not refuse client produce yet (its replica has not applied the drain), or it is still answering produce it admitted before it started to. It clears on its own; one that stays points at the node's dispatcher (its logs, `narad_ingress_dispatch_backlog_records`), or at a node whose replica does not catch up (`narad cluster members --detail` shows its `draining`).
- `move_target` (warn): a move still aims at the node. The leader clears such moves itself; abort one that stays with `narad cluster moves abort`. Logged at error when the move's source is dead or no longer a member: the draining node may then hold the only live copy, so the leader leaves the move alone and waits for the copy to be force-promoted and moved off, or for the source to come back. Abort it by hand only if dropping that copy is really intended.
- `move_budget_full` (warn): the node owns partitions and every move slot (8) is taken by other moves. It drains once they finish; a slot held by a blocked move frees once you [abort it](#moves-blocked).
- `leader_transfer` (warn): the node leads; leadership moves to another voter first, and the new leader removes it.

### `narad_moves_blocked` above 0 {#moves-blocked}

**New in v3.1.0.**

A partition move cannot finish on its own. The gauge is per node and per `reason`: a move's destination reports the first two reasons for the moves it runs, and the leader reports the last two for every move in flight.

**Cause.** By `reason`:

- `copy_unverifiable`: the move's staged copy failed verification, and failed again after one fresh copy, so the node stopped freezing the source ([staged copy cannot be verified](#log-move-unverifiable)). Also a dead source's copy that reaches its high watermark but fails verification.
- `source_dead_copy_behind`: the move's source is dead and the copy is behind the source's last high watermark, so promoting it would lose records ([source is dead](#log-move-dead-source-behind)).
- `source_dead` (leader): the partition's owner, the move's source, is dead. The destination force-promotes a complete copy after it has seen the source dead for 2 minutes; a copy that is behind waits as above. The leader logs `controller: move blocked: its source is dead` at error once per move and leadership term.
- `target_dead` (leader): the move's destination is dead. The leader clears the target once the destination has been dead for 2 minutes on its own clock, and logs `controller: move blocked: its destination is dead` at error once per move and term.

**Check.** The node's log for the error line of each reason; `narad cluster moves` shows each move's `from_status`, `to_status` and `blocked`, and `narad cluster moves --detail` adds the destination's own report (`worker`: phase, attempts, copied bytes, last error).

**Fix.** By reason, in the log lines below. Each blocked move holds one of the `MaxInFlightMoves` slots, so later rebalances and decommissions wait behind it. To give up on a move, abort it: `narad cluster moves abort <topic> <partition> --target <destination>` clears its target, the partition stays with its owner, and the destination discards its copy (a copy made while the source was dead is set aside instead). The leader may plan the partition again later.

### `narad_raft_nonvoters` stays above 0 {#nonvoters-stay}

`narad_raft_nonvoters` stays above 0 for more than a minute after a scale-out or a readmission.

**Cause.** A node joined as a Raft non-voter and has not been promoted to voter ([how promotion works](../understand/cluster-lifecycle.md#join-promotion)). It still replicates and serves traffic; it only does not vote. Either it never asked again (it runs 3.0.x, or its replica has not caught up with the leader), or the leader defers it. A node decommissioned while a 3.0.x node led can also be left behind as a non-voter: that release removes only voters.

**Check.** The log of each pod that joined after the initial members. A deferred node logs `cluster join: caught up as a raft non-voter; the leader defers promotion` at info, with the leader's `reason`, whenever the reason changes. A node that logs nothing of the kind is on an older release or still catching up (its `/readyz` says which).

**Fix.** By `reason`:

- `the leader has led for less than 12s`: nothing; the leader promotes it once it has led that long.
- `the leader's raft heartbeats to it are failing`: the leader cannot reach the node's Raft address. Check its `NARAD_CLUSTER_ADVERTISE_ADDR`, the network between them (7943/tcp), and the [Raft certificate](#raft-cert-untrusted).
- `it has no member record yet`, or `its member record is marked dead`: the node is not heartbeating its membership to the leader. Check its log for [`member heartbeat failing`](#log-member-heartbeat-failing) (warning level; on v3.0.1, `member heartbeat failed` at debug level) and the node-RPC port (7942/udp).
- `it is draining`: the node is being decommissioned, and decommission removes it from Raft once it owns nothing. Cancel the decommission to keep it.

A node on 3.0.x asks for promotion once it runs this release and restarts. A non-voter left behind by a decommission under a 3.0.x leader carries no quorum weight, but the leader keeps sending it heartbeats, and it has no member record, so the decommission cannot simply be run again. **New in v3.1.0:** remove it with `narad cluster members forget <id>`; see [A Raft server has no member record](#raft-server-no-member-record).

### A Raft server has no member record {#raft-server-no-member-record}

The Raft configuration holds a server that `narad cluster members` does not list. **New in v3.1.0:** a warning names it as `raft server "<id>" has no member record, so its release is unknown`, because it holds back new Raft entry types, for example in `user deleted, but its topics still name it as owner` (`component=audit`).

**Cause.** A node joined and never registered: it crashed right after its join request, could not reach the cluster, or was replaced under another ID. A 3.0.x leader admitted joiners straight into the voter set, so such a server can be a voter, and an unreachable voter counts against quorum: with one more voter down the cluster may lose its leader. A decommission under a 3.0.x leader can also leave a non-voter behind.

**Check.** `narad_raft_voters` and `narad_raft_nonvoters` on the leader against the members `narad cluster members` lists, and the leader's log for `raft: failed to heartbeat to: peer=<addr>` naming an address no member has. The ID to forget is the one in the `has no member record` warning; under the Helm chart a node's Raft ID is its pod name, which also starts its Raft address (`narad-3.narad-headless...`). `narad_raft_nonvoters` is only a count and names no server.

**Fix.** **New in v3.1.0:** remove it, from any node, while the cluster has a leader:

```bash
narad cluster members forget narad-3
```

It answers `{"id":"narad-3","voter":true}` (or `false` for a non-voter), and the leader logs `forgot a raft server with no member record` (`component=audit`). Forget moves and deletes no data. It refuses a server with a member record, alive, dead or draining (`409`; [decommission](scaling.md#decommission) it instead), one a partition assignment names (`409`), and the leader itself (`400`). It also refuses a voter while the voters left after the removal could lack a quorum (`409`, `forgetting the voter could leave the cluster without a quorum`). Raft commits the removal under the new configuration, so unless the leader and the voters it reaches are a majority of the voters left, the cluster loses its leader and no change can commit to undo it. Take voters `narad-0` (the leader), `narad-1` and the stray `narad-3` with `narad-1` down: forgetting `narad-3` would leave `narad-0` and `narad-1`, which cannot commit without `narad-1`. The message names the voters whose Raft heartbeats are failing: bring them back, then run forget again. A leader that has led for less than 12 s refuses every voter (`has led for less than 12s`) until it has seen its heartbeats long enough; run it again. A non-voter carries no quorum weight and is forgotten without this check. A leader on an older release answers `501`: finish the upgrade first. If the node comes back later, it asks to join again and is staged as a new non-voter.

### `narad_fanout_remote_state` is not `running` {#remote-link-stalled}

**Unreleased.**

A remote child's link holds in a state other than `running` or `paused`, and its lag grows. The node that owns the parent partition logs `remote child stalled` once as the cursor enters the state, with the `state`, the `remote` and, when the target's answer stalled it, the `status`: an error for every state that needs a fix (the list below), a warning for `unavailable` and `throttled`. A record the target refuses also logs `remote child blocked on a record the target refuses` with the `offset`, and `remote child running again` follows once the link sends again.

**Check.** `narad topic children <parent> --partitions` names the state per partition and `blocked_at`; [Link states](../reference/remote-children.md#link-states) says what each means. `narad remote ls` shows each node's credential state and `last_error`, and `narad remote test <remote> --topic <topic> --source <parent>` runs every check now.

**Fix.** By state:

- `auth_failed`, `forbidden`: the replicator user on the target was deleted, its password changed, or its `produce` grant is missing. Fix it on the target, or set the remote's password again ([Rotate a remote's password](remotes.md#rotate-password)).
- `target_missing`: create the topic on the target.
- `target_replaced`: the target topic was recreated, or the URL reaches another cluster. Check it is the topic you want, then `narad topic resume <parent> <child> --accept-target`.
- `target_has_remote_children`: the target topic has a remote child of its own, which would make a chain or a loop. Detach it on the target.
- `remote_missing`: the remote was deleted with `--force`. Create it again under the same name.
- `credential_unreadable`, `node_insecure`: see [below](#remote-credential-unreadable).
- `destination_refused`: the host now resolves to an address the guard refuses, or the URL's port or host is outside the node's bounds; `narad_remote_destination_refused_total{reason}` says which. Fix DNS or the `remotes.*` settings.
- `tls_failed`: the target's certificate does not verify against the remote's `ca_pem` or the system roots, or has expired. Fix the certificate, or set the CA bundle again with the password.
- `redirect_refused`: the URL answers with a redirect. Register the address that answers directly.
- `no_batch_produce`: upgrade the target to v3.1.0 or later.
- `rejected_record`, `record_too_large`: one record the target refuses for good, named in `blocked_at`. Fix the target's schema or upgrade the target, or [skip](../build/remote-children.md#skip) the record.
- `unavailable`, `throttled`: the target is down, overloaded or rate limiting. The link retries on its own; watch the headroom.

The link resumes on its own once the cause is fixed: a stalled cursor retries every 30 seconds, and at once when the remote changes. Nothing is lost while the headroom lasts.

### `narad_fanout_remote_retention_headroom_seconds` falls {#remote-headroom-low}

**Unreleased.**

A remote child's oldest unshipped record approaches the parent's retention.

**Cause.** The link is stalled or paused, or it ships slower than the parent is produced to.

**Fix.** Raise the parent's retention now (`narad topic edit <parent> --retention 168h`): it takes effect at once while the headroom is above 0. Then fix the link ([above](#remote-link-stalled)), or speed it up: more `lanes` (detach and attach again), a higher `max_in_flight` on the remote, `compression: zstd`. Once the headroom reaches 0, the oldest unshipped records age out and are counted in `narad_fanout_child_dropped_messages`.

### `narad_remote_credential_state` shows `credential_unreadable` or `node_insecure` {#remote-credential-unreadable}

**Unreleased.**

A node cannot use a remote's stored password, and its links hold in the same state. It logs `remote credential unreadable on this node` with the `remote` and whether the password's key is `current`, `previous` or `unknown` here, never the key itself.

**Cause.** For `credential_unreadable`, the node lacks the cluster secret the password was sealed under: the cluster secret changed without `NARAD_CLUSTER_SECRET_PREVIOUS`, or the node runs with a different secret from the others. For `node_insecure`, the node runs with security off or legacy cluster authentication on.

**Fix.** Give every node the same `NARAD_CLUSTER_SECRET`, and during a rotation the previous one too, then finish it with `narad remote reencrypt` ([Rotate the cluster secret](remotes.md#rotate-cluster-secret)). If the old secret is gone, enter each password again with `narad remote set <name> --remote-password-stdin`. For `node_insecure`, fix the node's security settings and restart it.

## Log lines

### `storage: fsync failed; log poisoned until reopened` {#log-fsync-poisoned}

Logged at error level with `dir`, `durable_tail` and `err`.

**Cause.** A disk sync of one partition failed. The node refuses every later write to that partition, because the data it tried to sync may already be gone, but it still serves the records committed before. New records wait in the ingress WAL, which moves them to other partitions of the topic.

**Check.** `err` and `dir` in the log line, `narad_errors_total{component="storage", kind="fsync_poisoned"}`, and the health of the disk.

**Fix.** Fix the disk, then restart the pod: the partition is reopened and checked, and the records waiting for it are committed. Why a failed sync is final: [Storage engine](../understand/storage-engine.md#fsync-failure).

### `metastore: stopped applying raft entries` {#log-metastore-stopped}

**New in v3.1.0.**

Logged at error level with `index`, `entry_type`, `build` and `error`, just before the node exits non-zero. The pod restarts and, until the cause is fixed, stops again on the same entry.

**Cause.** The node could not apply a committed metadata change, and stopped rather than skip it ([When a node stops applying](../understand/metastore-and-raft.md#fail-stop)). `error` says which:

- `raft entry at index <n> has entry type <t>, which this build (...) does not know`: a newer release proposed the entry, and this pod runs an older image than the rest of the cluster (a stale tag, or a pod rolled back on its own).
- `could not write raft entry at index <n> (entry type <t>) to .../fsm.db after retrying for <about 26s>: ...`: the data volume refused the write, usually `no space left on device`, sometimes an I/O error. The time is how long the node actually retried: the retries wait 100 ms doubling to 5 s, and the last one that fits in the 30 s budget ends after about 26 s. The node logged `metastore: could not write raft entry; retrying` at warning level about 26 s before.
- `the raft snapshot holds raft entry type <t>, written by a newer Narad release`: the leader sent this node a snapshot from a newer release.

**Check.** The image of every pod (`kubectl get pods -n narad -o custom-columns=NAME:.metadata.name,IMAGE:.spec.containers[0].image`), and the free space and kernel log of the pod's data volume.

**Fix.** Run the cluster's release on the pod, or free space on the volume (or replace it). Nothing needs repair: the entry was never counted as applied, so the restart applies it and the node rejoins. The rest of the cluster keeps working as long as a majority of voters can write; pods that share a full volume all stop.

### `metastore: set aside fsm.db as fsm.db.stale` {#log-metastore-set-aside}

**New in v3.1.0.**

Logged at warning level at start, with `reason` and `stale`.

**Cause.** The node found an `fsm.db` it could not use as the base for its Raft log: one with no applied index this release can trust (written last by v3.0.x: the first restart after an upgrade, or after a rollback and a new upgrade), or, on a node joining a running cluster, one left beside a missing Raft state or one newer than its Raft state (a node that does not join refuses to start in both cases instead: [no Raft state](#log-metastore-no-raft-state), [older Raft state](#log-metastore-raft-state-older)). It moved the file aside and rebuilds the database from the Raft log ([Restarts](../understand/metastore-and-raft.md#restarts)).

**Fix.** None. Delete `fsm.db.stale` (next to `fsm.db` under the data directory's `metastore` directory) once the node is ready. It is kept only for inspection, and the next set-aside overwrites it.

### `holds metadata, but there is no raft state beside it` at start {#log-metastore-no-raft-state}

**New in v3.1.0.**

`narad serve` exits at start with `metastore: .../fsm.db holds metadata, but there is no raft state beside it (raft.db is missing or empty and there is no raft snapshot), so this node would bootstrap a new cluster with an empty log and none of its topics; refusing to start`, followed by the two ways out below. The pod restarts and exits the same way until one is taken.

**Cause.** The node's Raft log (`raft.db`) and snapshots are gone while its metadata database survived: the file was deleted, or the volume was restored without it. A node with fewer than `cluster.raft_snapshot_threshold` metadata changes (8192 by default) has no snapshot, so `raft.db` held all of its Raft state. A node that would bootstrap (a single node, or an initial member none of whose peers answers) would start a new cluster on an empty database that holds none of its topics, and then remove their partition directories as orphans. It refuses instead and leaves `fsm.db` untouched. v3.0.1 replayed the new log onto the old file. A node that joins a running cluster sets the file aside instead ([previous section](#log-metastore-set-aside)).

**Check.** The pod's `metastore` directory under the data directory: `fsm.db` is there, `raft.db` is missing or was just created, and `snapshots` is empty.

**Fix.** To keep the node's topics, restore `raft.db`, the `snapshots` directory and `fsm.db` from the same backup of the volume and restart, and first copy the `topics` directory beside `metastore` somewhere safe: the node removes every topic directory that no topic names, so the topics created after the backup lose their partition data. Restoring `raft.db` alone beside the current `fsm.db` does not work: every member heartbeat is a Raft entry, so a backup of `raft.db` is always older than a running node's `fsm.db`, and the node refuses again with `raft state is older than fsm.db` ([next section](#log-metastore-raft-state-older)). A member of a multi-node cluster rejoins on its own once a peer answers at its start: it joins the running cluster instead of bootstrapping, and rebuilds the database from the leader. To start the node empty, move `fsm.db` out of the `metastore` directory, and move the `topics` directory beside it aside too if its partition data must be kept: a node started empty removes every topic directory that no topic names.

### `raft state is older than fsm.db` at start {#log-metastore-raft-state-older}

**New in v3.1.0.**

`narad serve` exits at start with `metastore: raft state is older than fsm.db: .../fsm.db has applied raft index <a>, past the end of the raft log (index <l>) and the latest raft snapshot (index <s>) in ...; starting would drop every metadata change after index <n>, so refusing to start`, followed by the ways out below. The pod restarts and exits the same way until one is taken.

**Cause.** The node's metadata database holds Raft entries its Raft log and snapshots do not: `raft.db` and the `snapshots` directory were restored from a backup and `fsm.db` was not. Every member heartbeat is a Raft entry, so a backup of `raft.db` is always older than a running node's `fsm.db`. Rebuilding the database from the older Raft state, or restoring its snapshot over it, would drop every topic created since the backup, and a node that leads would then remove their partition directories as orphans. It refuses instead and leaves `fsm.db` untouched. v3.0.1 replayed the older log onto the file, or restored the older snapshot over it. A node that joins a running cluster sets the file aside instead, and the cluster's log brings the rest ([Set aside](#log-metastore-set-aside)).

**Check.** The pod's `metastore` directory under the data directory, and where its `raft.db` came from.

**Fix.** If the `raft.db` and `snapshots` that went with this `fsm.db` still exist, put them back and restart. Otherwise restore `fsm.db` from the same backup as `raft.db` (or move it out of the `metastore` directory, and the node rebuilds it from the restored Raft state), and first copy the `topics` directory beside `metastore` somewhere safe: the node removes every topic directory that no topic names, so the topics created after the backup lose their partition data. On a member of a multi-node cluster whose other members are running, moving `fsm.db` out is enough: the node rebuilds it from its log and the leader's, and loses nothing.

### `written by a newer Narad release` at start {#log-metastore-newer-database}

**New in v3.1.0.**

`narad serve` exits at start with `metastore: fsm: .../fsm.db holds raft entry type <t>, written by a newer Narad release than this build (...); run that release or newer`.

**Cause.** The node's metadata database has applied an entry only a newer release proposes: the pod was rolled back, or started on an older image, after the cluster used a newer release's feature. This build would read that metadata by older rules, so it does not open it, and leaves the file untouched.

**Fix.** Run the release the rest of the cluster runs, or a newer one.

### `no raft leader for a while; running the cluster join loop` {#log-no-raft-leader}

Logged at warning level after a node has had no Raft leader for 15 seconds.

**Cause.** The node lost its leader: its peers are down, the network between them is cut, or the node was removed from Raft. It now asks its peers to admit it again every 2 seconds. The line reads `raft non-voter has had no leader for a while; running the cluster join loop` on a node that had joined and was not yet promoted.

**Check.** `/readyz` on every pod, and whether the node was decommissioned (`narad cluster members`).

**Fix.** Restore the network or the missing pods. A decommissioned node is refused ([next section](#log-join-rejected)).

### `cluster join refused: this node was decommissioned` {#log-join-rejected}

The full line is `cluster join refused: this node was decommissioned and removed; it will not rejoin with its old data directory. Scale it away, or delete its volume to rejoin as a new node`.

**Cause.** A node that was decommissioned restarted with its old volume. The leader refuses it, so it cannot undo its own decommission. If the line's `body` carries `older_release`, the cause is different: the node runs 3.0.x and the cluster's members all run a newer release ([next section](#log-join-older-release)).

**Fix.** Scale it away. To use the name again, delete its PersistentVolumeClaim so it starts empty ([Reuse a decommissioned name](scaling.md#reuse-name)).

### `cluster join refused: this node runs an older release than every member` {#log-join-older-release}

**New in v3.1.0.**

Logged at error level on the joining node, once, with `via`, the node's `entry_types` and the leader's answer in `body`. The leader logs `cluster join refused: the joiner runs an older release than the cluster` at error, at most once a minute per joiner, with the joiner's `id`, `joiner_entry_types`, `member_entry_types_min` (the fewest any recorded member applies) and `cluster_entry_type_used` (the newest type the cluster has applied). A joiner on 3.0.x logs the same refusal as `cluster join refused: this node was decommissioned`, with `older_release` in its `body`.

**Cause.** The node is not in the Raft configuration and applies fewer Raft entry types than every member of the cluster, so the cluster may already use entries it would skip or stop on, or fewer than the newest type the cluster has already applied, so its log holds such entries. A joiner that has been admitted but has not registered yet counts for neither ([Raft entry types and upgrades](../understand/metastore-and-raft.md#entry-types)). Typically a pod started on an older image than the rest of the cluster: a scale-out or a re-added node while `image.tag` pointed at an older release, or a rollback of one node that also lost its volume.

**Fix.** Run the cluster's release on the node (`image.tag`). It keeps asking every 2 seconds and is admitted on its next attempt. Nothing about the cluster needs to change.

### `cluster join rejected` {#log-join-rejected-status}

Logged at warning level with `via` (the address that answered) and `status`.

**Cause.** The leader could not admit the node. A `503` status means the leader failed to read the membership or to change the Raft configuration (it may have lost leadership meanwhile). A `400` status means the leader could not read the request, or it lacked the node's ID or Raft address.

**Fix.** For `503`, the node retries every 2 seconds; check the leader's log for `join cluster: admission failed` or `join cluster: readmit member`. For `400`, compare the node's `NARAD_NODE_ID` and `NARAD_CLUSTER_ADVERTISE_ADDR` with a working node's.

### `cluster stream rejected: invalid auth` {#log-stream-invalid-auth}

Logged at warning level (`component=audit`) by a node that refused a node RPC stream whose peer could not prove the node's cluster secret.

**Cause.** A peer with another secret, or none, reached this node's node-to-node port (7942/udp). One common case (from v3.1.0): a node with security on was started alone, without `NARAD_CLUSTER_SECRET`, and nodes are now joining it. Such a node generates a secret of its own for the life of the process, so a joiner carrying the shared secret cannot authenticate to it: the joiner's attempts fail (`cluster join attempt failed` at debug level, with `cluster rpc: read server auth proof: ...`) and it never joins.

**Fix.** Set the same `NARAD_CLUSTER_SECRET` on every node, the first one included, and restart the first node before the others join. The first node also needs a `cluster.addr` the others can reach, and either the Raft TLS files on every node or `security.allow_plaintext_raft` with 7943/tcp fenced: with no peers configured it never runs the join loop, so if the others cannot reach its Raft it stays cut off from their Raft. A first node whose Raft first started on a loopback `cluster.addr` cannot be grown by rebinding it: start a new cluster whose first node starts on an address the others can reach, and move the workload to it ([below](#log-raft-address-recorded)). Any other source of these lines is a process that should not be talking to the port: fence 7942/udp ([Networking and security](../understand/networking-and-security.md#ports)).

### `raft configuration records this node at an address other than the one it advertises` {#log-raft-address-recorded}

**New in v3.1.0.**

The full line is `raft configuration records this node at an address other than the one it advertises: other nodes dial the recorded address, and a later cluster.addr does not change it (a node with cluster.peers set re-registers its advertised address through its join loop after about 15 s without a leader), so if the recorded address does not reach this node they cannot reach its raft once it is not the leader; operator action required`, logged once at startup at error level with `node`, `recorded_addr`, `advertise_addr` and `other_servers` (how many other nodes the Raft configuration lists). A node alone in the Raft configuration that now advertises a loopback address logs `raft configuration records this node at an address other than the one it advertises; harmless while no other node is in the configuration` at info instead: it takes no peers, so no node dials either address.

**Cause.** The address a node's Raft first starts on is recorded in the Raft configuration: at bootstrap on the node that seeds the cluster, by the leader when a node joins. The other nodes dial that recorded address. Restarting the node on a new `cluster.addr` or `cluster.advertise_addr` changes where it listens and what it advertises, not the recorded address. The usual case is a node first started alone on a loopback `cluster.addr` such as `127.0.0.1:7943`, then rebound to an address the others can reach so it could take peers. If nodes join it, then once it is not the leader they cannot reach its Raft: it stays leaderless and not ready, and a node whose `cluster.addr` is port-only (`:7943`) that dials the loopback address reaches its own Raft and steps down, so metadata writes can stall across the cluster. With `other_servers=0`, no other node is in the configuration yet, so nothing has gone wrong yet.

**Fix.** If `recorded_addr` is another way of writing an address that reaches this node, nothing. If it is an address the other nodes can reach, restart the node on it. A node first started on a loopback address cannot be fixed by rebinding, because no other node can reach a loopback address: do not let any node join it, and to grow it, start a new cluster whose first node starts on an address the others can reach and move the workload to it (recreate topics, users and grants there, point producers at it, and retire this node once its consumers have drained it). If nodes have already joined it, move the workload the same way while it is still the Raft leader. A node with `cluster.peers` set re-registers the address it advertises: about 15 s after it finds no leader it runs the [join loop](../understand/cluster-lifecycle.md), and the leader updates its recorded address, so for such a node (one re-addressed host of a cluster, say) the line clears once the leader can reach the new address. Only a node with no peers configured keeps its recorded address for good.

### `node RPC plane is unauthenticated` {#log-node-rpc-unauthenticated}

**New in v3.1.0.**

The full line is `node RPC plane is unauthenticated: security is disabled and no cluster secret is set, so anything that can send UDP to the API port can create users and topics and produce, consume and ack without credentials`, logged once at startup at warning level with `component=audit` and `addr`.

**Cause.** The node runs with `security.enabled=false` and no `NARAD_CLUSTER_SECRET`. That mode leaves the API open too; this line names the node-to-node port, which listens even on a single node.

**Fix.** Fence 7942/udp so only the cluster's own nodes reach it, or set `NARAD_CLUSTER_SECRET` on every node, which authenticates the port even with security off. With security on, a node never serves the port without a secret ([Networking and security](../understand/networking-and-security.md#cluster-secret)).

### `consumer frontier fell behind retention` {#log-frontier-behind-retention}

The full line is `consumer frontier fell behind retention; skipped to oldest retained offset`, at warning level, with `topic`, `partition`, `from`, `to` and `skipped`.

**Cause.** [Retention](../reference/glossary.md#retention) deleted messages that no consumer had acked. Consumers were too slow, stopped, or kept failing on the message at the head. The `skipped` messages are gone.

**Check.** `narad_consumer_dropped_messages` and `narad_oldest_unconsumed_message_age_seconds` against the topic's `retention_ms`.

**Fix.** Give the topic a longer retention, add consumers, or fix the handler that keeps failing. What retention promises: [Delivery contract](../understand/delivery-contract.md#retention).

### `consumer offset commits cannot keep to their interval` {#log-offset-commit-slow}

**New in v3.1.0.**

Logged at warning level, at most once a minute, with `partitions`, `flush_took` and `interval`. A related line, `consumer offsets wait longer than their durability interval for a device flush`, carries `partitions`, `oldest` and `durability_interval`.

**Cause.** Writing acked consumer positions to disk takes longer than its schedule allows. The first line means a crash of the process would deliver again about one write's duration of acks; the second means a power loss would deliver again more acks than `storage.consumer_offset_commit_interval_ms` promises.

**Fix.** Fewer partitions per node, or a faster disk. Both lines are about duplicates after a crash, never loss. The setting: [Configuration reference](../reference/configuration.md#storage).

### `purge deferred: the local metastore still shows the topic incarnation` {#log-purge-deferred}

**New in v3.1.0.**

The full line is `purge deferred: the local metastore still shows the topic incarnation; the leader may ask again, and the startup orphan sweep is the backstop`, at warning level, with `topic` and `incarnation`.

**Cause.** A topic was deleted, and the leader asked this node to remove its files for the deleted [incarnation](../reference/glossary.md#incarnation), but after 5 seconds this node's metadata replica still showed it. Removing the files first could let a request that still sees the topic reopen them, so the node kept them and answered the leader that the purge was deferred. The leader asks again, up to three times in all.

**Check.** Whether this node's replica is behind: `narad cluster members` and the node's Raft metrics.

**Fix.** None, if the error line [below](#log-purge-unfinished) does not follow. If it does, see there.

### `topic purge unfinished on some members` {#log-purge-unfinished}

**New in v3.1.0.**

The full line is `topic purge unfinished on some members; their copies stay until their startup orphan sweep reclaims them`, at error level on the node that ran the delete (the Raft leader), with `topic`, `incarnation`, `members` and `err`.

**Cause.** After a topic delete, the leader asks every live member to remove its files of the deleted incarnation, detached from the client's request and under its own time budget of about 17 seconds. The members listed did not: one could not be reached, its replica did not apply the delete in time (each such member is asked up to three times), or it refused. The delete itself stands; the topic is gone for every client.

**Check.** `err` names each member and why. `narad cluster members` for the members' state.

**Fix.** The copies take disk space but are never served: a recreated topic of the same name is a different incarnation. Each listed member removes them at its next start (the startup orphan sweep). To reclaim the space sooner, restart the listed members one at a time.

### `orphan assignment row for <topic>/<partition>` {#log-orphan-assignment-row}

**New in v3.1.0.**

The full line is `orphan assignment row for <topic>/<partition>; it is pruned once every member runs 3.1.0`, at error level on the Raft leader, with `topic`, `partition` and `owner`, once per row.

**Cause.** The metadata holds an owner for a partition that does not exist: its topic was deleted, or the index is past the topic's partition count. A placement pass of a release before 3.1.0 could write such rows after a topic delete, and a topic created again under the name used to inherit them, owners and all. The leader prunes these rows with a Raft entry type that only this release applies, so it waits until every member, dead members and Raft servers without a member record included, runs it ([Raft entry types](../understand/metastore-and-raft.md#new-entry-types)).

**Check.** The leader's `metastore: not using a new raft entry type yet` line, at info, at most once a minute while the rows stay: its `reason` names the member holding the prune back and the build it last reported (`unknown build` for a v3.0.x member), or says the Raft server has no member record yet. `narad cluster members` does not show a member's release; use it only to see whether that member is `dead`.

**Fix.** Finish the upgrade, or remove the member that will not come back. The leader then prunes the rows within about a minute and logs `controller: pruned assignment rows that belonged to no partition`. No data moves: a row like this names no partition. Until then, a topic created again under that name takes over the rows' owners for the partitions they cover, so avoid recreating it before the upgrade completes.

### `move: set aside stale incarnation directory` {#log-stale-incarnation}

Logged at warning level with `topic` and `err`.

**Cause.** The node holds a directory left by a deleted and recreated topic of the same name (an older [incarnation](../reference/glossary.md#incarnation)), and it failed to rename it aside. Narad never serves such a directory; it renames it to `topics/<name>.stale-<id>` and removes it once the leader confirms that incarnation is gone.

**Check.** The `err` field, usually a permission or disk problem in the data directory. An `err` with `asked to prepare incarnation <id>, the local record now names <other>` (or a missing record) means this node's replica has not applied the latest delete or recreate of the name yet: the node prepares a topic directory only for the incarnation its own record still names, so it never sets a live successor's directory aside. A move onto the node logs `move: prepare topic directory for the incarnation; will retry` with the same `err` once per move, then at debug level.

**Fix.** Fix what `err` names. The node retries on its next pass; a refusal for a record that changed clears once the replica catches up, or once a re-plan cancels the move.

### `reclaim: local partition copy is AHEAD` {#log-partition-quarantined}

The full line is `reclaim: local partition copy is AHEAD of the position it was promoted at elsewhere; quarantined instead of deleted, records after the promoted hwm exist only here`, at error level, with `topic`, `partition`, `owner`, `local_next_offset`, `promoted_hwm` and `quarantine_dir`.

**Cause.** A partition moved away from this node while the node was cut off, and the destination [force-promoted](../reference/glossary.md#force-promote) its copy. This node kept taking records meanwhile, so its copy holds records the new owner never received. Instead of deleting it, the node renamed it to `quarantine_dir`.

**Fix.** Narad never serves or deletes that directory; it goes only when the topic is deleted. The records from `promoted_hwm` onwards exist only there, and Narad has no tool to merge them back. Decide whether they matter, copy the directory off if they do, and delete it when you are done. The move protocol: [Rebalance and decommission](../understand/rebalance.md).

### `reclaim: the new owner cannot vouch for the local partition copy` {#log-partition-set-aside}

**New in v3.1.0.**

The full line is `reclaim: the new owner cannot vouch for the local partition copy; quarantined instead of deleted, its records may exist only here`, at error level, with `topic`, `partition`, `owner`, `reason` and `quarantine_dir`. The sweep that triggered it logs `move: stale partition copy QUARANTINED, not deleted: the new owner cannot vouch for it; operator action required` next to it.

**Cause.** A partition moved away from this node, and when this node went to delete its old copy, the new owner held less than the move gave it. `reason` says which: the new owner lists no records (it came back on an empty volume, or rolled its install back), it holds records without a move marker (so they did not come from this copy), its move marker records a move from another node (the partition moved on again before this node's sweep ran, so the marker vouches for that node's records, not this copy's), or it lacks a segment, or holds one shorter, below the position it vouches for (a segment lost before it reached its disk). A node holding an install that never flipped (copied from the owner, see [Rebalance](../understand/rebalance.md#target-failure)) sets it aside the same way: the owner's move marker names how the owner got the partition, not this copy, so the owner cannot vouch for it. Its records are usually also on the owner; check before deleting it. The records of this copy may exist nowhere else, so the node renamed it to `quarantine_dir` instead of deleting it.

**Check.** The new owner's partition directory (`topics/<topic>/p<NNNNN>` under its data directory: the partition number zero-padded to 5 digits, such as `p00003` for partition 3) and whether it has a `move.marker`; `narad server report` for its high watermark.

**Fix.** Copy the quarantined directory off before anything else: its records may be the only ones left. Narad never serves or deletes it; it goes only when the topic is deleted. Narad has no tool to merge it back; decide whether its records matter, re-produce them from the copy if they do, and delete it when you are done. The sweep's rules: [Rebalance and decommission](../understand/rebalance.md#what-if-the-source-dies-mid-move).

### `move: the partition's path held an earlier copy with unexpired records` {#log-move-install-set-aside}

**New in v3.1.0.**

The full line is `move: the partition's path held an earlier copy with unexpired records; quarantined instead of replaced, since it may hold records the incoming copy lacks. Operator action required`, at error level, with `topic`, `partition` and `quarantine_dir`.

**Cause.** A partition moved onto this node while its path (`topics/<topic>/p<NNNNN>`, the partition number zero-padded to 5 digits) still held an older copy, usually this node's own copy from when it owned the partition, which its stale-copy sweep had not judged yet, and that copy held unexpired records. The install never deletes such a copy, because it cannot prove the incoming copy holds the same records: this node may have kept committing past a force-promote while it was cut off, or the new owner may have lost records since. Often the incoming copy does hold them all; the node set the old copy aside anyway, renamed to `quarantine_dir`, and installed the incoming one. The earlier copy can also be an earlier attempt's install of the same move, left when the node restarted or its worker was cancelled with the flip pending; it holds the source's records, so check the owner before treating them as the only copy.

**Fix.** As for [the sweep's set-aside](#log-partition-set-aside): copy the quarantined directory off first, since its records may be the only ones left. Narad never serves or deletes it; it goes only when the topic is deleted. Decide whether its records matter, re-produce them from the copy if they do, and delete it when you are done.

### `move: set aside the staging copy of a partition this node owns` {#log-move-keeping-staging}

**New in v3.1.0.**

The full line is `move: set aside the staging copy of a partition this node owns; the partition's records may not all be under its path. Operator action required`, at error level, with `topic`, `partition`, `quarantine_dir`, `partition_dir`, `moved_back` and `partition_dir_has_records`. A move that took its installed copy back off the partition's path and then cannot read the partition's owner logs `move: set aside the staging copy this move moved back, since the partition's owner cannot be read; it may hold the partition's records. Operator action required` the same way, with `quarantine_dir`, `partition_dir` and `err`.

**Cause.** A move to this node ended without seeing its own flip commit, yet this node owns the partition: a flip committed after all. The move's copy was in `dataDir/.moves/<topic>-<partition>` (the partition number not padded, such as `.moves/orders-3`); the node renamed it aside to `quarantine_dir` (such as `.moves/orders-3.quarantine`), where no later move of the partition onto this node clears it, and `partition_dir` is the partition's path (`topics/<topic>/p<NNNNN>`, the partition number zero-padded to 5 digits, such as `topics/orders/p00003`).

- `moved_back=true`: the leader read the flip as not committed, the move took its installed copy off the partition's path, and the flip committed anyway. `quarantine_dir` holds the partition's records as of the flip; `partition_dir` holds only what this node wrote since.
- `moved_back=false`: no copy installed from the move's source is under the partition's path, so `quarantine_dir` may hold records the path lacks.

When the path does hold a copy installed from the move's source and the move moved nothing back, the flip was an earlier attempt's (a restart cancelled that worker with its flip pending) and `staging` only holds a later attempt's re-copy: the node removes it and logs `move: the partition flipped to this node under an earlier attempt's install; removing this attempt's staging copy` at info instead.

**Check.** `partition_dir_has_records`, and the segment files in both directories (each file is named for the offset it starts at).

**Fix.** Stop the node and copy both directories off first. If `partition_dir_has_records=false`, move the set-aside directory into place as `partition_dir` (`.moves/orders-3.quarantine` goes to `topics/orders/p00003`) and start the node. If it is `true`, both copies can hold records the other lacks, at overlapping offsets: do not replace the live partition with the set-aside copy; compare the two, start the node, and re-produce from the set-aside copy the records you decide matter. Narad does not move or delete either directory on its own.

### `member heartbeat failing` {#log-member-heartbeat-failing}

**New in v3.1.0.**

Logged at warning level with `member`, `failures`, `since`, `failing_for` and `err`, once the node's member heartbeat has failed 3 times in a row over at least 10 seconds (two heartbeat intervals), then at most once a minute while it keeps failing. `member heartbeat recovered` at info level ends it. `narad_member_heartbeat_failures` counts the failures in a row, and every single failure is still logged at debug level as `member heartbeat failed`.

**Cause.** The node cannot register its membership with the Raft leader, and the leader marks a member dead after 30 seconds without one ([A node is down](#node-down) says what clients see then). `err` says why: `leader member address unavailable` means the node knows no leader (quorum is lost, or the node is cut off from Raft); a timeout or a refused stream means the leader does not answer on the node-RPC port or refuses the node's cluster secret.

**Check.** `err` in the line, `/readyz` on the node ([Start with readiness](#check-readiness)), `narad cluster members`, the node-RPC port (7942/udp) between the node and the leader, and that every node has the same cluster secret ([`cluster stream rejected: invalid auth`](#log-stream-invalid-auth)).

**Fix.** Restore the path to the leader. The next heartbeat that reaches the leader registers the node again, and a node marked dead is alive again.

A decommissioned node logs `member heartbeat refused` at error level instead, once: the leader removed it, and it can never register again. Scale it away, or delete its volume to rejoin as a new node ([`cluster join refused: this node was decommissioned`](#log-join-rejected)).

### `raft TLS certificate has expired` {#log-raft-tls-expired}

**New in v3.1.0.**

Logged at error level with `kind` (`leaf` for the node's certificate, `ca` for the earliest-expiring CA in its bundle, whose lines say `raft TLS CA certificate`), `not_after` and `expired_for`, at once and then every 24 hours. It follows warnings 30 and 7 days ahead and an error 1 day ahead (`raft TLS certificate expires in less than ...`). `/readyz` keeps answering `200` and lists `raft_tls_certificate_expired` or `raft_tls_ca_expired` under `degraded`.

**Cause.** The Raft certificate, or the CA it chains to, ran out. Narad reads the files only at startup, so a certificate renewed in the Secret is not used until the pod restarts. Peers refuse the new Raft connections the node opens or accepts; connections opened before the expiry carry on until they break, so the cluster can keep a leader for a while and then lose it.

**Check.** `narad_raft_tls_cert_not_after_seconds` on every node, and the date in the Secret:

```bash
kubectl get secret narad-cluster-tls -n narad -o jsonpath='{.data.tls\.crt}' \
  | base64 -d | openssl x509 -noout -enddate
```

**Fix.** Issue a new certificate (and CA, if it is the CA that expired) and replace the Secret ([Create the certificates](raft-tls.md#create-certificates)). Then restart the pods. If the certificate has expired on every node, a pod restarted with the new one and its peers on the old one refuse each other, so `kubectl rollout restart` stops at its first pod: delete the next pods down by hand until a majority runs the new certificate, as in step 3 of [Enable on a running cluster](raft-tls.md#enable-running-cluster). Renewing before the expiry avoids this: then a plain [rolling restart](raft-tls.md#renew) works.

### `move: staged copy cannot be verified; not freezing the source again` {#log-move-unverifiable}

**New in v3.1.0.**

Logged at error level on a move's destination, with `topic`, `partition`, `source`, `attempts`, `action` and `err`. The move counts in `narad_moves_blocked{reason="copy_unverifiable"}`.

**Cause.** The destination drained the partition under the source's freeze, and the staged copy did not verify as the partition at the source's high watermark: it recovered short of it, past it (`a frame straddles the high watermark`), or not at all. The destination threw the copy away and copied the partition once more from scratch (logged at warning as `move: copying the partition again from scratch`), and that copy failed too. Draining again would fail the same way, so the destination stopped. It no longer freezes the source; the source's last freeze lapses within 30 s, and the partition keeps serving from the source, which still owns it.

**Check.** `err`, and the source's log and partition directory: the source serves the partition, so a copy that recovers differently from it points at a storage problem on either node.

**Fix.** The move stays in flight until it is aborted (`narad cluster moves abort <topic> <partition>`) or re-planned, or this node restarts, which tries once more. Nothing was deleted: the source keeps its copy.

### `move: the source is dead and this node's copy` {#log-move-dead-source-behind}

**New in v3.1.0.**

The full line is `move: the source is dead and this node's copy is behind its last high watermark, so it cannot force-promote: promoting would lose records the source made visible. Waiting for the source to return; abort the move to give up on it`, at error level, with `topic`, `partition`, `source`, `copy_next_offset`, `source_last_hwm` and `err`. A copy that reaches the high watermark but fails verification logs `move: the source is dead and this node's copy fails verification, so it cannot force-promote` instead. Either is logged once each time the source dies, then at debug level; v3.0.1 logged `move: source dead but copy is behind its last hwm` at warning every 2 s. The move counts in `narad_moves_blocked` until the source reads alive again.

**Cause.** The move's source died before the destination's copy caught up with it. Records from `copy_next_offset` to `source_last_hwm` exist only on the source's disk. The destination keeps waiting, because a force-promote would drop them.

**Fix.** Bring the source back: the move resumes and completes. If the source is gone for good, the records past `copy_next_offset` are gone with it; aborting the move (`narad cluster moves abort <topic> <partition>`) leaves the partition with its dead owner, and the destination sets its partial copy aside ([below](#log-move-dead-source-staging)).

### `move: set aside the staging copy of a move that ended while its source is dead` {#log-move-dead-source-staging}

**New in v3.1.0.**

The full line is `move: set aside the staging copy of a move that ended while its source is dead; it may be the only copy of the partition's records. Operator action required`, at error level, with `topic`, `partition`, `quarantine_dir`, `owner`, `owner_state` and `target`.

**Cause.** A move to this node ended without a flip (it was aborted or re-planned, or the node shut down) while the partition's owner read dead (`owner_state` is `dead`, `no member record`, or why the record could not be read), and the move's staging copy held records. Those may be the only copy of the partition's records left, so the node renamed the copy to `quarantine_dir` (`.moves/<topic>-<N>.quarantine`) instead of deleting it. It is counted in `narad_quarantined_copies`.

**Fix.** If the owner comes back with its disk, its copy is the partition and the set-aside one can be deleted once you have checked. If it is gone for good, copy `quarantine_dir` off before anything else: Narad never serves it and never deletes it. Its records stop at the copy's last segment; re-produce the ones you need.

### `controller: refusing to mark voters dead` {#log-dead-marking-refused}

**New in v3.1.0.**

The full line is `controller: refusing to mark voters dead: the verdict would leave fewer alive voters than a Raft quorum, which a leader holding its lease rules out; check the cluster RPC plane (port, secret, certificates) into this leader`, at error level on the leader, with `refused`, `alive_voters_after`, `quorum` and `voters`. It is logged once per refusing streak, `narad_dead_marking_refused` is 1 while it lasts, and `controller: dead-marking breaker cleared` follows at info when the heartbeats are back.

**Cause.** The leader has not seen heartbeats from enough voters to keep a quorum alive, yet it still holds its Raft lease, so those voters still answer it over Raft. The fault is most likely on the path the heartbeats take into this leader: the node RPC port (UDP) blocked, a cluster secret that differs, or a wedged listener. The leader refuses the verdict for the voters in it, so routing keeps treating them as alive; members that are not voters are still marked dead as usual.

**Check.** Whether the leader's node RPC port (the API port, over UDP) is reachable from the listed voters, and whether they use the leader's cluster secret. At debug level the voters log `member heartbeat failed` with the error.

**Fix.** Restore the node RPC plane into the leader. While it is broken, forwarded requests to the refused voters fail instead of being rerouted, which is the safer of the two. If the voters really are down, this node cannot keep leading, and the next leader marks them.

### `x509: certificate signed by unknown authority` {#raft-cert-untrusted}

The Raft leader logs `raft: failed to heartbeat to: peer=<addr>` with `error="tls: failed to verify certificate: x509: certificate signed by unknown authority"`, and the node it names logs `failed to decode incoming command: error="remote error: tls: bad certificate"`.

**Cause.** The node's Raft certificate is signed by a CA its peers do not trust. It cannot join and stays not ready, and its partitions are unavailable.

**Fix.** Give the node a certificate from the trusted CA and restart it. To change the CA without this, follow the three rolls in [Rotate the CA](raft-tls.md#rotate-ca). Details: [Untrusted certificate](raft-tls.md#untrusted-cert).

### `this node's metastore holds remotes, so it must run with security.enabled` at start {#log-remotes-security-off}

**Unreleased.**

`narad serve` exits at start with this message, or with `this node's metastore holds remotes, so NARAD_CLUSTER_SECRET is required`.

**Cause.** The cluster holds a remote, whose password is sealed under the cluster secret, and this node runs with security off or without a secret. A node without API authorization must not hold outbound credentials.

**Fix.** Start the node with security on and the cluster's `NARAD_CLUSTER_SECRET`, as every other member.

### `this node holds remotes and its Raft transport runs without TLS` {#log-remotes-plaintext-raft}

**Unreleased.**

A warning at startup, or when the first remote appears, with `narad_remotes_plaintext_raft` 1.

**Cause.** The node runs Raft without TLS. Remote passwords cross Raft only as ciphertext, but whoever can reach the Raft port can delete remotes and stall links.

**Fix.** Turn on [Raft TLS](raft-tls.md). It is not required; the warning repeats on every start until it is on.

### `this node holds remotes and remotes.allowed_hosts is empty` {#log-remotes-allowlist}

**Unreleased.**

A warning at startup, with `narad_remotes_allowlist_configured` 0.

**Cause.** Remotes may point at any host the address guard allows.

**Fix.** Set `remotes.allowed_hosts` to the hosts your remotes use ([operating condition 4](remotes.md#operating-conditions)), then restart the node.

### `this node holds remotes and its cluster secret fails the strength rule` {#log-remotes-weak-secret}

**Unreleased.**

An error at startup. The node runs, but every create, password change and re-encrypt answers `412` until the secret is replaced.

**Cause.** The cluster secret is not at least 32 random bytes as `openssl rand -base64 32` (padded standard base64) or `openssl rand -hex 32` prints them, or it looks like a passphrase: base64 with no digit, `+` or `/` or with letters of one case only, or hex with no digit or no letter. A random secret has that shape about once in ten thousand; generate another.

**Fix.** Rotate to a secret made with `openssl rand -base64 32`, with the old one as `NARAD_CLUSTER_SECRET_PREVIOUS`, then `narad remote reencrypt` ([Rotate the cluster secret](remotes.md#rotate-cluster-secret)).

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

---
description: "Move a cluster to a new Narad release with a rolling restart, and roll it back safely if you need to."
---

# Upgrade Narad

Move a cluster to a new Narad release with a rolling restart, and roll it back safely if you need to.

Before you start: every pod ready, Helm access to the release, and the [changelog](https://github.com/DebanganThakuria/narad/blob/master/CHANGELOG.md) entries for every release between yours and the target.

## Upgrade {#upgrade}

1. Get the chart of the target release. In a clone made as in [Deploy on Kubernetes](deploy-kubernetes.md#install):

    ```bash
    git fetch --depth 1 origin tag v3.2.1
    git checkout v3.2.1
    ```

2. Read the [version notes](#version-notes) for every release you cross.

3. Upgrade, pinning the new image tag:

    ```bash
    helm upgrade narad ./charts/narad -n narad \
      --reset-then-reuse-values \
      --set image.tag=v3.2.1
    kubectl rollout status statefulset/narad -n narad
    ```

    `--reset-then-reuse-values` (Helm 3.14 or later) keeps your settings and gives any value the new chart adds its default. If you keep a values file, change `image.tag` there and pass it with `-f` instead.

The StatefulSet restarts one pod at a time, from the highest-numbered pod down, and waits for each to be ready before it restarts the next. A leader that stops hands leadership to another node first and logs `leadership transferred before shutdown`. While a pod restarts, the other nodes keep accepting produces. Records meant for the restarting pod's partitions wait in the accepting node's ingress WAL, and go to other partitions of the topic if the pod stays away for more than a few seconds. Consumers wait for that pod's partitions, and some messages can be delivered twice. The [failure matrix](../understand/delivery-contract.md#failure-matrix) has the details for a rolling restart.

What is stable across releases, and what the docs describe, is in [API stability and versions](../reference/api-stability.md).

## Roll back {#roll-back}

A rollback is the same upgrade with the older tag, using the chart of the release you go back to:

```bash
git fetch --depth 1 origin tag vX.Y.Z
git checkout vX.Y.Z
helm upgrade narad ./charts/narad -n narad \
  --reset-then-reuse-values \
  --set image.tag=vX.Y.Z
kubectl rollout status statefulset/narad -n narad
```

- `vX.Y.Z` is the release you go back to, in all three places.
- Do not pick v3.0.0: a v3.0.0 node that has been up for more than 24 hours cannot be reached by its peers ([version notes](#version-notes)).

Prefer this to `helm rollback`. A `helm rollback` to a revision with a smaller `replicaCount` is a scale-in that skips the decommission ([Scale out and in](scaling.md#scale-in)). `helm rollback` renders no templates, so only a hook can stop it: the scale-in guard (from v3.1.0), a pre-rollback hook Job of the revision you roll back to, refuses while a pod being deleted is still a cluster member. A revision rendered by an older chart, v3.0.1's included, has no guard and refuses nothing.

### Leave a build newer than v3.0.1 {#roll-back-newer}

Releases from v3.1.0 on write some data and accept some settings that v3.0.1 and earlier do not understand. Before a node goes back to v3.0.1 or earlier:

1. **Drain the ingress WAL.** Newer builds write accepted produces in a record format older binaries cannot read; a rolled-back node stops delivering from the first such record it still holds, while it keeps answering `202`. Pause producers, keep every partition owner up, and wait until `narad_ingress_dispatch_backlog_records` is 0 on every node, in a sample taken after the pause (the gauge refreshes every 5 seconds). Then roll back and resume.
2. **Turn off segment preparation, if you turned it on.** Remove `storage.ingress_wal_prealloc` from `narad.config`, restart every pod once on the newer image (`kubectl rollout restart statefulset/narad -n narad`), then roll back. Skipping this can leave an older binary unable to start after a crash, with a `corrupt frame` error.
3. **Remove the new config keys.** Take `storage.consumer_offset_commit_interval_ms`, `storage.ingress_wal_prealloc` and `http.max_produce_in_flight_per_identity` out of `narad.config`, even where they hold their defaults. An older binary with one of them in its config file stops at startup with `storage.ingress_wal_prealloc is an internal setting and cannot be configured`, or `json: unknown field "max_produce_in_flight_per_identity"`, before it opens any data. The environment variable `NARAD_HTTP_MAX_PRODUCE_IN_FLIGHT_PER_IDENTITY` can stay.
4. **Move clients off the batch forms.** An older node answers a batch produce `404`, ignores `max` on a consume and returns one message in the single shape, and answers a batch ack `400` (`receipt_handle required`).
5. **Set `GOMEMLIMIT` if you relied on the derived one.** Older binaries do not set it from the pod's memory limit; add it to `extraEnv`.
6. **Stop every pod cleanly, and restart a crashed one on the newer image first.** Give pods a `terminationGracePeriodSeconds` long enough to finish their shutdown: after the HTTP drain (up to `http.shutdown_grace`, 10 s by default), a clean stop syncs the consumer offsets of every partition acked since their last sync, then writes and syncs the `hwm` file of every partition written to since it was opened, one partition after another. Measured on local disk under Linux (in a container), that is about 0.1 to 0.4 ms and 0.6 to 1.6 ms per partition, so the chart's default 30 s covers roughly 10,000 such partitions; a network volume syncs slower, so measure on yours. A pod that crashed, or was killed at the end of its grace period, can leave a partition's `hwm` file empty, and an older binary handles an empty file badly in two ways:
    - It reads the file as "take the record tail" when it opens the partition's log, but as a boundary of 0 when it reads a closed partition, and rewrites the file only at its first commit there. On v3.0.1 and earlier the transfer listing is one of those readers. If the rolled-back node is the source of a move that [force-promotes](../understand/rebalance.md#what-if-the-source-dies-mid-move) (the source dies after a catch-up listing and stays dead past `ForcePromoteAfter`), the new owner can get a copy that hides every record until its next commit and then redelivers the whole partition, or a partial copy. The returning source quarantines its own copy, so those records can be recovered by hand. Live moves and decommissions are not affected: their freeze opens the log.
    - It takes the boundary from the record tail without syncing the segment first. A power loss before its first commit on the partition can take back records it already served, and once their acks persist, their offsets are reused and records committed there later can be skipped.

    So after any unclean stop, start the pod once on the newer image and stop it cleanly before you roll it back: startup opens every partition the pod owns, and a clean stop writes each one's exact boundary.
7. **Roll back only while a member still runs v3.0.x.** A release that adds a Raft entry type uses it only once every member reports a release that knows it ([Raft entry types and upgrades](../understand/metastore-and-raft.md#entry-types)). That moment is the rollback boundary: the last member to upgrade starts on this release and its first heartbeat reaches the leader. Before it, a rollback is safe: the member still on v3.0.x holds every new type back while you roll the others back. After it, a rollback to v3.0.x is unsupported, even if the leader has not used a new type yet. A node you stop to roll it back keeps counting as this release for as long as it is down, because its member record holds the last report it sent, so the leader can use a new type in that time: the dead mark it writes for that node after 30 s of silence is one, and so is a topic create. v3.0.x skips such an entry silently, so a topic created in that time can be missing on that node, and on the others once they are rolled back too; later releases stop applying instead ([`metastore: stopped applying raft entries`](troubleshooting.md#log-metastore-stopped)). To see where the roll stands, check the image every pod runs (`kubectl get pods -n narad -o custom-columns=NAME:.metadata.name,IMAGE:.spec.containers[0].image`) or the `version` on each pod's `narad serve starting` line; `narad cluster members` does not show a member's release. While a member holds the new types back, the leader names it and its build in the `reason` of `metastore: not using a new raft entry type yet`, logged whenever a write would have used one. Neither that line's absence nor the absence of the first-use line (`metastore: every member applies raft entry type`) makes a rollback safe once every pod runs this release. A node that rolls back and also loses its volume is refused when it rejoins a cluster whose every member runs the newer release ([`409`, `older_release`](troubleshooting.md#log-join-older-release)). v3.1.0 adds entry types 23 to 32 ([the list](../understand/metastore-and-raft.md#new-entry-types)).

Partition segments, consumer position files and fan-out cursor files need nothing: each binary reads what the other wrote. Neither does `fsm.db`: v3.0.1 ignores the `fsm_meta` bucket newer builds add, and a newer build that finds the file written by v3.0.1 rebuilds it or restores a snapshot ([Restarts](../understand/metastore-and-raft.md#restarts)). An empty `hwm` file left by a crash hides no acked record from an older binary once it opens the log; step 6 covers what such a file costs on a closed partition. One gap stays open: a message stored without a key and fanned out to a [replica child](../reference/glossary.md#replica-child) after the rollback is placed round-robin, so both copies can land on one node. The changelog's "Upgrade and rollback notes" have every detail.

### Leave a build newer than v3.1.0 {#roll-back-from-v3-2}

**New in v3.2.0.**

1. **Never after a remote write.** v3.2.0 adds Raft entry types 33 to 37 for [remotes](remotes.md). Once a remote has been created or a remote child attached, a rollback to v3.1.0 or earlier is unsupported, even after every remote and remote child is deleted: v3.1.0 stops applying at the first such entry, and refuses at startup a database that applied one ([`written by a newer Narad release`](troubleshooting.md#log-metastore-newer-database)). Before the first remote write a rollback is fine, but make no remote write while any node is being rolled back: a stopped node keeps counting as v3.2.0 (step 7 above), so the leader would accept the write, and the node would stop applying when it starts on v3.1.0.
2. **Remove the new config keys.** v3.1.0 refuses to start with `http.max_batch_body_bytes_in_flight` or any `remotes.*` key in `narad.config`. The chart passes its `remotes` values, and the `cluster-secret-previous` key, as environment variables, which v3.1.0 ignores.
3. **Move producers back to v3.1.0's batch limits.** v3.1.0 answers a batch of more than 100 messages `400`, a body over 1 MiB `413`, and a compressed body `400`.

## Version notes {#version-notes}

Read the notes for each release boundary you cross, in either direction.

- **To v3.2.1.** Nothing to do in either direction: v3.2.1 changes no stored format, config key or API, so it rolls onto, and back to, v3.2.0 node by node.
- **To v3.2.0.**
    - *Remotes and remote children need every member upgraded.* No action during the roll. Remote writes and remote child attaches answer `412`, naming the member, until every member, dead ones and Raft servers without a member record included, runs this release with security on and legacy cluster authentication off ([Manage remotes](remotes.md#before-you-start)). The rollback boundary for their Raft entry types is the first remote write ([above](#roll-back-from-v3-2)).
    - *Batch produce takes more.* A node on this release takes up to 1,000 messages in a body of up to 16 MiB, each payload at most 1 MiB, optionally zstd or gzip compressed, and answers a body over 1 MiB `503` when its budget for large bodies is full (`http.max_batch_body_bytes_in_flight`). Until every node a client can reach runs this release, keep batches within v3.1.0's limits.
    - *New refusals clients can see.* `409` for a produce, consume or ack on a remote child's stub, for a change to a stub, for a retention below 24 hours on a parent with remote children, and for a delete or detach while a remote child's records are unshipped; `412` and `429` on the remote routes ([Status codes](../reference/status-codes.md#status-412)).
    - *Deletes during the roll.* A delete or detach that involves a remote child travels to the leader as a remote write; while the leader still runs v3.1.0 there are no remote children to involve, and every other delete keeps v3.1.0's path.

- **To v3.1.0.**
    - *Single nodes with security on close their node RPC plane to other processes.* No action. A node with no peers and no `NARAD_CLUSTER_SECRET` generates a secret for the life of the process, so only it can use its node-to-node port ([Networking and security](../understand/networking-and-security.md#cluster-secret)). Before you grow such a node into a cluster, set the same `NARAD_CLUSTER_SECRET` on it and restart it; a joiner cannot authenticate to it otherwise. It also needs a `cluster.addr` the other nodes can reach, and either the Raft TLS files on every node or `NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT=true` with 7943/tcp fenced: a node with no peers configured never runs the join loop, so one the others cannot reach over Raft stays cut off from their Raft. A node first started on a loopback `cluster.addr` cannot be grown this way: the Raft configuration keeps the address its Raft first started on, and a later `cluster.addr` does not change it. Start a new cluster whose first node starts on an address the others can reach, and move the workload to it ([Networking and security](../understand/networking-and-security.md#raft-tls)). Nothing is persisted, so a rollback needs no step. With security off the plane stays open, and startup now says so in a warning.
    - *Secured single nodes need Raft TLS off loopback.* A node with security on and no peers, whose `cluster.addr` is not a loopback address, now refuses to start without the Raft TLS files or `security.allow_plaintext_raft`, as a node with peers already did. Before upgrading such a node, set the TLS files on every node of the cluster, bind `NARAD_CLUSTER_ADDR` to `127.0.0.1:7943` only on a node that will never take peers (a node whose Raft first starts on loopback keeps that address in the Raft configuration, so it cannot be grown by rebinding it later), or set `NARAD_SECURITY_ALLOW_PLAINTEXT_RAFT=true` once 7943/tcp is fenced ([Networking and security](../understand/networking-and-security.md#raft-tls)). The quickstart's `docker run`, `--dev` and Helm installs are unaffected.
    - *Metadata restarts.* No action. Each node now records in `fsm.db` which Raft entries it holds, and a restart applies only the ones it lacks. The first restart on this release of a node with no Raft snapshot yet (fewer than `cluster.raft_snapshot_threshold` metadata changes, 8192 by default, since the cluster started) rebuilds `fsm.db` from the Raft log and keeps the old file as `fsm.db.stale`: delete it once the pod is ready ([Troubleshooting](troubleshooting.md#log-metastore-set-aside)). A node rolled back to v3.0.1 and upgraded again does the same, or restores its snapshot.
    - *A node whose metastore cannot write stops.* A full data volume or an I/O error on a metadata write now stops the node after 30 s of retries, and it restarts in a loop until the volume is fixed ([Troubleshooting](troubleshooting.md#log-metastore-stopped)); v3.0.1 dropped the change on that node and stayed ready. Keep free space on the data volume and alert on it: pods that share a full volume all stop.
    - *Heartbeats and joins report the release.* No action. Each node's heartbeat now carries its build and the newest Raft entry type it applies, and a join request carries the latter, so the leader can hold a new entry type back until every member knows it ([Raft entry types and upgrades](../understand/metastore-and-raft.md#entry-types)). A v3.0.x node refuses those requests with a `400`, and the upgraded node resends them at once without the new fields, so no heartbeat is missed. A node that applies fewer Raft entry types than every member is refused when it joins (`409`, code `older_release`): upgrade it to the cluster's release ([Troubleshooting](troubleshooting.md#log-join-older-release)). For rollbacks, see step 7 of [Leave a build newer than v3.0.1](#roll-back-newer).
    - *New Raft entry types.* No action during the roll, but once the last pod starts on this release a rollback to v3.0.x is unsupported ([step 7 above](#roll-back-newer)). Once every member, dead members and Raft servers without a member record included, reports this release, the leader starts using the entry types 23 to 32 for topic creates, topic changes, placement, the orphan assignment row prune, dead marks and user deletes, and logs `metastore: every member applies raft entry type <n>; using <name>` at info the first time it uses each. Until then it writes the entries v3.0.x applies and, whenever a write would have used a new type, logs `metastore: not using a new raft entry type yet; proposing the entries every member applies` at info, at most once a minute per type, naming the member holding it back and its build in `reason`. The first-use line is not the rollback boundary: the boundary is the moment every member has reported this release, and a rollback after it is unsupported even if no new type has been used yet, because a node stopped for a rollback still counts as this release while it is down. Clients can see one new answer: `409` `topic changed since it was read` ([Status codes](../reference/status-codes.md#status-409)).
    - *Partition-move answers change.* No action. `CompleteMove` answers `409` for a refused flip and `404` for a partition with no assignment (was `503`), and `GetAssignment` is answered only by the Raft leader. While the leader still runs an older release, a move whose flip fails waits with its copy installed until the leader is upgraded or the move is re-planned; no data is at risk ([Rebalance](../understand/rebalance.md#flip)). When the move is re-planned, the worker ends and leaves the install at the partition's path, and the destination's stale-copy sweep quarantines it (renamed to `<partition>.quarantine` and logged at error level) instead of reclaiming it. Its records are usually also on the owner it was copied from; check the owner, then decide ([Troubleshooting](troubleshooting.md#log-partition-set-aside)).
    - *The chart fences Raft with its own NetworkPolicy.* `networkPolicy.enabled` defaults to `true` and `security.allowPlaintextRaft` to `false`. An upgrade with `--reset-then-reuse-values`, as in [Upgrade](#upgrade), takes both defaults: the release gains a NetworkPolicy that admits 7943/tcp and 7942/udp only from its own pods and leaves the API and metrics ports open. It needs a CNI that enforces NetworkPolicy; on one that does not, the ports stay as open as before. To upgrade without the policy, pass `networkPolicy.enabled=false` with `security.clusterTLS.enabled=true` or `security.allowPlaintextRaft=true`; with neither, the render fails and names the three fixes ([Production checklist](production-checklist.md#raft-tls)). An upgrade with `--reuse-values` keeps the old `allowPlaintextRaft: true` and renders no policy, as before: check that something fences the port, or pass `networkPolicy.enabled=true`. The nodes themselves need nothing.
    - *Scale-in is approved per size, and checked on rollback.* `allowScaleIn` is no longer read: after decommissioning, set `allowScaleInTo` to the new `replicaCount`. A hook Job, the scale-in guard, runs before every `helm upgrade` and `helm rollback` and refuses one that would delete a pod still listed by `narad cluster members` ([Scale in](scaling.md#scale-in)). On a scale-in it needs the security secret's `admin-password` key to hold root's current password. A rollback to a revision rendered by the v3.0.1 chart runs no guard.
- **From v3.0.0.** Roll every node. A v3.0.0 node that has been up for more than 24 hours cannot be reached by its peers for forwarded produce, consume and ack until it restarts. v3.0.1 fixes this.
- **Across v2.2.0 (node-to-node authentication).** Nodes before and after v2.2.0 cannot talk over the node RPC plane. Roll twice: first with `--set security.allowLegacyClusterAuth=true`, so upgraded pods still speak the old protocol to the pods not yet rolled, then, once every pod runs the new image, with it back at `false`. If you skip the first roll, forwarded requests between old and new pods fail until the roll finishes.
- **Across v2.2.0 (Raft TLS).** From v2.2.0, a secured multi-node cluster refuses to start unless Raft runs over TLS or `allowPlaintextRaft` says the port is fenced. Up to v3.0.1 the chart sets `allowPlaintextRaft: true` by default; from v3.1.0 it says the port is fenced only when its NetworkPolicy or your `allowPlaintextRaft` does (see the v3.1.0 notes above). See the [Production checklist](production-checklist.md#raft-tls).
- **Across v2.2.0 (file modes).** From v2.2.0, Narad creates its data directories with mode `0700` and its files with `0600`. It sets the mode only when it creates a file, so files an older release created keep `0755` and `0644`. On a host where other users can read the data directory, tighten those by hand.
- **Config files.** Every release since v1.0.0 refuses a config file key it does not know, and fails at startup. When you roll back, remove keys the older release does not have first ([Configuration reference](../reference/configuration.md#config-file)).
- **`replicaCount` in a rollback.** Going back to fewer replicas is a scale-in. Decommission first ([Scale out and in](scaling.md#decommission)).

## Next steps

- [Changelog](https://github.com/DebanganThakuria/narad/blob/master/CHANGELOG.md): every change and upgrade note, per release.
- [API stability and versions](../reference/api-stability.md): what can change in `/v1`, and which release these docs describe.
- [Monitor and alert](monitoring.md): watch the cluster through the roll.

---
description: "Learn how Narad keeps its metadata: a Raft-replicated state machine on every node, read locally, and the rules that keep stale replicas from destroying data."
search:
  boost: 0.5
---

# Metastore and Raft

Learn how Narad keeps its metadata: a Raft-replicated state machine on every node, read locally, and the rules that keep stale replicas from destroying data.

The [metastore](../reference/glossary.md#metastore) holds everything that is not a message payload: topics, partition assignments, cluster members, users and grants, schemas, and fan-out links. Every node holds a full replica, kept in step by Raft.

## Metastore shape {#shape}

<figure class="nr-dia nr-dia--doc" id="fig-metastore-write-path">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/metastore-write-path.html"
</div>
<figcaption>Reads never leave the node. A write goes through the leader, and the node that forwarded it answers <code>201</code> only once its own FSM has applied it, so the next read there sees the write.</figcaption>
</figure>

- **Writes** (create a topic, register a member, attach a child) are Raft commands: forwarded to the leader, committed by a quorum, then applied to every node's state machine (FSM). A node that forwarded a write does not answer the client until its own FSM has applied it: it asks the leader for the index it applied and waits, for a bounded time, for its replica to reach it. So a read on the same node right after the response is never behind the write. Each command family bumps a per-domain **version counter**, so caches (such as topic lookups on the produce path) are invalidated precisely.
- **Reads** are local: every node answers topic lookups from its own bbolt replica, with no network hop. This is what makes request routing fast, and it is also the reason for the rules in [Stale replicas](#stale-replicas).
- The FSM persists in **bbolt** (`fsm.db`); Raft keeps its log in boltdb, plus periodic **snapshots** on disk. A restarting node keeps its `fsm.db` and applies only the entries it does not hold yet ([Restarts](#restarts)).

## Metastore contents {#contents}

| Domain | Contents |
|---|---|
| Topics | name, partitions, retention, visibility, caps, schema, role (parent or child), fan-out links with attach epochs and delay |
| Assignments | partition to owner node, plus a move target while a rebalance or decommission moves it |
| Members | node ID, addresses, heartbeat, alive or dead |
| Users | usernames, bcrypt hashes, grants |
| Cluster | Raft voter configuration (managed by Raft itself) |

## Stale replicas {#stale-replicas}

Local reads are fast because they trust the local replica. But **a freshly restarted node's replica is restored from a snapshot that can be hours old**, and until it catches up, that node sees the past: topics that were deleted still exist, and topics created since do not. The node has no local way to know it is stale, since its own bookkeeping says "everything I know is applied."

<figure class="nr-dia nr-dia--doc" id="fig-metastore-log-vs-state">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--lilac">
--8<-- "diagrams/metastore-log-vs-state.html"
</div>
<figcaption>Winning an election proves the Raft log is complete, not that the FSM has applied it. Before acting on its own state, a leader runs a barrier and reads again.</figcaption>
</figure>

Chaos testing proved this is not theoretical: four separate data-loss bugs came from code trusting a stale local view (see [Cluster lifecycle](cluster-lifecycle.md#crash-recovery-bugs)). The defenses are now systematic:

```mermaid
flowchart TD
    accTitle: Deciding whether a node may destroy data
    accDescr: A node that wants to destroy something based on metastore state first asks whether its local replica is provably current. If no leader is known or there was no recent leader contact, it keeps the data and retries later. If it looks current and the node is not the leader, it asks the leader over RPC, and destroys only if the leader confirms the thing is gone. If the node is the leader, it runs a Raft barrier until its state machine has applied every committed entry, then reads its local state again, and destroys only if the thing is still gone. Any other outcome keeps the data.
    Q([Node wants to destroy something based on metastore state]) --> A{Is the local replica<br/>provably current?}
    A -->|"no leader known /<br/>no fresh leader contact"| KEEP0[Keep the data. Retry later]
    A -->|looks current| B{Am I the leader?}
    B -->|no| RPC[Ask the LEADER over RPC]
    RPC -->|leader confirms gone| DEL1[OK to destroy]
    RPC -->|anything else| KEEP1[Keep the data. Retry later]
    B -->|yes| BAR["Raft Barrier: wait until my FSM has applied every committed entry"]
    BAR -->|barrier ok| REREAD[Re-read local state]
    REREAD -->|still gone| DEL2[OK to destroy]
    REREAD -->|reappeared| KEEP2[Keep the data. Retry later]
    BAR -->|failed| KEEP2
```

Three primitives implement this:

- **`AppliedCaughtUp`** answers: "does this node have a leader, has it heard from it *recently*, and has it applied everything it knows to be committed?" The recency requirement matters. Right after a snapshot restore, a node's local indexes agree with each other trivially while being hours stale; only fresh contact with the leader makes the comparison meaningful.
- **Leader confirmation RPC.** Before deleting a topic directory, discarding a WAL record, or resetting a fan-out cursor, the node asks the leader "does this still exist?" Only a definite "no" allows destruction.
- **`Barrier`** is the subtle one. *Winning an election proves a node's Raft log is complete, not that its FSM has applied it.* A just-elected leader restored from an old snapshot legally serves stale reads while replay finishes. So "I am the leader, my state is the authority" is valid only after `raft.Barrier()` has blocked until the FSM is fully applied, and the state must be read again *after* the barrier.

## Snapshots {#snapshots}

**Unreleased:** in master, not in v3.0.1.

Raft compacts its log into a snapshot of the metadata database once 8192 entries have piled up since the last one (checked about every 120 s; both are [configurable](../reference/configuration.md#cluster)), and keeps the newest two. A node that falls too far behind gets the leader's latest snapshot instead of the entries it missed, and a restarting node may restore its own.

Neither direction holds the database in memory. To take a snapshot, the node copies `fsm.db` to a temporary file beside it (`fsm.db.snapshot-<random>`) under a read transaction that ends with the copy; applying entries waits for the copy, as it waited for the in-memory one before. Raft writes that copy into its snapshot file while the node goes on applying entries, then deletes it. A node that installs a snapshot streams it into `fsm.db.restore` and syncs it, checks it, and renames it over `fsm.db`. A crash can leave either file behind, and the next start deletes them.

The copy needs free disk equal to `fsm.db`'s size (`narad_metastore_fsm_bytes`), on top of Raft's two snapshot files. Without it the snapshot fails: the node logs `metastore: could not copy the database for a raft snapshot` at error, counts it in `narad_metastore_snapshot_failures_total`, and Raft tries again at its next interval, while its log keeps growing until a snapshot succeeds. Metadata reads and writes carry on meanwhile. v3.0.1 held the whole database in memory instead: a 26 MiB `fsm.db` allocated 64 MiB per snapshot and 54 MiB per restore, where this release allocates under 100 KiB for either. The snapshot file is the same bbolt image as before, so v3.0.1 and this release install each other's snapshots.

## Restarts {#restarts}

**Unreleased:** in master, not in v3.0.1.

`fsm.db` outlives the process, and Raft hands the FSM its log again on every start: from index 1 when there is no snapshot yet, or the tail after the latest snapshot. Each entry's index is written into `fsm.db` (the `fsm_meta` bucket) in the same transaction as its effects, and a refused entry's index in a small transaction of its own, so the FSM skips every entry the database already holds. No entry is applied twice, and an entry that was refused when it first applied (an attach whose schemas did not match, say) cannot succeed on one node's replay.

The index describes the file only while nothing else writes it. Beside it is the bbolt transaction id of the commit that wrote it; any other write (an older release after a rollback, which ignores `fsm_meta`, or a tool) moves the file's id past it, and the index is no longer trusted. Before Raft starts, the node decides:

| `fsm.db` | Raft state | What happens |
|---|---|---|
| holds anything | none (`raft.db` and the snapshots are gone) | A node joining a running cluster sets it aside as `fsm.db.stale`, and the leader's log or snapshot rebuilds it. A node that would bootstrap a new cluster refuses to start and leaves the file untouched: a new cluster on an empty database would hold none of its topics ([Troubleshooting](../operate/troubleshooting.md#log-metastore-no-raft-state)). |
| trusted index past the end of the log and the latest snapshot | older than `fsm.db` (`raft.db` and the snapshots were restored from a backup that `fsm.db` was not) | A node joining a running cluster sets it aside as `fsm.db.stale` (or restores the snapshot over it), and the cluster's log brings the rest. Any other node refuses to start and leaves the file untouched: rebuilding it from the older Raft state would drop every topic created since the backup ([Troubleshooting](../operate/troubleshooting.md#log-metastore-raft-state-older)). |
| trusted index at or past the latest snapshot, within the log | any | The snapshot is not restored (Raft still takes its index and configuration), and the replay skips what `fsm.db` holds: the restart re-applies nothing. |
| trusted index behind the latest snapshot | a snapshot | The snapshot is restored over `fsm.db`, then the tail after it is applied. |
| index missing or untrusted | a snapshot | The snapshot is restored over `fsm.db`, as in v3.0.1. |
| index missing or untrusted | no snapshot, the log starts at index 1 | Set aside as `fsm.db.stale` and rebuilt from the log, one synced transaction per entry. This is the first restart after upgrading from v3.0.x on a cluster with no snapshot yet. |
| index missing or untrusted | no snapshot, the log is compacted | Kept and replayed onto as it is, logged at error: there is nothing to rebuild from. |

A snapshot is not taken while `fsm.db` is ahead of what the replay has handed it since the start. Raft labels the image with its own applied index, and a node that restores an image whose content ran past its label would replay entries on top of it. The replay closes the gap in milliseconds, since it only skips.

## When a node stops applying {#fail-stop}

**Unreleased:** in master, not in v3.0.1.

Every node must apply every committed entry, in order, or its replica parts from the others for good. Two failures are local to one node, and that node stops instead of skipping the entry:

- **An entry type it does not know.** A newer release proposed it. A leader proposes a new entry type only once every member reports a release that knows it ([Raft entry types and upgrades](#entry-types)), so this means a node runs an older image than the rest of the cluster.
- **A write its disk refuses.** bbolt allocates pages when it commits, so a full volume or an I/O error shows up as a failed commit. The node retries for up to 30 s (100 ms doubling to 5 s), logging `metastore: could not write raft entry; retrying` once, then stops.

A refused entry is neither: a topic that already exists, a compare-and-set that missed, a schema out of order are refused the same way on every node, so they are answered and counted as applied. So is an entry whose bytes do not decode, which every node's log holds alike (it is logged at error).

A node that stops logs `metastore: stopped applying raft entries` at error level with the entry's index and type and its own build, shuts Raft down (it no longer votes, leads or acknowledges writes its replica lacks), answers metadata writes and barriers with a `503`-class error, reports not ready, and exits non-zero. The entry is not counted as applied, so a restart replays it: once the disk has room, or on the cluster's release, it applies; until then the node stops again on the same entry, a crash loop with the reason in its log ([Troubleshooting](../operate/troubleshooting.md#log-metastore-stopped)). Why not keep running: a node whose replica is stuck would route requests and decide partition ownership from a view the rest of the cluster has moved past, while a stopped node is a dead member the others route around.

A database or snapshot that has applied an entry type newer than the build knows is refused too: the node does not open such an `fsm.db` (and leaves it untouched), and stops rather than install such a snapshot.

## Raft entry types and upgrades {#entry-types}

**Unreleased:** in master, not in v3.0.1.

Each metadata change is a Raft entry of one type (create topic, register member, and so on), and a release that adds a type must not propose it while a node that does not know it is in the cluster. A 3.0.x node skips such an entry silently, and a later release stops applying ([above](#fail-stop)). So during a rolling upgrade the cluster keeps writing the entry types every member knows:

- **Every member reports what it applies.** The heartbeat a node sends every 5 s carries its build and the newest Raft entry type it applies, and the leader records both on the node's member record. A heartbeat from 3.0.x carries neither, which reads as the set every 3.0.x release applies. Each heartbeat replaces the record, and nothing else changes it: a node that is down keeps the report it sent last, and a node rolled back reports the older set only from its first heartbeat on the older release.
- **The leader checks every member before it proposes a new type.** A type 3.0.x does not know is proposed only when every server in the Raft configuration (voters and non-voters) and every member record (alive, dead or draining) reports a release that applies it. A server with no member record yet (a joiner that has not registered) holds the type back, and so does a dead member whose last report was an older release, until it is removed. The leader counts itself as its own release. Until every member qualifies, the leader keeps using the entries it used before, and the reason names the first member holding the type back and the build it reported. The check reads the leader's own replica and each member's last report, however old. A running member's report is at most about 5 s old, but a member that is down, marked dead or not, counts with the report it sent before it stopped for its whole downtime: a node stopped to roll it back still counts as the newer release until it heartbeats on the older one.
- **A joiner older than every member is refused.** A join request carries the newest entry type the joiner applies (none from 3.0.x, which reads as the 3.0.x set). A node not yet in the Raft configuration that applies fewer types than every member with a record (this node included) is answered `409` with code `older_release`: the cluster may already use entries it would skip. So is one that applies fewer types than the newest the leader's `fsm.db` has applied (`fsm_meta`, carried in every snapshot), whatever the records say: the log it would receive holds such entries. A server with no member record yet, such as a staged joiner, is left out of the first check, since its release is unknown and counting it as nothing would let any joiner in. A server already in the configuration is never refused; its own heartbeat holds new types back instead. The answer is a `409` so that a 3.0.x node with an empty volume, which reads only `200`, `421` and `409` as "a cluster exists", joins (and is refused) instead of bootstrapping a rival cluster ([Cluster lifecycle](cluster-lifecycle.md#join)).
- **Older peers get the frame they know.** A 3.0.x node refuses a heartbeat or join request carrying these fields with a `400` naming trailing data, before it looks at anything else. The sender resends the same request without them at once, so a rolling upgrade needs no step and no heartbeat window is missed.

Once every member has reported a release that adds an entry type, rolling a node back to a release without that type is unsupported, whether or not the leader has used the type yet. The node being rolled back counts as the newer release while it is down, so the leader may use the type in that time (the dead mark it writes for that node after 30 s of silence, a topic create), and 3.0.x would skip those entries while a later release stops on them. Roll back only while a member still runs the older release ([Upgrade](../operate/upgrade.md#roll-back-newer)), and never add a node on an older release to a fully upgraded cluster; the leader refuses it.

### The entry types this release adds {#new-entry-types}

This release adds ten entry types, 23 to 32. Each one carries what its write was checked against, so the state machine refuses the write (identically on every node) when the metadata changed in between. The entry types every release applies keep their exact meaning, so a 3.0.x node and an upgraded one never apply the same entry differently.

| Type | Name in the logs | What it does |
|---|---|---|
| 23 | create topic with schema and link | Creates a topic, its first schema version and its fan-out parent link in one transaction. A leader change can no longer leave half a create behind (a topic without its schema, or a "child" that copies nothing), and a placement pass can no longer run between the record and the link. The parent must still be the incarnation the create was checked against. A name that differs from an existing one only in letter case is refused, and assignment rows or schema versions left under the name by an earlier topic are dropped. |
| 24 | compare-and-set topic update | Applies an alter only to the incarnation it was read from, never shrinks the partition count, and keeps the owner, creation time, visibility timeout and fan-out link as stored. |
| 25 | compare-and-set topic delete | Deletes the topic only if it is the incarnation the delete was checked against. |
| 26 | compare-and-set schema version | Appends a schema version only to the incarnation it was checked against, within the schema byte budgets (each fan-out child's copy counts against the child). |
| 27 | compare-and-set fan-out attach | Links a child only if parent and child are the incarnations checked; compares their schema histories as JSON values (key order, whitespace and number spelling do not matter), and counts a history the child adopts against the budgets. |
| 28 | compare-and-set fan-out detach | Unlinks a child only if parent and child are the incarnations checked. |
| 29 | insert-only partition placement | Places a partition that has no owner on record, of the topic incarnation it was computed for, inside its range, on a member that was not removed. It never replaces an owner; moves still change owners through their own flip. |
| 30 | orphan assignment prune | Deletes an assignment row that belongs to no partition (its topic is gone, or the index is past the partition count), and refuses a live one. |
| 31 | dead mark from an observed heartbeat | Marks a member dead unless a heartbeat newer than the one the decision was made from is on record. |
| 32 | user delete that releases its topics | Deletes a user and clears the owner of every topic it owned, in the same entry, so the topics fall to admins instead of passing to the next user created under the name. Until every member applies it, the delete is the 3.0.x one, which removes only the user, and the leader logs `user deleted, but its topics still name it as owner` (warning, `component=audit`) with the topics, their count, and the member holding the type back. Do not create a user under that name again until an admin has dealt with those topics. |

The topic manager on the leader asks before each write whether every member applies the type. While one does not, it proposes the entry every release applies and the leader-side checks are all the protection (the name lock, the leader barrier and the owner re-check); it logs `metastore: not using a new raft entry type yet; proposing the entries every member applies` at info, at most once a minute per type, with the member holding it back. The first time the leader uses each type it logs `metastore: every member applies raft entry type <n>; using <name>` at info. That line marks the first use, not the rollback boundary, which comes earlier: once every member has reported this release, a rollback is unsupported even if no such line was logged ([above](#entry-types)). When the state machine refuses a write because the topic changed, the manager reads the topic again, checks the request again and retries once; a second refusal answers `409` (`topic changed since it was read`).

A Raft server with no member record holds every new type back until it registers. If it never will (a joiner that crashed or was replaced), remove it with `narad cluster members forget <id>` ([Troubleshooting](../operate/troubleshooting.md#raft-server-no-member-record)).

## Leader and controller {#controller}

The Raft leader is also the **cluster controller**. It:

- assigns the partitions of new topics, round-robin over live members. A fan-out child is the exception: its partition p deliberately skips the owner of the parent's partition p (`metastore.ChildAwareOwner`), so a [replica child](../reference/glossary.md#replica-child)'s copy is placed on a different disk from the original;
- marks members dead when their heartbeats stop for 30 s;
- seeds the root admin.

Leadership transfer on graceful shutdown makes planned restarts nearly seamless (about 150 ms of failover); failover after a crash takes an election timeout (about 1 s).

A dead node's partitions are **not reassigned**, because their data lives only on that node's disk. The cluster waits for the node, and its volume, to come back, and the produce path routes around the dead [owner](../reference/glossary.md#owner) meanwhile (see [Produce path](produce-path.md#dispatch)). Assignments do move in a [rebalance or decommission](rebalance.md), which copies each partition to its new owner before ownership changes.

The first seconds of a cluster need care. A partition placed on the only member registered so far is soon moved by rebalance, and a topic created before any member registered used to wait for the controller's 10 s tick, with produces parked and consumers answered `204`. So (unreleased):

- A node retries its member registration every 250 ms until it first succeeds, then heartbeats at the normal 5 s.
- The leader watches membership every 250 ms, and runs its assignment and rebalance passes outside the 10 s tick when a member turns alive (new, or back after a restart). The pass waits until every Raft voter is alive, or until 1 s after the latest arrival if a voter is still missing.
- A topic create or partition increase made while the cluster is still forming (some Raft voters have a member record, others have none) waits up to 2 s for the rest to register before it places partitions, so they are spread rather than moved later. The 2 s are counted from when a forming cluster was first seen, so a voter that never registers costs one wait, not one per create.
- A create with no alive member at all still succeeds, and logs the warning "topic created without immediate partition assignment" (the error it carries reads "no alive member to own new partitions"). The controller assigns its partitions within about a second of the first member turning alive.
- Creates and the controller's sweep take one assignment lock, so one can no longer overwrite the other's owners. Once every member applies [insert-only placement](#new-entry-types), the state machine refuses a placement for a partition that already has an owner, so a pass computed from a lagging replica cannot replace one either.
- Assignment rows that belong to no partition (an earlier release could leave them behind a topic delete) are pruned by the leader when it takes over and about every minute after. Until every member applies the prune, each such row is logged once at error: `orphan assignment row for <topic>/<partition>; it is pruned once every member runs 3.1.0` ([Troubleshooting](../operate/troubleshooting.md#log-orphan-assignment-row)). No data moves: the row names no partition.
- A dead mark carries the heartbeat the controller judged, and is refused when a newer one committed in between.

## Metastore constants {#constants}

| Thing | Value |
|---|---|
| FSM store | bbolt (`fsm.db`), buckets: `topics`, `schemas`, `assignments`, `members`, `users`, `removed_members`, and `fsm_meta` (applied index, the transaction that wrote it, newest entry type applied) |
| Raft log store | boltdb (`raft.db`); snapshots: file store, **2 retained** |
| Snapshot copy | `fsm.db.snapshot-<random>` beside `fsm.db`, deleted once Raft has written its snapshot; a restore streams into `fsm.db.restore` ([Snapshots](#snapshots)) |
| Heartbeat / dead marking | every 5s / after 30s silence |
| `AppliedCaughtUp` contact freshness | leader contact within 5s (followers) |
| `Barrier` timeout | 5s |
| Failed metastore write | retried 100 ms doubling to 5 s, for up to 30 s, then the node stops ([When a node stops applying](#fail-stop)) |
| New Raft entry type | proposed only once every Raft server and member record reports a release that applies it ([Raft entry types](#entry-types)) |
| Entry types added by this release | 23 to 32 ([the list](#new-entry-types)); the leader logs the first use of each at info |
| Orphan assignment row prune | on the leader, when it takes over and every 6th reconcile tick (about a minute) |
| Joiner older than every member | refused with `409`, code `older_release`; the leader logs it at error at most once a minute per joiner |
| Startup reconcile wait for caught-up | up to 60s, then the destructive sweep is skipped rather than rushed |

Schema history is **append-only** and capped at 1000 versions per topic, and (unreleased) at 4 MiB of stored versions per topic and 256 MiB for every schema in the cluster, counting each fan-out child's copy; the leader checks the byte budgets before it proposes, and once every member applies the [compare-and-set schema entry types](#new-entry-types) the state machine checks them again. `opPutSchema` is applied only when the version is exactly the topic's persisted latest plus one and within the cap (and the same for every fan-out child's copy). The proposer (the topics manager on the leader) reads the persisted history, checks compatibility against the persisted latest, and proposes latest plus one. A proposer working from a stale view can therefore never overwrite an earlier version on any replica; it gets `ErrAlreadyExists`, reads again and retries.

The produce path keys its loaded schema by the topic's schema version counter and reloads when that moves. So a version registered on another node, a delete and recreate under the same name, or a schema adopted at attach time is picked up on the next produce. A reload reads only the latest version and runs as one flight per topic: every produce waiting on it shares one metastore read and one compile, and a flight that raced a newer schema change loads again, so the last schema compiled on a node is always the newest.

Every write is one `command` envelope (an op byte plus a JSON payload) applied identically on every node's FSM. Op families bump per-domain **version counters** (topics version, members version, and so on) that the hot paths use as cache keys: a produce checks its cached topic record against the topic version with one atomic load, instead of a bbolt read per message. The per-name counters (topic, assignments, schema) of a deleted topic are retired rather than kept for every name ever created.

## Boot order {#boot-order}

`cmd/narad/serve.go` reads like a checklist because it is one:

1. the metastore starts first, then the broker;
2. the **create gate is armed**, so topic creates block on every transport, *before* the QUIC listener starts. The startup orphan sweep can then never race a create forwarded by a peer into deleting a directory it just missed;
3. the gate opens when startup reconcile finishes, and `/readyz` reports ready only after that.

The comments in that file explain each ordering constraint where it is made.

## Next steps

- [Networking and security](networking-and-security.md): how nodes reach each other, and how requests are authenticated.
- [Rebalance and decommission](rebalance.md): how assignments move without losing a record.

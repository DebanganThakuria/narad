---
description: "Learn how a produce becomes a durable, visible record: the local write behind the 202, the background dispatch, and the commit on the owner."
search:
  boost: 0.5
---

# Produce path

Learn how a produce becomes a durable, visible record: the local write behind the `202`, the background dispatch, and the commit on the owner.

!!! abstract "In short"
    - The node that receives a produce fsyncs it into its own ingress WAL and answers `202`. The client waits for one local fsync.
    - A background dispatcher on that node commits each record to its partition's owner, at most one commit in flight per partition, and retries until it succeeds.
    - The owner appends, fsyncs, reads back and CRC-checks the records before it makes them visible. Only then does the WAL copy become reclaimable.
    - A partition whose owner is dead, or whose commits keep failing for 3 seconds, has its records rerouted to a live sibling partition.
    - Every seam in this path can duplicate a record; none can lose one that got a `202`.

The produce path runs on two clocks: how fast the client can be made safe (one local fsync), and how reliably the job gets finished (asynchronously, retried until it succeeds).

## Accept stage: the local WAL {#accept}

--8<-- "contract/produce-202.md"

<figure class="nr-dia nr-dia--doc" id="fig-produce-group-commit">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--mint">
--8<-- "diagrams/produce-group-commit.html"
</div>
<figcaption>Every caller whose record is staged before a flush shares its one write and one fsync, and each gets <code>202 Accepted</code> only when its own bytes are durable.</figcaption>
</figure>

Every node has one [ingress WAL](../reference/glossary.md#ingress-wal): a segmented, append-only log with group commit. Concurrent produces are staged into a shared buffer and fsynced together, so under load the fsync cost per message falls toward zero.

Each WAL record stores:

- the topic, and its [incarnation](../reference/glossary.md#incarnation) id (the `id` of the topic record the payload was validated against, when the record has one);
- the key, or none for a keyless produce;
- the target partition;
- the payload and a timestamp.

Once the fsync returns, the client gets its `202`: the message now survives any crash of this node.

The target [partition](../reference/glossary.md#partition) is chosen at accept time from the local metastore replica. A keyed message goes to the partition its [key](../reference/glossary.md#key) hashes to, and an explicit `?partition=` pins it. A keyless message is stored with no key and placed round-robin (unreleased; v3.0.1 gives it an invented `key-<n>` and hashes that). Each topic has its own rotation on each node, starting at a random partition, and `partition.HashRoundRobin` keeps the rotations of recently used topics and forgets idle ones.

### Batch produce {#batch-produce}

**Unreleased:** in master, not in v3.0.1.

A [batch produce](../reference/http-api.md#produce-batch) (`POST /produce/batch`, up to 100 records) takes the same path for all of its records at once. Every record is checked (key, payload, partition range, schema) before any is appended, and the first failure refuses the whole batch.

The records are then staged into the WAL in one append (`wal.Log.AppendManyWith`): in batch order, under one hold of the append lock, so they take consecutive sequence numbers with nothing staged between them. They share one accept time, the incarnation id the payloads were validated against, and one group commit. The `202` goes out once the last of them is durable.

The dispatcher commits a partition's records in WAL order, so records of one batch that share a key reach their partition in batch order, unless a [reroute](#dispatch) moves some of them. The failure cases are those of the WAL append, in [Group commit](#group-commit).

## Dispatch stage: the background mover {#dispatch}

A per-node dispatcher drains the WAL from a durable checkpoint and commits records to their partition owners:

<figure class="nr-dia nr-dia--doc" id="fig-produce-dispatcher">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--mint">
--8<-- "diagrams/produce-dispatcher.html"
</div>
<figcaption>Each partition has its own queue with at most one commit in flight, and the checkpoint only passes sequence numbers whose commit landed, so a crash replays from a point that never skips a record. In the strip, a shaded cell is done, a heavy one is held (queued or in flight), and a dashed one is skipped: left in the WAL and read again at least once a second.</figcaption>
</figure>

The reader reads each record once, as soon as it is durable, and puts it on the queue of its destination partition. Each destination has **at most one commit in flight**: when it lands, whatever queued meanwhile goes out as the next batch. Different destinations commit independently, up to 16 commits at once, so a slow or unreachable owner holds up its own partitions and nothing else.

Every sequence number between the checkpoint and the read frontier is in one of three states: done (committed, or [discarded](#discarding)), held (queued or in flight), or skipped (left in the WAL). The checkpoint is the first one that is not done, so it never passes a record that is not committed.

- **Batching comes from queueing.** The longer a commit takes, the more records the next one carries, so a busy owner gets fewer, larger commits, one fsync each. A destination's queue holds up to a quarter of the window, or 4,096 records if that is more. The reader stops once the records held (queued or in flight) reach the window, but at least four destinations' worth, so a hot or slow partition never takes more than half of it and the others keep being read while its commit runs. The window sizes itself to the fan-out it sees (64 records per destination partition, between 4,096 and 65,536), and the reader never reads more than 16 windows past the checkpoint.
- **Small batches linger.** A record becoming durable wakes the dispatcher at once, and each WAL group commit makes only a few records durable. On its own, every group commit would go out as a handful of records per partition, one partition fsync each, on the disk the next group commit waits for. So while other commits are in flight, a destination holding fewer than 64 records waits for more: until twice its owner's recent commit latency has passed since its oldest queued record, and at most 50 ms. The latency is a moving average per owner, so a slow remote owner does not lengthen the wait of this node's own partitions. An idle dispatcher, an owner with no commit measured yet, and a failing destination commit at once.
- **Remote commits have a byte budget.** A commit to another node carries at most 8 MiB of encoded records, and always at least one record; the rest stays queued, in order, for the next commit. The budget is half the cluster stream's 16 MiB frame limit, because the transport refuses a larger frame before sending it, so an oversized batch would fail the same way on every retry. A local commit has no such bound: the partition log splits a large batch into frames itself.
- **A hung commit stops counting.** A commit in flight for more than 1 s no longer takes one of the 16 slots and returns whatever queued behind it to the WAL. Its destination takes no new records until it lands. So an owner that stops answering holds nothing of anyone else's while its commit waits out its 30 s budget.
- **A failing destination waits alone.** After a failed commit, the destination keeps one record as a probe, returns the rest to the WAL, and retries with that single record once a second, on a 5 s budget. Nothing else waits for it: the dispatcher as a whole backs off only when it cannot read the WAL or store its checkpoint. A destination whose owner does not resolve at all (a new topic not assigned yet) is looked up again every 10 ms.
- **Persistent failure reroutes.** A destination whose commits keep failing for 3 s, counted from the start of the first failed attempt, has its records [rerouted](../reference/glossary.md#reroute) to a live sibling partition of the same topic: the next partition, counting up and wrapping around, whose owner membership reports alive and whose own commits are neither failing nor hung. Its probe keeps trying the original once a second, so the original gets its records back the moment it recovers. A destination whose owner membership already reports dead is rerouted at once. The grace is measured in time, not passes, so a partition handoff freeze or an owner restart does not scatter records across partitions. Records of a commit in flight are never sent again or rerouted.
- **Records left in the WAL come back in order.** A record is skipped rather than held when holding it would cost memory for nothing or break partition order: its destination is failing and already holds its probe, is hung, or has a full queue; an earlier record of the same partition is still skipped; or a delete or incarnation change is not confirmed yet. Skipped records are read again, in WAL order, when their destination recovers or drains, and at least once a second. The WAL read passes over the records that need no work before decoding them.
- **The checkpoint is stored lazily.** Each time the checkpoint moves, its 8 bytes are overwritten in place at once, so a process crash keeps the value. A background flush makes the value durable within 250 ms, with one `fdatasync` for however many stores landed meanwhile, and again on shutdown. The WAL compacts behind the stored value. The gauge `narad_ingress_dispatch_backlog_records` (unreleased) is the durable next sequence number minus that stored value: the records a restart would replay (see [Metrics reference](../reference/metrics.md)).
- **Duplicates come from these seams.** Which records above the checkpoint already committed lives only in memory, so a crash commits them again. An OS crash or power loss can also bring back a checkpoint up to 250 ms old, with the same result. And a commit RPC carries no idempotency token, so one that succeeds after its client gave up is retried (and, past the reroute grace, rerouted), which duplicates the batch; the 30 s budget makes that rare, and a probe's 5 s budget risks a single record. All of these are duplicates, never loss, as the [delivery contract](delivery-contract.md#at-least-once) allows.

The reroute is the availability trade made explicit: messages keep flowing while a node is dead, at the cost of arriving on a different partition. It is one of the reasons Narad [does not promise ordering](delivery-contract.md#ordering).

## Commit stage: the durability boundary {#commit}

On the owner, a commit batch goes through `commitDurable`, the only place in Narad where a message becomes visible:

1. Append all records, wrapped in the keyed envelope, to the partition log's buffer.
2. **Fsync.**
3. **Read back and CRC-check** every frame just written, so a torn or corrupt write is caught now, not at consume time.
4. Advance the [high watermark](../reference/glossary.md#high-watermark) in memory, which makes the records visible to consumers and fan-out.

Steps 2 to 4 run as one pass on the partition's flusher goroutine (`storage.CommitDurable`), and a commit costs one fsync. The new boundary is not written to disk, because a restart recovers it from the CRC-verified record tail, which the fsync in step 2 already covers. The details, and the one-time emptying of the `hwm` file this relies on, are in [Storage engine](storage-engine.md#the-high-watermark-and-the-hidden-tail).

Only after the owner confirms does the dispatcher's checkpoint move, so the WAL copy lives until the partition copy is durable, uncorrupted, and visible after a restart. A message that got a `202` is never in zero verified places.

### Commit combining {#commit-combining}

**Unreleased:** in master, not in v3.0.1.

The partition's produce lock spans the append and the durable commit. A failed commit discards everything above the high watermark, other callers' records included, so the two can never be split. Commit batches for one partition often arrive together (each node's dispatcher sends its own, and fan-out cursors add theirs), and each used to pay its own write, fsync and read-back, one after another, behind that lock.

Now a batch queues on the partition's combiner. With no cycle running, its caller becomes the leader: it takes the produce lock, drains every batch queued by then, appends them as one run, and commits them with one `CommitDurable`, without releasing the lock between the append and the commit. Everyone in the cycle shares the outcome: contiguous offsets on success, or the same error on failure, which the ingress dispatcher and the fan-out runner already handle by appending again. Batches that arrive during a cycle wait for the next one, which the leader hands to the oldest of them on its way out.

<figure class="nr-dia nr-dia--doc" id="fig-commit-combining">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--mint">
--8<-- "diagrams/commit-combining.html"
</div>
<figcaption>Batches that queue together share one append, one <code>CommitDurable</code> and one outcome, so they pay for one fsync together instead of one after another.</figcaption>
</figure>

Under the lock, before anything is appended, the cycle checks again what the caller checked on its way in:

- **Ownership and the handoff freeze.** A commit that passed the check before a [rebalance handoff](rebalance.md) armed its freeze, but reached the lock after the handoff read the final high watermark, is refused with nothing appended, and retries at the new owner.
- **The log still belongs to the live topic.** The leader resolved the partition's log before the drain, and batches keep queueing until the drain. A delete and recreate of the topic applied in between would send a batch of the new incarnation into the old incarnation's log, where it would be acked and then quarantined with it. So after the drain the cycle asks the log map whether its log is still current: whether the topic's metadata version has moved since the log was last checked against the topic record. When it has moved, the cycle lets go of the lock, resolves the log again (which retires the old incarnation's log), and runs these checks on the same batches against the new one. A topic record that keeps changing under every resolution ends the cycle after three tries with a retriable `ErrNotPartitionOwner` for every batch in it, and nothing appended. This also covers batches without an incarnation id: the fan-out runner's, a synchronous produce's, and those of an older accepting node.
- **The caller is still waiting.** A batch whose caller gave up (its commit RPC timed out) is left out: it will be retried anyway, and appending it would only commit a duplicate.
- **The topic incarnation.** A batch whose records carry an incarnation id other than the live topic's is refused with a retriable `ErrTopicIncarnationMismatch`, and nothing is appended. That happens when the topic was deleted and recreated while the records waited in the WAL, or when this node's metadata lags. So is a batch whose id is the live topic's but not that of the log the cycle holds (the record moved on between the two checks), which the next cycle resolves again. Records without an id commit by name, as before. The id travels with the records on the commit RPC to another owner too (see [Discarding](#discarding)), so the check covers every commit, local or remote. Only an owner on a release from before the field gets the records without it, and commits them by name. The single-record `CommitProduce` op carries the id as well (an optional trailing field), and its owner checks it the same way, though the dispatcher and the fan-out runner send only batches.

A remote owner answers the incarnation refusal with `412 Precondition Failed`, the message naming both incarnation ids, and logs it at info level rather than as an error. The owner did answer; it only holds another incarnation under the name, which a delete and recreate, or a lagging replica on either side, explains.

The dispatcher reads a `412` as the same retriable mismatch a local commit returns. For either, the records, and whatever queued behind them for that partition, go back to the WAL in order. The destination is not marked failing, nothing is rerouted, and the dispatcher logs a warning: `owner refused produce records accepted for another topic incarnation; checking them again`.

The next rescan, at least once a second, checks each record against the current incarnation again. A record is discarded once the leader confirms its incarnation is gone, or committed on its own partition once the owner's replica has caught up. Those retries are not spaced by the failure backoff, so an owner whose replica lags can cost a refused commit, with a warning here and an info line there, each time the records are placed again, until it catches up. A dispatcher from before the `412` treats it as any other non-2xx answer and retries it; no release before this one sends incarnation ids, so none is refused this way.

The cycle also keeps commit times from going backwards along a partition, which the fan-out [delay gate](fanout-engine.md#the-delay-gate) relies on (see [Storage engine](storage-engine.md#on-disk)).

## Discarding records of deleted topics {#discarding}

A record whose topic was **deleted** while it sat undispatched can never be committed, and it must be discarded or it would pin the checkpoint. Discarding a record that got a `202` is destruction, so it takes the full [stale-replica defense](metastore-and-raft.md#stale-replicas): the topic must be locally absent, **and** the replica caught up, **and** the leader must confirm the topic is gone (a node that is itself the leader must pass a barrier and read again). Anything less keeps the record for a later attempt.

The confirmation is asked once per topic per read, not once per record, and an unconfirmed answer is remembered for a second. So deleting a topic with a backlog behind it costs one leader round trip rather than one per record, and a leader that cannot be reached is asked about once a second. Every discard is logged as a warning: `discarding undispatched record for deleted topic` for one record, or `discarding undispatched produce records for deleted topic` with a `records` count for a destination's whole queue.

A topic that was deleted and **recreated under the same name** is the harder case. The name resolves, so nothing would stop the old records from committing into the new topic, past its schema and ownership. Every record of a topic that has an incarnation id carries the id it was accepted under (format 2, below), and the dispatcher checks it against the local replica before anything resolves or reroutes the record by name. When the replica shows another incarnation under the name, the record stays in the WAL until the leader confirms the old incarnation is gone (the name is absent, or belongs to another id), and is then discarded. The confirmed id is remembered, so its other records go without asking again.

The same checks run just before a batch leaves (records whose incarnation was replaced meanwhile go back to the WAL) and after a failed commit, on that commit's own goroutine, so a slow leader holds up only that destination. Each such discard is a warning per record: `discarding undispatched record of a deleted topic incarnation` (with `topic`, `topic_id`, `partition` and `seq`) when the record is being placed, and `discarding undispatched record for deleted topic` (with `topic_id` and the commit's error) after a failed commit.

The owner checks the id too, under the partition's produce lock (see [Commit combining](#commit-combining)), which is why the commit RPC carries it. `CommitProduceBatch` has an optional trailing section for it, written only when some record has an id: a count of runs, then per run a record count and the id the run shares. A batch for one partition costs a few bytes, and a batch without ids is byte for byte the frame older releases send and accept. An owner on an older release refuses a batch with the section as trailing data (`400`). The dispatcher then sends it again without the ids, on what is left of the same budget, and keeps sending that owner's batches without ids for 2 minutes before trying again, so a rolling upgrade costs one refused batch per old owner every 2 minutes.

## WAL compaction {#wal-compaction}

The checkpoint compacts segments whose records are all dispatched, and a fully dispatched *active* segment past 1 MiB is rotated so it can be reclaimed too. On a lightly loaded node the WAL stays under a megabyte of disk without any operator action.

### Record formats {#record-formats}

**Unreleased:** in master, not in v3.0.1.

A WAL record starts with a format byte. Format 1 holds topic, key, partition, timestamp and payload. Format 2 adds the topic's incarnation id after the topic name, and every accept for a topic with an incarnation id now writes it; a topic created before v2.2.0 has no id, and its records stay format 1. The decoder reads both, so a WAL written by an older binary replays unchanged.

The reverse is not true: a binary from before format 2 (v3.0.1 and earlier) cannot decode a format-2 record. Its dispatcher stops at the first one, and everything the node accepts after it waits behind it. Nothing is lost, and delivery resumes once a newer binary is back. So a node's ingress WAL must be drained before a downgrade: every accepted record dispatched, which `narad_ingress_dispatch_backlog_records` reading 0 shows. The procedure is in [Upgrade Narad](../operate/upgrade.md#roll-back).

### Segment preparation {#segment-preparation-opt-in}

**Unreleased:** in master, not in v3.0.1.

With `storage.ingress_wal_prealloc: true` (off by default; see [Configuration reference](../reference/configuration.md#storage)), the WAL prepares each next segment off the append path. Once the active segment is half full, a background goroutine creates `next-segment.prep`, fills it with zeros to the full segment size (64 MiB), and ends it with a 16-byte trailer (`NWPREP01` plus the write limit below); the roll renames it into place. Appends then overwrite blocks that are already allocated, so a group commit's `fdatasync` no longer changes the file's size. On journaling file systems (ext4, XFS) it therefore no longer commits the inode through the journal on the path every produce waits on. A roll that finds no spare ready (preparation failed, the disk is full) creates an empty segment as before, and the roll that seals a prepared segment trims it to its data, so sealed segments look exactly as they always did.

Overwriting in place changes what a power loss can leave. An appended write that tears loses its end; an overwrite can leave a hole of zeros in front of valid frames of the same write. So writes into a prepared segment are bounded: a group commit larger than 16 MiB is written and synced in runs of whole frames of at most 16 MiB. Recovery accepts "a bad frame followed by valid frames" as a torn tail only in the last segment, only when that segment ends in the trailer, and only within 16 MiB of the bad frame. It then truncates at the bad frame; anything else is still a loud corruption failure at open.

The cost of the rule: inside that window, damage to frames that were already synced cannot be told apart from a tear and is truncated too, as damage to the last frame of any active segment always was.

Preparation costs up to two segments of extra disk (the prepared active segment and a ready spare) and one extra segment of background zero-filling per segment. Its latency gain was measured on ext4 only; APFS showed no difference. A binary from before preparation opens a cleanly stopped prepared WAL without losing records. It can refuse to start (a `corrupt frame` error) on a WAL where a crash tore a write inside a prepared segment and left valid frames behind a hole, so start the new binary once to recover it before rolling back. Either way the older binary refuses the `storage.ingress_wal_prealloc` key itself, `true` or `false`, so remove it from the config file first.

## Group commit {#group-commit}

`wal.Log.Append` (and `AppendWith`, which the ingress uses) does not fsync per message. It stages the record into a shared buffer, wakes the sync loop, and **blocks on the batch's completion channel**. Every producer that arrived in the same instant shares one write and fsync (`syncBatch`), so under load the fsync cost per message approaches zero, while each caller still returns only after its own bytes are durable.

Once a record is staged, `Append` ignores context cancellation and waits for the true sync outcome. Reporting failure for a record that actually became durable would make a well-behaved retrying client produce duplicates for no reason. The sync loop alternates between two staging buffers (each kept up to 4 MiB), so a busy WAL does not allocate a fresh buffer per batch.

`wal.Log.AppendManyWith` is the same bargain for several records acked together (a batch produce). It checks every size, the fill function and the context before staging anything. It then stages all the records in order under one hold of the append lock, grows the staging buffer once for the whole batch, wakes the sync loop once, and waits once, on the group commit that holds the last record. The lock is held while the batch encodes and checksums its records, measured at about 0.2 µs per record, so a full batch holds it for tens of microseconds next to a sync's milliseconds; that is the total the records would have held it for one by one.

A batch that outgrows the room left in the active segment rolls it between two records, exactly as single appends would: the records staged so far are written and synced inline, then the segment rolls. So no frame spans two segments, and on disk a batch is the frames that many single appends would have written, which replay cannot tell apart. A crash can keep a prefix of a batch but never a gap, since frames are written in order and recovery truncates at the first torn one.

An error means the batch is not durable as a whole, and none of it is acked. Records that a roll in the middle of the batch had already synced stay in the log and are replayed and dispatched, as with any failed sync, so a client that retries the failed batch can duplicate them, within the at-least-once contract. A fill that fails or panics withdraws the records staged since the call started or since that roll, and gives their sequence numbers back.

### WAL disk failure {#wal-disk-failure}

A write or fsync failure on the ingress WAL (`ENOSPC`, `EIO`) fails the whole batch that was in flight. Every producer in it gets a `500`, logged as `http server error` and counted in `errors_total{component="http",kind="5xx"}`, and nothing in that batch was acked.

The failure is then **latched**: the WAL refuses every later append with the same error, because a second write on top of a region of unknown content could be acked and then lost. The latch clears only when the node restarts, which rescans the WAL and truncates its torn tail. So a node that ran out of disk keeps answering `500` to produce after space is freed, until it is restarted. Everything it acked before the failure is replayed and committed after the restart.

Records written before the failing point of a failed batch survive the rescan too, so a produce that got a `500` may still be delivered: the at-least-once contract, exactly as for a commit RPC that succeeds after its client timed out.

The latch is exported as the gauge `narad_ingress_wal_failed` (unreleased; 1 once latched). Consume and `/readyz` are unaffected by it, so alert on the gauge rather than waiting for readiness to notice. The symptom and the fix are in [Troubleshooting](../operate/troubleshooting.md#produce-500).

## Dispatch constants {#constants}

| Constant | Value | Where |
|---|---|---|
| Ingress WAL sync backstop | 10ms (`ingress_wal_sync_interval_ms`) | group commit fires on every append; this bounds a missed wakeup |
| Dispatcher idle backstop | 10ms | `defaultProduceDispatchInterval`: a record becoming durable wakes the dispatcher at once (`DurableProduceAdvanced`), so this only bounds a missed wakeup; it is also how often an owner that does not resolve is looked up again |
| Window base / hard cap | 4,096 / 65,536 records | `produceDispatchBaseWindow`, `defaultProduceDispatchBatchSize`: records held in memory at once (queued or in flight, hung commits aside) |
| Per-partition batch target | 64 records | `produceDispatchTargetPerPartition`: the window aims for this per destination, and a smaller batch lingers while other commits run |
| Linger cap | 50ms | `produceDispatchMaxLinger`: a small batch waits up to twice its owner's recent commit latency, never longer than this |
| Per-destination queue cap | a quarter of the window, at least 4,096 | `perDestCap`: one destination holds at most a batch in flight and a full queue |
| Remote commit byte budget | 8 MiB | `produceRemoteBatchBytes`: half the 16 MiB stream frame limit; at least one record per commit |
| Lookahead past a stuck record | 16 windows | `produceDispatchLookaheadWindows`: bounds the per-seq marks and the work of a rescan |
| Commit fan-out | 16 commits in flight | `defaultProduceDispatchCommitFanout`: hung commits (past 1s) do not count |
| Hung-commit threshold | 1s | `produceDispatchSlowAfter` |
| Commit budget / probe budget / failure backoff | 30s / 5s / 1s | `produceCommitRPCTimeout`, `produceProbeRPCTimeout`, `defaultProduceDispatchFailureBackoff`: generous on purpose, since a commit that succeeds after the client gave up duplicates; the backoff is per destination |
| Reroute after | 3s of failed commits | `produceDispatchRerouteGrace` (at once if membership already says the owner is dead) |
| Rescan backstop | 1s | `produceDispatchRescanInterval`: records left in the WAL are read again at least this often |
| Legacy-owner memory | 2 minutes | `produceLegacyOwnerTTL`: how long commits to an owner that refused topic ids go without them |
| Checkpoint flush delay | 250ms | `checkpointSyncDelay`: how long a stored checkpoint may sit written but not yet fdatasynced |

## Two produce paths {#two-produce-paths}

There are two partition pickers in `broker/messaging`, and the difference is deliberate:

- **`resolveAcceptedProducePartition`**, the WAL-first accept that every client produce over HTTP takes: a pure hash, with no liveness check. The accept must cost no more than a local fsync, and the dispatcher deals with dead owners later, by [rerouting](#dispatch).
- **`pickProducePartition`** (`routing.go`), the synchronous path behind the node RPC `Produce` op: it walks forward past partitions whose owner membership reports dead, so a dead node does not swallow its share of the keyspace. HTTP produces do not take this path.

## Next steps

- [Storage engine](storage-engine.md): what the commit writes on disk, and how the partition log recovers.
- [Produce messages](../build/producing.md): the client side of this path.
- [Troubleshooting](../operate/troubleshooting.md#produce-500): what to do when every produce answers `500`.

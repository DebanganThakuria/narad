# Fan-out Engine

Fan-out looks like magic from outside — attach a child, copies appear — but it's a deliberately boring machine: **per-partition cursors tailing the parent's committed log**, with durable positions and an at-least-once commit protocol. No double-publish from producers, no broker-side subscriptions; just log readers that never lose their place.

## Where the work runs

For each (parent partition × child), one **cursor goroutine** runs on the node that *owns that parent partition* — so slab reads are always local disk, and the cursor's durable state lives in the same directory (same durability domain) as the log it tails:

```mermaid
flowchart LR
    subgraph owner["owner of orders/p3"]
        LOG[("orders/p3 log")]
        CUR1["cursor → analytics"] --> LOG
        CUR2["cursor → retry (1h delay)"] --> LOG
        OFF[("fanout-analytics.offset<br/>fanout-retry.offset")]
    end
    CUR1 -->|"commit batch"| A[("analytics/p* owners")]
    CUR2 -->|"commit batch"| R[("retry/p* owners")]
```

A reconciler on every node diffs *desired cursors* (from the metastore: links × owned partitions) against *running cursors* once a second — cursors spawn on attach, stop on detach/delete/ownership change.

## The cursor loop: commit-before-advance

```mermaid
flowchart TD
    READ["read slab of committed parent records<br/>(fill-or-linger batching)"] --> REKEY["re-key each record with the<br/>child's partitioner (key preserved)"]
    REKEY --> COMMIT["commit per-child-partition batches<br/>(local or one RPC to the owner)"]
    COMMIT -->|all batches acked| PERSIST["persist cursor offset"]
    PERSIST --> READ
    COMMIT -->|any failure| RETRY["back off, re-read from<br/>unadvanced offset"] --> READ
```

The invariant is the whole guarantee: **the cursor's durable offset only advances past records whose child commits were acknowledged** (and child commits are the same fsync-and-verify as any produce). A crash mid-flight re-commits the last slab — duplicates into the child, never a gap.

## Attach epochs: why re-attach never replays

Each attachment gets a fresh **epoch** ID, stamped into the cursor's offset file. Detach + re-attach must start from the parent's *current tail* (the client contract says "no backfill"), so a cursor that finds an offset file from a *different epoch* refuses to resume it. Epochs turn "is this my state?" from a guess into an equality check.

Tail-anchoring — the act of skipping to the tail and overwriting the offset file — is the engine's only destructive move, and chaos testing showed a stale metastore replica can fabricate exactly the epoch mismatch that triggers it. So an anchor now requires the **Raft leader to confirm the epoch** (with the barrier rule for self-leaders); anything unconfirmed defers, and the reconciler retries a second later. Cursor offset files get the same protection before deletion. The incident that forced this is told in [Cluster Lifecycle](cluster-lifecycle.md).

## The delay gate

A delay child's cursor adds one filter: **only read records whose parent commit time is ≤ now − delay.**

```mermaid
flowchart LR
    subgraph parent log
        r1["r₁ committed 12:00:00"] --> r2["r₂ committed 12:00:01"] --> r3["r₃ committed 12:00:02"]
    end
    GATE{"now − delay ≥ commit time?"} -->|"yes: fan out"| r1
    GATE -->|"not yet: sleep until r₂ is due"| r2
```

Because commit times are **monotonic per partition** (assigned under the partition lock — a [storage-engine](storage-engine.md) property), the first not-yet-due record proves everything behind it isn't due either. So an idle delay cursor is O(1): peek the head, sleep until its due time (capped so gauges stay fresh). No timer wheels, no scan-the-backlog polling — a million pending delayed messages cost the same as one.

Delivery is therefore *never early* (the gate is checked against the owner's clock at read time) and usually lands within a second of due (long-poll wakeups + the linger window).

## Edge behaviors

- **Drop-behind**: if a cursor falls behind the parent's *retention* (child down for days), aged-out offsets are skipped and counted on an explicit loss metric — bounded, alarmed loss instead of a wedged parent. The retention floor (`≥ delay + 1h` for delay children) makes this unreachable in sane configs.
- **Dead child-partition owner**: fan-out never reroutes to a sibling child partition (unlike produce, a cursor can afford to wait — rerouting would scatter a key's records across child partitions for no availability gain); the cursor stalls on that bucket and retries until the owner returns.
- **Lag observability**: `fanout_lag_messages` (parent HWM − cursor) is the health signal for normal children; `fanout_due_lag_seconds` (how far behind the *due frontier*) is the one for delay children — raw offset lag on a delay child is permanently ≈ rate × delay *by design*.
## The numbers

| Constant | Value |
|---|---|
| Reconcile interval (desired vs running cursors) | 1s |
| Slab long-poll / retry backoff | 1s / 1s |
| Batch caps | 4,096 records / 4 MiB (`fanout.max_batch_records/bytes`) |
| Linger to fatten a partial batch | 25ms |
| Delay cursor max sleep (metadata freshness bound) | 30s (`defaultFanoutDueWakeCap`) |
| Orphan cursor-file sweep | every 30th reconcile pass |
| Max children per parent / max delay | 108 / 1 year |
| Retention floor for a delay child's parent | delay + 1h (`topic.MinRetentionMs`) |
| Attach epoch | 8 random bytes, hex — e.g. `67953471cc57a32a` |

## The cursor's durable state, in full

One JSON file per (parent partition, child), living next to the log it indexes:

```
topics/orders/p00003/fanout-analytics.offset
{"epoch":"67953471cc57a32a","next_offset":98332}
```

That's the entire recovery story: epoch says *which attachment* this position belongs to; `next_offset` says where to resume. Written via atomic temp+rename only after the batch below it is committed to the child (commit-before-advance). Everything else — running goroutines, batches in flight, lag gauges — is disposable.

## Reading a slab: fill-or-linger

`readBatch` long-polls the parent for up to 1s, then tops up until the batch hits 4,096 records / 4 MiB or a 25ms linger expires — so a busy parent produces fat child commits (one fsync each on the child side) while a trickling parent still ships within ~25ms. For a delay child, every read carries `MaxCommittedAt = now − delay`; the reader stops at the first undue record and reports *when* it becomes due, which is what lets the cursor sleep instead of spin.

# Guarantees & Errors

This page is the contract. Everything Narad promises, the limits of those promises, and every status code you can see.

## The delivery guarantee

**At-least-once.** Every message that gets a `202` will be delivered to a consumer, and will keep being redelivered until someone acks it (or retention expires). The flip side: **duplicates happen**, after consumer crashes, visibility timeouts, nacks, ambiguous produce retries, and node failures. Your consumer must be idempotent. This is not optional advice; it's the other half of the contract.

```mermaid
flowchart LR
    A["202 Accepted"] --> B{delivered and acked?}
    B -->|yes| C[settled, never seen again]
    B -->|"consumer crashed / timed out"| D[redelivered, possibly a duplicate]
    D --> B
    B -->|"never acked until retention"| E[deleted by retention]
```

## The durability contract: read before trusting Narad with anything important

- A `202 Accepted` means your message is **fsynced to disk** on the accepting node before the response is sent. A crash one millisecond later does not lose it.
- On its final partition, a message is **fsynced and read-back-verified** before it becomes visible to consumers or is removed from the accepting node's log.
- **Narad keeps exactly one copy of each partition** (plus the transit copy above). There is no replication subsystem. If a node's *disk* is permanently destroyed, the messages on that disk's partitions are gone. Process crashes, restarts, and node reboots lose nothing (this has been chaos-tested extensively), but disk loss is unprotected by default. Run it on storage you trust (cloud persistent volumes, RAID), and for topics that need a second copy, create a replica child: [replication, when you ask for it](fanout-and-delay.md#replication-when-you-ask-for-it), an async full copy, deliberately placed on different nodes.
- Cluster **metadata** (topics, users, assignments) is Raft-replicated across all nodes and survives any minority of disks failing.

## Ordering: not guaranteed

Narad is explicit about this where other brokers are shy: **there is no delivery-order guarantee.** In steady state, keyed messages stick to one partition and tend to arrive in produce order, but five deliberate mechanisms reorder, and your design must assume them:

- **Redelivery**: a crashed or slow consumer's message reappears after newer ones were consumed.
- **A dead consumer holds its partition's frontier**: a message leased by a consumer that never comes back is redelivered when its visibility timeout expires, and until then that partition can run out of anything else to serve (everything above it is acked). Expect quiet windows up to `visibility_timeout_ms` after an outage.
- **Broker restart**: acks are persisted in batches (every 100ms by default, the broker's `storage.consumer_offset_commit_interval_ms`), including acks that landed out of order, so a crash can redeliver the messages acked in about the last interval (on a node whose disk cannot sync every changed partition within the interval, about the last commit's duration; the broker logs a warning when that happens). A graceful restart redelivers nothing that was acked.
- **Dead-owner skip**: while a node is marked dead, keyed produces walk forward to a live partition: the key-to-partition mapping itself moves.
- **Dispatch reroute**: accepted messages destined for an unreachable owner are committed to a live sibling partition rather than delayed indefinitely.

Need a sequence? Carry it in the payload and order on your side. Need to collapse duplicates and reorders at once? Make handlers idempotent keyed on a payload ID, which the at-least-once contract requires anyway.

## Availability: the deliberate trade

Ordering was not lost by accident; it was spent on availability. In CAP terms Narad's **data plane is AP**:

- **Produce is available while any node lives.** Any live node accepts a produce with a local fsync: no leader election, no quorum, no coordination on the hot path. Delivery is worked out afterwards and routed around dead machines. One caveat when a majority is down: a survivor still accepts a produce sent to it directly, but it reports not ready while it has no Raft leader, so a load balancer stops routing to it until quorum returns. Losing a minority of nodes never stops produces through the load balancer.
- **Consume is available for every partition whose owner is alive**, and since new traffic reroutes to live owners, fresh messages stay consumable even mid-outage. Messages already stored on a dead node wait for it to return (their partition answers `503` meanwhile).
- **The control plane is the one consistent piece**: creating/altering topics and managing users go through Raft and need a quorum of nodes. Your *data* flows at one node; *administration* waits for a majority.

```mermaid
flowchart LR
    subgraph ap["Data plane: available (AP)"]
        PRO[produce: any live node]
        CON[consume: live owners]
    end
    subgraph cp["Control plane: consistent (CP)"]
        META["topics / users / membership<br/>need a Raft quorum"]
    end
```

## Timing

- Produce→consumable: typically single-digit milliseconds.
- Delay children: never early; usually within ~1s after the delay elapses; can be later under failures.
- Retention: messages are deleted *at least* `retention_ms` after writing: deletion happens in coarse chunks, so data often lives somewhat longer, never shorter.

## Status codes

| Code | Where | Meaning | What you should do |
|---|---|---|---|
| `200` | consume, reads, batch ack | Here's your data; for a [batch ack](consuming.md#acking-in-batches), one result per handle, each with the status a single ack would have got | Process it; retry the batch-ack handles answered `502` or `503` |
| `201` | topic/user create | Created | Nothing |
| `202` | produce, batch produce | Durably accepted: the delivery promise (for a [batch](producing.md#producing-in-batches), every message in it) | Nothing. Never retry a 202 |
| `204` | ack/extend/nack, delete, empty consume | Done / nothing available | Loop or move on |
| `400` | anywhere | Malformed request, bad param, schema violation, incompatible or unregistrable schema | Fix the request; don't retry as-is |
| `401` | anywhere | Missing/wrong credentials | Fix auth |
| `403` | anywhere | Authenticated but not allowed | You need a grant |
| `404` | anywhere | Topic/user doesn't exist; for a batch produce, also a node on a release before batch produce | Check the name; fall back to single produces |
| `409` | create/attach/alter | Conflict: already exists, role conflict, retention-vs-delay violation, schema `schema_base_version` no longer current, schema history full, schema of an attached child | Read the error body; for a schema conflict, re-read `schema_version` and retry with the new base |
| `410` | ack/extend | Your lease lapsed; message was handed elsewhere | Stop working on it; expect a duplicate |
| `413` | produce, batch produce, topic create/alter, batch ack | Body over 1 MiB (a schema document itself is capped at 256 KiB, answered as `400`); a batch ack body over 64 KiB | Shrink the payload |
| `429` | consume, produce | Too many concurrent consumes for your identity on this node (`http.max_consume_in_flight_per_identity`, 1024 by default; a [batch consume](consuming.md#consuming-in-batches) counts as its `max`), or too many concurrent produces (`http.max_produce_in_flight_per_identity`, off by default; a [batch produce](producing.md#producing-in-batches) counts as its message count) | Back off and retry, or run fewer concurrent requests |
| `502` | ack/extend/nack, partition-pinned consume | The node you reached forwarded the request to the partition's owner and got no answer (the owner stopped responding, or the connection to it failed) | Back off and retry, as for `503`; an ack can never settle twice, so retrying one that had already landed changes nothing (the retry answers `410`) |
| `503` | produce/consume/ack | Temporarily unavailable: partition owner down, quorum lost | Back off and retry |

## Retry cheat sheet

- `202` → never retry. `4xx` → never retry unchanged, except `429`, which is a retry-later. `502`, `503` and timeouts → retry with backoff; for a batch ack, retry just the handles whose result says so.
- A **timed-out produce is ambiguous**: the message may have been accepted. Retrying may duplicate it; that's fine, because your consumer is idempotent. Right? A timed-out batch produce is ambiguous as a whole: any of its messages may have been accepted.

## API stability

The `/v1` surface is stable: routes, parameters, status codes, and JSON field names documented in this guide won't change or disappear within `/v1`. New optional fields and new endpoints may appear (your JSON parsing should ignore unknown fields; it already does, right?). Anything we ever need to break moves to a `/v2` with both versions serving during a deprecation window. The node-to-node RPC protocol is versioned by the same rule: opcodes are append-only, new fields are optional and trailing, and an older node answers either with a clean `400` that the sender retries in the shape the old node understands, which is what makes mixed-version rolling upgrades boring.

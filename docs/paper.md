# One design choice, and everything that followed

*An engineering report on Narad: what happens when the client refuses to route.*

Narad takes partitions from Kafka and a routing-unaware client from SQS. Almost everything else in the system is a consequence of holding both at once, including the parts we did not anticipate and the one bug that took a week to find.

This report is for engineers deciding whether to run it, or borrowing the design. It states the measurements with their conditions, links each claim to the code that implements it, and says plainly where the evidence stops. Where Narad loses to something else, it says so.

---

## 1. The choice

Two decisions, made at the start, that look independent and are not.

**The client speaks HTTP and does not route.** `curl` is a complete client. There is no cluster metadata protocol, no partition leader discovery, no client-side assignment state machine. You send produce, consume and ack to a load balancer and whichever pod catches the request is the right pod.

**Messages live in partitioned durable logs.** Each partition is a directory owned by exactly one node, an append-only segmented log with a visibility watermark. That is what makes throughput scale with nodes and what makes replay possible.

Kafka has the second without the first: its client goes to the partition leader, so the broker never has to gather across nodes. SQS has the first without the second: it exposes no partitions, so there is nothing to gather. Narad holds both, and the gather is the interaction term.

Everything below follows from that sentence.

---

## 2. Produce: any node accepts, one node owns

A produce arrives at a node that probably does not own the target partition. The client is already waiting. Two clocks matter, and they are separated on purpose: how fast we can make the client safe, and how reliably we can finish the job.

**Stage one, accept.** The receiving node authorizes the request, resolves the target partition from its local metastore replica, appends the record to its own ingress write-ahead log, and waits for a group-commit fsync. Then it answers `202`. From that moment the message survives a crash of this node. Concurrent produces are staged into a shared buffer and fsynced together, so per-message fsync cost amortizes toward zero under load.

**Stage two, dispatch.** A per-node dispatcher drains the write-ahead log from a durable checkpoint, buckets records by `(topic, partition)`, and commits each batch to its owner, locally or over QUIC. The drain window sizes itself to keep each batch fat, because a commit batch is one fsync on the owner and batch size is therefore the throughput lever.

**Stage three, commit.** On the owner, `storage.CommitDurable` is the only place a message becomes real:

1. Append the records to the partition log's buffer.
2. Fsync.
3. Read back and CRC-verify every frame just written.
4. Persist the new high watermark, an eight-byte in-place write plus fdatasync.
5. Advance the high watermark in memory, which is what makes the records visible.

Step three is unusual and worth pausing on: a torn or corrupt write is caught at commit, not months later when a consumer reads it. The checkpoint only moves after the owner acknowledges, so the write-ahead log copy outlives the partition copy until the partition copy is proven durable, uncorrupted and visible after a restart. There is never a moment when a `202`-acked message exists in zero verified places.

**The consequence nobody can opt out of.** If a partition's owner is down, its records cannot commit. The dispatcher keeps everything else moving with a skip-set rather than head-of-line blocking, and after three failed passes, with membership agreeing the owner is dead, it reroutes those records to a live sibling partition of the same topic.

That reroute is why Narad does not promise ordering. The alternative was to block until the owner returned, which is the other half of the CAP coin and a worse answer for a queue. We took availability and wrote the cost into the client contract instead of hiding it.

Detail: [Produce Path](internals/produce-path.md), [Storage Engine](internals/storage-engine.md).

---

## 3. Consume: leases, not consumer groups

There are no consumer groups, no generation ids, no rebalancing. Run one worker or a hundred against a topic. Each message goes to one of them, and a worker that dies hands its messages back.

The mechanism is a lease per message. Each owned partition holds an in-flight shard:

| state | where it lives | survives a crash |
|---|---|---|
| committed frontier | `consumer.offset`, eight bytes, fdatasynced | yes |
| acked-ahead set | `consumer.ahead`, two checksummed 4 KiB slots | yes |
| reservations and nonces | shard memory | no, leases evaporate and messages redeliver |
| corrupt-skip set | shard memory plus a metric | no, re-derived on read |

A consume reserves the lowest unresolved offset above the frontier, stamps it with an expiry and a random nonce, reads the record, and returns `partition:offset:nonce` as the receipt handle. An ack is accepted only if that offset still holds a live reservation with the same nonce, so an expired-then-re-reserved offset gives the late acker `410 Gone` rather than silently settling someone else's lease.

The asymmetry in that table is the design. Everything cheap to reconstruct is memory. The one thing that must never move backwards and then forwards inconsistently is a single fsynced file per partition.

**Nobody scans and nobody races.** An empty consume does not park on a broadcast channel. It joins the topic's waiter queue and blocks on one buffered channel, and a single pump goroutine per broker does the reservation once and hands the record to exactly one waiter. The shape this replaced closed a broadcast channel on every commit: every parked consumer woke, rebuilt a select set, took the partition mutex, scanned, and all but one found nothing. Cost per commit now scales with messages delivered rather than with consumers waiting.

Detail: [Consume Path](internals/consume-path.md).

---

## 4. The gather: a token protocol

Here is the interaction term.

A consumer's request lands wherever the load balancer put it. That node probably owns some partitions of the topic and not others. The consumer wants any message from the topic, so there is no single correct node to forward to, and the record it wants may arrive on any of the others a second from now.

Two obvious answers, both bad. Broadcasting a wake to every waiter on every commit is the thundering herd we just removed locally, now with network in the middle. Parking a forwarded long poll on one rotated owner makes a record produced to any other node invisible for the client's whole wait, and holds a reservation on the node you happened to pick, which then has to be given back whenever someone else wins.

What Narad does instead: the node holding the consumer probes every remote owner once with an ordinary non-blocking consume, and if they all come back empty it leaves a **token** with each of them, in one batched frame. When a record commits on any owner, that owner spends one token to notify the waiting node. The consumer wakes, claims from that specific owner with a non-blocking consume carrying a claim flag, and gets the record.

Four properties carry the correctness:

- **A token reserves nothing.** It says only "I am here, tell me if records show up". That is what removes the give-back problem entirely: an owner that never hears back has stranded no record, because it never took one out of circulation. Only the claim reserves, and it is aimed at exactly one node.
- **One token, one record, one peer told.** Never a broadcast. An `outstanding` counter gates notifications against the available-record estimate, which is the high watermark minus the ack frontier, minus records in flight or acked ahead of a gap. The same record is never promised to two peers.
- **A hold lives exactly as long as the claim is in flight.** Between notification and claim the owner holds the record back. The claim's flag is what retires the hold; a plain probe is never a claim, so it cannot release a hold promised to someone else.
- **Tokens are single use and connection scoped.** Firing one consumes it, so a stale token costs one notification rather than one per record forever. They die with the peer's connection, which makes crash recovery for this subsystem "do nothing": a restarting node rebuilds them by registering again.

Three delivery speeds fall out, and which applies depends only on whether the consumer arrived before or after the data:

| round trips | when |
|---|---|
| 0 | the record is on a partition this node owns |
| 1 | the data was already there, so the probe returns it inline |
| 2 | the consumer waited, then data arrived: notify, then claim |

Under load the first two dominate, because a busy topic rarely empties and the notification path is taken only on the empty-to-non-empty edge. Busy topics use the pump less than quiet ones.

---

## 5. The bug that proved the invariants

A protocol description with only its happy path is worth little. This one earned its invariants the hard way.

We measured end-to-end latency with one message at a time on a keyed partition, a consumer long-polling on one node and the record landing on another, 100 samples per placement, on a three-node cluster:

| placement | before | after |
|---|---|---|
| consume A, produce A (local) | 16.8 ms | 16.0 ms |
| consume B, produce A | 960 ms | 15.8 ms |
| consume B, produce B | 960 ms | 17.2 ms |
| consume B, produce C | 960 ms | 17.2 ms |

Every cross-node placement sat at 960 milliseconds with a p99 three milliseconds above the median. That flatness is the tell. Real latency varies; a constant is a timer.

**Why it hid.** Nothing failed. Every RPC on the path completed in under two milliseconds, the notification was sent, and the claim succeeded with a `200` on the first attempt. There was no error to grep for and no slow call to find. It was also invisible under load, which is when we had always tested: only a sparse stream exposed it.

**The mechanism.** The hold that stops a record being promised twice had two release paths in the design and one in the implementation. The claim was supposed to retire it; only the one-second deadline did. On a sparse stream the previous message's hold was still counted when the next message landed, so the pump saw nothing available and each message waited out the remainder of the earlier message's timer. Hence 960 milliseconds, just under the deadline.

A second defect fed it. The available-record estimate subtracted only the ack frontier, so records already in flight or acked ahead of a gap looked free. That produced notifications nobody could claim, and each empty claim burned another full second of hold.

**The fix**, in one sentence: retire the hold when the claim arrives, not only when the timer fires, and count in-flight and acked-ahead records as taken. Cross-node delivery became indistinguishable from local.

**The transferable lesson.** A slot taken by an event must be released by that event, not only by the timeout that bounds it. A timeout is a safety net, and when it is also the only release path, the system runs at the speed of the net.

---

## 6. The trades, stated

Every one of these is deliberate, and each is a real cost to somebody.

**No ordering.** Failover reroutes messages across partitions and redelivery replays older messages after newer ones. If you need a sequence, carry one in the payload. What you get in exchange is a broker that keeps accepting while machines burn: as long as one node is alive, produces land.

**One copy per partition.** A `202` means fsynced to disk, which is stronger than most brokers' default, and it is one disk. Lose the volume and you lose those partitions until you restore a snapshot. Worse in daily terms: while a node is down, the messages already on its disk are undeliverable even though nothing was lost. Replication as a fan-out child topic exists, is asynchronous and opt-in, and is a live backup rather than a hot standby. This is the gap we would close first.

**Throughput in the tens of thousands, not the millions.** Fsync before ack and two HTTP round trips per message are both real costs, and both are chosen. On identical compute, one broker at a time in Docker with two CPUs each, a driver waiting for every system's per-message confirmation:

| system | produce msg/s | p50 / p99 | consume+ack msg/s | what the ack means |
|---|---|---|---|---|
| NATS JetStream | 39,525 | 0.4 / 0.8 ms | 21,600 | in the R1 file stream, fsync every 2 min |
| Redis Streams | 31,643 | 0.5 / 0.8 ms | 41,698 | in the AOF buffer, fsync every 1 s |
| Kafka, 6 partitions | 14,728 | 0.8 / 4.0 ms | 7,830 | page cache, no per-message fsync |
| RabbitMQ quorum queue | 13,014 | 1.2 / 1.9 ms | 14,361 | fsynced before the confirm |
| Pulsar standalone | 7,632 | 2.0 / 3.0 ms | 10,984 | standalone disables the journal fsync |
| Narad | 5,597 | 2.0 / 10.0 ms | 8,567 | fsynced, group commit, before the 202 |

The ordering is mostly the durability column priced in milliseconds. Within the fsync-per-confirm class, RabbitMQ's quorum queue outproduced us by about 2.3 times on this workload. That is the cost of plain HTTP where binary protocols pipeline, and we would rather publish it than omit the table.

On a real cluster, three nodes at 4.5 vCPU each, we sustained 50,000 msg/s through the full produce, consume and ack flow, about 3,700 per vCPU. The run ended because the load generator saturated, not the broker, so read that as a floor and not a ceiling.

---

## 7. Operations as a design goal

A load balancer, a StatefulSet, and persistent volumes. That is the complete architecture. Cluster metadata lives in Raft inside the same binary, so there is no ZooKeeper, no BookKeeper, no external metadata store, and no sidecar quorum service.

This is not minimalism for its own sake. Every component you add to a deployment diagram is a component somebody has to understand at three in the morning, and the failure modes multiply rather than add. Scaling out is raising `replicaCount`: a new pod refuses to bootstrap, joins the existing cluster instead, and the leader computes the minimal set of partition moves to rebalance, copying each partition verbatim and cutting over with a millisecond freeze at the end.

The client API is the other half of the same argument: eleven endpoints for topics and messages, plus three for user administration, and that is the whole surface. There is no client library to vendor and no binary protocol to debug at three in the morning. The produce path from HTTP handler to fsync is four files in one repository, and the [Internals](internals/index.md) pages name the functions in order, which is the difference between a system a team owns and a system one person knows.

Detail: [Deployment](operate/index.md), [Cluster Lifecycle](internals/cluster-lifecycle.md), [Rebalance and Decommission](internals/rebalance.md).

---

## 8. Evidence

**Chaos, on the released build.** Each scenario is a separate run of a driver that tracks every message id and verifies each is acked exactly once:

| scenario | messages | result |
|---|---|---|
| rolling restart of all three brokers under load | 90,000 | zero loss, 23 duplicates |
| kill the Raft leader mid-load | 60,000 | zero loss, 13 duplicates |
| quorum loss, two of three killed at once | 90,000 | zero loss, 37 duplicates |
| decommission under load, then rejoin | 150,000 | zero loss, zero duplicates |
| graceful node delete under sustained load | 367,792 | every message consumed and acked exactly once |

Duplicates are within the at-least-once contract and come from acks in the last flush window before a kill.

**Four data-loss bugs, found by chaos and not by unit tests.** All four shared one root cause: a node restored from a Raft snapshot believes it is current while being hours stale, and each bug was a different subsystem trusting that belief with something destructive. A startup sweep deleted live topic directories. Fan-out cursors tail-anchored and skipped a delay child's backlog. The dispatcher discarded acked records whose topic looked absent. The fourth is the one worth the read: after the first three were fixed, each fix still trusted "if I am the leader, my local state is authoritative", and then a double kill made a freshly restarted node win an election legally, because its log was complete, while its state machine was still replaying an old snapshot. Election proves the log; only a Raft barrier proves the state.

**A soak shaped like production.** Since 12 September 2026, three processes drive about 1,000 messages a second through seven topics modelling a payments company: two firehoses with fast handlers, an outbound-webhook topic whose handlers are slow, uneven, and sometimes give up entirely, slow refunds, a five-minute settlement burst with 8 KB payloads, an audit topic drained periodically, and a config topic idle for minutes at a time. That mix is deliberate: it reaches out-of-order acks, stranded leases, cold partitions and retention expiry, none of which a uniform benchmark ever touches. Redelivery-after-ack and undelivered-message counters have both stayed at zero.

---

## 9. What we have not shown

The limitations are the part of this report we would want a skeptical reader to check first.

- **No evaluation at scale.** Three nodes, 4.5 vCPU each, and a load generator that saturated before the broker did. There are no scaling curves across nodes, partitions or consumers, and no latency distribution under controlled load. The cross-system table is laptop-grade and single-run.
- **No synchronous replication**, and therefore no measurement of what it would cost.
- **No mixed-version upgrade test.** The cross-node claim flag carries a fallback for older peers and it has unit and fuzz coverage only; we have never run a v2 node beside a v3 node.
- **Weeks of track record, not years.** The soak is days old at the time of writing. Kafka, RabbitMQ, NATS and Redis have all been through Jepsen; our evidence is our own chaos matrix, self-administered, and worth exactly that.
- **The harness is ours.** In one afternoon of building the soak, three separate harness bugs each imitated a broker failure convincingly, one of them reporting 22 exactly-once violations a second against a healthy cluster. We now treat the measurement apparatus as part of the system under test, and we suspect it first.

---

## 10. What we would do next

Replication, and not because a table has a gap in it. The operational reality is that a node being replaced today means a slice of the queue waits, and for a broker people put payments on, waiting is an outage even when nothing is lost.

Then an evaluation worth the name: scaling curves, a controlled-load latency study, and a run on standardized hardware rather than our own bench.

If you are deciding whether to run Narad: it is a good fit when you want a queue with per-message leases and HTTP semantics, you can tolerate reordering, and you would rather operate one binary than five. It is the wrong choice when you need ordering guarantees, synchronous replication, millions of messages a second, or a stream-processing ecosystem. Those are not oversights; three of them are the direct cost of the choice in section one.

---

*Every claim here links to the subsystem page that explains it, and the code is one repository. The comparison numbers live in [Narad vs. Everything Else](compare.md), with the sources and hardware attached to each row.*

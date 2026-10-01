---
description: "Look up the terms these docs use for Narad's parts and behaviour, each with the page that covers it in full."
---

# Glossary

Look up the terms these docs use for Narad's parts and behaviour, each with the page that covers it in full.

## Acked-ahead set {#acked-ahead-set}

The acks a partition holds for messages above its oldest unacked message, until the gap below them closes. It is capped per partition by `max_acked_ahead_per_partition` and kept on disk.

See: [Consume path](../understand/consume-path.md).

## At-least-once delivery {#at-least-once}

Narad's delivery guarantee: an accepted message is delivered until a consumer acks it, as long as retention keeps it, and it can be delivered more than once.

See: [Delivery contract](../understand/delivery-contract.md#at-least-once).

## Attach epoch {#attach-epoch}

An ID a child topic gets each time it is attached to a parent. Fan-out progress saved under an earlier attachment is never reused, so a detach followed by an attach starts afresh.

See: [Fan-out engine](../understand/fanout-engine.md).

## Attach point {#attach-point}

The parent's position, one offset per parent partition, recorded when a child is attached. The child receives the messages from there on, never older ones.

See: [Fan-out engine](../understand/fanout-engine.md).

## Committed frontier {#committed-frontier}

The offset below which every message of a partition is acked. It only moves forward, and it is kept on disk.

See: [Consume path](../understand/consume-path.md).

## Consume {#consume}

A request that takes the next available message of a topic, together with a lease on it.

See: [Consume and acknowledge messages](../build/consuming.md) and [Consume messages](http-api.md#consume).

## Decommission {#decommission}

Draining a node before it is removed: its partitions move to the other nodes, then it leaves the Raft voters.

See: [Scale out and in](../operate/scaling.md#decommission) and [Rebalance and decommission](../understand/rebalance.md).

## Delay child {#delay-child}

A fan-out child that receives each parent message a fixed time after the parent committed it. Nothing can be produced to it directly.

See: [Fan out and delay messages](../build/fanout-and-delay.md#delay-children).

## Delivery token {#delivery-token}

A marker a node leaves with each node that owns a partition of a topic, while one of its consumers waits for a message. The owner uses it to tell that node when a message arrives, so nobody polls.

See: [Consume path](../understand/consume-path.md).

## Dispatch {#dispatch}

The background step that moves an accepted message from the ingress WAL of the node that accepted it to the owner of its partition.

See: [Produce path](../understand/produce-path.md).

## Extend {#extend}

An ack with `extend=true`. It renews a lease for a full visibility timeout, for a handler that needs more time.

See: [Consume and acknowledge messages](../build/consuming.md#extend).

## Fan-out child {#fan-out-child}

A topic linked to a parent topic that receives a copy of every message produced to the parent from the attach point on. It has its own partitions, consumers and retention.

See: [Fan out and delay messages](../build/fanout-and-delay.md).

## Force-promote {#force-promote}

What ends a partition move whose source node stays dead: after 2 minutes, the destination takes over the partition with the copy it already holds.

See: [Rebalance and decommission](../understand/rebalance.md).

## Gateway node {#gateway-node}

The node a client's request reaches. It serves the request and forwards work to the owners of the partitions involved. For a topic it owns no partitions of, a node is only a gateway.

See: [Consume path](../understand/consume-path.md).

## Grant {#grant}

A permission: one action (`produce`, `consume`, `create` or `admin`) on the topics that match a list of patterns.

See: [Access model and grants](access-model.md).

## Hidden tail {#hidden-tail}

Records on a partition's disk above its high watermark, which consumers cannot see, such as those written by a commit that a crash interrupted.

See: [Storage engine](../understand/storage-engine.md).

## High watermark {#high-watermark}

One past the last offset of a partition that consumers can see. Everything below it is committed and synced to disk.

See: [Storage engine](../understand/storage-engine.md).

## Idempotent handler {#idempotent-handler}

A message handler that has the same effect whether it runs once or several times on the same message, for example because it records an ID from the payload and skips IDs it has seen. At-least-once delivery requires one.

See: [Handle retries and dead letters](../build/handling-retries.md).

## Incarnation {#incarnation}

One life of a topic name, identified by the topic's `id`. A topic deleted and created again under the same name is a new incarnation with a new `id`, so nothing of the old one can mix with it.

See: [Cluster lifecycle](../understand/cluster-lifecycle.md).

## Ingress WAL {#ingress-wal}

Each node's write-ahead log of the produces it accepted. A produce is written there and synced to disk before its `202`, then dispatched to its partition's owner.

See: [Produce path](../understand/produce-path.md).

## Key {#key}

An optional string sent with a produce. Messages with the same key go to the same partition while the partition count and the partition owners stay the same. It is not an ordering guarantee.

See: [Produce messages](../build/producing.md#keys).

## Lease {#lease}

The exclusive, time-limited hold a consumer gets on a message it consumed. It lasts the topic's visibility timeout, and an ack, extend or nack settles it.

See: [Core concepts](../get-started/concepts.md#leases).

## Metastore {#metastore}

The cluster's metadata (topics, users and grants, partition assignments, members), replicated to every node through Raft.

See: [Metastore and Raft](../understand/metastore-and-raft.md).

## Nack {#nack}

An ack with `extend=0`. It ends a lease at once, so the message can be delivered again right away.

See: [Consume and acknowledge messages](../build/consuming.md#nack).

## Owner {#owner}

Two meanings, told apart by context. A partition's owner is the node that stores the partition and serves its consumers. A topic's owner is the user that created it, who may change and delete it.

See: [Architecture overview](../understand/index.md) and [Access model and grants](access-model.md#ownership).

## Partition {#partition}

A slice of a topic, stored on one node. Partitions spread storage and consumption across the cluster.

See: [Core concepts](../get-started/concepts.md#topics-and-partitions).

## Quarantine {#quarantine}

Setting a directory aside by renaming it instead of serving or deleting it. Two kinds:

- A topic directory that belongs to a deleted incarnation of a topic is renamed `<name>.stale-<id>`. It is removed later, once the leader confirms that incarnation is gone.
- A moved partition's old copy that the stale-copy sweep cannot prove is covered by the new owner (it is ahead of the position the partition was promoted at, or, unreleased, the new owner cannot vouch for it) is renamed `<partition>.quarantine`. Narad never serves it and removes it only when the topic is deleted; its records may exist only there, so an operator decides what to do with it.

See: [Cluster lifecycle](../understand/cluster-lifecycle.md), [Rebalance and decommission](../understand/rebalance.md#what-if-the-source-dies-mid-move) and [Troubleshooting](../operate/troubleshooting.md#log-partition-set-aside).

## Receipt handle {#receipt-handle}

The string a consume returns with a message, in the form `partition:offset:nonce`. It names the lease; send it back to ack, extend or nack the message.

See: [Ack, extend or nack a message](http-api.md#ack).

## Replica child {#replica-child}

A fan-out child created with `parent`, so that its partitions sit on other nodes than the parent's: an asynchronous second copy of a topic.

See: [Back up and replicate topics](../operate/backups.md#replica-children).

## Reroute {#reroute}

Sending accepted messages to a live sibling partition when their partition's owner is dead or keeps failing, so produce stays available. It is one of the reasons Narad does not guarantee order.

See: [Produce path](../understand/produce-path.md) and [Delivery contract](../understand/delivery-contract.md#ordering).

## Retention {#retention}

How long a topic keeps a message after it is written, whether or not it was acked: at least one hour, or forever.

See: [Manage topics](../build/topics.md).

## Target {#target}

In a partition move, the node the partition should end up on. The assignment records it next to the current owner until the move completes.

See: [Rebalance and decommission](../understand/rebalance.md).

## Topic {#topic}

A named stream of messages that producers write to and consumers read from, split into partitions.

See: [Core concepts](../get-started/concepts.md).

## Visibility timeout {#visibility-timeout}

How long a lease lasts, set for each topic when it is created (30 seconds by default). When it runs out, the message is delivered again.

See: [Core concepts](../get-started/concepts.md#leases).

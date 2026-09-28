---
description: "Learn how Narad is put together: what every node runs, where data lives, and how one message travels from producer to consumer."
---

# Architecture overview

Learn how Narad is put together: what every node runs, where data lives, and how one message travels from producer to consumer.

The Understand pages describe the code on `master`. Where v3.0.1 behaves differently, the text says so; see [which release these docs describe](../reference/api-stability.md#docs-version).

## The whole system {#whole-system}

Every Narad node runs the same binary with the same components. Nodes differ only in which data they own and whether they currently lead Raft.

Inside one node, a request takes this path:

```mermaid
flowchart TB
    accTitle: The request path inside one node
    accDescr: A load balancer sends each request to any node. Inside the node, the HTTP API hands it to the router. The router writes a produce to the ingress WAL, and the produce dispatcher commits it to the partition's owner, this node or another one over QUIC. The router sends a consume or an ack to the broker engine, which works on the partition logs this node owns. The fan-out runner copies committed records into child partitions.
    LB[Load balancer] --> H
    subgraph node["one node"]
        direction TB
        H[HTTP API] --> R[Router]
        R -->|produce| ING[Ingress WAL]
        ING --> D[Produce dispatcher]
        R -->|"consume, ack"| B[Broker engine]
        D -->|"commit, here or<br/>on the owner (QUIC)"| S[("partition logs<br/>(owned partitions)")]
        B --> S
        F[Fan-out runner] -->|child commits| S
    end
```

Across the cluster, every node holds a full copy of the metadata and the partitions it owns:

```mermaid
flowchart LR
    accTitle: Three nodes of a Narad cluster
    accDescr: Each of three nodes holds a Raft replica of the metastore and the partition logs it owns. The metastore replicas stay in step over Raft. The nodes commit messages to each other's partition logs over QUIC. The controller runs only on the node that leads Raft, here narad-0.
    subgraph n0["narad-0"]
        MS0[("metastore<br/>Raft replica")]
        S0[("partition logs")]
        C0["controller<br/>(Raft leader only)"]
    end
    subgraph n1["narad-1"]
        MS1[("metastore<br/>Raft replica")]
        S1[("partition logs")]
    end
    subgraph n2["narad-2"]
        MS2[("metastore<br/>Raft replica")]
        S2[("partition logs")]
    end
    MS0 <-->|Raft| MS1
    MS1 <-->|Raft| MS2
    S0 <-.->|QUIC| S1
    S1 <-.->|QUIC| S2
```

## Five design ideas {#design-ideas}

**1. Any node accepts any request; ownership decides where data lives.**
Clients talk to any node through the load balancer. Each [partition](../reference/glossary.md#partition) has exactly one [owner](../reference/glossary.md#owner), the node whose disk holds its data. A node forwards what it cannot serve locally to the owner over QUIC. See [Networking and security](networking-and-security.md).

**2. Produce writes a local log first.**
A produce is fsynced into the receiving node's [ingress WAL](../reference/glossary.md#ingress-wal) and answered `202` at once. A background dispatcher then moves it to the partition's owner, which commits it durably. The client waits for one local fsync; delivery to the owner is asynchronous and retried until it succeeds. See [Produce path](produce-path.md).

**3. Metadata is replicated by Raft; message data has one owner.**
Topics, users, assignments and fan-out links live in the Raft-replicated [metastore](../reference/glossary.md#metastore): every node holds a full replica, and one node leads. Message data is not replicated: one owner, one copy, fsynced and verified. See [Metastore and Raft](metastore-and-raft.md) and [One copy per partition](delivery-contract.md#one-copy).

**4. Consume is a queue with leases.**
Each message is reserved on its own, even in a batch consume (unreleased), which takes up to 100 in one request. A reservation is a [lease](../reference/glossary.md#lease) that lasts the [visibility timeout](../reference/glossary.md#visibility-timeout), held in the owner's memory, with a durable [committed frontier](../reference/glossary.md#committed-frontier) behind it. Acks move the frontier forward, and a background committer [writes it to disk](consume-path.md#how-acks-reach-the-disk); a crash only means redelivery. See [Consume path](consume-path.md).

**5. Fan-out tails the parent's log.**
A child topic is fed by a cursor on the owner of each parent partition. The cursor reads committed parent records in bulk, commits them to the child, and keeps its own durable position, so no parent message is skipped and none is copied twice except by at-least-once retries. A delay child adds a due-time gate to the same cursor. See [Fan-out engine](fanout-engine.md).

## One message, end to end {#message-end-to-end}

A produce is accepted by any node and committed on the partition's owner:

```mermaid
sequenceDiagram
    accTitle: One message from producer to owner
    accDescr: The producer posts to any node, which fsyncs the message into its ingress WAL and answers 202. That node's dispatcher commits it to the partition owner, which appends, fsyncs, verifies the checksum and advances the high watermark, so the message becomes visible, then confirms so the WAL entry can be reclaimed.
    participant P as Producer
    participant A as Accepting node
    participant O as Partition owner
    P->>A: POST /produce
    A->>A: fsync into ingress WAL
    A-->>P: 202
    A->>O: dispatcher: commit batch (QUIC)
    O->>O: append + fsync + CRC verify
    O->>O: advance high watermark (visible)
    O-->>A: committed (WAL entry now reclaimable)
```

Once it is visible, a fan-out cursor copies it to child topics and a consumer takes it from the owner:

```mermaid
sequenceDiagram
    accTitle: One message from owner to consumer
    accDescr: A fan-out cursor on the owner reads the committed records and commits copies to the owners of the child partitions. A consumer gets the message with a receipt handle from the owner and acks it.
    participant F as Fan-out cursor
    participant O as Partition owner
    participant CH as Child owner
    participant C as Consumer
    F->>O: read committed slab
    F->>CH: commit copies to child partitions
    C->>O: GET /consume
    O-->>C: message + receipt handle
    C->>O: POST /ack
```

## Design principles {#design-principles}

Two rules appear in every subsystem. Both came out of chaos testing, where each was learned from a bug that lost data.

- **Destroying data needs the Raft leader's confirmation.** No node deletes data (topic directories, cursor files, WAL records) on its local view alone, because a freshly restarted replica can be far behind while believing it is current. Every deletion asks the leader first, and a node that is the leader must pass a Raft barrier before it trusts its own state.
- **Every failure keeps the data.** When a check cannot complete (no leader, a peer unreachable, a barrier failed), the answer is always to keep the data and try again later.

The bugs that taught these rules are in [Cluster lifecycle](cluster-lifecycle.md#crash-recovery-bugs).

## Reading order {#reading-order}

The pages in this section follow the path of a message, then the cluster around it:

1. [Delivery contract](delivery-contract.md): what Narad promises, and what each failure does to your messages.
2. [Linearizability check](linearizability.md): how a nightly run checks that contract.
3. [Produce path](produce-path.md): from a produce request to a durable, visible record.
4. [Storage engine](storage-engine.md): segments, frames, the high watermark and retention.
5. [Consume path](consume-path.md): leases, acks and how they reach the disk, and consumes that cross nodes.
6. [Fan-out engine](fanout-engine.md): cursors that copy a parent's log to its children.
7. [Metastore and Raft](metastore-and-raft.md): the replicated metadata, and the stale-replica problem.
8. [Networking and security](networking-and-security.md): the HTTP and QUIC planes, authentication and authorization.
9. [Cluster lifecycle](cluster-lifecycle.md): bootstrap, joins, crash recovery and topic incarnations.
10. [Rebalance and decommission](rebalance.md): moving a partition between nodes without losing a record.

## Codebase map {#codebase-map}

If you are about to read the source, start from this map:

| Package | What lives there |
|---|---|
| `cmd/narad` | Wiring: config, boot order, the join loop, startup reconcile. `serve.go` is the table of contents for the whole process |
| `internal/transport/httpserver` | HTTP routes, auth middleware, handlers. Thin on purpose |
| `internal/cluster` | Everything node-to-node: router, produce dispatcher, fan-out runner, QUIC RPC client and server, leader confirmation |
| `internal/broker/ingress` | The produce WAL: accept, replay, checkpoint, compaction |
| `internal/broker/messaging` | The engine: produce commit, consume, ack, fan-out slab reads |
| `internal/broker/runtime` | Partition-log registry, offset committer, orphan sweeps, lifecycle |
| `internal/consumer` | The in-flight lease table: reservations, nonces, acked-ahead sets |
| `internal/persistence/storage` | Partition log engine: segments, frames, flusher, retention, high watermark |
| `internal/persistence/wal` | The generic segmented WAL under ingress |
| `internal/persistence/metastore` | Raft and the bbolt state machine: topics, members, users, assignments |
| `internal/domain/*` | Pure types: topic, user, records |
| `internal/platform/*` | Config, metrics, partitioner, network utilities |

## One produce, by function name {#produce-by-function}

The same journey as the diagram above, with function names you can search for, from the handler to the disk:

```
POST /v1/topics/orders/produce?key=k
 └─ messaging.Produce (transport/httpserver/handlers/messaging/produce.go)
     └─ Engine.AcceptProduce (broker/messaging/produce_accept.go)
         ├─ resolveAcceptedProducePartition: hash the key, no liveness
         │   check
         └─ ingress.Manager.AcceptProduceWithTopicID
             (broker/ingress/produce.go)
             └─ wal.Log.AppendWith: staged into the group-commit
                buffer, blocks until the shared fsync lands
                                                  <- the 202 line
... in the background, woken as soon as the record is durable ...
 └─ ProduceDispatcher.run (cluster/produce_dispatcher.go)
     ├─ read, then place (cluster/produce_dispatch.go): read the newly
     │   durable records, queue each on its (topic, partition),
     │   reroute dead owners
     ├─ launch, startCommit, runJob (cluster/produce_commit.go): at
     │   most one commit in flight per partition, local or to the
     │   owner over QUIC
     │   └─ Engine.CommitAcceptedProduceBatch
     │       (broker/messaging/produce_commit.go)
     │       ├─ storage.Log.AppendBatchOwned: keyed-envelope records
     │       └─ commitDurable, then storage.Log.CommitDurable:
     │          fsync, CRC read-back, advance the high watermark
     │                                   <- consumers can see it
     └─ advanceCheckpoint: first seq not yet committed, stored; the
        WAL compacts behind it
```

Each deep dive follows the same pattern: the concept first, then the constants and function names behind it.

## Next steps

- [Delivery contract](delivery-contract.md): the promises these components add up to.
- [Produce path](produce-path.md): the first stop on a message's journey, in detail.

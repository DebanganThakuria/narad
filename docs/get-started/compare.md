---
description: "See how Narad compares with Kafka, NATS JetStream, RabbitMQ, SQS, Redis Streams and Pulsar, and which problems each one fits best."
---

# Compare Narad with other brokers

See how Narad compares with Kafka, NATS JetStream, RabbitMQ, SQS, Redis Streams and Pulsar, and which problems each one fits best.

The rule for this page: when in doubt, the other system gets the benefit. Each of these tools is good at what it was built for, so the useful question is which one was built for your problem. Where Narad is weaker, the page says so. Prices and citations are as of August 2026; the Kafka and NATS entries were updated in September 2026.

!!! abstract "In short"
    - Narad is a durable work queue over plain HTTP: per-message acks and leases, delayed and fan-out child topics, and replay, in one binary.
    - It fsyncs every message before it answers `202`, but keeps one copy of each partition. A second copy is an opt-in replica child.
    - It does not order messages, has no stream-processing ecosystem, and has a short track record: its first release was in June 2026.
    - Choose Kafka for event streaming at millions of messages per second, RabbitMQ for complex routing, SQS for a queue with no servers on AWS, and Narad for SQS-style queues you run yourself.

## Feature matrix {#feature-matrix}

Narad beside Kafka, NATS JetStream and RabbitMQ:

|  | **Narad** | **Kafka** | **NATS JetStream** | **RabbitMQ** |
|---|---|---|---|---|
| **Core model** | Queue-first on durable logs | Partitioned ordered log | Streams + consumers | Broker with exchanges/queues |
| **Client protocol** | **Plain HTTP: curl is a client** | Binary protocol, SDK required | NATS protocol, SDK required | AMQP, SDK required |
| **Per-message ack + visibility lease** | ✓ native | ✓ with share groups (Kafka 4): per-record acks and a time-limited lock; ✗ with consumer groups (offsets only) | ✓ (ack wait + redelivery) | ✓ |
| **Delayed delivery** | ✓ native (delay child topics) | ✗ | ✓ per message since 2.12 (message schedules) | Plugin, or TTL plus a dead-letter exchange |
| **Fan-out (one message to many independent streams)** | ✓ native child topics | ✓ (consumer groups re-read the log) | ✓ (multiple consumers per stream) | ✓ (exchanges, its main strength) |
| **Schema validation at the broker** | ✓ built in: JSON Schema per topic, checked on produce, append-only versions with a fail-closed compatibility check | Separate Schema Registry (Confluent, Karapace); richer (Avro/Protobuf, per-subject modes) but a second service, and validation lives in the client serializer | ✗ | ✗ (payloads are opaque) |
| **Replay from offset** | ✓ native, non-destructive | ✓ native | ✓ native | ✓ with streams; ✗ with classic and quorum queues |
| **Ordering** | **✗, deliberately none** | ✓ per partition | ✓ per stream | ✓ per queue (mostly) |
| **Replication** | Async, opt-in per topic ([replica child](../operate/backups.md#replica-children)) | ✓ synchronous (ISR) | ✓ Raft (R3/R5) | ✓ quorum queues |
| **Binary payloads over the wire API** | ✓ raw octet-stream | ✓ (opaque bytes) | ✓ | ✓ |
| **Deployment footprint** | **1 binary, Raft inside** | Brokers + KRaft (historically ZooKeeper) | 1 binary | 1 broker (Erlang runtime) |
| **Runs on your laptop unchanged** | ✓ | Heavier | ✓ | ✓ |
| **Stream processing ecosystem** | ✗ | ✓✓ (Streams, Connect, ksql) | Modest | ✗ |
| **Maturity** | **Young**: first release June 2026, a small track record | Since 2011, widely deployed | Mature, CNCF | Since 2007 |

Narad beside SQS, Redis Streams and Pulsar:

|  | **Narad** | **SQS** | **Redis Streams** | **Pulsar** |
|---|---|---|---|---|
| **Core model** | Queue-first on durable logs | Managed queue | In-memory log + consumer groups | Segmented log |
| **Client protocol** | **Plain HTTP: curl is a client** | HTTP + SigV4 signing (SDK in practice) | RESP, client library | Binary protocol, SDK required |
| **Per-message ack + visibility lease** | ✓ native | ✓ (the model Narad's leases resemble) | ✓ (PEL + claim) | ✓ |
| **Delayed delivery** | ✓ native (delay child topics) | ✓ per message, up to 15 min | ✗ | ✓ native, arbitrary |
| **Fan-out (one message to many independent streams)** | ✓ native child topics | Needs SNS in front | ✓ (multiple groups) | ✓ (subscriptions) |
| **Schema validation at the broker** | ✓ built in: JSON Schema per topic, checked on produce, append-only versions with a fail-closed compatibility check | ✗ | ✗ | ✓ built in, Avro/JSON/Protobuf with compatibility modes; broader than Narad |
| **Replay from offset** | ✓ native, non-destructive | ✗ | ✓ (XRANGE) | ✓ native |
| **Ordering** | **✗, deliberately none** | FIFO queues only, throughput-capped | ✓ per stream | ✓ per partition |
| **Replication** | Async, opt-in per topic ([replica child](../operate/backups.md#replica-children)) | Managed, invisible | Async (loss windows) | ✓ BookKeeper quorums |
| **Binary payloads over the wire API** | ✓ raw octet-stream | ✗ text-only bodies, 1 MiB cap | ✓ | ✓ |
| **Deployment footprint** | **1 binary, Raft inside** | None: AWS runs it | Your existing Redis | Brokers + BookKeeper (+ZK/Oxia) |
| **Runs on your laptop unchanged** | ✓ | ✗ (emulators only) | ✓ | Standalone mode, which differs from production |
| **Stream processing ecosystem** | ✗ | ✗ | ✗ | ✓ (Functions) |
| **Maturity** | **Young**: first release June 2026, a small track record | Fully managed since 2006 | Mature | Mature |

## Durability: what an ack means {#durability}

"Durable" hides three separate questions: is the message **on disk** when you get your ack, is it **on more than one machine**, and **which faults can still lose it**? For every promise Narad makes, see the [Delivery contract](../understand/delivery-contract.md).

| | Ack means fsynced? | Replication | Acked-message loss windows |
|---|---|---|---|
| **Narad** | **✓: group-commit fsync before the 202** | ✗ single copy per partition; [replica children](../operate/backups.md#replica-children) are async, opt-in | Losing a node's disk loses its partitions; plan volume snapshots or replica children |
| **Kafka** | ✗ by default: flush to disk is effectively disabled ([`log.flush.interval.messages` = Long.MAX](https://kafka.apache.org/documentation/#brokerconfigs)); durability comes from replication | RF=3 by convention, sync to the ISR with `acks=all` | `acks=1`; ISR shrunk to 1 with `min.insync.replicas=1`; correlated power loss across replicas |
| **NATS JetStream** | ✗ by default: file storage fsyncs on a **2-minute interval**; `sync_always` exists but the docs warn it drops to a few hundred msg/s | R1 by default; R3/R5 is Raft, but the quorum ack is in-memory | [Jepsen (Dec 2025, v2.12.1)](https://jepsen.io/analyses/nats-2.12.1) demonstrated acked-write loss **even at R3/R5** under crash faults; R1 power loss can drop up to 2 min |
| **RabbitMQ** | ✓ quorum queues fsync on a quorum before the confirm; ✗ [streams don't fsync](https://www.rabbitmq.com/docs/streams) (OS writeback, like Kafka) | 3 Raft members | Quorum queues: majority disk loss only. Streams: correlated quorum power failure |
| **SQS** | Stored redundantly across multiple AZs before the response (internals opaque, but that is the contract) | Multi-AZ, managed | None documented |
| **Redis Streams** | ✗: default is RDB snapshots; AOF `everysec` still loses about 1 s; `appendfsync always` drops to the disk's fsync rate | None by default; replication is **always async** (`WAIT` is best-effort) | Failover discards replication lag; [the Redis docs say so](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/) |
| **Pulsar** | **✓: BookKeeper fsyncs the journal on the ack quorum by default** (standalone mode: no) | E=2/W=2/A=2 defaults, synchronous | Narrow: needs `journalSyncData=false` (a common performance tuning) or simultaneous loss of the ack quorum |

<figure class="nr-dia nr-dia--doc" id="fig-compare-durability">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--lilac">
--8<-- "diagrams/compare-durability.html"
</div>
<figcaption>Narad alone fsyncs before the ack without replicating first: a <code>202</code> survives a crash or a power cut, but not the loss of the disk that holds the message.</figcaption>
</figure>

Narad, Pulsar and RabbitMQ quorum queues are the only systems here that fsync before acking by default. Narad gives up the other axis: it has **no synchronous replication**, so a destroyed disk loses data where Kafka, JetStream at R3 and quorum queues survive it. Each system guards against a different failure.

Kafka, RabbitMQ, NATS and Redis have all been through [Jepsen](https://jepsen.io/analyses) analyses. Narad's evidence is its own. Before v1.0.0 it was a chaos matrix. Today every pull request runs a three-node cluster through load and through node restarts ([how the contract is tested](../understand/linearizability.md)).

## Throughput on similar compute {#throughput}

Cross-system benchmarks are hard to compare fairly, so every number below comes from the linked source, with its hardware and durability settings. The fsync difference above is the largest confounder: Kafka's headline numbers do not fsync each message, and Pulsar's do. Message sizes differ too. Treat every figure as an order of magnitude.

### Published results {#published-results}

| System | Published result | Compute | About per broker vCPU | Config during test |
|---|---|---|---|---|
| **Kafka** | [605k msg/s @ 1 KB](https://www.confluent.io/blog/kafka-fastest-messaging-system/) (2020) · [~1M msg/s @ 1 KiB](https://jack-vanlightly.com/blog/2023/5/15/kafka-vs-redpanda-performance-do-the-claims-add-up) (2023, independent re-run) | 3 nodes, 24 / 72 vCPU | ~14k–25k | RF=3, `acks=all`, **no fsync** |
| **Kafka, fsync per message** | [420k msg/s @ 1 KB](https://streamnative.io/blog/perspective-on-pulsars-performance-compared-to-kafka) (2020) | 3 nodes, 24 vCPU | ~17k | RF=3, `flush.messages=1` |
| **Pulsar** | [300k](https://streamnative.io/blog/perspective-on-pulsars-performance-compared-to-kafka)–[800k msg/s @ 1 KB](https://streamnative.io/blog/apache-pulsar-vs-apache-kafka-2022-benchmark) (2020/2022, vendor) | 3 nodes, 24 / 72 vCPU | ~11k–13k | RF=3 equivalent, **journal fsync on** |
| **RabbitMQ quorum queues** | [66–67k msg/s @ 1 KB](https://www.rabbitmq.com/blog/2020/06/21/cluster-sizing-case-study-quorum-queues-part-1) (2020, official) | 72–112 vCPU clusters | ~0.6k–0.9k | RF=3, confirms, **fsync** |
| **NATS JetStream** | [134k msg/s @ 128 B](https://github.com/nats-io/nats-server/discussions/7599) (file R1, real network; the [official docs number](https://docs.nats.io/using-nats/nats-tools/nats_cli/natsbench), 404k, is loopback on a MacBook) | 8 cores | ~17k | R1, async batch publish, lazy fsync. R3 costs [~40% more throughput](https://amirhossein-najafizadeh.medium.com/benchmarking-nats-jetstream-cluster-hypothesis-testing-for-enhancements-4a500d11ce7b) |
| **Redis Streams** | [~136k msg/s unpipelined](https://learn.arm.com/learning-paths/servers-and-cloud-computing/redis-cobalt/redis-benchmark-and-validation/) · [0.5–1M pipelined](https://redis.io/docs/latest/develop/data-types/streams/) (tiny payloads) | 1 core (single-threaded) | ~136k | RDB only: the fastest number here carries the weakest durability |
| **SQS** | Quota-bound, not compute-bound: standard is effectively unlimited in aggregate; FIFO is [3k msg/s batched by default, up to 700k in high-throughput mode](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html) in the biggest regions | n/a (managed) | n/a | Multi-AZ, always |
| **Narad** | [~377k msg/s sustained, full produce → consume → ack, in batches of 100](../reference/capacity.md#batched-throughput) (2026) · [50k msg/s one message per request](../reference/capacity.md#measured-capacity) | 3 nodes; 4 to 6 busy cores each (batched) · 3 × 4.5 vCPU (per message) | ~25k per busy core (batched) · ~3.7k (per message, a floor) | fsync before ack, single copy, zstd |

What the Narad row is and is not. Both numbers are our own bench, not an [OpenMessaging](https://openmessaging.cloud/docs/benchmarks/) run on standard hardware. The batched number ran on shared Kubernetes nodes with 256-byte keyless messages and one copy of each partition; past 64 producers, produce kept rising but consume and ack fell behind, so about 377k is where the whole flow kept pace, not a produce ceiling ([every step](../reference/capacity.md#batched-throughput)). The per-message run ended because the **load generator saturated, not the broker**, so 50k is a floor. One copy per partition is the largest difference from the replicated Kafka, Pulsar and RabbitMQ numbers above, and it favors Narad. The fair reading: one message per request, Narad sits in the same fsync-durable class as RabbitMQ quorum queues and JetStream; batched, it moves hundreds of thousands of messages a second, still with every batch on disk before the `202`. For millions of messages per second, use [Kafka](#kafka).

### Same compute, measured ourselves {#same-compute-measured-ourselves}

Published numbers come from different hardware, years and vendors. So in August 2026 we also ran every self-hostable system here on **identical resources**, and reran Narad on 2026-10-01 from master (the produce, consume and ack work since v3.0.1, which shipped in **v3.1.0**): one broker at a time in Docker, 2 CPUs and 2 GB each, with the same driver and workload. The workload was 50,000 messages of 256 bytes. Every produce waited for the system's per-message confirmation, then a full consume and ack drain followed. Each system ran as a single node with no replication, in its default durable configuration.

The runs were laptop-grade (the Narad row is the median of three runs, the others are single runs), so treat the ordering as directional. It is still the only table on this page where "similar compute" is literally true.

| System | Produce msg/s | p50 / p99 | Consume+ack msg/s | What the produce ack means |
|---|---|---|---|---|
| **NATS JetStream** | 39,525 | 0.4 / 0.8 ms | 21,600 | in the R1 file stream, fsync every 2 min |
| **Redis Streams** | 31,643 | 0.5 / 0.8 ms | 41,698 | in the AOF buffer, fsync every 1 s |
| **Kafka** (6 partitions) | 14,728 | 0.8 / 4.0 ms | 7,830 | page cache, no per-message fsync (default) |
| **RabbitMQ** (quorum queue) | 13,014 | 1.2 / 1.9 ms | 14,361 | **fsynced** before the confirm |
| **Narad** (October 2026) | 10,454 | 1.5 / 2.5 ms | 9,239 | **fsynced** (group commit) before the 202 |
| **Pulsar** (standalone) | 7,632 | 2.0 / 3.0 ms | 10,984 | standalone disables the journal fsync |

The ordering mostly follows the durability column: the systems that do not fsync per confirm lead the produce column. Within the fsync-per-confirm class, RabbitMQ's quorum queue produced about 1.25 times faster than Narad on this workload and consumed and acked about 1.55 times faster. Narad's plain-HTTP consume also pays two round trips per message (consume, then ack) where binary protocols pipeline; the batch forms (**v3.1.0**), which carry up to 100 messages a round trip, were not used here; on three nodes they sustained about eight times the per-message rate ([Batched throughput](../reference/capacity.md#batched-throughput)). Both are real costs of the plain-HTTP design. Every system finished with zero produce failures and a full drain.

## Running cost {#running-cost}

Self-hosted systems cost machines; managed ones cost usage. The managed column assumes a steady 100 messages per second of 1 KB, about 259 million messages a month. Prices are for us-east-1 in August 2026. Self-hosted figures are three m6i.large-class EC2 instances plus disks, and leave out the people who operate them, which is usually the larger cost.

| | Minimum HA self-hosted footprint | About $/month self-hosted | Managed option, about $/month at 100 msg/s |
|---|---|---|---|
| **Narad** | 3 nodes, one binary each | ~$220 | ✗ none; you run it yourself |
| **Kafka** | 3 brokers (KRaft) | ~$235–445 | [MSK ~$477](https://aws.amazon.com/msk/pricing/) · [Confluent Basic ~$30](https://www.confluent.io/confluent-cloud/pricing/) (usage-priced) |
| **NATS JetStream** | 3 nodes, one binary each | ~$220 | [Synadia Cloud $49–199](https://docs.synadia.com/cloud/pricing) |
| **RabbitMQ** | 3-node quorum cluster | ~$220 | [Amazon MQ ~$651](https://aws.amazon.com/amazon-mq/pricing/) · [CloudAMQP HA ~$297](https://www.cloudamqp.com/plans.html) |
| **SQS** | ✗ can't self-host | n/a | [~$31 batched / ~$311 unbatched](https://aws.amazon.com/sqs/pricing/) ($0.40/M requests × 3 calls per message) |
| **Redis Streams** | primary + replica + sentinels | ~$160 | [ElastiCache ~$218](https://aws.amazon.com/elasticache/pricing/) · Redis Cloud from ~$25 |
| **Pulsar** | 3 brokers + 3 bookies + 3 metadata nodes [per the official docs](https://pulsar.apache.org/docs/4.0.x/deploy-bare-metal/) | ~$725 | [StreamNative ~$75–110](https://streamnative.io/pricing) |

At low, steady volume the usage-priced services (SQS with batching, Confluent Basic) cost almost nothing, and self-hosting any broker to save $30 a month does not pay. Self-hosting wins when volume grows: SQS at 5,000 msg/s unbatched is about $15k a month, while a 3-node cluster stays about $220. It also wins when payloads are binary, or when the queue cannot leave your network. Narad's cost profile matches NATS, the lightest self-hosted tier. Pulsar's minimum footprint costs about three times the others.

## Lock-in and exit paths {#lock-in}

| | License | Protocol | Who steers it | Exit path |
|---|---|---|---|---|
| **Narad** | Apache 2.0 | Plain HTTP | One young project with [one maintainer](https://github.com/DebanganThakuria/narad/blob/master/MAINTAINERS.md): the bus factor is one | Replay any topic over HTTP into anything; no SDK to unwind |
| **Kafka** | Apache 2.0 | De facto industry standard: Redpanda, WarpStream, AutoMQ and others reimplement it | ASF | The lowest lock-in of the log systems; your clients outlive your broker vendor |
| **NATS JetStream** | Apache 2.0, CNCF | NATS-specific | Synadia, which [moved to take the server out of the CNCF in 2025](https://www.cncf.io/blog/2025/05/01/cncf-and-synadia-align-on-securing-the-future-of-the-nats-io-project/) and then agreed to keep it there | Consumer-side migration; SDKs per language |
| **RabbitMQ** | MPL 2.0 | AMQP, an open standard | Broadcom (via VMware) | AMQP portability across brokers is real |
| **SQS** | Proprietary | AWS API + SigV4 | AWS | The deepest lock-in on this page; emulators cover local development, not production |
| **Redis Streams** | AGPLv3 (after the [2024 license change](https://redis.io/blog/agplv3/) and the 2025 partial return) | RESP, widely reimplemented | Redis Ltd., with [Valkey](https://valkey.io/) (BSD, Linux Foundation) as a compatible fork | Valkey speaks the same protocol, Streams included |
| **Pulsar** | Apache 2.0 | Pulsar-specific (a Kafka-compatible layer exists) | ASF | Fewest alternative implementations of the open-source options |

## Operating burden {#operating-burden}

The matrix's "deployment footprint" row, extended to running the system day to day. A rough ranking, lightest first:

1. **SQS**: nothing to operate; that is the product.
2. **NATS and Narad**: a single static binary with flat configuration. Narad has no client SDKs to keep in step, and any HTTP client, curl included, can inspect it. Its operating tasks start at [Deploy on Kubernetes](../operate/deploy-kubernetes.md).
3. **Redis**: simple until you need high availability, then Sentinel or Cluster, and its persistence caveats.
4. **RabbitMQ**: long-standing tooling and a good management UI, with Erlang tuning and cluster-partition handling to learn.
5. **Kafka**: KRaft removed ZooKeeper, but partition counts, consumer-group rebalancing and JVM tuning remain a skill set.
6. **Pulsar**: brokers, BookKeeper and a metadata store, each with its own operating knowledge, in exchange for the longest feature list.

## Choosing a broker {#choosing}

### Kafka

**Choose Kafka when** you are streaming events: high fan-in pipelines, stream processing, the Connect ecosystem, strict per-partition ordering, or millions of messages per second. Narad does not compete there.

**Choose Narad when** you need a job queue and were about to deploy a log to get one. Kafka's classic consumer groups track one offset per partition, so per-message acks, visibility timeouts, delayed delivery and dead-letter topics are yours to build. Share groups, added in Kafka 4, bring per-record acks, a time-limited lock that works like a visibility timeout, and a limit on delivery attempts. Delayed delivery is still yours to build. Narad starts from queue semantics and keeps a replayable log underneath.

### NATS JetStream

The closest system on this page: also a single binary, also Raft, also lease-style redelivery. The main difference is the protocol. JetStream speaks the NATS protocol, with an SDK per language. Narad speaks only HTTP, so a shell script, a cron job and a Python service are all clients with no extra dependencies.

They also delay messages differently. JetStream, since 2.12, can schedule a single message for a future time. Narad's [delay children](../build/fanout-and-delay.md#delay-children) hold a copy of every message in a topic for a fixed delay. Narad's fan-out children are also independent topics with their own retention. JetStream offers synchronous replication and years of production use. If JetStream fits your team and an SDK per language is acceptable, it is a good choice.

### RabbitMQ

**Choose RabbitMQ when** routing is your problem: topic exchanges, header matching and complex delivery topologies. Narad does not attempt that.

**Choose Narad when** you use RabbitMQ only as a work queue and no longer want its costs: AMQP client libraries, Erlang tuning, recovery from cluster partitions, and a plugin for delays.

### SQS

The closest match in semantics: visibility timeouts, receipt handles and at-least-once delivery, so SQS users already know Narad's consume model. **Choose SQS when** you run on AWS and want no servers at all; a fully managed queue is a real advantage.

**Choose Narad when** you want those semantics self-hosted: no cloud lock-in, no per-request bill at high volume, and **binary payloads**. SQS bodies are text only, up to 1 MiB, so every image or protobuf needs base64 on the client. Narad also offers fan-out without SNS in front, and replay, where an SQS message is gone once a consumer deletes it.

### Redis Streams

**Choose Redis Streams when** you already run Redis, your queue fits in memory, and sub-millisecond latency matters more than durability. **Choose Narad when** the queue is the system of record: an fsync before every `202`, retention bounded by disk rather than memory, and durability that does not depend on snapshot or AOF settings.

### Pulsar

The richest feature set on this page: native delayed delivery, tiered storage and multi-tenancy. **Choose Pulsar when** you have a platform team to run it: brokers, BookKeeper and a metadata store make the largest footprint here. **Choose Narad when** the features you need (queues, delay, fan-out, replay) fit in one binary that one engineer can operate and read.

## Narad's trade-offs {#trade-offs}

What Narad gives up, in one place:

- **No ordering guarantee.** Narad spends ordering on availability. If you need a sequence, carry one in the payload; the [Delivery contract](../understand/delivery-contract.md#ordering) lists every way order breaks.
- **No synchronous replication.** Each partition has one owner. The [replica child](../operate/backups.md#replica-children) pattern is asynchronous and opt-in.
- **No stream-processing ecosystem.**
- **A short track record.** The first release was in June 2026. v1.0.0 shipped after 300M+ soaked messages and a chaos matrix ([release notes](https://github.com/DebanganThakuria/narad/releases/tag/v1.0.0)), and every pull request now runs a three-node cluster through node restarts. That is little next to more than a decade of production use for Kafka and RabbitMQ.

If one of these is a hard requirement, pick the tool that has it. If your problem is a durable work queue over plain HTTP, run as one binary, that is what Narad was built for.

## Next steps {#next-steps}

- [Quickstart](quickstart.md): run Narad on your machine and send a message in about five minutes.
- [Delivery contract](../understand/delivery-contract.md): every promise Narad makes, and the failures that bend them.
- [Capacity and disk sizing](../reference/capacity.md): the measured numbers behind the throughput row, and how to size a cluster.

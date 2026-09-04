# Narad vs. Everything Else

One comparison table teaches faster than a thousand docs pages. So here it is, written with a rule: **when in doubt, the other system gets the benefit.** Every one of these tools is excellent at what it was built for; the honest question is never "which is best" but "which was built for your problem." Where Narad loses, this page says so.

## The matrix

| | **Narad** | **Kafka** | **NATS JetStream** | **RabbitMQ** | **SQS** | **Redis Streams** | **Pulsar** |
|---|---|---|---|---|---|---|---|
| **Core model** | Queue-first on durable logs | Partitioned ordered log | Streams + consumers | Broker with exchanges/queues | Managed queue | In-memory log + consumer groups | Segmented log |
| **Client protocol** | **Plain HTTP: curl is a client** | Binary protocol, SDK required | NATS protocol, SDK required | AMQP, SDK required | HTTP + SigV4 signing (SDK in practice) | RESP, client library | Binary protocol, SDK required |
| **Per-message ack + visibility lease** | ✓ native | ✗ (offsets only; build it yourself) | ✓ (ack wait + redelivery) | ✓ | ✓ (the model Narad's leases resemble) | ✓ (PEL + claim) | ✓ |
| **Delayed delivery** | ✓ native (delayed child topics) | ✗ | ✗ native (NAK-with-delay on redelivery only) | Plugin or TTL+DLX tricks | ✓ per-message, ≤15 min | ✗ | ✓ native, arbitrary |
| **Fan-out (one msg → many independent streams)** | ✓ native child topics | ✓ (consumer groups re-read the log) | ✓ (multiple consumers per stream) | ✓ (exchanges, its superpower) | Needs SNS in front | ✓ (multiple groups) | ✓ (subscriptions) |
| **Replay from offset** | ✓ native, non-destructive | ✓ native | ✓ native | ✗ (gone is gone) | ✗ | ✓ (XRANGE) | ✓ native |
| **Ordering** | **✗, deliberately none** | ✓ per partition | ✓ per stream | ✓ per queue (mostly) | FIFO queues only, throughput-capped | ✓ per stream | ✓ per partition |
| **Replication** | Async opt-in per topic (fan-out replica) | ✓ synchronous (ISR) | ✓ Raft (R3/R5) | ✓ quorum queues | Managed, invisible | Async (loss windows) | ✓ BookKeeper quorums |
| **Binary payloads over the wire API** | ✓ raw octet-stream | ✓ (opaque bytes) | ✓ | ✓ | ✗ text-only bodies, 1 MiB cap | ✓ | ✓ |
| **Deployment footprint** | **1 binary, Raft inside** | Brokers + KRaft (historically ZooKeeper) | 1 binary | 1 broker (Erlang runtime) | Zero: it's AWS's problem | Your existing Redis | Brokers + BookKeeper (+ZK/Oxia) |
| **Runs on your laptop unchanged** | ✓ | Heavier | ✓ | ✓ | ✗ (emulators only) | ✓ | Painful |
| **Stream processing ecosystem** | ✗ | ✓✓ (Streams, Connect, ksql) | Modest | ✗ | ✗ | ✗ | ✓ (Functions) |
| **Maturity** | **Weeks old**: honest evidence, tiny track record | Decades, battle-hardened everywhere | Mature, CNCF | Decades | Fully managed since 2006 | Mature | Mature |

## Beyond features

Features tell you what a system *does*. The next four tables cover what it costs you to run one, what its promises actually mean, and how hard it is to leave. Same rule as above: when in doubt, the other system gets the benefit. All prices and citations are as of August 2026.

### What an ack actually means (durability)

The most misunderstood dimension on this page. "Durable" hides three separate questions: is the message **on disk** when you get your ack, is it **on more than one machine**, and **what faults can still lose it**?

| | Ack means fsynced? | Replication | Acked-message loss windows |
|---|---|---|---|
| **Narad** | **✓: group-commit fsync before the 202** | ✗ single copy per partition; [replica children](client/fanout-and-delay.md) are async, opt-in | Losing a node's disk loses its partitions; plan volume snapshots or replica children |
| **Kafka** | ✗ by default: flush to disk is effectively disabled ([`log.flush.interval.messages` = Long.MAX](https://kafka.apache.org/documentation/#brokerconfigs)); durability comes from replication | RF=3 by convention, sync to the ISR with `acks=all` | `acks=1`; ISR shrunk to 1 with `min.insync.replicas=1`; correlated power loss across replicas |
| **NATS JetStream** | ✗ by default: file storage fsyncs on a **2-minute interval**; `sync_always` exists but docs warn it drops to a few hundred msg/s | R1 by default; R3/R5 is Raft, but the quorum ack is in-memory | [Jepsen (Dec 2025, v2.12.1)](https://jepsen.io/analyses/nats-2.12.1) demonstrated acked-write loss **even at R3/R5** under crash faults; R1 power loss can drop up to 2 min |
| **RabbitMQ** | ✓ quorum queues fsync on a quorum before the confirm; ✗ [streams don't fsync](https://www.rabbitmq.com/docs/streams) (OS writeback, like Kafka) | 3 Raft members | Quorum queues: majority disk loss only. Streams: correlated quorum power failure |
| **SQS** | Stored redundantly across multiple AZs before the response (internals opaque, but that's the contract) | Multi-AZ, managed | None documented |
| **Redis Streams** | ✗: default is RDB snapshots; AOF `everysec` still loses ~1 s; `appendfsync always` collapses to disk fsync rate | None by default; replication is **always async** (`WAIT` is best-effort) | Failover discards replication lag; [the docs say so themselves](https://redis.io/docs/latest/operate/oss_and_stack/management/replication/) |
| **Pulsar** | **✓: BookKeeper fsyncs the journal on the ack quorum by default** (standalone mode: no) | E=2/W=2/A=2 defaults, synchronous | Narrow: needs `journalSyncData=false` (a common perf tuning) or simultaneous loss of the ack quorum |

Honest placement: Narad and Pulsar and RabbitMQ quorum queues are the only ones here that fsync before acking *by default*. Narad then concedes the other axis: it has **no synchronous replication**, so a dead disk is data loss where Kafka/JetStream-R3/quorum queues survive it. Different systems, different failure they're paranoid about. And Kafka, RabbitMQ, NATS, and Redis have all been through [Jepsen](https://jepsen.io/analyses); Narad's evidence is its own chaos matrix, self-administered, worth exactly that.

### Messages per second on similar compute

Cross-system benchmarks are the least honest genre in infrastructure, so ground rules: every number below is from the linked source, with its hardware and durability config attached, because **the fsync asymmetry above is the biggest confounder**: Kafka's headline numbers don't fsync per message; Pulsar's do. Message sizes differ. Treat everything as order-of-magnitude.

| System | Published result | Compute | ≈ per broker vCPU | Config during test |
|---|---|---|---|---|
| **Kafka** | [605k msg/s @ 1 KB](https://www.confluent.io/blog/kafka-fastest-messaging-system/) (2020) · [~1M msg/s @ 1 KiB](https://jack-vanlightly.com/blog/2023/5/15/kafka-vs-redpanda-performance-do-the-claims-add-up) (2023, independent re-run) | 3 nodes, 24 / 72 vCPU | ~14k–25k | RF=3, `acks=all`, **no fsync** |
| **Kafka, fsync per message** | [420k msg/s @ 1 KB](https://streamnative.io/blog/perspective-on-pulsars-performance-compared-to-kafka) (2020) | 3 nodes, 24 vCPU | ~17k | RF=3, `flush.messages=1` |
| **Pulsar** | [300k](https://streamnative.io/blog/perspective-on-pulsars-performance-compared-to-kafka)–[800k msg/s @ 1 KB](https://streamnative.io/blog/apache-pulsar-vs-apache-kafka-2022-benchmark) (2020/2022, vendor) | 3 nodes, 24 / 72 vCPU | ~11k–13k | RF=3 equivalent, **journal fsync on** |
| **RabbitMQ quorum queues** | [66–67k msg/s @ 1 KB](https://www.rabbitmq.com/blog/2020/06/21/cluster-sizing-case-study-quorum-queues-part-1) (2020, official) | 72–112 vCPU clusters | ~0.6k–0.9k | RF=3, confirms, **fsync** |
| **NATS JetStream** | [134k msg/s @ 128 B](https://github.com/nats-io/nats-server/discussions/7599) (file R1, real network; the [official docs number](https://docs.nats.io/using-nats/nats-tools/nats_cli/natsbench), 404k, is loopback on a MacBook) | 8 cores | ~17k | R1, async batch publish, lazy fsync. R3 costs [~40% more throughput](https://amirhossein-najafizadeh.medium.com/benchmarking-nats-jetstream-cluster-hypothesis-testing-for-enhancements-4a500d11ce7b) |
| **Redis Streams** | [~136k msg/s unpipelined](https://learn.arm.com/learning-paths/servers-and-cloud-computing/redis-cobalt/redis-benchmark-and-validation/) · [0.5–1M pipelined](https://redis.io/docs/latest/develop/data-types/streams/) (tiny payloads) | 1 core (single-threaded) | ~136k | RDB only: the fastest number here carries the weakest durability |
| **SQS** | Quota-bound, not compute-bound: standard is effectively unlimited in aggregate; FIFO is [3k msg/s batched by default, up to 700k in high-throughput mode](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html) in the biggest regions | n/a (managed) | n/a | Multi-AZ, always |
| **Narad** | [50k msg/s sustained, full produce → consume → ack](operate/scaling-and-recovery.md#measured-capacity) | 3 nodes × 4.5 vCPU | ~3.7k (a floor, not a peak) | fsync before ack, single copy, zstd |

What the Narad row honestly is and isn't: the run ended because the **load generator saturated, not the broker**, so 50k is a floor, but it is our own bench, not an [OpenMessaging](https://openmessaging.cloud/docs/benchmarks/) run on standardized hardware, and we haven't measured the saturation point. Until an OMB-style run exists, the fair reading is: Narad sits in the "tens of thousands per node, fsync-durable" class with RabbitMQ quorum queues and JetStream, an order of magnitude below Kafka's log-append ceiling, which is fine, because [we already told you](#kafka) to use Kafka if you need millions per second.

#### Same compute, measured ourselves

Published numbers come from different hardware, years, and vendors, so we also ran every system here (SQS can't be self-hosted) on **identical resources**: one broker at a time in Docker, 2 CPUs / 2 GB each, same driver, same workload (50k × 256 B; every produce waits for the system's per-message confirmation, then a full consume+ack drain; single node, no replication for anyone; each system in its default durable config). Laptop-grade and single-run (treat the ordering as directional), but it is the only table on this page where "similar compute" is literally true:

| System | Produce msg/s | p50 / p99 | Consume+ack msg/s | What the produce ack means |
|---|---|---|---|---|
| **NATS JetStream** | 39,525 | 0.4 / 0.8 ms | 21,600 | in the R1 file stream, fsync every 2 min |
| **Redis Streams** | 31,643 | 0.5 / 0.8 ms | 41,698 | in the AOF buffer, fsync every 1 s |
| **Kafka** (6 partitions) | 14,728 | 0.8 / 4.0 ms | 7,830 | page cache, no per-message fsync (default) |
| **RabbitMQ** (quorum queue) | 13,014 | 1.2 / 1.9 ms | 14,361 | **fsynced** before the confirm |
| **Pulsar** (standalone) | 7,632 | 2.0 / 3.0 ms | 10,984 | standalone disables the journal fsync |
| **Narad** | 5,597 | 2.0 / 10.0 ms | 8,567 | **fsynced** (group commit) before the 202 |

The ordering is mostly the durability column priced in milliseconds: the systems that don't fsync per confirm top the produce chart; that's the trade, made visible. Within the fsync-per-confirm class, RabbitMQ's quorum queue outproduced Narad ~2.3× on this workload, and Narad's plain-HTTP consume pays two round trips per message where binary protocols pipeline: real costs of the curl-is-a-client design, stated rather than hidden. Zero produce failures and a full drain for every system.

### What it costs to run

Two very different bills: machines (self-hosted) or usage (managed). Worked example for the managed column: a steady 100 msg/s × 1 KB ≈ 259M messages/month. Prices are us-east-1, August 2026; self-hosted figures are 3× m6i.large-class EC2 + disks and exclude ops labor, which is the real cost, and the reason "zero servers" is a feature.

| | Minimum HA self-hosted footprint | ≈ $/month self-hosted | Managed option, ≈ $/month at 100 msg/s |
|---|---|---|---|
| **Narad** | 3 nodes, one binary each | ~$220 | ✗ none; you run it, that's the deal |
| **Kafka** | 3 brokers (KRaft) | ~$235–445 | [MSK ~$477](https://aws.amazon.com/msk/pricing/) · [Confluent Basic ~$30](https://www.confluent.io/confluent-cloud/pricing/) (usage-priced) |
| **NATS JetStream** | 3 nodes, one binary each | ~$220 | [Synadia Cloud $49–199](https://docs.synadia.com/cloud/pricing) |
| **RabbitMQ** | 3-node quorum cluster | ~$220 | [Amazon MQ ~$651](https://aws.amazon.com/amazon-mq/pricing/) · [CloudAMQP HA ~$297](https://www.cloudamqp.com/plans.html) |
| **SQS** | ✗ can't self-host | n/a | [~$31 batched / ~$311 unbatched](https://aws.amazon.com/sqs/pricing/) ($0.40/M requests × 3 calls per message) |
| **Redis Streams** | primary + replica + sentinels | ~$160 | [ElastiCache ~$218](https://aws.amazon.com/elasticache/pricing/) · Redis Cloud from ~$25 |
| **Pulsar** | 3 brokers + 3 bookies + 3 metadata nodes [per the official docs](https://pulsar.apache.org/docs/4.0.x/deploy-bare-metal/) | ~$725 | [StreamNative ~$75–110](https://streamnative.io/pricing) |

The pattern: at low, steady volume the usage-priced managed services (SQS batched, Confluent Basic) are nearly free and unbeatable; self-hosting *anything* to save $30/month is bad math. Self-hosting wins when volume grows (SQS at 5,000 msg/s unbatched is ~$15k/month; a 3-node cluster is still ~$220), when payloads are binary, or when the queue can't leave your network. Narad's cost profile is the same as NATS's: the lightest self-hosted tier. Pulsar's floor is ~3× everyone else's: that's the platform-team tax in dollars.

### Lock-in

| | License | Protocol | Who steers it | Exit path |
|---|---|---|---|---|
| **Narad** | Apache 2.0 | Plain HTTP | One young project; the bus factor is real and we won't pretend otherwise | Replay any topic over HTTP into anything; no SDK to unwind |
| **Kafka** | Apache 2.0 | De-facto industry standard: Redpanda, WarpStream, AutoMQ and others reimplement it | ASF | The lowest lock-in of the log systems; your clients outlive your broker vendor |
| **NATS JetStream** | Apache 2.0, CNCF | NATS-specific | Synadia, who [moved to pull the server out of CNCF in 2025](https://www.cncf.io/blog/2025/05/01/cncf-and-synadia-align-on-securing-the-future-of-the-nats-io-project/) before backing down. Resolved, but it happened | Consumer-side migration; SDKs per language |
| **RabbitMQ** | MPL 2.0 | AMQP, an actual open standard | Broadcom (via VMware) | AMQP portability across brokers is real |
| **SQS** | Proprietary | AWS API + SigV4 | AWS | The deepest lock-in on this page; emulators cover local dev, not production |
| **Redis Streams** | AGPLv3 (after the [2024 license exit](https://redis.io/blog/agplv3/) and 2025 partial return) | RESP, widely reimplemented | Redis Ltd., with [Valkey](https://valkey.io/) (BSD, Linux Foundation) as the fork insurance policy | Valkey speaks the same protocol, Streams included |
| **Pulsar** | Apache 2.0 | Pulsar-specific (a Kafka-compat layer exists) | ASF | Fewest alternative implementations of the open-source options |

### Operating burden

The matrix's "deployment footprint" row, extended to day 2. Rough ranking, lightest first: **SQS** (there is no day 2; that's the product), then **NATS and Narad** (single static binary, flat config; Narad adds the operational surface described in [Operate](operate/index.md), and subtracts client SDKs; `curl` debugging is an underrated ops feature), then **Redis** (trivial until HA; then sentinels/cluster and the persistence asterisks), then **RabbitMQ** (decades of tooling and a good management UI, but Erlang tuning and cluster-partition lore come with it), then **Kafka** (KRaft killed ZooKeeper, but partition counts, consumer-group rebalancing, and JVM care remain a skill set), and finally **Pulsar** (three subsystems that each want understanding, the price of its feature list).

## System by system

### Kafka

**Choose Kafka when** you're event-streaming: high-fan-in pipelines, stream processing, the Connect ecosystem, strict per-partition ordering, or throughput in the millions of messages per second. Nothing here competes with that, including Narad.

**Choose Narad when** what you actually need is a *job queue* and you were about to deploy a log to get one. Kafka has no per-message acks, no visibility timeouts, no delayed delivery, no DLQ; you build all of it on top of consumer groups, and consumer-group rebalancing becomes your on-call's hobby. Narad starts from queue semantics and keeps the replayable log underneath.

### NATS JetStream

The closest cousin on this page, and a system we genuinely admire: also a single binary, also Raft, also lease-style redelivery. The differences are philosophical: JetStream speaks the NATS protocol (SDK per language); Narad is HTTP-only, so your shell script, cron job, and Python service are equal citizens with zero dependencies. Narad adds native *delayed topics* (JetStream can delay a redelivery via NAK, but not a first delivery) and fan-out children that are full independent topics with their own retention. JetStream counters with synchronous replication and years of production maturity. **If JetStream fits your brain and your team is happy vendoring SDKs, use it with our blessing.**

### RabbitMQ

**Choose RabbitMQ when** routing *is* your problem: topic exchanges, header matching, complex delivery topologies. That flexibility is real and Narad doesn't attempt it.

**Choose Narad when** you use RabbitMQ as "just a work queue" and the price (AMQP client libraries, Erlang runtime tuning, cluster partitions healing, a plugin for delayed messages) stopped feeling worth it.

### SQS

The semantics twin: visibility timeouts, receipt handles, at-least-once: if you know SQS, Narad's consume model needs no explanation. **Choose SQS when** you're all-in on AWS and want zero servers; fully managed is a real feature.

**Choose Narad when** you want those semantics *self-hosted*: no cloud lock-in, no per-request bill at high volume, **binary payloads** (SQS bodies are text-only, 256 KB; every image or protobuf gets client-side base64), native fan-out without bolting SNS in front, and replay (SQS messages, once consumed, are simply gone).

### Redis Streams

**Choose Redis Streams when** you already run Redis, your queue fits in RAM, and sub-millisecond latency matters more than durability guarantees. **Choose Narad when** the queue *is* the system of record: fsync-before-ack durability, disk-bounded retention instead of RAM-bounded, and a broker whose persistence story doesn't have an asterisk.

### Pulsar

Feature-for-feature the richest system here: native delayed delivery, tiered storage, multi-tenancy. **Choose Pulsar when** you have a platform team to feed it: brokers plus BookKeeper plus coordination is the heaviest footprint on this page. **Choose Narad when** the feature list you actually need (queues, delay, fan-out, replay) fits in one binary a single engineer can operate and read.

## What Narad concedes, in one place

So nobody has to hunt for it: **no ordering guarantee** (AP by design; if you need a sequence, carry one in the payload), **no synchronous replication** (single-owner partitions; the [replica pattern](client/fanout-and-delay.md) is async and opt-in), **no stream processing ecosystem**, and **no decade of production scars**: the [evidence](https://github.com/DebanganThakuria/narad/releases/tag/v1.0.0) is 300M+ soaked messages and a chaos matrix, which is a lot for week one and nothing next to Kafka's twenty years. If any single row above is your hard requirement, pick the tool that has it. If the *shape* of your problem is "durable work queue, plain HTTP, one binary, honest tradeoffs", that's the lane this whole system was built for.

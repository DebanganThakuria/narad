# Narad vs. Everything Else

One comparison table teaches faster than a thousand docs pages. So here it is — written with a rule: **when in doubt, the other system gets the benefit.** Every one of these tools is excellent at what it was built for; the honest question is never "which is best" but "which was built for your problem." Where Narad loses, this page says so.

## The matrix

| | **Narad** | **Kafka** | **NATS JetStream** | **RabbitMQ** | **SQS** | **Redis Streams** | **Pulsar** |
|---|---|---|---|---|---|---|---|
| **Core model** | Queue-first on durable logs | Partitioned ordered log | Streams + consumers | Broker with exchanges/queues | Managed queue | In-memory log + consumer groups | Segmented log |
| **Client protocol** | **Plain HTTP — curl is a client** | Binary protocol, SDK required | NATS protocol, SDK required | AMQP, SDK required | HTTP + SigV4 signing (SDK in practice) | RESP, client library | Binary protocol, SDK required |
| **Per-message ack + visibility lease** | ✓ native | ✗ (offsets only — build it yourself) | ✓ (ack wait + redelivery) | ✓ | ✓ (the model Narad's leases resemble) | ✓ (PEL + claim) | ✓ |
| **Delayed delivery** | ✓ native (delayed child topics) | ✗ | ✗ native (NAK-with-delay on redelivery only) | Plugin or TTL+DLX tricks | ✓ per-message, ≤15 min | ✗ | ✓ native, arbitrary |
| **Fan-out (one msg → many independent streams)** | ✓ native child topics | ✓ (consumer groups re-read the log) | ✓ (multiple consumers per stream) | ✓ (exchanges — its superpower) | Needs SNS in front | ✓ (multiple groups) | ✓ (subscriptions) |
| **Replay from offset** | ✓ native, non-destructive | ✓ native | ✓ native | ✗ (gone is gone) | ✗ | ✓ (XRANGE) | ✓ native |
| **Ordering** | **✗ — deliberately none** | ✓ per partition | ✓ per stream | ✓ per queue (mostly) | FIFO queues only, throughput-capped | ✓ per stream | ✓ per partition |
| **Replication** | Async opt-in per topic (fan-out replica) | ✓ synchronous (ISR) | ✓ Raft (R3/R5) | ✓ quorum queues | Managed, invisible | Async (loss windows) | ✓ BookKeeper quorums |
| **Binary payloads over the wire API** | ✓ raw octet-stream | ✓ (opaque bytes) | ✓ | ✓ | ✗ text-only bodies, 256 KB cap | ✓ | ✓ |
| **Deployment footprint** | **1 binary, Raft inside** | Brokers + KRaft (historically ZooKeeper) | 1 binary | 1 broker (Erlang runtime) | Zero — it's AWS's problem | Your existing Redis | Brokers + BookKeeper (+ZK/Oxia) |
| **Runs on your laptop unchanged** | ✓ | Heavier | ✓ | ✓ | ✗ (emulators only) | ✓ | Painful |
| **Stream processing ecosystem** | ✗ | ✓✓ (Streams, Connect, ksql) | Modest | ✗ | ✗ | ✗ | ✓ (Functions) |
| **Maturity** | **Weeks old** — honest evidence, tiny track record | Decades, battle-hardened everywhere | Mature, CNCF | Decades | Fully managed since 2006 | Mature | Mature |

## System by system

### Kafka

**Choose Kafka when** you're event-streaming: high-fan-in pipelines, stream processing, the Connect ecosystem, strict per-partition ordering, or throughput in the millions of messages per second. Nothing here competes with that, including Narad.

**Choose Narad when** what you actually need is a *job queue* and you were about to deploy a log to get one. Kafka has no per-message acks, no visibility timeouts, no delayed delivery, no DLQ — you build all of it on top of consumer groups, and consumer-group rebalancing becomes your on-call's hobby. Narad starts from queue semantics and keeps the replayable log underneath.

### NATS JetStream

The closest cousin on this page, and a system we genuinely admire — also a single binary, also Raft, also lease-style redelivery. The differences are philosophical: JetStream speaks the NATS protocol (SDK per language); Narad is HTTP-only, so your shell script, cron job, and Python service are equal citizens with zero dependencies. Narad adds native *delayed topics* (JetStream can delay a redelivery via NAK, but not a first delivery) and fan-out children that are full independent topics with their own retention. JetStream counters with synchronous replication and years of production maturity. **If JetStream fits your brain and your team is happy vendoring SDKs, use it with our blessing.**

### RabbitMQ

**Choose RabbitMQ when** routing *is* your problem — topic exchanges, header matching, complex delivery topologies. That flexibility is real and Narad doesn't attempt it.

**Choose Narad when** you use RabbitMQ as "just a work queue" and the price — AMQP client libraries, Erlang runtime tuning, cluster partitions healing, a plugin for delayed messages — stopped feeling worth it.

### SQS

The semantics twin: visibility timeouts, receipt handles, at-least-once — if you know SQS, Narad's consume model needs no explanation. **Choose SQS when** you're all-in on AWS and want zero servers; fully managed is a real feature.

**Choose Narad when** you want those semantics *self-hosted*: no cloud lock-in, no per-request bill at high volume, **binary payloads** (SQS bodies are text-only, 256 KB — every image or protobuf gets client-side base64), native fan-out without bolting SNS in front, and replay — SQS messages, once consumed, are simply gone.

### Redis Streams

**Choose Redis Streams when** you already run Redis, your queue fits in RAM, and sub-millisecond latency matters more than durability guarantees. **Choose Narad when** the queue *is* the system of record: fsync-before-ack durability, disk-bounded retention instead of RAM-bounded, and a broker whose persistence story doesn't have an asterisk.

### Pulsar

Feature-for-feature the richest system here — native delayed delivery, tiered storage, multi-tenancy. **Choose Pulsar when** you have a platform team to feed it: brokers plus BookKeeper plus coordination is the heaviest footprint on this page. **Choose Narad when** the feature list you actually need (queues, delay, fan-out, replay) fits in one binary a single engineer can operate and read.

## What Narad concedes, in one place

So nobody has to hunt for it: **no ordering guarantee** (AP by design — if you need a sequence, carry one in the payload), **no synchronous replication** (single-owner partitions; the [replica pattern](client/fanout-and-delay.md) is async and opt-in), **no stream processing ecosystem**, and **no decade of production scars** — the [evidence](https://github.com/DebanganThakuria/narad/releases/tag/v1.0.0) is 300M+ soaked messages and a chaos matrix, which is a lot for week one and nothing next to Kafka's twenty years. If any single row above is your hard requirement, pick the tool that has it. If the *shape* of your problem is "durable work queue, plain HTTP, one binary, honest tradeoffs" — that's the lane this whole system was built for.

---
description: "Look up the throughput Narad has been measured at, and estimate how much disk each node needs."
---

# Capacity and disk sizing

Look up the throughput Narad has been measured at, and estimate how much disk each node needs.

To measure your own cluster, run the CLI's load generator from a client machine:

```sh title="Command"
narad bench orders --count 10000 --consume
```

It produces 10,000 messages of 256 bytes from 8 workers, then consumes and acks them, and prints the produce rate with its p50, p95 and p99 latency, then the consume-and-ack rate. `--size` and `--workers` change the payload and the concurrency ([CLI command reference](cli.md#bench)). Numbers from one client machine are a floor: the client often saturates before the cluster does.

## Measured capacity {#measured-capacity}

| Setup | Result | Conditions |
|---|---|---|
| 3 nodes, 4.5 vCPU each (13.5 vCPU in total) | 50,000 messages/s sustained through produce, consume and ack; about 3,700 messages/s per vCPU | Every produce synced to disk before its `202`, one copy of each partition, zstd compression on. The run ended because the load generator saturated, so this is a floor, not a ceiling. |
| 3 nodes on a shared Kubernetes cluster, **v3.2.1**, October 2026, batch API | 376,673 messages/s sustained through produce, consume and ack in batches of 100, with every message delivered; produce alone reached 623,706/s | The brokers used 4 to 6 CPU cores each. Each batch synced to disk before its `202`, one copy of each partition, zstd. Method and every step: [Batched throughput on three nodes](#batched-throughput). |
| 1 node in Docker, 2 CPUs and 2 GB, October 2026 (master, shortly before **v3.1.0**) | 10,454 messages/s produced (p50 1.5 ms, p99 2.5 ms), then 9,239 messages/s consumed and acked; 50,000 messages of 256 bytes | The median of three runs on a laptop; an August 2026 build measured 5,597 and 8,567 on the same test. The same test of five other brokers on the same resources is in [Compare Narad with other brokers](../get-started/compare.md#same-compute-measured-ourselves). |

Syncing to disk before answering costs throughput; that is the price of what a [`202` promises](../understand/delivery-contract.md#what-202-means). Batch produce (**v3.1.0**) shares one sync across up to 100 messages (1,000 from v3.2.0), and batch consume and ack share one round trip across up to 100; see [Produce a batch](http-api.md#produce-batch). On the same three nodes, batches of 100 moved about eight times as many messages as one message per request: [Batched throughput on three nodes](#batched-throughput).

## Batched throughput on three nodes {#batched-throughput}

In October 2026 we measured how far the batch API carries a three-node cluster, end to end: every message produced, consumed and acked, and every one checked off.

**The cluster.** Three brokers on **v3.2.1**, deployed with the Helm chart on a shared Kubernetes cluster. Each broker pod requested 0.5 CPU and had no CPU limit, on nodes of 8 to 16 vCPU shared with other workloads. Security was on (Basic auth and grants), Raft used mutual TLS, `storage.codec` was zstd, and each pod had a 10 GiB gp3 volume. Every batch was synced to disk before its `202`, and each partition had one copy.

**The load.** Four or six load pods ran on other nodes, each built on the Go client ([narad-go](https://github.com/DebanganThakuria/narad-go)) and each logged in as its own user. Every pod created three topics of 24 partitions, so a step used 288 or 432 partitions in all. Each producer sent a batch of 100 keyless messages of about 256 bytes, waited for its `202`, and sent the next. Consumers took batches of 100 and acked them in batches, ten workers per topic. Producers ran for 60 seconds; consumers then drained what was left.

**The check.** Every message carried its producer and a sequence number. After each step, every message produced had been consumed and acked exactly once: none missing, no duplicates, about 135 million messages across the steps below.

| Load | Produced, msg/s | Consumed and acked while producing, msg/s | Sustained, msg/s | Messages |
|---|---|---|---|---|
| 4 pods, 32 producers | 221,521 | 221,344 | 220,783 | 13,293,800 |
| 4 pods, 32 producers, again | 215,403 | 215,212 | 213,984 | 12,926,700 |
| 4 pods, 48 producers | 297,059 | 295,848 | 292,680 | 17,835,700 |
| **4 pods, 64 producers** | **378,244** | **377,713** | **376,673** | 22,699,700 |
| 6 pods, 96 producers | 511,333 | 349,682 | 359,445 | 30,687,900 |
| 6 pods, 144 producers | 623,706 | 293,059 | 343,743 | 37,439,300 |

*Sustained* is the messages consumed and acked divided by the time from the first produce to the last ack, drain included: the rate at which the cluster finished the whole job.

**What it shows.**

- Up to 64 producers, consumption kept pace with production, so production was the limit and the sustained rate rose with the load, to about 377,000 messages a second.
- Past that, produce kept rising, to 623,706 a second, but consume and ack fell behind and the sustained rate dropped. Under that much produce load the brokers had less left for consumers. The brokers used 4 to 6 cores each at the peak.
- One message per request, on the same cluster the same day: about 50,500 messages a second produced and 47,200 consumed and acked while producing (1,800 producers and 3,600 consumers over six load pods). Batches of 100 carried about eight times as much.
- Each load pod has its own user because of the per-user cap on consumes in flight (`http.max_consume_in_flight_per_identity`, 1,024 per node by default), which counts a batch poll as its batch size. One user's workers times its batch size is what the cap sees ([Configuration reference](configuration.md#http)).

**Read it with these limits.**

- Shared nodes with other tenants, 60-second steps, and single runs except the 32-producer step (220,783 and 213,984). Expect your numbers to differ; measure your own.
- Keyless messages, no fan-out children, no schema. Children multiply what each partition writes; a schema adds validation to every produce.
- Batches of 100, the Go client's limit. Batch produce takes up to 1,000 messages from v3.2.0; that was not measured.
- Our own bench, not an [OpenMessaging](https://openmessaging.cloud/docs/benchmarks/) run on standard hardware, and one copy per partition, so it does not compare directly with replicated numbers from other systems.

To use the batch API from Go, see `ProduceBatch` and `WithBatch` in narad-go; over HTTP, [Produce a batch](http-api.md#produce-batch) and the batch forms of consume and ack in the [HTTP API reference](http-api.md). `narad bench` sends one message per request, so it measures the per-message rate.

## Disk sizing {#disk-sizing}

Per node, roughly:

```text title="Formula"
bytes per node ≈ (stored messages/s ÷ nodes)
                 × stored bytes per message
                 × retention in seconds
                 × 1.3
```

- **Stored messages/s** is the produce rate times the number of copies: 1 for the topic, plus 1 for each [fan-out child](glossary.md#fan-out-child), because every child stores its own full copy.
- **Stored bytes per message** is the payload plus the key plus about 14 bytes of per-message framing, and 27 bytes per frame of messages written together. With `storage.codec: zstd`, multiply by your compression ratio. For JSON-like payloads it runs from about 0.6 when messages trickle in (one message per frame) to about 0.05 under load (hundreds of similar messages per frame). Measure yours.
- **× 1.3** covers how retention deletes: a whole 64 MiB segment at a time, so a partition holds up to its retention plus the time it takes to fill one segment.
- **Also on the disk:** the cluster metadata (tens of MB) and the [ingress WAL](glossary.md#ingress-wal). The WAL removes what it has delivered, so it stays small in steady state, but it grows while messages wait for a partition owner that cannot be reached. With `storage.ingress_wal_prealloc` (**v3.1.0**) it keeps up to two more 64 MiB segments.

A worked example: 3 nodes; one topic with two children (3 copies); 100 messages/s produced; 250-byte JSON payloads with 20-byte keys; 12 hours of retention; zstd at an assumed ratio of 0.3.

```text title="Arithmetic"
stored messages/s per node = 100 × 3 ÷ 3                 = 100
stored bytes per message   = 250 + 20 + 14               = 284
retention                  = 12 h                        = 43,200 s
per node                   = 100 × 284 × 43,200 × 1.3 × 0.3
                           ≈ 0.48 GB
cluster                    ≈ 1.4 GB
```

Size volumes for the peak, not the average, and leave room for growth: a full disk stops the node from accepting produces. The Helm chart's default volume is 10 GiB per pod ([`persistence.size`](helm-values.md#persistence)). Watch `narad_data_dir_available_bytes` ([Metrics reference](metrics.md#storage-engine)).

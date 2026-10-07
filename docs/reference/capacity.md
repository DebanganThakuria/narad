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
| 1 node in Docker, 2 CPUs and 2 GB, October 2026 (master, shortly before **v3.1.0**) | 10,454 messages/s produced (p50 1.5 ms, p99 2.5 ms), then 9,239 messages/s consumed and acked; 50,000 messages of 256 bytes | The median of three runs on a laptop; an August 2026 build measured 5,597 and 8,567 on the same test. The same test of five other brokers on the same resources is in [Compare Narad with other brokers](../get-started/compare.md#same-compute-measured-ourselves). |

Syncing to disk before answering costs throughput; that is the price of what a [`202` promises](../understand/delivery-contract.md#what-202-means). Batch produce (**v3.1.0**) shares one sync across up to 100 messages (1,000 from v3.2.0); see [Produce a batch](http-api.md#produce-batch).

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

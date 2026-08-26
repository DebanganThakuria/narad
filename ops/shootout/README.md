# Same-compute shootout

One question, held constant: **messages per second through the full
produce → consume → ack flow, when every broker gets the same 2 CPUs
and 2 GB.** Each system runs *alone* in Docker (compose profiles), the
driver runs on the host, and the workload is identical: N messages of
S bytes, every produce waiting for the system's per-message confirmation,
then a full drain with explicit acks.

```sh
./run.sh                      # all: narad nats rabbitmq redis kafka
COUNT=100000 ./run.sh narad   # bigger run, one system
```

## The honesty table

Same compute does not mean same promise. Every system runs its
**default durable configuration**, and what the produce confirmation
actually buys differs — this is the single biggest reason cross-system
numbers mislead:

| system | the produce ack means | loss window on power loss |
|---|---|---|
| narad | group-commit **fsync** on the ingress WAL | none for acked messages (single copy: a dead disk is the risk) |
| nats-jetstream | written to the R1 file stream, fsync every 2 min (server default) | up to 2 min of acked writes |
| rabbitmq-quorum | quorum-queue Raft **fsync** before the confirm | none for confirmed messages (single member here) |
| redis-streams | in the AOF buffer, fsync every 1 s | up to 1 s of acked writes |
| kafka | acks=all to the page cache — **no per-message fsync by default** | OS writeback interval of acked writes |

So narad and rabbitmq pay for an fsync on every confirm; kafka, nats,
and redis do not. Read the throughput column with that column next to
it — that's the deal each system is offering, and the numbers are the
price.

Other deliberate choices, one per line so nobody hunts for them:

- **Single node everywhere.** No replication for anyone; this isolates
  the engine cost, not the cluster design.
- **Per-message confirmed produce, per-message-batch acks.** This is a
  work-queue workload (Narad's home turf). Log systems (Kafka) shine
  with fire-and-forget batching that this workload deliberately does
  not exercise — see the compare page for their headline numbers.
- **Driver on the host, brokers limited.** The client is intentionally
  unconstrained so the broker is the bottleneck under test.
- **Best effort, laptop-grade.** Relative order is meaningful because
  everything shares one machine and one workload; absolute numbers are
  not publishable benchmarks.

Results land in `results.jsonl`; `run.sh` prints the markdown table.

## Field result — 2026-08-26

50k × 256B, 16 workers, colima VM (arm64) on an Apple-silicon laptop,
each broker alone at 2 CPUs / 2 GB. Zero produce failures and a full
drain everywhere. Read the last column before comparing rows:

| system | produce msg/s | p50 | p99 | consume+ack msg/s | the produce ack means |
|---|---|---|---|---|---|
| narad | 5,597 | 2.0ms | 10.0ms | 8,567 | **fsync** (group commit) |
| nats-jetstream | 39,525 | 0.39ms | 0.77ms | 21,600 | no fsync (2 min interval) |
| rabbitmq-quorum | 13,014 | 1.2ms | 1.9ms | 14,361 | **fsync** (quorum WAL) |
| redis-streams | 31,643 | 0.49ms | 0.76ms | 41,698 | AOF buffer (everysec) |
| kafka (6 partitions) | 14,728 | 0.80ms | 4.0ms | 7,830 | page cache (no fsync) |
| pulsar-standalone | 7,632 | 2.0ms | 3.0ms | 10,984 | no fsync in standalone |

What it says, honestly: the non-fsync systems (NATS, Redis) top the
produce chart because their ack is cheaper — that's the durability
trade priced in milliseconds. Within the fsync-per-confirm class,
RabbitMQ's quorum queue outproduced Narad ~2.3× on this workload;
Narad's plain-HTTP protocol also pays two round trips per consumed
message where binary protocols pipeline. Narad's consume+ack beat
Kafka's group-rebalance-and-commit path, and its p99 was the widest —
the fsync tail. Single run, laptop-grade: treat the ordering as
directional, not gospel.

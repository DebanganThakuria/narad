# Narad v1 Semantics

This document defines the target semantics for Narad v1. It intentionally
keeps the system dumb, explicit, and operationally understandable.

Narad v1 is not trying to be Kafka. Narad is an SQS-like durable queue with
Kafka-like partition logs: simple HTTP APIs, at-least-once delivery, topic
fanout, native schemas, and any-node ingress.

## Product Contract

### Produce

Produce success means the message has been durably accepted by the Narad pod
that received the request.

Durably accepted means:

- the message is written to that pod's local ingress WAL;
- the WAL write participates in grouped fsync;
- once the produce response is returned, Narad expects the record to survive
  process restart and normal pod restart as long as the Kubernetes volume
  survives.

Produce success does not mean:

- the final owner partition has appended the message;
- the message is already visible to consumers;
- another Narad pod has a copy;
- Narad has deduplicated the message.

The produce response should return a message id and an accepted status. It
should not require a final partition offset unless the API explicitly asks to
wait for visibility.

### Visibility

Message visibility is asynchronous.

After ingress WAL acceptance, a background dispatcher sends the message to the
actual owner of the selected partition. The owner appends it to its partition
log. Once the owner commit path completes, the message becomes visible to
consumers.

Narad must expose this gap with metrics:

- ingress WAL pending records;
- oldest ingress WAL pending age;
- ingress to owner latency;
- produce accepted to visible latency.

### Consume

Consume provides at-least-once delivery.

Consumers must be idempotent. Narad may deliver a message more than once when:

- the consumer does not ack before the visibility timeout;
- the ack is lost or delayed;
- a process crashes during delivery/ack handling;
- a retried produce creates a new message.

Queue-style consume is not tied to a specific partition. A consumer asks for a
topic, and Narad returns a currently available message from any available
partition.

If the local pod has ready records in owned partitions, it should return those
first. If local partitions are empty, Narad may ask other nodes whether they
have ready messages. Remote probing is expected to be slower than local hits.

### Ack

Ack is best effort.

Ack success means Narad accepted the ack request. It does not guarantee the
owning partition has permanently applied it before the response returns.

Consumers must tolerate redelivery after successful ack. Receipt handles are
used to reject stale or malformed acks when possible, but ack durability is not
the core guarantee.

### Durability

Durability in Narad v1 means the record exists on some disk.

For produce, that disk is the ingress pod's persistent volume. For committed
partition data, that disk is the owner pod's persistent volume.

Kubernetes storage matters:

- a PVC backed by durable block storage is valid for this model;
- `emptyDir` and container-local writable layers are not durable;
- local persistent volumes are acceptable only if the operator accepts node
  affinity and delayed recovery when the physical node is unavailable.

### Replication

Narad v1 has no follower in the produce hot path.

Follower replication is not required for the v1 durability contract. A future
version may add async followers or configurable replicated durability tiers,
but v1 should not block produce latency on cross-node replication.

### Failure Model

If a pod dies but its volume survives, its ingress WAL and partition logs remain
on disk. When the pod comes back, Narad replays the WAL and continues moving
accepted messages into their final owner partitions.

If a pod or its volume is unavailable, the partitions owned by that pod are
temporarily unavailable. The topic can still make progress through other
partitions. Operators should run at least three Narad pods and create topics
with at least three partitions for basic partial availability.

If a volume is permanently lost, Narad v1 can lose records stored only on that
volume. This is an explicit v1 tradeoff.

### Dedupe

Narad v1 does not deduplicate.

Message ids are for tracing and observability. They are not an exactly-once
contract. Retried produce requests can create multiple messages.

### Recommended SLOs

Initial targets:

- produce accepted p95 under 30 ms with grouped fsync;
- produce visible p95 under 100 ms under healthy cluster conditions;
- consume local hit p95 under 20 ms;
- ack accepted p95 under 20 ms;
- zero message loss across process restart when the WAL volume survives.

These SLOs should be reported separately. Produce accepted latency, produce
visible latency, consume hit latency, and empty long-poll latency are different
things and must not be collapsed into one number.

## Implementation Direction

The v1 data path should be:

```text
HTTP produce -> validate topic/schema -> append ingress WAL -> grouped fsync -> return accepted
                                               |
                                               v
                              async dispatcher batches by owner partition
                                               |
                                               v
                              owner append -> message visible to consume
```

Ack remains best effort in v1:

```text
HTTP ack -> decode receipt handle -> route/proxy to owner if needed -> commit in-memory reservation state -> return
```

Ack persistence is not part of the v1 durability contract. The first reusable
durability primitive is the produce ingress WAL: a segmented WAL with grouped
fsync and replay.

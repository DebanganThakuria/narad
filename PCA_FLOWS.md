# PCA Flows

PCA means Produce, Consume, and Ack: the three hot paths Narad optimizes
for. These diagrams describe the current WAL-first design.

## Produce

Produce can hit any Narad pod. The receiving pod validates the request,
writes it to its local ingress WAL, and returns `202 Accepted`. A
background dispatcher later commits the record to the partition owner.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant API as Any Narad Pod<br/>HTTP API
    participant MS as Local Metadata<br/>Raft+bbolt replica
    participant WAL as Ingress WAL<br/>durable PV
    participant D as Produce Dispatcher
    participant Q as QUIC Node RPC
    participant O as Owner Pod<br/>Broker
    participant L as Owner Partition Log

    C->>API: POST /v1/topics/{topic}/produce
    API->>MS: Read topic, schema, assignment
    API->>API: Validate JSON/schema<br/>choose target partition
    API->>WAL: Append accepted produce record
    WAL-->>API: fsynced to ingress WAL
    API-->>C: 202 Accepted<br/>message_id, partition, accepted_at

    loop background replay/dispatch
        D->>WAL: Read undispatched records
        D->>MS: Resolve current owner for partition
        alt owner is local pod
            D->>O: CommitAcceptedProduceBatch
        else owner is remote pod
            D->>Q: CommitProduceBatch
            Q->>O: QUIC frame request
        end
        O->>L: Append record(s)
        O->>L: Synchronous fsync
        O->>L: Read back, verify frame CRC
        O->>L: Advance high-watermark (now visible)
        O-->>D: committed offset(s)
        D->>WAL: Persist dispatch checkpoint, then compact
    end
```

**Guarantee boundary:** once the HTTP response is `202 Accepted`, the
record is durable in the ingress WAL. It becomes consumable only after
the dispatcher commits it to the owner partition log — and the owner does
not report success until the record is fsynced and read back CRC-clean,
nor does the WAL compact past it until then. Narad has no follower
replication: the owner's durably-fsynced log is the sole copy.

## Consume

Consume can also hit any pod. Narad first tries local owned partitions.
If no local message is available, it probes remote owner pods over QUIC,
bounded by node count instead of making one call per partition.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant API as Any Narad Pod<br/>HTTP API
    participant MS as Local Metadata<br/>Raft+bbolt replica
    participant B as Local Broker
    participant Q as QUIC Node RPC
    participant O as Remote Owner Pod
    participant IF as In-flight Table
    participant L as Partition Log

    C->>API: GET /v1/topics/{topic}/consume?wait=...
    API->>MS: Read topic and partition ownership

    alt local partition has available message
        API->>B: Consume local partition
        B->>L: Read next visible offset
        B->>IF: Reserve offset until visibility timeout
        B-->>API: message + receipt_handle
    else local partition empty or not owner
        API->>Q: Probe remote owner nodes
        Q->>O: QUIC Consume request
        O->>L: Read next visible offset
        O->>IF: Reserve offset until visibility timeout
        O-->>Q: message + receipt_handle or empty
        Q-->>API: first successful message or empty
    end

    API-->>C: 200 message, or 204/empty timeout
```

**Guarantee boundary:** delivery is at least once. A consumed message is
made invisible for its visibility timeout. If the client does not ack in
time, the message can be delivered again.

## Ack

The receipt handle contains the topic, partition, offset, and reservation
nonce. Any pod can receive the ack; if it is not the owner, it forwards
the ack to the owner over QUIC.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant API as Any Narad Pod<br/>HTTP API
    participant MS as Local Metadata<br/>Raft+bbolt replica
    participant Q as QUIC Node RPC
    participant O as Owner Pod<br/>Broker
    participant IF as In-flight Table
    participant OC as Offset Committer

    C->>API: POST /v1/topics/{topic}/ack<br/>receipt_handle
    API->>API: Decode handle<br/>topic, partition, offset, nonce
    API->>MS: Resolve partition owner

    alt owner is local pod
        API->>O: Ack local
    else owner is remote pod
        API->>Q: Ack
        Q->>O: QUIC Ack request
    end

    O->>IF: CommitHandle(offset, nonce)
    alt handle is valid and still reserved
        IF->>IF: Advance committed offset<br/>or mark acked-ahead gap
        O->>OC: Best-effort async offset persistence
        O-->>API: 204 No Content
        API-->>C: 204 No Content
    else stale, expired, malformed, or wrong topic
        O-->>API: 400/410
        API-->>C: 400/410
    end
```

**Guarantee boundary:** ack removes a reservation from Narad's in-flight
state and advances queue progress when possible. Ack durability is
best-effort; consumers must be idempotent.

## Summary

```mermaid
flowchart LR
    C[Client] -->|Produce HTTP| I[Any ingress pod]
    I -->|fsync accepted record| W[Ingress WAL]
    W -->|background dispatch| D[Dispatcher]
    D -->|local call or QUIC CommitProduceBatch| O[Owner partition broker]
    O --> L[Partition log]

    C -->|Consume HTTP| R[Any receiving pod]
    R -->|local read or QUIC probe| O
    O --> F[In-flight reservation]
    O -->|message + receipt handle| C

    C -->|Ack HTTP| A[Any receiving pod]
    A -->|local call or QUIC Ack| O
    O -->|commit handle| F
```

Narad node-to-node PCA RPCs use QUIC. Raft metastore replication remains
Hashicorp Raft's TCP transport.

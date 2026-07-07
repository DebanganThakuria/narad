# PCA Flows

PCA means Produce, Consume, and Ack: the three hot paths Narad optimizes
for. These diagrams describe the current WAL-first design.

## Produce

Produce can hit any Narad pod. The body is the raw message payload;
optional metadata such as `key` and `partition` is sent as query params.
For topics without a schema, the receiving pod does not parse the body as
JSON. Schema-enabled topics validate the raw body before accepting it.
Accepted records are written to the local ingress WAL and the API returns
`202 Accepted`. A background dispatcher later commits the record to the
partition owner. The response intentionally does not include a message
ID, partition, or offset.

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

    C->>API: POST /v1/topics/{topic}/produce?key=...<br/>body = raw payload
    API->>MS: Read topic, schema, assignment
    API->>API: Validate schema if configured<br/>choose target partition
    API->>WAL: Append accepted produce record
    WAL-->>API: fsynced to ingress WAL
    API-->>C: 202 Accepted<br/>empty body

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
replication: the owner's durably-fsynced log is the sole copy. The WAL
record ID used during dispatch is internal bookkeeping and is not exposed
to clients.

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

The receipt handle contains partition, offset, and reservation nonce as
`partition:offset:nonce`; the request path supplies the topic. Any pod
can receive the ack; if it is not the owner, it forwards the ack to the
owner over QUIC.

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

    C->>API: POST /v1/topics/{topic}/ack?receipt_handle=...
    API->>API: Decode handle<br/>partition, offset, nonce
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

The same endpoint doubles as the lease surface: `extend=true` renews the
reservation's visibility window in place (same handle, new deadline) and
`extend=0` releases it for immediate redelivery (nack). Both validate
the handle exactly like ack — a lapsed lease returns 410 and can never
be revived.

## Fan-out (parent → child)

Fan-out sits entirely after the produce hot path: producing to a parent
is a normal produce, and a background cursor on each parent partition's
owner tails the committed log and re-commits records to the attached
children through the same commit paths the dispatcher uses. One cursor
exists per (child, parent-partition); its persisted offset advances only
after the child batch is durably committed.

```mermaid
sequenceDiagram
    autonumber
    participant P as Producer
    participant PO as Parent Partition Owner<br/>Broker
    participant PL as Parent Partition Log
    participant CU as Fan-out Cursor<br/>(same pod as PL)
    participant CF as Cursor Offset File
    participant CO as Child Partition Owner<br/>local or via QUIC
    participant CL as Child Partition Log

    P->>PO: Produce (normal PCA produce flow)
    PO->>PL: append + fsync + advance HWM

    loop per slab (fill-or-linger)
        CU->>PL: read committed slab from cursor offset
        CU->>CU: re-key each record with the<br/>child's partitioner
        CU->>CO: CommitAcceptedProduceBatch<br/>(one batch per child partition)
        CO->>CL: append + fsync + CRC verify + advance HWM
        CO-->>CU: committed
        CU->>CF: persist advanced cursor offset<br/>(commit-before-advance)
    end
```

For a **delay child** (attached with `delay_ms`), the cursor adds a due
gate: it delivers only records with `commitTime + delay <= now`, and —
because commit times are monotonic per partition — an undue head means
the cursor simply sleeps until it becomes due (O(1) while idle).

**Guarantee boundary:** fan-out is at-least-once within the parent's
retention window. A crash between the child commit and the cursor
persist re-delivers the last slab (duplicates, never loss). A child that
falls behind the parent's retention drops to the oldest retained record
and the loss is counted on `narad_fanout_child_dropped_messages`; the
uniform 1-hour retention floor bounds how quickly that can happen.

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

    L -->|fan-out cursor tails committed log| FO[Fan-out cursor]
    FO -->|re-keyed batch, local or QUIC| CO[Child partition broker]
    CO --> CL[Child partition log]
```

Narad node-to-node PCA RPCs use QUIC. Raft metastore replication remains
Hashicorp Raft's TCP transport.

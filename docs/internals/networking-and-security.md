# Networking & Security

Two planes, one port each: clients speak **HTTP** to any node; nodes speak a compact **RPC protocol over QUIC** to each other. Raft has its own TCP transport with mutual TLS.

```mermaid
flowchart TB
    C[Clients] -->|"HTTP + Basic auth<br/>(TLS at the ingress)"| ANY[any node :7942]
    ANY <-->|"node RPC over QUIC :7942<br/>(cluster shared secret)"| PEERS[peer nodes]
    ANY <-->|"Raft :7943<br/>(mutual TLS)"| PEERS
```

## The HTTP plane

Everything a client does is plain HTTP under `/v1` (topics CRUD, produce/consume/ack, children, users) plus unauthenticated `/healthz`, `/readyz`, and `/metrics`. `/healthz` means "process up"; `/readyz` means "safe to route traffic here" — held down until the node's metastore is caught up (and, for a joining node, until it's admitted).

## Routing: any node serves any request

Each node routes with its **local metastore replica** — no lookup service, no proxy tier:

```mermaid
flowchart TD
    REQ[request arrives at node X] --> Q{who handles this?}
    Q -->|"produce"| WAL["accept into X's own WAL<br/>(dispatcher moves it later)"]
    Q -->|"consume, partition owned by X"| LOCAL[serve from local log]
    Q -->|"consume, partition owned by Y"| FWD[forward over node RPC to Y]
    Q -->|"metadata write"| LEADER[forward to the Raft leader]
```

Produce is the special case that makes the cluster feel fast: it's *always* local (WAL-first), regardless of where the partition lives. Queue consumes prefer local partitions, then probe remote owners, then long-poll.

## The node RPC plane

Node-to-node calls — commit batches from the dispatcher, fan-out child commits, forwarded consumes/acks, leader confirmations, membership, cluster join — ride one multiplexed QUIC connection per peer pair. Each request is a single-byte opcode plus a compact binary payload; responses reuse HTTP status vocabulary so errors translate 1:1 at the boundary. QUIC gives stream multiplexing without head-of-line blocking and connection migration across pod restarts.

Two transport-level guards:

- **Cluster shared secret**: every node RPC connection authenticates with a symmetric secret from the deployment's Kubernetes Secret. No secret, no cluster plane — a stray client can't speak node protocol.
- **Raft mutual TLS**: metadata replication runs over mTLS when certs are configured (and warns loudly when it's plaintext).

## AuthN and AuthZ

- **Authentication**: HTTP Basic against bcrypt-hashed users stored in the Raft metastore — credentials replicate with everything else, so any node can authenticate any request locally. TLS is expected to terminate at the ingress in front of Narad.
- **Authorization**: per-request grant check — action (`produce`/`consume`/`create`/`admin`) × topic name, with prefix wildcards, plus topic *ownership* for management rights. Enforcement lives in the HTTP handlers, ahead of any routing, so a forwarded request was authorized on the node the client actually reached. Grant semantics from the client's view are in [Users & Access](../client/users-and-access.md).
- The **root admin** is seeded once, leader-gated, from the operator's secret at first startup.

## Trust model, honestly stated

Narad assumes the *cluster network* (node RPC + Raft ports) is a private, operator-controlled network — the shared secret and mTLS are guards, not a substitute for network policy. The client plane is hardened for untrusted callers: authenticated, authorized, size-capped (1 MiB bodies), and strict about malformed input.
## The node RPC wire format

Every request is a one-byte **opcode** followed by length-prefixed fields (strings/bytes get a 4-byte big-endian length; integers are big-endian). Responses carry an HTTP-vocabulary status, a content type, and a body — so errors translate 1:1 at the HTTP boundary with zero mapping tables at call sites.

The full opcode registry (`internal/protocol/node/types.go` — values are stable on the wire, appended only):

| Op | Name | Op | Name |
|---|---|---|---|
| 1 | Produce | 11 | CommitProduceBatch |
| 2 | Consume | 12 | CreateUser |
| 3 | Ack | 13 | UpdateUser |
| 4 | CreateTopic | 14 | DeleteUser |
| 5 | AlterTopic | 15 | AttachChild |
| 6 | DeleteTopic | 16 | DetachChild |
| 7 | PurgeTopic | 17 | FanoutCursors |
| 8 | TopicPartitionStats | 18 | ExtendAck |
| 9 | RegisterMember | 19 | Nack |
| 10 | CommitProduce | 20 | GetTopic · 21 JoinCluster |

An unknown opcode gets a clean 400 — which is also the mixed-version story during rolling upgrades: an old node politely declines ops it hasn't heard of, and the caller retries elsewhere or later.

## Timeouts worth knowing

| Path | Timeout |
|---|---|
| Default peer RPC reply | 5s |
| Produce/fan-out commit RPC | 30s (a slow fsync is not a dead node) |
| Leader-confirmation RPCs | 5s |
| Cluster join attempt cadence | one sweep of the peer list every 2s |
| Forwarded topic create | 75s (the leader may lawfully park it behind its startup create gate) |

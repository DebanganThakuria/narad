---
description: "Learn how Narad nodes talk to clients and to each other over HTTP, QUIC and Raft, and how each path is authenticated and bounded."
search:
  boost: 0.5
---

# Networking and security

Learn how Narad nodes talk to clients and to each other over HTTP, QUIC and Raft, and how each path is authenticated and bounded.

!!! abstract "In short"
    - Clients speak HTTP with Basic auth to any node on port 7942. TLS for clients terminates at your ingress.
    - Nodes speak a compact RPC protocol over QUIC on the same port number, over UDP (7942/udp), and prove a cluster secret bound to each TLS session. The QUIC listener always runs, even on a single node: with security on every stream proves a secret (the shared `NARAD_CLUSTER_SECRET`, or on a node with no peers a random per-process one, from v3.1.0), and with security off and no secret the plane is open and startup warns.
    - Raft runs on its own TCP port (7943) and needs its own mutual TLS, because Raft has no authentication of its own.
    - State-changing requests must carry an API content type or an `X-Narad-Client` header, or they get `415`, and so must a batch consume, or it gets `400`: this blocks cross-site requests from a browser.
    - The cluster network is assumed private. Fence 7942/udp and 7943/tcp with a network policy.
    - **New in v3.2.0:** a node that runs a remote child's cursor is an HTTPS client of another Narad cluster, the only traffic Narad starts toward anything outside its cluster ([Outbound plane](#outbound)).

Narad has two planes, each on its own port: clients speak **HTTP** to any node, and nodes speak a compact **RPC protocol over QUIC** to each other. Raft has its own TCP transport with mutual TLS.

<figure class="nr-dia nr-dia--doc" id="fig-network-planes">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/network-planes.html"
</div>
<figcaption>Clients use <code>7942/tcp</code>; node RPC uses the same number over UDP, <code>7942/udp</code>, and Raft uses <code>7943/tcp</code>. Fence both cluster ports, and keep <code>9100/tcp</code> internal.</figcaption>
</figure>

## HTTP plane {#http-plane}

Everything a client does is plain HTTP under `/v1` (topics, produce, consume, ack, children, users, cluster), plus the unauthenticated `/healthz` and `/readyz`. `/healthz` means "the process is up". `/readyz` means "safe to route traffic here": it stays down until the node's metastore has caught up, and for a joining node until it is admitted. `/metrics` is served on its own listener when `http.metrics_addr` is set (the chart's default). Otherwise it is on the API port behind the API's Basic auth, because the exposition names every topic.

The listener is bounded:

- 64 KiB of headers, and a 5 s budget to read them.
- A cap on open connections (`http.max_connections`, through `netutil.LimitListener`). Extra clients wait in the accept backlog rather than each getting a goroutine.
- A per-identity cap on concurrent consume requests (`http.max_consume_in_flight_per_identity`, `429` beyond it), since every long-poll pins a goroutine and, on a node that does not own the partition, a forwarded RPC stream slot for up to `max_consume_wait`. A batch consume of N counts as N, clamped to the cap.
- An optional cap of the same kind on produce (`http.max_produce_in_flight_per_identity`, from v3.1.0, off by default). A batch produce counts as one from before its body is read, so the cap bounds the batch bodies being read and decoded too, and as its message count, clamped to the cap, once its body is decoded. It has no default because a produce holds its goroutine only until its write-ahead log fsync, not for a long-poll's wait.
- A batch body is decoded one element at a time and refused at the 101st message (the 1,001st from v3.2.0) or receipt handle, and a JSON body with a second value after the first is refused after one token of it. So decoding a request never costs more than its bound: before, a 1 MiB batch body of `[0,0,...]` allocated about 280 times its size before the count check refused it.
- A request body is not allocated at its declared `Content-Length` before it arrives. Up to 64 KiB is read into one buffer of exactly that size; a larger body starts at 64 KiB and grows fourfold as it fills. So a client that declares 1 MiB and then stalls pins at most 64 KiB, or four times what it actually sent, whichever is larger.

- **New in v3.2.0:** a batch produce body may be up to 16 MiB, and zstd or gzip compressed, decoded under the same cap. A body over 1 MiB takes its share of a node-wide budget (`http.max_batch_body_bytes_in_flight`, 256 MiB by default) before it is read, a megabyte at a time as it grows, and is answered `503` with `Retry-After: 1` when the budget is full, so many large batches at once cannot exhaust a node's memory.

The limits a client sees are listed in [Connect and authenticate](../build/connect.md#limits).

### Cross-site guard {#cross-site-guard}

State-changing requests (`POST`, `PUT`, `PATCH`) must carry `Content-Type: application/json` or `application/octet-stream`, or an `X-Narad-Client` header, or they are answered `415`. This guards Basic-auth sessions against cross-site request forgery. Browsers attach cached Basic credentials to cross-origin requests, and a `POST` with `text/plain` or a form encoding needs no CORS preflight.

Without the guard, a hostile page could create topics, produce, ack, or decommission a member on behalf of an operator who had used the API from that browser. The API content types and any custom header force a preflight, which Narad never approves. `DELETE` is preflighted anyway.

A consume is a `GET`, but it is not read-only: it reserves messages and hides them for their visibility window, and a cross-origin page can send one (an image tag will do) with the cached credentials. The page cannot read the response, so it can delay delivery but can neither see nor lose a message. A batch consume (`max=N`) reserves up to 100 messages a request, so it must carry `X-Narad-Client` or it is answered `400`. That holds for a `GET` and for a `HEAD`, which the router serves on the same route and which a page can also send without a preflight. A single consume stays open to plain clients such as `curl`.

The guard sits inside the auth middleware, so an anonymous request is still answered `401` first.

## Request routing {#routing}

Each node routes with its **local metastore replica**: no lookup service, no proxy tier.

<figure class="nr-dia nr-dia--doc" id="fig-network-routing">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/network-routing.html"
</div>
<figcaption>Every route is decided from the local metastore replica, and a produce never leaves the node on the request path.</figcaption>
</figure>

Produce is the special case that keeps the cluster fast: it is *always* local (WAL first), wherever the partition lives. Queue consumes prefer local partitions, then probe remote owners, then long-poll (see [Consume routing](consume-path.md#routing)).

## Node RPC plane {#node-rpc}

Node-to-node calls ride one multiplexed QUIC connection per pair of peers. They cover commit batches from the dispatcher, fan-out child commits, forwarded consumes and acks, leader confirmations, membership and cluster joins. A node holds a single peer client, shared by the router, the dispatcher, the fan-out runner, the mover and the heartbeater. Each request is a single-byte opcode plus a compact binary payload, and responses reuse the HTTP status vocabulary, so errors translate one to one at the boundary. QUIC gives stream multiplexing without head-of-line blocking.

Streams are pooled per **lane**, so bulk traffic cannot starve control calls: produce commits ride the produce lane, consumes the consume lane, and ack, extend and nack the ack lane (16 streams each); everything else rides the 4-stream control lane. Requests round-robin across a lane's streams and are matched to replies by request ID, so many RPCs share one stream.

Partition-transfer traffic (fan-out cursor listings, segment chunks, handoff freezes) rides a second peer client (from v3.1.0), with its own UDP socket and its own connection to each peer, made on first use. A multi-megabyte chunk therefore shares neither a stream nor a connection flow-control window with commit replies, and a node holds two connections to a peer it has moved partitions with.

Peer restarts are detected at once rather than at the 30 s idle timeout. Both ends run a QUIC transport whose stateless reset key is derived from the cluster secret (HMAC-SHA256 with a fixed context). So a restarted node answers packets for connection IDs it no longer knows with a reset the peer can verify, and the peer's pool drops the dead connection on the spot. A lone secured node's generated secret (see [below](#cluster-secret)) changes on every restart, and so does its reset key; that matters to nobody, because no other process holds a connection to it.

As a backstop for resets that never arrive (a peer that came back at a new address, a path that drops packets), any request that ends on a deadline triggers a liveness ping on its stream, off the caller's path (from v3.1.0). The deadline can be its context's, the per-call budget its caller passed, or the 5 s fallback. At most one ping runs per connection at a time, and one per second. The connection is closed, so the next request dials again, only if the pong does not arrive within 1 s **and** no stream on that connection received anything meanwhile; a busy, healthy connection whose pong is queued behind other replies is never dropped. Before, only the fallback timeout pinged, and every hot-path caller carries a deadline of its own, so a dead connection stayed pooled, failing every request, until QUIC's 30 s idle timeout.

A stream is multiplexed, so one request's trouble must not end it. A request that times out or is cancelled drops only its own waiter and sends a cancel frame, and a write that times out before sending a byte leaves the stream open. Dials to one address, and stream opens per lane and shard, are shared by concurrent requests and run under the pool's own context (a 1 s dial timeout, the pool timeout for opens), not under the first caller's. A caller that is cancelled or runs out of time fails alone, and only a genuine dial failure is cached, for a backoff that doubles from 250 ms to 2 s, during which requests to that peer fail fast.

### Cluster shared secret {#cluster-secret}

The QUIC listener runs on every node, a single node included, and the node RPC plane runs the whole control plane with authorization bypassed (forwarded requests carry no identity: the ingress node already decided). So with security on, every stream must prove a secret before any request reaches a handler:

- With peers configured, the secret is `NARAD_CLUSTER_SECRET`, shared by every node (taken from the deployment's Kubernetes Secret in the chart); startup refuses a secured node with peers and no secret.
- On a node with no peers and no `NARAD_CLUSTER_SECRET` (from v3.1.0), startup generates a random 32-byte secret (crypto/rand) for the life of the process, kept in memory only and never logged or written, and logs `single node with no cluster secret: generated a per-process secret, so node RPC is closed to other processes`. Only the node itself can use its node RPC. To add peers to such a node, set the same `NARAD_CLUSTER_SECRET` on it and restart it before the first node joins. Before the first node joins, it also needs a `cluster.addr` the other nodes can reach, and either the Raft TLS files on every node or `security.allow_plaintext_raft` with 7943/tcp fenced: Raft has no authentication of its own, and a node with no peers configured never runs the join loop, so one the others cannot reach over Raft stays cut off from their Raft. This works only for a node whose Raft first started on the address the others will reach it at, because the Raft configuration keeps the address a node's Raft first started on: a node first started on a loopback `cluster.addr` cannot be grown by rebinding it ([Raft mutual TLS](#raft-tls) has why, and the way out).
- With security off and no secret, the plane is open, as that mode opts into, and startup logs `node RPC plane is unauthenticated` at warning level. A multi-node cluster must also set `security.allow_insecure_cluster`.

The proof is bound to the TLS session:

- Both ends export 32 bytes of keying material from the QUIC connection's TLS 1.3 session (the RFC 8446 exporter, label `narad-cluster-auth-v1`), and send `HMAC-SHA256(secret, role || ekm)`.
- The client's proof is the stream's first frame. The server answers with its own, server-role proof, and the client sends no request on a stream whose server cannot prove the secret.
- Because the keying material is unique to that TLS session, a proof captured by a rogue endpoint (a spoofed peer address, a decommissioned pod's IP) cannot be replayed to a real node, and an impostor server cannot pass for a peer.
- The proof frame is read under a 64-byte cap before authentication, so an unauthenticated peer cannot make a node allocate the 16 MiB general frame buffer per stream.

The listener also refuses to serve a secured node that has no secret, whatever its peer count, and stops the node instead (from v3.1.0).

### Raft mutual TLS {#raft-tls}

Metadata replication runs over mutual TLS when certificates are configured. With security on, the Raft TLS files are required unless `security.allow_plaintext_raft` is set explicitly, because Raft itself has no authentication and the cluster secret does not cover it. Raft listens whatever the peer count, so this holds for a node with no peers too, unless its `cluster.addr` is a loopback address, which only processes on the same host can reach (from v3.1.0; it used to apply only with peers configured). Bind it to loopback only on a node that will never take peers. The address a node's Raft first starts on is recorded in the Raft configuration (at bootstrap, or by the leader when the node joins), the nodes that join later dial that recorded address, and a later `cluster.addr` does not change it. So a node first started on a loopback `cluster.addr` cannot be grown into a cluster by rebinding it: once it is not the leader the others cannot reach its Raft, so it stays leaderless and stale, and a node whose `cluster.addr` is port-only (`:7943`) that dials the loopback address reaches its own Raft and steps down, so metadata writes can stall across the cluster. Such a node logs `raft configuration records this node at an address other than the one it advertises` at error level when it starts on the new address (from v3.1.0; see [Troubleshooting](../operate/troubleshooting.md#log-raft-address-recorded)). To grow it, start a new cluster whose first node starts on an address the others can reach, and move the workload to it. Raft TLS works only when every node of the cluster has the files: a node with no peers configured that other nodes have joined keeps its reachable address and needs the TLS files on every node or `security.allow_plaintext_raft`. Setting it up and rotating the certificates is in [Raft TLS certificates](../operate/raft-tls.md).

### QUIC certificate {#quic-certificate}

The QUIC layer itself is TLS 1.3 with an ephemeral ECDSA P-256 certificate per process, and ALPN pinning (`narad-cluster-quic-v2`). Peers do not verify the certificate's issuer: the session-bound secret proof is the authentication. The certificate is created at startup and never renewed while the process runs, and the dialling side does not check its validity dates. The certificate carries no trust, so its dates carry none, and a peer stays reachable however long it has been up.

Releases before v3.0.1 created it for 24 hours and refused an expired one, which cut the data plane of any cluster whose nodes stayed up longer than a day. During a rolling upgrade from such a release, the one-year lifetime that new nodes use keeps old dialers working. Clients keep a small TLS session cache, so a new dial resumes a session rather than doing a full handshake; a resumed session still yields fresh exporter material, so proofs never repeat across connections.

### Ports to fence {#ports}

The QUIC listener shares the **API port number over UDP** (7942/udp by default), not the Raft port. Network policies that fence the cluster plane must cover 7942/udp and 7943/tcp, not just 7943. The recommended policy is in the [Production checklist](../operate/production-checklist.md#network-policy). A node with security off serves 7942/udp unauthenticated, so fence it even on a single node. A fence is not authentication: with security on, every stream proves a secret whether or not the port is fenced.

### Legacy cluster authentication {#legacy-cluster-auth}

Releases before session binding proved the secret with one fixed token (`HMAC(secret, "narad-cluster-auth-v1")`, ALPN `narad-cluster-quic-v1`), sent one way. A node running the current protocol will not talk to one running the old one: they share no ALPN, so the TLS handshake fails rather than authenticating wrongly.

`security.allow_legacy_cluster_auth: true` (`NARAD_SECURITY_ALLOW_LEGACY_CLUSTER_AUTH`, `security.allowLegacyClusterAuth` in the chart) bridges the two for a rolling upgrade. Upgraded nodes then also offer the legacy ALPN, accept the fixed token from old peers (logging each such connection), and fall back to it when dialling an old peer, while two upgraded nodes always use the new protocol. While the flag is on, any peer that asks for the legacy ALPN is served with the replayable token, so turn it off once every node runs the new release. Without the flag an upgrade still works, but forwarded requests between old and new nodes fail for the duration of the roll (Raft is unaffected). The procedure is in [Upgrade Narad](../operate/upgrade.md#version-notes).

### Server-side concurrency {#server-bounds}

On the serving side, produce commits run under one concurrency bound, and the other messaging handlers (acks, extends, nacks, non-blocking consume scans) under another of the same size, max(64, 4 x GOMAXPROCS). A commit holds its slot across a segment fsync, and sharing one gate queued acks and probes behind the disk.

Acks and non-blocking consumes wait for a slot only while their request is live. One whose requester gave up while it queued is answered without touching the broker: a probe or claim with `204` (a claim's hold retired), and an ack, extend or nack with `503`, unapplied, which is safe because acks are idempotent by nonce.

Commits wait the same way: one whose requester gave up while it queued is answered `503`, unapplied, and its records are freed then rather than when a slot frees. A commit that gets its slot runs as before (the commit path refuses one whose context is already cancelled). An `AckBatch` takes one messaging slot for all its records (see [Forwarded acks](consume-path.md#forwarded-acks)). Partition-transfer ops (segment listings and chunk reads, 8 at a time) keep their slot until the reply has been written, so the bound covers the memory a chunk reply holds. The frame read loop never blocks, and long-poll consumes and any op that calls other peers are exempt from the bounds. Replies are encoded into recycled buffers, and a forwarded reply without a body (a successful forwarded ack, extend or nack, and every forwarded `204`) is written to the client as a bare status.

Peer RPCs are observable as `narad_cluster_rpc_requests_total{op,outcome}` and `narad_cluster_rpc_request_seconds{op}`, recorded by the calling node, one observation per RPC. An `AckBatch` (v3.1.0) is `op="ack_batch"`, whether it carries a coalesced queue of forwarded acks, extends and nacks (up to 64) or one owner's share of a client's batch ack (up to 100 handles). So `op="ack"`, `op="extend_ack"` and `op="nack"` count only the ones sent on their own. Under load, and for batch acks, a panel that reads `op="ack"` as the forwarded-ack rate undercounts, and its latency view misses the batched ones unless it includes `op="ack_batch"`.

## Authentication and authorization {#auth}

### Authentication {#authentication}

Clients authenticate with HTTP Basic against bcrypt-hashed users stored in the Raft metastore. Credentials replicate with everything else, so any node can authenticate any request locally. TLS is expected to terminate at the ingress in front of Narad.

Each node caches verified credentials, keyed by the users domain version. It rejects unknown usernames before running bcrypt (a deliberate timing leak that limits bcrypt cost to real users), and bounds bcrypt concurrency process-wide. Concurrent requests carrying the same credentials share one bcrypt run, and a requester that disconnects while it waits leaves on its own, without failing the others. Password hashing for user create and password change has a bound of its own (2 at a time, from v3.1.0; it used to share the verification bound), so a failed-login flood cannot delay it, and a loop of password changes cannot delay logins.

### Failed-login throttle {#auth-throttle}

Failed attempts are throttled per username: a burst of 5, then one attempt earned back every 12 s. Beyond that, requests for the username are answered `429` (`too many failed authentication attempts`). The bucket is per node, so N nodes behind a load balancer allow 5N attempts in a burst.

Two more rules keep a flood of wrong passwords from starving everyone else (from v3.1.0):

- **Clean attempts first.** A password check for a username with no recent failures (its bucket is full, as it is for every honest first login) runs ahead of a check for a username with some. A flood therefore delays an honest login by at most one attempt per username the flood has not tried yet, plus the checks already running.
- **A node-wide failure budget.** A check for a username with recent failures also takes a token from a budget shared by the whole node: a burst of 32, refilled at 4 a second. A failed first attempt spends one when one is left, and a correct password gives back what it took. While the budget is empty, those checks are answered `429` at once, without running bcrypt, and the node logs `authentication failure budget exhausted` at most once every 10 s. A username with no recent failures is never refused by the budget.

The cost: someone who mistyped their password recently counts as having failures until their bucket refills, about 12 s per failed attempt, and can be answered `429` while a flood keeps the node's budget empty. `narad_auth_verify_queued` shows the password checks admitted and not yet finished ([Metrics reference](../reference/metrics.md#authentication)).

### Authorization {#authorization}

Every request is checked against the caller's [grants](../reference/glossary.md#grant): an action (`produce`, `consume`, `create` or `admin`) on a topic name, with prefix wildcards, plus topic *ownership* for management rights. Topic reads need any grant on the name, or ownership. Attaching a child needs management rights on both ends. Cluster topology and user management are admin-only.

Enforcement lives in the HTTP handlers, ahead of any routing, so a forwarded request was authorized on the node the client actually reached. The full model is in [Access model and grants](../reference/access-model.md).

The **root admin** is seeded once, by the leader, from the operator's secret at first startup. Without one, the seeding node generates a password and writes it to a file in its data directory before it proposes the seed, never to the log ([Manage the root user](../operate/users.md#root-admin)).

## Outbound plane {#outbound}

**New in v3.2.0.**

A node that runs a [remote child](../reference/glossary.md#remote-child)'s cursor sends to another Narad cluster: `POST /v1/topics/{t}/produce/batch` with Basic auth, plus the reads its checks make (`GET /v1/topics/{t}`, `GET /v1/topics/{t}/children` and `GET /v1/users`). It is the only traffic Narad starts toward anything outside its own cluster. [Remote replication](remote-children.md) has the whole design; in short:

- **Credentials.** The password is sealed with AES-256-GCM under a key derived from the cluster secret and a per-cluster salt, bound to the remote's name, ID, URL, username and trust anchor, and stored in the metastore. Each node decrypts it once per credential version into a ready `Authorization` value and HTTP client; the send path does no cryptography. [Manage remotes](../operate/remotes.md#accepted-risk) says what that protects and what it does not.
- **TLS, and where it ends.** `https` only, with verification always on: TLS 1.2 or later, key exchange X25519MLKEM768 or X25519, the remote's CA bundle alone when it has one, else the system roots. TLS from this cluster ends at the target's ingress, and the hop from there to the target's pods carries the Basic header, so it must be encrypted on the target, which this cluster cannot check. `HTTP_PROXY` and `HTTPS_PROXY` are ignored.
- **The address guard.** The URL's port must be in `remotes.allowed_ports` and its host in `remotes.allowed_hosts` when that is set. Every dial is checked again after DNS resolution, which also defeats DNS rebinding: loopback, link-local, multicast and cloud metadata addresses, in any spelling, are refused unless `remotes.allow_addresses` lists them. Private ranges stay reachable.
- **Redirects are never followed**, so the `Authorization` header never reaches a `Location` host.
- **Blind by design.** Answers from a remote reach an admin as a status and a class, never a body, and nothing from the target is logged but its status and class.
- **Who may point it.** Remotes and remote children are admin only, with security on. The node that takes a write authorizes it and seals any password; the leader checks the gates, runs the checks on every member and proposes through Raft; each write is audited on both. The ingress-to-pod hop on this cluster carries the password in the body of a create or a password change, so those answer `412` unless the node attests that hop is encrypted (`remotes.api_hop_encrypted`).

## Trust model {#trust-model}

Narad assumes the *cluster network* (the node RPC and Raft ports) is a private network the operator controls. The shared secret and mutual TLS are guards, not a substitute for network policy. The client plane is hardened for untrusted callers: authenticated, authorized, size-capped (1 MiB bodies; 16 MiB for a batch produce from v3.2.0), and strict about malformed input.

## Node RPC wire format {#wire-format}

Every request is a one-byte **opcode** followed by length-prefixed fields (strings and bytes get a 4-byte big-endian length; integers are big-endian). Responses carry an HTTP-vocabulary status, a content type and a body, so errors translate one to one at the HTTP boundary with no mapping tables at call sites.

The full opcode registry (`internal/protocol/node/types.go`; values are stable on the wire and only ever appended):

| Op | Name | Op | Name |
|---|---|---|---|
| 1 | Produce | 17 | FanoutCursors |
| 2 | Consume | 18 | ExtendAck |
| 3 | Ack | 19 | Nack |
| 4 | CreateTopic | 20 | GetTopic |
| 5 | AlterTopic | 21 | JoinCluster |
| 6 | DeleteTopic | 22 | ListPartitionSegments |
| 7 | PurgeTopic | 23 | FetchSegmentChunk |
| 8 | TopicPartitionStats | 24 | PrepareHandoff |
| 9 | RegisterMember | 25 | DecommissionMember |
| 10 | CommitProduce | 26 | CompleteMove |
| 11 | CommitProduceBatch | 27 | AbortMove |
| 12 | CreateUser | 28 | GetAssignment |
| 13 | UpdateUser | 29 | AppliedIndex |
| 14 | DeleteUser | 30 | TokenRegister |
| 15 | AttachChild | 31 | TokenNotify |
| 16 | DetachChild | 32 | AckBatch |
| | | 33 | NodeStatus |
| | | 34 | ForgetServer |
| | | 35 | RemoteWrite (v3.2.0) |
| | | 36 | RemoteCheck (v3.2.0) |

An unknown opcode gets a clean `400` (`unsupported rpc operation`), and so does a trailing field the decoder does not know. That is how mixed versions work during a rolling upgrade: an old node declines what it has not heard of, and the caller sends the request again in a shape the old node understands, and keeps doing so for that node for 2 minutes. An `AckBatch` becomes single acks, a commit batch goes out without its topic ids, a forwarded batch consume asks for one record, and a claim becomes a plain probe.

`RemoteWrite` (a remotes or remote child write, forwarded to the leader with the caller's name and a request ID) and `RemoteCheck` (a member's checks, its credential cache status, or its ingress backlog of one topic) have no older shape. An older node's `400` becomes a `412` (`old_release`), and the [release gate](remote-children.md#gates) keeps both from being needed before every member knows them.

## Timeouts {#timeouts}

| Path | Timeout |
|---|---|
| Default peer RPC reply | 5s for a caller without a deadline. After any request that ends on a deadline, a 1s liveness ping decides whether the connection is dropped |
| Produce and fan-out commit RPC | 30s (a slow fsync is not a dead node); 5s for the one-record probe of a failing produce destination. The produce dispatcher hands its budget to the transport rather than deriving a context per commit |
| Forwarded ack, extend or nack | 2s, covering the dial and stream open too, and for an ack queued behind busy slots its wait for one (acks are idempotent by nonce, so a retry after a timeout cannot commit twice). A batch ack's `AckBatch` to each owner gets the same 2s |
| Non-blocking remote consume probe | 500ms per owner, covering the dial and stream open too (a stalled owner is skipped for the round; a consumer that leaves stops the round before the next owner) |
| Remote consume probe pacing | 100ms after an empty round, doubling to 1s, within the client's wait budget |
| Forwarded long-poll consume | the client's wait (capped by `http.max_consume_wait` on the router and the RPC server alike) plus 2s of grace |
| Reply write on the server | 5s plus 1s per 256 KiB of reply |
| Peer dial | 1s, then a failure backoff of 250ms doubling to 2s |
| Leader-confirmation RPCs | 5s |
| Cluster join attempt cadence | one sweep of the peer list every 2s |
| Forwarded topic create | 75s (the leader may lawfully hold it behind its startup create gate) |

## Cancellation on the cluster stream {#cancellation}

Every request frame on a cluster stream gets its own context on the serving node. A client that stops waiting for a reply (its caller's context ended, or its reply timeout fired) sends a `StreamFrameCancel` carrying the request ID. The server cancels that request's context and, for a consume that had already reserved a message the client will never read, nacks it at once.

The record of a delivered handle lives for a 2 s grace after the reply. A client that read the reply never cancels, so the record only covers a cancel that races the reply. Records expire from the front of a deadline queue, so remembering deliveries costs a map insert per forwarded consume rather than a scan. When a stream ends, every request still running on it is cancelled. Servers that predate the frame answer it with an error frame for a request nobody is waiting on, which the client ignores, so mixed-version clusters are unaffected.

## Next steps

- [Cluster lifecycle](cluster-lifecycle.md): how nodes join, recover from crashes and handle topic incarnations.
- [Production checklist](../operate/production-checklist.md): the network and security settings to check before production.
- [Raft TLS certificates](../operate/raft-tls.md): turn on mutual TLS for Raft, then renew and rotate it.

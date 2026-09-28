# Changelog

All notable changes to Narad are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Versions below 1.0.0 (the `alpha` and `beta` tags) were pre-releases and are
summarized more briefly than the 1.x and later entries.

## [Unreleased]

### Added
- This changelog, covering every release back to the first alpha.
- `MAINTAINERS.md`, stating who reviews changes, what a single-maintainer project means for review turnaround and bus factor, and how to become a maintainer.
- Container images are signed with Sigstore cosign (keyless, no long-lived key) and carry an SBOM and SLSA build provenance. This starts with the first image built after the v3.0.1 release; verifying an earlier tag fails because those images were published without them.
- A `govulncheck` job in CI, run on every pull request and weekly against the latest vulnerability database, so an advisory against a dependency surfaces without waiting for a code change.
- `scripts/check-release-refs.sh`, wired into `make check` and CI, which fails when a pinned image tag in the documentation has drifted from the newest release tag.
- A nightly linearizability check. A three-node cluster takes load while nodes are killed and cut off from their peers on the cluster plane; every client operation is recorded with the interval it was in flight for, and the history is checked against a sequential specification of the delivery contract. A message redelivered after a confirmed ack now has to sit inside a fault window, and one that does not fails the run. A second leg injects no faults and runs strict, which asserts that a healthy broker never redelivers an acked message.
- The load driver records an operation history with `--history`, in a format shared with the checker (`tests/linearizability/history`).
- Documentation for all of it, including what the check cannot catch: [Checking the Delivery Contract](docs/internals/linearizability.md).
- A [Go SDK](https://github.com/DebanganThakuria/narad-go) in its own repository, depending on nothing but the standard library, with a guide on the documentation site. Three calls cover almost everything, and the consumer handles the visibility lease: it renews the lease while a handler runs, cancels the handler if the lease is lost, acks on success and hands the message back on failure.
- `storage.consumer_offset_commit_interval_ms` (default 100, 10 to 60000, settable in the config file): how often acked consumer frontiers and out-of-order ack sets are made durable. The offset committer used to run on the storage flush interval, so the two could not be tuned apart; the default keeps the crash redelivery window it always had.
- `storage.ingress_wal_prealloc` (default `false`, settable in the config file): prepares ingress WAL segments ahead of use so a group commit's `fdatasync` no longer commits the inode through the file system journal. Off by default because it changes crash recovery and rollback; see the upgrade notes below and [Configuration](docs/operate/configuration.md#ingress-wal-segment-preparation).
- The `narad_ingress_wal_failed` gauge: 1 once a write or sync of the node's ingress WAL has failed and produce is latched off until a restart, else 0. Consume and `/readyz` are unaffected by that latch, so this gauge is the signal to alert on.
- Batch consume: `GET /v1/topics/{topic}/consume?max=N` (N from 1 to 100) answers `{"messages":[...]}` with up to N messages, each exactly as a single consume returns it, with its own receipt handle and visibility window, or `204`. On a node that owns some of the topic's partitions the messages come from one scan of them. Only when that finds nothing does the request ask the other owners for one message, which comes back alone as a one-message batch, and only when they have none does it wait, for one message, which a second scan tops up. A node that owns none of the topic's partitions (or not the pinned one) forwards the request asking the owner for up to N, from its opening probes and from its long-poll wait alike (the token claim, the 2 s re-probe and the polling fallback), and passes the owner's batch through: over loopback QUIC on macOS, batches of 100 cost about 1.35 µs per record against 45.8 µs forwarded one at a time. A request is never held to fill N, nor filled from several owners. One scan stops reserving once the keys and payloads it has taken reach 4 MiB (the record that crosses the bound is kept), and a response carries at most 8 MiB of encoded messages, the first always included; messages left out, which in practice only text inflated by JSON escaping causes, are nacked at once for the next consume, so they show as nacks and are counted as consumed again when redelivered. `max` does not combine with `offset`. See [Consuming in batches](docs/client/consuming.md#consuming-in-batches).
- Batch ack: `POST /v1/topics/{topic}/ack` without a `receipt_handle` parameter and with a JSON body `{"receipt_handles":[...]}` (1 to 100 handles, at most 64 KiB) settles every handle on its own, with `extend` applied to all of them, and answers `200` with `{"results":[...]}`: one status per handle, in request order, each the one a single ack of that handle would have answered, with its error message when it failed. Handles other nodes own go out as one RPC per owner, the owners in parallel. See [Acking in batches](docs/client/consuming.md#acking-in-batches).
- The `narad_ingress_dispatch_backlog_records` gauge, per node: the ingress WAL's durable next sequence minus the stored dispatch checkpoint, which is how many records a restart would replay. It is the signal to wait on before a rollback (see the upgrade notes below).
- Batch produce: `POST /v1/topics/{topic}/produce/batch` with a JSON body `{"messages":[...]}` of 1 to 100 messages, each `{"key","key_encoding","payload","payload_encoding","partition"}`, within the 1 MiB body cap of a single produce and under the produce permission. A payload is a JSON value stored exactly as written, or any bytes as base64 with `"payload_encoding":"base64"`; a key that is not valid UTF-8 goes as base64 with `"key_encoding":"base64"`; the `key` and `partition` query parameters are refused on this path. It answers `202` with `{"accepted":N}` once every message is durable, and it is all or nothing: a message that fails gets the status a single produce of it would get, its error prefixed `message <i>: `, and nothing is stored. The checks run in two passes, the format of every message (encodings, an empty payload, a negative partition) before any message's schema and partition range, so a later message's bad base64 is reported ahead of an earlier message's schema failure. The messages go into the ingress WAL in one append, in batch order (so same-key messages keep batch order in normal operation), and wait for one group commit, two when the WAL rolls a segment inside the batch. Measured at the WAL on macOS with one caller: about 51 µs per message in batches of 100 and 0.5 ms in batches of 10, against about 4.7 ms for a single produce. A timeout or 5xx is ambiguous for the whole batch, so a retry may duplicate part of it; a batch that fails after the WAL rolled a segment inside it may still have its leading messages delivered. At-least-once holds either way. A server that predates batch produce answers `404`: fall back to single produces. See [Producing in batches](docs/client/producing.md#producing-in-batches).
- `http.max_produce_in_flight_per_identity` (env `NARAD_HTTP_MAX_PRODUCE_IN_FLIGHT_PER_IDENTITY`, default 0, off): a cap on concurrent produce requests per authenticated user, or per client IP with security off, answered `429` beyond it. A batch produce counts as its message count, clamped to the cap. It is off by default because a produce holds its goroutine only until the ingress WAL's group commit, not for a long-poll's wait. Settable in the config file, which v3.0.1 rejects; see the upgrade notes below.
- On Linux, when `GOMEMLIMIT` is unset, `narad serve` sets the Go soft memory limit to 90% of the process's cgroup memory limit (cgroup v2 `memory.max` or v1 `memory.limit_in_bytes`, the tightest from the process's cgroup up to the root, unlimited ones ignored) and logs it once (`go memory limit set from the cgroup memory limit (set GOMEMLIMIT to override)`). Any non-empty `GOMEMLIMIT`, `off` included, always wins (an empty one counts as unset), and `GOGC` is never changed. Under the chart, `resources.limits.memory` is now enough; the chart's default sets no memory limit, so nothing is set there. See [Values that matter](docs/operate/index.md#values-that-matter-grounded-in-our-live-cluster).

### Changed
- The linearizability verdict can now fail on a broker that never finishes. `OVERDUE` used to cover any backlog and exit zero, so a broker refusing every ack left every message unacked and still passed; past `--max-overdue` the verdict is `STALLED` and the run fails.
- Fault coverage is enforced rather than only reported. Where faults cover most of the run, every redelivery is explained by construction and a clean result means nothing, so past `--max-fault-coverage` the verdict is `UNKNOWN` with that as the stated reason.
- The load driver aborts when two consumers confirm the same message concurrently. That is a double lease, and the linearizability model is untimed, so it is the only leg that can see one.
- The fault injector's spacing is derived from the visibility timeout rather than hardcoded, so a run with a longer timeout cannot overlap consecutive grace periods into one continuous excuse. The evidence floor is derived from the requested rate for the same reason: a flat floor was a fraction of a percent of what a run records.
- The nightly fails when its fault injector recorded nothing. The injector is a background process, and a run that lost it was reporting `PASS` on evidence it never gathered.
- Released images report their release version from `narad version` instead of a commit SHA.
- The formatters are pinned rather than installed at `@latest`, and CI now runs the format check. A formatter that moves version on its own reformats files nobody touched, and the first person to find out is whoever's unrelated pull request fails; a file had already drifted on `master`, so the documented `make check` failed for anyone who ran it.
- `SECURITY.md` states which versions receive fixes, target response times, and which documented configurations are out of scope, replacing a supported-versions note that still described the project as pre-1.0.
- The README carries a project status section: what the tests cover, and the three structural limits (no ordering guarantee, no synchronous replication, months of track record rather than years).
- Narad builds on Go 1.27. Building from source now needs a 1.27.1 toolchain, and the container image builds on `golang:1.27-alpine`. The pinned `govulncheck` moved with it: v1.1.4 vendors a copy of `x/tools` whose type traversal panics on the type forms 1.27 introduced, so it crashed instead of scanning. Nothing in the broker changed to accommodate the release. `encoding/json` is the one item that looked like it might, since it is now backed by the v2 implementation, but the v1 API keeps v1 semantics: duplicate object names and invalid UTF-8 decode as they always did, which is what the metastore and the Raft snapshots depend on.
- A produce without a key is stored without one. Consumers get no `key` field for it, where they used to see an invented `key-<n>`; keyless messages are now spread round-robin (the invented key used to be hashed like any other), also when fan-out copies them into a child. Messages stored before the upgrade keep the key they were given. The round-robin counter belongs to the node, not the topic: every topic produced to through a node shares one, and the node's fan-out shares another across every child, so a producer that writes to several topics in a fixed rotation through one node can put all of one topic's keyless messages on the same partition (three topics of 3 partitions each, written A, B, C, A, B, C with nothing else producing through that node, send every A message to one partition); the invented keys, hashed, used to spread them. And because a keyless message's child copy is placed independently of its parent copy, the replica pattern's guarantee that the two live on different nodes now holds for keyed messages only (see the upgrade notes).
- A consume response's `key` is always a valid JSON string. A key that is not valid UTF-8 comes back base64-encoded with `"key_encoding": "base64"` beside it, mirroring `payload_encoding`; `narad sub` prints such a key in hex, marked `(binary)`.
- The partition `hwm` file holds a boundary only while the log is closed: `Close` writes the exact high-watermark, the first commit after an open empties the file (at most one truncate and fsync per log open), and an empty file recovers the boundary from the CRC-verified record tail. Two consequences are visible. A crash's hidden tail (records fsynced by a commit that never returned) becomes visible when the log reopens rather than at its next commit, as duplicates the ingress WAL also re-commits, never as loss. And the `hwm` file can fail a commit only until the first commit after an open has emptied it (an unusable path keeps failing commits, which the ingress WAL retries); after that commits never touch the file, and a failure to write it surfaces at `Close`.
- `narad_storage_high_watermark_persist_duration_seconds` now observes at most one release per log open (at its first commit) and at most one write per close, not one fsync per commit. Its rate no longer tracks commits, so dashboards and alerts that read it as a per-commit cost no longer measure one.
- Commit batches that reach one partition together share one append and one durable commit under the produce lock, and share its outcome: a failed cycle fails every batch in it, and each is retried by the ingress WAL or the fan-out cursor as before.
- The owner re-checks ownership, the handoff freeze, whether the caller is still waiting, and the topic incarnation under the partition's produce lock. A batch whose caller already gave up is not appended, and a batch stamped with another incarnation is refused with a retriable error.
- Commit times never decrease along a partition within one process: a batch stamped earlier than the partition's newest commit is raised to it under the produce lock. The fan-out delay gate relies on this.
- A node that owns none of a topic's partitions parks its consumers on delivery tokens instead of polling the owners, with a re-probe every 2 s as a safety net. It still polls while an owner of the topic refused a registration within the last 2 minutes (an older node during a rolling upgrade).
- Stale delivery tokens are left to lapse at their TTL instead of being dropped with a frame to every owner; consumers parking within 250 ms of a registration share it. Owners still accept drop frames from older peers.
- A queue-style long-poll on a topic whose partitions are not assigned yet, or whose owners are all down, waits out its `wait` for a route to appear instead of answering `204` at once. These held requests count against `http.max_consume_in_flight_per_identity`.
- A forwarded ack, extend or nack whose requester gave up while it queued for a handler slot on the owner is answered `503` without being applied, and a probe or claim in the same position is answered `204` (a claim's hold is retired). Produce commits have a concurrency bound of their own, the same size as the other messaging handlers' (max(64, 4 x GOMAXPROCS)).
- Partition transfers (segment chunks, fan-out cursor listings, handoff freezes) ride a second peer client with its own UDP socket and connection to each peer, opened on first use, so a node holds two connections to a peer it has moved partitions with.
- Any cluster RPC that ends on a deadline, not only one that hits the transport's fallback timeout, triggers a liveness ping of its connection. The connection is dropped only if the ping goes unanswered for 1 s and nothing else arrived on it meanwhile.
- A topic create or partition increase made while the cluster is still forming (some Raft voters have not registered as members) waits up to 2 s for them, so its partitions are spread instead of landing on the first node and being moved. A create or partition increase that finds no alive member at all still succeeds and now logs a warning with the error `metastore: no alive member to own new partitions`: "topic created without immediate partition assignment" for a create, "topic partitions increased without immediate assignment" for an increase. Both used to succeed without a word.
- The per-partition gauges (lag, oldest unconsumed age, in-flight and acked-ahead sizes, partition size, segments) are exported for owned partitions whose log is closed, read from disk and refreshed at most every 30 s, and a partition with no consumer state loaded reports its persisted frontier instead of 0. Three more series follow from the same change. `narad_partitions_total` and `narad_topic_bytes` are summed over those partitions, so they now count owned partitions with a closed log too and step up after the upgrade on a node with idle-evicted partitions or ones not reopened since a restart. `narad_consumer_dropped_messages` is exported for closed partitions as well, and a partition with no consumer state loaded no longer reports everything below its log start as dropped.
- The fan-out cursor file (`fanout-<child>.offset`) is a fixed 256-byte, CRC-checked JSON record advanced in place with one write and one `fdatasync`, instead of a temp file and a rename per slab. Old files are read and migrated on the first advance.
- Accepted produce records for a topic with an incarnation id (any topic created on v2.2.0 or later) are written to the ingress WAL in record format 2, which adds that id. Records for an older topic stay in format 1.
- The produce dispatcher commits each destination partition on its own instead of in passes: at most one commit in flight per partition, up to 16 across partitions, each new batch carrying whatever queued while the last one ran. A failing destination holds one probe record, retried on a 5 s budget once a second, while its other records wait in the ingress WAL, and after 3 s of failed commits (it was three failed dispatch passes) its records are rerouted to a live sibling partition. Only a failure to read the WAL or store the checkpoint backs off the whole dispatcher.
- A produce commit to another node carries each record's topic incarnation id, so the owner refuses records of a replaced incarnation the way a commit on the accepting node already did. An owner on an older release refuses the field (`400`, trailing data); it gets the batch again without the ids, and batches without them for the next 2 minutes. The owner answers such a refusal with `412 Precondition Failed`, naming both ids and logged at info, and the dispatcher treats it as the retriable mismatch a local commit returns: the records, and what queued behind them for that partition, go back to the WAL and are checked again on the next rescan (discarded once the leader confirms their incarnation is gone, committed on their own partition once the owner's replica has caught up), without the owner being marked failing or the records being rerouted to a sibling partition. The single-record commit op carries the id and is checked the same way.
- A produce commit to another node carries at most 8 MiB of encoded records; the rest of the destination's queue follows in the next commit (see Fixed).
- The per-identity cap on concurrent consumes (`http.max_consume_in_flight_per_identity`) counts a batch consume as its `max`, clamped to the cap.
- The `hwm`, fan-out cursor and ingress WAL changes above, and the three new config file settings, have rollback conditions; see the upgrade notes below.
- The broker logs `consumer offset commits cannot keep to their interval: persisted offsets lag acks by about the flush time`, at most once a minute (with `partitions`, `flush_took` and `interval`), when committing the changed partitions takes longer than `storage.consumer_offset_commit_interval_ms`. Commits then run back to back and the crash redelivery window is about one commit's duration rather than the interval; a longer interval gives produce the disk back, and fewer partitions per node or a faster disk shortens the lag.
- Log messages that a search or alert may match changed. The produce dispatcher's `rerouting produce records for dead partition owner` is now `rerouting produce records for unavailable partition owner`, and `rerouting produce records for stuck partition owner` gained an `err` field with the commit error. `drop topic schemas after retiring incarnation` is now `drop topic schemas after retiring or purging it`. A new warning, `discarding undispatched record of a deleted topic incarnation` (with `topic`, `topic_id`, `partition` and `seq`), is logged when the dispatcher, placing a `202`-acked record, destroys it because its topic was deleted and recreated under the same name while the record waited in the ingress WAL. One found that way only after a failed commit is logged as `discarding undispatched record for deleted topic`, as a deleted topic's records are (see [Discarding](docs/internals/produce-path.md#discarding-the-one-way-a-wal-record-dies-unfinished)).
- Opening a partition reads only its active segment (see Performance). A corrupt record in a sealed segment is found by the read that reaches it, where the open used to skip it in silence, and it is never served: consume skips it and counts it in `narad_consumer_corrupt_skipped_total` with a warning, replay answers `410`, fan-out counts it in `narad_fanout_child_dropped_messages`, all as before. An unreadable (`EIO`) sealed segment no longer fails the whole partition open; the reads that need it fail instead. See [Storage Engine](docs/internals/storage-engine.md#recovery).
- CI's race-detector step runs with an explicit 20-minute per-package timeout, and the unit-test job may run for 25 minutes, so a slow or hung package fails with Go's goroutine dump rather than a runner kill.

### Deprecated
- `storage.high_watermark_sync_interval_ms` has no effect, since an open log no longer persists its high-watermark. The field is kept and any value passes validation, so code that sets it keeps loading. It was never settable from the config file or the environment.

### Performance
- A partition commit pays one fsync instead of two: the high-watermark file is no longer fsynced on every commit under the produce lock.
- Commit batches that queue on one partition while a commit runs share the next cycle's single write, fsync and read-back instead of paying one each.
- On macOS, WAL, segment and directory syncs issue `F_FULLFSYNC` through `x/sys` as a proper system call, so the Go scheduler releases the goroutine's P for the length of the sync and garbage collection no longer waits behind it. Durability is unchanged.
- The ingress WAL recycles two staging buffers across group commits instead of allocating one per batch, and its replay aliases payloads, interns topic names and pools its read buffers.
- The consumer offset committer makes one data sync per changed partition: when the out-of-order ack set changed, its `consumer.ahead` record carries the frontier and `consumer.offset` is not synced as well. Under out-of-order acks that halves the syncs.
- Frames written while a partition has readers go into the frame cache as they are written, so a consumer at the tail reads them without a disk read or a decode. Frames decode without copying each record, a zstd frame read by a log whose codec is not zstd shares one decoder, frame encoders and read-back buffers come from shared pools instead of being pinned per partition, and the navigation cache stops at the first frame that covers an offset.
- Partition log lookups (`Logs.Get`) and the produce lock no longer allocate, and the pause checks cost one atomic load while nothing is paused. A consume scan resolves its first partition's log alone, so a hit there opens nothing else, and only after a miss resolves the rest of the topic's local partitions in one lookup (`Logs.GetMany`, up to 16): an empty consume costs 18% less over 8 local partitions, 22% less over 12 and 58% less under parallel load, and a hit costs what it did.
- Schema validation walks produce payloads with a pooled token decoder. After a schema change each node reads the topic's latest schema once and compiles it once, shared by every produce waiting on it; before, every waiting produce read the whole history and compiled it.
- The authentication middleware decodes the Basic credentials into a stack buffer on its cache hit path, and the consume encoder writes keys, topics and receipt handles with an allocation-free JSON string appender.
- Cluster RPC: frames over 256 KiB are written without a staging copy, replies are encoded into recycled buffers, request decoders stay on the stack, a forwarded reply without a body is written as a bare status, and forwarded acks, probes, claims, token calls and remote produce commits hand the transport a per-call budget instead of deriving a timeout context each.
- Consumers parked on one node share token registrations, and a parked consume no longer advances the owner probe cursor.
- Forwarded acks to one owner share RPCs under heavy load. While fewer than max(128, 8 x GOMAXPROCS) ack RPCs to an owner are in flight (twice the owner's messaging-handler bound), an ack goes out at once on its own, as before; past that, further acks queue and leave together as one `AckBatch` RPC (up to 64 records) the moment a slot frees. Each HTTP ack still gets exactly the response its own RPC would have, and the owner applies a batch under one handler slot. Measured against v3.0.1 with 1 to 512 acks in flight to one owner and round trips of 0, 200 µs and 1 ms, no point was slower; from 256 in flight, a half to a quarter as many RPCs go out. These batches, and a client batch ack's batch to each owner, are counted under a new `op="ack_batch"` on the cluster RPC metrics (see the Metrics upgrade note).
- The produce dispatcher wakes as soon as a record becomes durable instead of on its next 10 ms poll, so at light load a record no longer waits up to 10 ms (about 5 on average) for its commit to start.
- While other commits run, a produce destination holding fewer than 64 records waits for more, up to twice its owner's recent commit latency and at most 50 ms, so an ingress WAL group commit no longer turns into one small commit and one partition fsync per partition on the disk the next group commit waits for.
- The dispatch checkpoint's `fdatasync` is off the dispatch path: the value is written in place at once and flushed in the background within 250 ms, one flush for however many stores landed meanwhile.
- Fan-out commits a slab's child-partition batches concurrently (up to 16 at once), and reads parent slabs without copying the records.
- The lease tracker's expiry heap is typed (no interface boxing), its clock is read without a lock, and its shard table is a `sync.Map`.
- Opening a partition no longer reads or CRC-checks its sealed segments: a sealed segment's range ends at its successor's base offset, taken from the file names. On 256 MiB of 4 KiB frames an open went from about 94 ms and 287 MiB allocated to about 0.5 ms and 13 KiB (macOS, warm page cache), which speeds up restarts and the reopens after idle eviction, retention changes, move installs and the cold-retention walk. The active segment's walk streams its frames through one buffer, about 20% faster. Startup opens the owned partitions of up to min(8, GOMAXPROCS) topics at once: 93 partitions over 31 topics holding 1.2 GiB went from about 297 ms to 18 ms with both changes. A node's whole restart-to-ready time, dominated by its metastore catch-up, did not move measurably at 400 MiB per node.
- The fan-out and move reconcilers skip their once-a-second pass while the replica's topic, assignment, schema, user and routing-member versions have not moved, read through a new metastore accessor, `LatestDomainVersion` (member heartbeats and drain flags do not move them). A full pass still runs at least every 30 s, on every fan-out orphan-sweep tick, after a failed or unfinished pass, on the first tick after the replica catches up, and after any cursor or move worker exits. One tick of both, over 12-partition topics with no links or moves, takes about 91% less time at 100, 1,000 and 5,000 topics (29.5 ms to 2.8 ms at 5,000).
- Replacing a peer's consume token on the owner, and a consumer giving up its place in the owner's waiter queue, cost O(1) amortized instead of a scan and copy of the topic's queue: with 1,000 entries queued, a re-registration went from about 550 ns to 100 ns.
- The owner remembers each forwarded consume's delivery for 2 s so a cancel that races the reply can give the records back, and expiring those records costs O(1) amortized: every expiry used to shift the whole queue under its lock, about 60 µs per delivery at 50,000 deliveries a second in a benchmark, 141 ns now.
- A local consume writes the delivered record into its pooled buffer without boxing it for `WriteJSON`: 2 fewer allocations per delivery (the HTTP edge benchmark went from 721 ns and 10 allocations to 657 ns and 8).

### Security
- The Go toolchain is pinned to 1.27.1. The pin started at 1.26.6, which carried fixes for four standard-library advisories the new `govulncheck` job found on its first run against 1.26.0: quadratic complexity in `net/url` path resolution (GO-2026-6218), unbounded post-handshake messages in `crypto/tls` (GO-2026-6090), `ReadHeaderTimeout` not applied during the unencrypted HTTP/2 check in `net/http` (GO-2026-6089), and unbounded recursion in `encoding/asn1` (GO-2026-5972). A scan on 1.27.1 reports none of them, and no others.

### Fixed
- Stale version references in the documentation. The README advertised v2.2.0 and the deployment page told people to run a v0.2.0 beta image, five releases after it was superseded.
- A link on the schemas page that pointed at an anchor on a different page, so it silently went nowhere.
- The signature verification recipe in the README pinned only the repository, so it would have accepted a signature from any workflow on any branch. It now pins the publishing workflow on `master` or a release tag, and CI verifies the exact identity it just signed with.
- `scripts/check-release-refs.sh` no longer passes a pinned pre-release. Its pattern had no right anchor, so `v1.2.0-rc.1` matched as far as `v1.2.0` and compared equal to the release, which is the one case the check exists to catch. It also checks every reference on a line rather than the first.
- The nightly heals leftover firewall rules before it starts. Its cleanup does not run on `SIGKILL`, so a cancelled run left `DROP` rules on the ports the next run uses, which then failed while pointing at the broker.
- The nightly's numeric options are validated before they reach shell arithmetic, and the workflow passes its dispatch input as a single argument. Neither an arithmetic payload nor a smuggled second option can reach the run.
- A message key containing a control byte, invalid UTF-8, or certain non-printable characters made the whole consume response invalid JSON, which could stall a consumer on that partition. Keys are now JSON-escaped, and a non-UTF-8 key is returned base64-encoded (see Changed).
- JSON error bodies built from a request path value with a control byte or invalid UTF-8 were invalid JSON.
- A produce with a large declared `Content-Length` allocated the whole declared size before the body arrived, so a slow or stalled upload could pin up to 1 MiB per request. Bodies over 64 KiB are now read into a buffer that grows as data arrives.
- Requests sharing one bcrypt verification all failed with `499` when the first of them disconnected.
- A cancelled or timed-out caller could close a whole peer connection while a stream was being opened for it, failing every RPC in flight on it, and its cancellation armed the shared dial backoff.
- One late, expired or oversized request could abort a shared multiplexed cluster RPC stream and every request on it, frames over 256 KiB included.
- A dead or black-holed peer connection was only detected after QUIC's 30 s idle timeout when the timed-out callers carried their own deadline, which every hot-path caller does.
- A commit that passed the ownership and freeze check just before a rebalance handoff froze the partition could still append after the handoff read the final high-watermark.
- Records accepted before a topic was deleted and recreated under the same name could be committed into the new topic, by the node that accepted them or by the owner it sent them to.
- One slow, frozen or hung partition owner held up produce dispatch for every partition on the node, its own local ones included: each dispatch pass waited for every commit it started, up to 30 s for an owner that stopped answering. Any failed commit also put the whole dispatcher to sleep for a second, even when everything else in the pass had committed.
- Deleting a topic that had records waiting in the ingress WAL stalled dispatch on every partition while each record was confirmed deleted with its own leader round trip.
- A backlog of large records (about 4 KiB each or more) for one partition on another node could build a commit batch over the 16 MiB cluster frame limit, which failed the same way on every retry, so the partition never drained.
- The produce dispatcher's owner-lookup cache kept an entry for every topic ever deleted, so a workload that creates and deletes short-lived topics grew it without bound.
- Commit times could go backwards along a partition (a batch that lost the race for the produce lock, or a wall-clock step back), which held due records back at a fan-out delay gate.
- A retention change, partition reclaim or shutdown that overlapped a produce commit could make that batch visible twice.
- A move install racing a commit on the same partition could deadlock.
- Deleting a large topic, or opening a closed partition with a large backlog, stalled produce and consume of every other topic on the node for the length of the unlink or the recovery.
- The consumer frontier file could move backwards when two acks' frontier advances reached the offset committer in reverse order, so a restart, graceful or not, could redeliver acked messages.
- A consumer could wait out its whole `wait` although records were available: a wake arriving while the pump held a popped waiter was dropped when the topic's last other waiter left.
- A consume could answer `410` to a consumer that held no receipt handle when a concurrent consume skipped the same retention gap first.
- A consumer woken by a token notification it could not use (served locally, or out of budget, at that moment) took the wake with it, and the node's other parked consumers waited for the next registration refresh, up to 5 s.
- A consume kept probing further owners after its client had gone.
- A partition transfer's multi-megabyte chunks shared a connection with commit replies and could delay them.
- The lag, oldest-unconsumed-age and size series of a partition vanished once its log was idle-evicted, and a partition with no consumer state after a restart reported its whole log as lag.
- A deleted topic's storage and counter series were re-created while one of its logs stayed open.
- A schema-hydrate race could leave a node validating produces against an older schema until the next schema change.
- A schema registry drop under a live topic of the same name (a purge of a deleted incarnation) let later produces through unvalidated.
- Deleted topics left cached topic records, assignments, schema markers, consume cursors and queue state behind on every node that had served them, and a metastore version cell per name on every node.
- On a fresh or fully restarted cluster, a topic created right after `/readyz` could wait up to 10 s for owners, with produces parked and consumers answered `204` in a busy loop. Nodes now retry member registration every 250 ms until it first succeeds, and the leader assigns and rebalances within about a second of a member turning alive.
- A topic create and the controller's assignment sweep could overwrite each other's partition owners.
- While a fan-out child partition's owner was down, every retry re-sent the whole slab, duplicating it into the healthy child partitions each cycle.

### Removed
- The design report from the documentation site.
- `.github/REPOSITORY_PUBLIC_READY.md`, a pre-launch checklist whose content had gone stale and whose live parts are covered by `.github/settings.yml`.

### Upgrade and rollback notes
- **Ingress WAL record format 2.** Every accepted produce for a topic with an incarnation id (any topic created on v2.2.0 or later) is now written in record format 2, which carries that id; records for an older topic stay in format 1. This release reads format 1, so the upgrade needs nothing, and a mixed-version cluster is fine (see the mixed-version note below). v3.0.1 and earlier cannot decode format 2: a node rolled back with an undispatched format-2 record stops dispatching at it, keeps answering produce with `202`, and delivers nothing it accepted from then on until it runs a newer binary again. Nothing is lost. Before rolling a node back, drain its ingress WAL: pause producers, keep every partition owner up, and wait until `narad_ingress_dispatch_backlog_records` is 0 on every node (in a sample taken after the pause; the gauge is refreshed every 5 s). A node stops dispatching as soon as it receives `SIGTERM`, so a plain rolling rollback under produce load can leave a few such records behind. See [Rolling back to an earlier release](docs/operate/helm-chart.md#rolling-back-to-an-earlier-release).
- **The `hwm` file.** After a clean stop it holds the exact boundary, as before. After a crash it can be empty (the first commit after an open empties it), which every earlier release reads as "take the record tail", so rolling back is safe after a clean stop and after a crash alike. Until a crashed partition's log is opened and closed again, readers of the closed partition (topic describe stats, the consume pump's backlog estimate, fan-out reads of a closed parent, the transfer listing) find no boundary on disk; startup opens every owned partition before the node reports ready, so this is not normally visible. An older binary rolled back after a crash rewrites the empty file only at its first commit on that partition, so if it idle-evicts the partition first, its closed-partition readers treat it as empty until then.
- **Fan-out cursor files.** `fanout-<child>.offset` has a new fixed-size, CRC-checked format. Older binaries parse it (they ignore the `crc` field and the padding); an older binary that advances a cursor rewrites it in the old format, and this release reads and migrates that again.
- **Ingress WAL segment preparation** (`storage.ingress_wal_prealloc`) is opt-in and off by default. With it on, the WAL keeps up to two extra 64 MiB segments (the prepared active segment and a ready spare, `next-segment.prep`), and crash recovery changes: a group commit torn inside a prepared segment, including a hole of zeros followed by valid frames of the same write, is truncated at the first bad frame instead of failing the open, but only in the last segment, only in a segment that ends in the preparation trailer, and only within 16 MiB of the bad frame (larger group commits are written and synced in runs of at most 16 MiB). Within that window, damage to frames that were already synced is truncated too. A cleanly stopped prepared WAL opens on v3.0.1 and earlier without losing records, but a crash that tore a write inside a prepared segment, leaving valid frames behind a hole, can make those binaries refuse to start (a `corrupt frame` error): run this release once to recover it, or roll back only after a clean stop. Either way, remove the key from the config file before rolling back (see Configuration below). Turning the setting off on this release needs nothing: the next start trims the prepared segment and removes the spare.
- **Mixed-version clusters.** A produce commit to another node now carries topic incarnation ids, and a forwarded ack may travel in the new `AckBatch` op. An owner on an older release refuses both with a `400`: the sender resends the commit batch without the ids and the acks one at a time, and keeps doing so for that owner for 2 minutes, so a rolling upgrade costs each sending node one refused commit batch and one refused ack batch per old owner every 2 minutes, and neither reaches a client as an error. Records committed to an old owner that way are committed by name, as before this release. A batch consume forwarded to another node carries its count as another trailing field of the `Consume` op (`Max`, after `Claim`); an older owner refuses it with `400`, is asked again for one record (a claim drops `Max` and then `Claim`, within its one 500 ms budget), and is remembered for 2 minutes, so a batch through an older owner carries one message. An owner of this release answers a produce commit whose records carry another incarnation's id with `412`; no earlier release sends those ids, so a mixed pair never sees it.
- **Batch produce, batch consume and batch ack** need the node that serves them to run this release. An older node answers a batch produce `404`, ignores `max` and answers a single message in the single-message shape, and answers a batch ack `400` (`receipt_handle required`), so move clients to the batch forms once every node they can reach is upgraded, or have them fall back to single produces on a `404`.
- **Configuration.** New file settings `storage.consumer_offset_commit_interval_ms` (default 100, the cadence the offset committer had before), `storage.ingress_wal_prealloc` (default `false`) and `http.max_produce_in_flight_per_identity` (default 0, off). v3.0.1 and earlier reject all three keys: a node rolled back with any of them in its config file (the chart's `narad.config`) fails to start, with `storage.<key> is an internal setting and cannot be configured` for a storage key and `json: unknown field "max_produce_in_flight_per_identity"` for the http one. Before rolling back to v3.0.1 or earlier, remove `storage.consumer_offset_commit_interval_ms`, `storage.ingress_wal_prealloc` and `http.max_produce_in_flight_per_identity` from the config file, even where they hold their default values. The env var `NARAD_HTTP_MAX_PRODUCE_IN_FLIGHT_PER_IDENTITY` can stay: older binaries ignore env vars they do not know. The failed start happens at config load, before any storage is opened, so removing the key is all a stuck pod needs. `storage.high_watermark_sync_interval_ms` is deprecated and ignored.
- **Metrics.** New gauges `narad_ingress_wal_failed` (alert on 1) and `narad_ingress_dispatch_backlog_records` (wait for 0 on every node before a rollback). `narad_storage_high_watermark_persist_duration_seconds` no longer has one observation per commit, only at most one per log open and one per close. The per-partition gauges now include owned partitions whose log is closed, so series that used to disappear on idle eviction stay, and `narad_partitions_total` and `narad_topic_bytes` count those partitions too: expect both to step up after the upgrade on a node with idle partitions. `narad_consumer_dropped_messages` is exported for closed partitions as well and no longer reports a partition's whole prefix below its log start as dropped when its consumer state is not loaded. `narad_cluster_rpc_requests_total` and `narad_cluster_rpc_request_seconds` gain the `op="ack_batch"` label value: one RPC carrying several forwarded acks, extends and nacks for one owner (a coalesced batch of up to 64 records, or a client batch ack's handles for that owner, up to 100). The series count RPCs, not records, so under load and for batch acks `op="ack"`, `op="extend_ack"` and `op="nack"` no longer count every forwarded ack, extend or nack: a panel that reads `op="ack"` as the forwarded-ack rate undercounts, and latency panels on those ops miss the batched ones. Add `op="ack_batch"` to them. An older node never sends the op, so it reports no such series.
- **Partition segments.** Opening a partition now reads only its active segment, but the segment files are written as before, so either binary opens what the other wrote (checked by restarting one node of a 3-node cluster 14 times, alternating between a binary from before the change and this one, with high-watermarks and replay across every sealed boundary intact).
- **Go memory limit.** A node rolled back no longer derives the Go memory limit from its cgroup limit. If your pods rely on it, set `GOMEMLIMIT` explicitly (in the chart's `extraEnv`) before rolling back.
- **Message keys.** Keyless produces no longer get synthetic `key-<n>` keys, so consumers that read one see no `key` field instead; messages stored before the upgrade keep theirs. A key that is not valid UTF-8 now comes back base64-encoded with `"key_encoding": "base64"`; a client that ignores that field sees the base64 text as the key (before this release such a message made the response unparseable). Both depend on the node that handles the message, so until every node is upgraded: a keyless produce accepted by a node on an older release still gets an invented `key-<n>`, and a consume served by a partition owner on an older release still returns the old key encoding (no `key_encoding`, and a key with a control byte or invalid UTF-8 can still make the response invalid JSON), including when an upgraded node forwards the consume, since it passes the owner's reply through unchanged. On a replica child (a fan-out child created with `parent`), a keyless message's child copy is placed round-robin, independently of its parent copy, so it can land on the node that holds the parent copy; the placement guarantee now covers keyed messages only, so give the messages of a topic you replicate that way a key.

## [3.0.1] - 2026-09-16

### Fixed
- Brokers no longer became undialable by every peer after a day of uptime; the cluster-RPC certificate is now minted for a year and its dates are no longer checked when dialling, so forwarded produce, consume, and ack traffic no longer fails cluster-wide with a TLS error while the control plane still looks healthy.
- A partition-pinned long poll at the head of the waiter queue no longer blocks every unpinned consumer behind it; pinned waiters sit on a per-partition queue that is pumped first.
- A parked consumer is now served from partitions the node gained after it parked, and a partition that moved away is never reopened locally.
- Cross-node consume no longer stranded all but one consumer per node; delivery tokens are kept while consumers are parked, re-registered after a winning claim, and refreshed every few seconds.
- A consumer parked while a partition owner was down is now told when that owner comes back with a backlog.
- A transient assignment lookup failure no longer ejects a parked pinned consumer, and a read the pump could not complete is logged (rate limited) instead of looking like an idle topic.
- Replay reads are no longer counted in `narad_messages_consumed_total`.

### Changed
- Upgrading from v3.0.0 requires rolling every node: a v3.0.0 broker that has been up for more than 24 hours cannot be dialled by anyone until it restarts.
- The Helm chart keeps readiness on the API port; only the liveness and startup probes moved to the metrics listener.
- The consume documentation dropped the stale claim that acks return 503 when the acked-ahead set is full, and now describes receipt-handle nonces and token lifetimes as they actually behave.

## [3.0.0] - 2026-09-12

### Added
- A token-based cross-node consume protocol replaced the broadcast wake-and-scan: a consumer parked on a node that does not own the partition is served in about 16ms instead of waiting up to a second.
- Retention now runs on idle topics; a scheduled cold-partition walk (`storage.cold_retention_walk_ms`, default 5 minutes) sweeps expired data from partitions nobody has produced to or consumed from.
- Out-of-order acks above the committed frontier survive a restart: they are persisted alongside the frontier and carried to the new owner by a partition move, so a graceful restart redelivers nothing that was acked (previously about 200k messages per client in a load test).
- `/healthz` and `/readyz` are also served on the metrics listener, and the chart points the startup, liveness, and readiness probes there when metrics are enabled.
- New metrics `narad_cold_retention_swept_total` and `narad_reaper_restarts`.
- The steady-load test driver gained cross-node latency and edge-case modes.

### Changed
- Local delivery goes through a per-topic dispatcher instead of waking every waiter.
- The retention reaper supervises itself: a sweep that panics is logged and skipped, and a loop that stops ticking is replaced.
- Probe timeouts widened to 5s for startup and liveness (six liveness failures before a kill) and 3s for readiness, so heavy client traffic on the API port can no longer get a healthy broker killed and restarted.
- The segment flusher timer is armed only while a flush is owed instead of ticking on every open log.
- The Grafana stage dashboard gained a rate-window variable plus run totals and peak rates that read correctly at a 60s scrape interval.
- The load driver reports an undrained backlog as `UNDELIVERED` rather than `LOSS`, with the default drain budget raised from 90s to 240s.
- Dependencies refreshed (golang.org/x sync, crypto, net, and sys; klauspost/compress; prometheus/client_model).

### Fixed
- Deleting a topic now wakes the consumers parked on it, so they get their 204 or 404 at once instead of at the end of their wait.
- Two freeze-TTL tests that raced the wall clock.

### Removed
- A 9.5MB test binary that had been tracked at the repository root.

## [2.2.2] - 2026-09-06

### Fixed
- The `--dev` startup banner's curl example now sends a JSON content type, so it is no longer refused with 415 by the content-type guard introduced in v2.2.1.
- The nightly benchmark workflow, which had failed on every run since that guard landed, could race the leader election, and could silently measure an empty topic.

### Changed
- A mechanical `go fix` modernizer pass across the tree, with no behavior change.

## [2.2.1] - 2026-09-06

### Added
- Optional Raft snapshot tuning settings (`cluster.raft_snapshot_threshold`, `cluster.raft_snapshot_interval`, `cluster.raft_trailing_logs`), which keep the library defaults when unset.
- Fuzz targets covering every wire decoder, the cluster stream loops, the RPC dispatcher, the HTTP router, and the schema compiler, validator, and compatibility check.
- Disk-fault injection and snapshot-restore-under-load test suites, plus a documented Raft TLS rotation runbook.

### Fixed
- A node restarting without a recent snapshot could open its ownership gate mid-replay, latch a stale view, and commit accepted messages into a partition copy it no longer owned; the gate now also requires that no command entry sits between the applied index and Raft's.
- Recovery now truncates a full-length corrupt last frame instead of keeping it, where its header used to shadow every later commit so records read as corrupt until restart.
- A partition whose records had all aged out can be moved again; previously the failing move pinned the move budget so a freshly joined node never received a partition.
- A failed segment roll or a failed WAL file create no longer latches the node until restart, and the metastore now closes its Raft log store on shutdown.
- Payload or schema numbers with enormous exponents are refused instead of crashing or hanging the validator.
- Validation error messages are rendered under the size cap rather than after it: a case that took over ten minutes and gigabytes now takes 24ms.
- Schema compatibility checks on large enums were quadratic and are now indexed, and registration refuses schemas whose `$ref` graph makes validation exponential.
- Six soundness holes and three completeness gaps in the fail-closed schema compatibility check, verified against 28.7 million generated payloads with zero violations.
- The metrics middleware no longer labels request counters with the raw pre-authentication method, which allowed unbounded series cardinality and a panic on a non-UTF-8 method.
- A commit-batch header can no longer reserve over 1 GiB before failing, and out-of-range partition numbers are refused at encode time instead of silently wrapping.
- A client that closes mid-request is answered 499 rather than logged as a 500 authentication store failure, usernames `.` and `..` are refused, and stream error frames refuse trailing bytes.
- A failed leadership transfer on shutdown is logged instead of silently costing a heartbeat-timeout election on the next roll.

## [2.2.0] - 2026-09-06

### Added
- Schema support end to end: a schema history endpoint, `schema` and `schema_version` on topic details, idempotent re-registration, an optional base version so racing registrations get a 409, a 1000-version cap, CLI `--schema` support, and a Schemas page on the documentation site.
- Read-your-writes on forwarded control-plane writes: a topic, user, schema, or fan-out change sent to a follower is applied there before it answers, so an immediate read or produce on the same node sees it.
- A consume waiting on one partition owner now also sees a message that lands on another owner during the wait.

### Changed
- Forwarded consumes no longer serialise on a per-call scan of delivery records; acks on a 3-node devstack went from 29.6k/s to 48k/s mean with even broker CPU.
- Fewer syscalls and copies on produce, consume, and ack: positional writes, reused frame buffers, held-open watermark and checkpoint descriptors, buffered WAL replay, targeted consumer wake-ups, and a cached owned-partition list.
- `GET /v1/topics/{topic}` no longer opens idle partition logs and fetches remote partition stats concurrently.
- Cluster RPC moved bulk traffic onto its own lanes, detects restarted peers immediately, and hands a message back when a forwarded long poll's client leaves.
- The Helm chart pins the peer list to the initial cluster size, so raising `replicaCount` no longer restarts every pod; PDB `maxUnavailable` is 1 and a silent scale-in is refused.
- The CLI pools connections with drained bodies and spreads them across every address the broker name resolves to.
- A mixed-version cluster now needs `security.allow_legacy_cluster_auth: true` on the upgraded nodes during the roll, because the cluster RPC handshake changed.
- Dependency bumps (quic-go 0.62, x/crypto 0.55, klauspost/compress 1.19.2, prometheus client 1.24.1, jsonschema 6.0.3) and a Go 1.27 modernizer sweep.

### Fixed
- A committed message could stay hidden after a crash until the partition's next commit; the high watermark is now persisted before the batch is exposed.
- A node restarting with a lagging Raft replica could take ownership of partitions reassigned while it was down and lose the records; ownership reads now wait until the replica has caught up with the leader, and `/readyz` is a live check that flips back when the leader is lost.
- Partition moves carry the fan-out cursor files, so rebalancing or decommissioning a parent partition no longer drops a child's backlog (for a delay child, its entire pending window).
- A handoff reports the consumer frontier after draining in-flight leases, so the new owner no longer redelivers the last acked messages, and the handoff freeze is fenced by a token the flip must present.
- A topic deleted and recreated under the same name can no longer resurrect the old incarnation's data.
- One lost ack no longer made a partition crawl at one batch per visibility timeout: the acked-ahead cap gates fresh deliveries only, acks for already-delivered messages are always accepted, and long pollers wake when an expired lease frees the frontier.
- Fan-out cursors anchor exactly at the attach point (and at offset 0 for partitions added later), so records committed between an attach and the cursor's first read are delivered.
- A node that received a partition back inside the freeze window of a move it had just sourced no longer refuses every commit for about a minute after a join-then-decommission cycle.
- A failed commit followed by the dispatcher's retry no longer delivers the batch twice, fsync errors poison the log until reopen instead of being retried, retention now bounds record lifetime for low-volume topics through a time-based segment roll, and data files are created 0600.
- Schema versioning is driven by the persisted history and fails closed, so a leader whose registry never loaded a topic can no longer overwrite v1, and every node picks up a schema registered elsewhere.
- A move's destination no longer serves stale files (and loses later writes) when it still held the partition's log open from an earlier ownership.
- Reservations are released on transient read errors, a consumer behind retention jumps to the oldest offset in one step, and moves of idle partitions or moves whose target died no longer pin the cluster.

### Security
- Cluster members and moves are admin-only, attaching a child requires manage rights on the child as well, and topic reads require a grant on the topic (the topic list is filtered).
- Cluster peers prove the shared secret bound to the TLS session in both directions; the previous fixed token was replayable and servers were never authenticated.
- User updates are field-scoped Raft operations, so a password change proposed from a stale replica can never resurrect revoked grants; passwords over 72 bytes are rejected with 400.
- Raft requires TLS when security is on unless plaintext is explicitly allowed, and pre-auth frames, segment chunk reads, and the transfer and control RPCs all gained size bounds.
- HTTP gained header and connection caps, a per-identity in-flight consume cap, a required JSON or octet-stream content type (or the `X-Narad-Client` header) on state-changing requests, and `/metrics` on its own listener or behind auth; the CLI gained `--password-stdin`.
- Decommissioned members are removed from membership and cannot be resurrected by heartbeats, and a removed or state-less initial member joins instead of bootstrapping a rival cluster.

## [2.1.0] - 2026-08-26

### Changed
- Every file-data durability point now uses the cheapest kernel primitive for the platform (`fdatasync` on Linux and the BSDs, `F_FULLFSYNC` on macOS, `FlushFileBuffers` elsewhere), roughly doubling single-node produce throughput (about 5.4k to 10.5k msg/s) and cutting produce p99 from 52ms to 3.5ms with unchanged durability semantics.

### Added
- A nightly throughput-regression benchmark against a committed baseline.
- A comparison page covering durability, cost, lock-in, and operations research alongside same-compute shootout results.

## [2.0.2] - 2026-07-29

### Security
- Wire codecs enforce int32 partition bounds explicitly across the ack family, consume encode, and the ingress produce-record codec.
- Responses proxied from a partition owner always carry an explicit `Content-Type` plus `X-Content-Type-Options: nosniff`, so a body that happens to look like HTML is never served as HTML.

### Changed
- Dependencies refreshed (prometheus/client_golang 1.24.0, klauspost/compress 1.19.1, golang.org/x/crypto 0.54.0, golang.org/x/sync 0.22.0) and the GitHub Actions bumped.

## [2.0.1] - 2026-07-20

### Added
- Prometheus metrics for partition moves: `narad_moves_inflight`, `narad_moves_total`, `narad_moves_duration_seconds`, and `narad_moves_bytes_total`.

### Changed
- A consume or ack pinned to a partition whose owner is down now answers a retryable 503 instead of 421 Misdirected Request, which clients read as terminal.

### Fixed
- The old owner's leftover partition directory is reclaimed after a move's ownership flip instead of sitting on disk forever, gated on a caught-up replica plus leader confirmation that the partition lives elsewhere.

## [2.0.0] - 2026-07-19

### Added
- Automatic partition rebalance on scale-out: a joining node absorbs existing partitions, each copied verbatim (same offsets, high watermark, and consumer position) and cut over with a millisecond freeze at the very end.
- Node decommission: `narad cluster decommission <node>` drains a node's partitions onto the others and removes it from the Raft voter set, never dropping below three voters and transferring leadership away before removing a leader.
- Force-promote: a caught-up destination promotes its copy when the source node dies mid-move, strictly gated so it never exposes a truncated partition.
- An operator surface for moves and membership: the decommission endpoints, `GET /v1/cluster/moves`, `GET /v1/cluster/members`, and the `narad cluster` CLI.

### Changed
- Partition copies are two-phase (a freeze-free bulk catch-up with produce still flowing, then a short freeze for the tail), with a stop-and-copy fallback so a partition written faster than it copies still cuts over.

## [1.3.3] - 2026-07-19

### Fixed
- Creating or altering a delayed fan-out child whose delay exceeds the parent's retention buffer returns 400 Bad Request instead of 409, so clients can tell it apart from "topic already exists"; this applies to both the direct and leader-forwarded paths.

## [1.3.2] - 2026-07-18

### Fixed
- Control-plane operations return a retryable 503 during elections, partitions, and rolling restarts instead of 500 or 502, so clients retry correctly; data-plane produce, consume, and ack are unchanged.
- A `0.0.0.0` HTTP bind no longer silently breaks clustering: the advertised member address is derived from a routable host, with a startup warning if it is still unroutable.

### Added
- Go native fuzz targets for the storage parse and recovery surfaces (test only; recovery held against arbitrary corruption and no product bugs were found).

## [1.3.1] - 2026-07-18

### Removed
- The legacy `narad client` subcommands; the verb tree introduced in v1.3.0 is the CLI. `narad serve` (the container entrypoint) and `narad version` are unchanged.

## [1.3.0] - 2026-07-18

### Added
- A full command-line surface with no new runtime dependencies: `server start --dev`, `ctx`, `topic`, `pub`, `sub` (including a read-only `--peek` tail that never reserves anything), `replay`, `bench`, `user`, and `server report`.
- Homebrew installation (`brew install debanganthakuria/narad/narad`), built from the tag's source.

## [1.2.0] - 2026-07-15

### Added
- Idle log eviction: a partition log untouched for `storage.idle_log_eviction_ms` (default 30 minutes, `0` disables) is closed and reopened lazily on the next produce, consume, or replay, so abandoned topics stop holding goroutines, file descriptors, and memory forever.
- New metrics `narad_open_partition_logs` and `narad_idle_logs_evicted_total`.

### Fixed
- The example config file no longer shows locked storage internals that the strict loader rejects.

## [1.1.0] - 2026-07-15

### Added
- Opt-in replication through fan-out: a child topic's partitions are deliberately placed away from the owners of the parent's matching partitions, so a record and its copy never share a disk (RPO is roughly the fan-out lag, typically sub-second).
- `POST /v1/topics` accepts `parent` and `fanout_delay_ms` to create, attach, and assign a child in one call, which is the only moment anti-affine placement can act; the CLI gained `--parent` and `--fanout-delay-ms`.
- `owner_node` in partition stats, so the placement guarantee can be verified directly.

### Fixed
- Create-as-child works through the leader-forward path, with a handler-to-RPC field-parity test so that class of drift cannot return.

## [1.0.0] - 2026-07-15

First production release.

### Added
- Produce accepts a raw octet stream of any content type: JSON comes back verbatim, text as text, and binary base64-flagged with `payload_encoding`.
- A client guide, an operator handbook including the Helm chart, and code-level internals documentation.

### Changed
- Promoted to a production release on the strength of a 47h 28m soak of 170.9M messages with zero loss, a zero-loss chaos matrix, 50,000 msg/s sustained through the full produce, consume, and ack flow on three nodes, and operational drills covering rolling upgrade, backup and restore, live scale-out, and offset replay.

### Fixed
- Ack backpressure: when a partition's acked-ahead set is full, only the frontier hole is reservable, instead of feeding a redelivery spiral.
- Replay error contract: offsets reaped by retention return 410 Gone and negative offsets 400, rather than 500.

## [0.2.0-beta.4] - 2026-07-11

*Pre-release.*

### Fixed
- A consumer that did not retry failed acks could trap a partition in a redelivery spiral under backlog (623k duplicate deliveries over 7 hours were observed, with no loss); fresh offsets are no longer handed out while the acked-ahead set is at capacity, so acking the frontier hole restores normal service.

### Changed
- The consuming documentation now states the client contract plainly: retry acks that return 503.

## [0.2.0-beta.3] - 2026-07-08

*Pre-release.*

### Added
- Cluster scale-out: any fresh node beyond the configured initial cluster size starts join-only, walks its peers until the leader admits it, and holds readiness until admitted, so scaling out is a replica count change. Existing partitions are not rebalanced and scale-down is not yet supported.
- Ingress WAL auto-reclaim: a fully dispatched active segment is rotated past a size floor and reclaimed at the normal checkpoint cadence, instead of being pinned on disk once its topics go quiet.

### Fixed
- The produce dispatcher no longer discards accepted, durable, undelivered records when the topic is missing from a stale local metastore replica; discard now also requires a caught-up replica and leader confirmation.
- A freshly elected leader no longer trusts its still-replaying state machine: every destructive action that treats local state as authority now runs a Raft barrier and re-reads first, and consumer offsets recover lazily from the per-partition file.

## [0.2.0-beta.2] - 2026-07-08

*Pre-release.*

### Fixed
- A restarted node's startup orphan sweep could delete live topic data; a directory is now removed only when the leader confirms the topic is absent.
- Fan-out cursors could silently rewind to the tail and drop a delay backlog (about 2,000 lost deliveries were measured); tail-anchoring now requires leader confirmation of the attach epoch, and reconcile passes wait until the replica is caught up with fresh leader contact.

## [0.2.0-beta.1] - 2026-07-07

*Pre-release.*

### Added
- Topic fan-out: a parent topic replicates every message into up to 108 independent child topics, preserving per-key ordering within a child under an at-least-once contract, with detach and re-attach guarded by attach epochs.
- Delay children: attach a child with `delay_ms` and each record is delivered only once the parent's commit time plus the delay has passed, giving retry-backoff tiers and scheduled reprocessing from one flag.
- Ack lease operations: `extend` renews the visibility window on the same receipt handle and `extend=0` is a NACK for immediate redelivery, with a lapsed lease returning 410.
- A keyed record envelope: consumers now receive the produce key and the real commit timestamp.
- CLI `topics attach/detach/children` and `ack --extend/--nack`, plus fan-out, delay, and ack lease metrics.

### Changed
- Every topic now has a uniform one-hour retention floor, which backs the fan-out and delay buffer guarantee.
- Breaking: the partition-log record envelope changed, so pre-beta partition data does not decode and topic data must be wiped when upgrading from any alpha build; the attach and detach cluster RPCs also changed shape.

## [0.1.0-alpha.2] - 2026-07-06

*Pre-release. The security milestone of the 0.1.0 line.*

### Added
- HTTP API authentication over a bcrypt-hashed user store with a per-node verification cache, and a root admin seeded at first boot.
- RBAC with produce, consume, create, and admin grants over literal or prefix-wildcard topic patterns, plus a user-management API with no-privilege-escalation invariants.
- Topic ownership: the creator owns a topic, and only the owner or an admin may alter or delete it.

### Security
- Narad is secure by default; existing unauthenticated clients receive 401 after upgrade unless security is explicitly disabled.
- The node-to-node QUIC transport requires a shared-secret HMAC handshake per stream, and the Raft metadata transport gained optional mutual TLS.
- A pentest on a live cluster held 44 of 44 designed invariants; the single finding (public `/metrics` exposure through the ingress) was fixed.

### Fixed
- A 40-bug correctness pass across storage, WAL, cluster, and broker.
- arm64 images now contain arm64 binaries; previously every arm64 image shipped an amd64 binary and crash-looped.

### Removed
- The inert `SyncBytes` knob (strict config decoding now errors on the removed key), and debug-only metrics were pruned to the operator-useful set.

## [0.1.0-alpha.1] - 2026-06-27

*Pre-release.*

### Added
- Narad's first alpha, published for early evaluation, local development, and design review, with a container image on GHCR. The remaining production-readiness gates (API auth, rate limiting, TLS, and a tested durability and disaster-recovery contract) were documented as still open.

[Unreleased]: https://github.com/DebanganThakuria/narad/compare/v3.0.1...HEAD
[3.0.1]: https://github.com/DebanganThakuria/narad/compare/v3.0.0...v3.0.1
[3.0.0]: https://github.com/DebanganThakuria/narad/compare/v2.2.2...v3.0.0
[2.2.2]: https://github.com/DebanganThakuria/narad/compare/v2.2.1...v2.2.2
[2.2.1]: https://github.com/DebanganThakuria/narad/compare/v2.2.0...v2.2.1
[2.2.0]: https://github.com/DebanganThakuria/narad/compare/v2.1.0...v2.2.0
[2.1.0]: https://github.com/DebanganThakuria/narad/compare/v2.0.2...v2.1.0
[2.0.2]: https://github.com/DebanganThakuria/narad/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/DebanganThakuria/narad/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/DebanganThakuria/narad/compare/v1.3.3...v2.0.0
[1.3.3]: https://github.com/DebanganThakuria/narad/compare/v1.3.2...v1.3.3
[1.3.2]: https://github.com/DebanganThakuria/narad/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/DebanganThakuria/narad/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/DebanganThakuria/narad/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/DebanganThakuria/narad/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/DebanganThakuria/narad/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/DebanganThakuria/narad/compare/v0.2.0-beta.4...v1.0.0
[0.2.0-beta.4]: https://github.com/DebanganThakuria/narad/compare/v0.2.0-beta.3...v0.2.0-beta.4
[0.2.0-beta.3]: https://github.com/DebanganThakuria/narad/compare/v0.2.0-beta.2...v0.2.0-beta.3
[0.2.0-beta.2]: https://github.com/DebanganThakuria/narad/compare/v0.2.0-beta.1...v0.2.0-beta.2
[0.2.0-beta.1]: https://github.com/DebanganThakuria/narad/compare/v0.1.0-alpha.2...v0.2.0-beta.1
[0.1.0-alpha.2]: https://github.com/DebanganThakuria/narad/compare/v0.1.0-alpha.1...v0.1.0-alpha.2
[0.1.0-alpha.1]: https://github.com/DebanganThakuria/narad/releases/tag/v0.1.0-alpha.1

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

### Security
- The Go toolchain is pinned to 1.26.6, which carries fixes for four standard-library advisories the new `govulncheck` job found on its first run against 1.26.0: quadratic complexity in `net/url` path resolution (GO-2026-6218), unbounded post-handshake messages in `crypto/tls` (GO-2026-6090), `ReadHeaderTimeout` not applied during the unencrypted HTTP/2 check in `net/http` (GO-2026-6089), and unbounded recursion in `encoding/asn1` (GO-2026-5972).

### Fixed
- Stale version references in the documentation. The README advertised v2.2.0 and the deployment page told people to run a v0.2.0 beta image, five releases after it was superseded.
- A link on the schemas page that pointed at an anchor on a different page, so it silently went nowhere.
- The signature verification recipe in the README pinned only the repository, so it would have accepted a signature from any workflow on any branch. It now pins the publishing workflow on `master` or a release tag, and CI verifies the exact identity it just signed with.
- `scripts/check-release-refs.sh` no longer passes a pinned pre-release. Its pattern had no right anchor, so `v1.2.0-rc.1` matched as far as `v1.2.0` and compared equal to the release, which is the one case the check exists to catch. It also checks every reference on a line rather than the first.
- The nightly heals leftover firewall rules before it starts. Its cleanup does not run on `SIGKILL`, so a cancelled run left `DROP` rules on the ports the next run uses, which then failed while pointing at the broker.
- The nightly's numeric options are validated before they reach shell arithmetic, and the workflow passes its dispatch input as a single argument. Neither an arithmetic payload nor a smuggled second option can reach the run.

### Removed
- The design report from the documentation site.
- `.github/REPOSITORY_PUBLIC_READY.md`, a pre-launch checklist whose content had gone stale and whose live parts are covered by `.github/settings.yml`.

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

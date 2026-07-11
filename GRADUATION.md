# Production Graduation Checklist

The gates between "beta that survived everything we threw at it" and a
release we'll defend in public. Each gate lists its evidence.

## Passed

- [x] **Feature complete** — topics, WAL-first produce, queue consume with
  leases (ack/extend/nack), fan-out, delay children, RBAC, replay,
  cluster scale-out. (v0.2.0-beta.x series)
- [x] **Chaos matrix, zero loss** — leader kill, child-owner kill, 2-node
  quorum loss ×3, kill mid-rolling-restart, scale-out 3→5, 5-node double
  kill; loss-detecting harness read OVERDUE=0 after every scenario.
  Four data-loss bugs found by this process and fixed (#71–#74).
- [x] **Retention & disk boundedness, proven live** — age-based segment
  reaping observed in production metrics (64 MiB / 439k messages reaped
  on one partition); ingress WAL self-reclaims to sub-MB; metastore
  snapshot-bounded.
- [x] **Ack-503 spiral broken** (#91, v0.2.0-beta.4) — consume serves only
  the frontier hole while a partition's acked-ahead set is full; client
  contract documented ("retry 503 acks").
- [x] **Mixed-version rolling upgrade** — beta.3 + beta.4 served together
  for 12 minutes under 300 msg/s (partitioned StatefulSet roll): zero
  loss, zero version-related errors, then completed to beta.4.
- [x] **Restore drill** — live data-dir snapshot of a node; total disk
  loss simulated; cluster routed around it (empty node rejoined via the
  join path); snapshot restored; node rejoined on restored state with
  stale-replica defenses engaging; loss confined to the post-snapshot
  window (RPO = snapshot age), exactly as the durability contract
  states.
- [x] **Capacity measured** — 5-node devstack cluster (≈5 CPU / 1 GiB
  memory limit per node, zstd on): **12,000 msg/s produce sustained at
  p99 = 14 ms, zero errors** (highest stage tested; the single
  load-generator pod saturated first, so this is a floor, not the
  ceiling), and ~13,000 consume+acks/s in the drain phase.

## In progress

- [ ] **Uninterrupted soak window** — 72 hours at OVERDUE=0, running
  in-cluster (immune to laptop lifecycle) since 2026-07-11T11:04:32Z.
  Window closes 2026-07-14.

## Deferred past 1.0 (deliberately)

- Partition rebalancing on scale-up and node decommission on scale-down
  (one feature; scale up freely, scale down never, until it lands).
- Broker-side rate limiting (deploy a limiter at your ingress).

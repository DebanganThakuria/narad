# Narad Fan-out (Parent → Child Topics)

Status: **implemented** (pre-1.0 feature). This document is the design as
built; §11 records where the implementation deliberately refines the original
sketch.

## 1. Summary

Fan-out lets a **parent** topic replicate every message it receives into one or
more **child** topics. Producing to a parent behaves exactly like producing to a
normal topic; in addition, each attached child independently receives a copy.

This is deliberately **not** consumer groups. Children are independent topics
with their own partitions, offsets, retention, and consumers.

### Roles are exclusive and flat

Every topic has a role:

| Role | Meaning |
|---|---|
| `standalone` | An ordinary topic (default). |
| `parent` | Has one or more children; its messages fan out to them. |
| `child` | Receives fan-out from exactly one parent. |

Invariants (enforced atomically in the Raft metastore):

- A topic is exactly one role at a time. `parent` and `child` are **mutually
  exclusive** — a child can **never** become a parent, and a parent can never
  become a child.
- Fan-out is **depth 1**. A child has no children of its own, so there are no
  chains and, by construction, **no cycles**.
- A child is attached to **exactly one** parent.
- A parent has at most **108 children** (matches the topic partition cap — a
  safety rail on amplification, see §6).

Both parents and children are otherwise **normal topics**:

- You may **produce directly to a child** — fan-out is additive, not exclusive.
- You may **consume from a parent** directly — the parent materializes its own
  log like any topic.

## 2. Why "materialize + tail the parent log" (Model C)

A produce to a parent with N children is an **N× write amplification**. The only
real design question is *where* that amplification is paid. Three options were
considered:

- **A — expand at accept:** write N ingress-WAL records per produce. Rejected:
  multiplies the ingress WAL append + fsync by N on the hot path, destroying the
  WAL-first "one durable write per produce" property.
- **B — expand at dispatch:** one WAL record, the dispatcher expands to N child
  commits. Workable, but a single dispatch checkpoint means one dead child
  head-of-line-blocks the parent, and the ingress WAL cannot compact while any
  child lags (unbounded growth).
- **C — materialize the parent, then fan out by tailing its committed log**
  (**chosen**). Producing to a parent is a normal produce (hot path untouched,
  `202` stays O(1)). Each child then tails the parent's committed partition logs
  from its own cursor.

Model C wins because it reuses primitives Narad already has (offsets, ordered
per-partition logs, the commit-batch RPC, the partitioner) and gives three
properties the others can't:

1. **Zero hot-path cost.** Producing to a parent is byte-for-byte a normal
   produce.
2. **Independent children.** Each child advances its own cursor; a dead or slow
   child stalls only itself.
3. **Bounded buffering for free.** The "buffer" for a lagging child is the
   parent's committed log, which already has retention. Nothing grows unbounded.

The one cost is one extra hop of latency (produce → parent commit → fan-out read
→ child commit), which is an acceptable trade for a fan-out feature.

## 3. The fan-out mechanism

### 3.1 Cursors

The unit of fan-out is a **cursor**, one per `(childTopic, parentPartition)`:

```
cursor key:   (child, parentPartition)
cursor value: (attachEpoch, nextParentOffset)   // next parent-log offset to fan out
```

Cursors are persisted like consumer offsets — one small file
(`fanout-<child>.offset`) in the **parent partition's directory**, next to the
log it tails — and recovered on restart. Each attach stamps the link with a
fresh **epoch** (recorded on the child's topic record and in every cursor
file), so cursor state from an earlier attachment is never resumed: a
detach/re-attach always starts at the parent's current tail, never replays.

- Total cursor state per parent = `parentPartitions × numChildren` small offset
  records (e.g. 6 × 108 = 648). Trivial.
- A newly-attached child's cursors start at the **parent's current tail**
  (only-new; **no backfill**). The tail is read when the cursor anchors —
  within a reconcile interval (~1s) of the attach; `lag_complete` in the
  list-children API reports the anchor.
- Attaching/detaching a child is "spawn / stop that child's cursors" — it
  touches nothing else.

### 3.2 Owner-driven placement

**The owner of a parent partition runs the cursors that tail it** (one per
attached child). Consequences:

- The **parent read is always local** — no internal read RPC exists or is
  needed, and the cursor's persisted offset lives in the same durability
  domain as the log it tails.
- The child write is local when the child partition is co-owned, otherwise it
  reuses the existing batched commit RPC — the same path every remote produce
  dispatch already takes.
- All cursors spread across the cluster along with the parent partition
  assignments, so the amplification divides by cluster size rather than piling
  onto one node, and cursor placement is unaffected by child partition growth.

(The original sketch placed cursors on child-partition owners; that is
unimplementable as specified — see §11.)

### 3.3 Large batches (fill-or-linger)

Each cursor reads a **large slab** of parent records (thousands of records / a
few MiB) in one pass, re-keys it, and commits it to the child as **one
`CommitAcceptedProduceBatch` per touched child partition** — one append +
fsync + high-watermark advance per partition batch, not per message. This is the primary lever that makes N× amplification
survivable: it collapses the fsync count by orders of magnitude, and the
durability CRC-readback verify amortizes across the whole batch.

Per-cursor knobs (default large; exposed for tuning):

- `fanout.max_batch_bytes`, `fanout.max_batch_records` — batch fills at
  either bound.
- `fanout.linger_ms` — a batch also flushes when the linger timer fires, so a
  low-traffic child still drains promptly.

Batch size trades **latency for throughput**: bigger batches → fewer fsyncs →
higher ceiling, but a child's messages wait until a batch fills or lingers.

### 3.4 Re-keying and ordering

Each parent record is re-keyed for each child independently: parent key `k` →
child partition = **that child's own partitioner(`k`)**. Therefore:

- **Per-key order is preserved within a child** — a single parent partition is
  read strictly in offset order, and each key deterministically lands in one
  child partition.
- Cross-partition and cross-child ordering are **not** preserved — identical to
  base Narad semantics.

## 4. Durability and delivery semantics

- **At-least-once.** A cursor advances its persisted offset **only after** the
  child batch is durably committed (commit-before-advance). A crash mid-flight
  therefore **re-commits the last batch → duplicates in the child**. This is
  consistent with Narad's existing at-least-once contract; consumers must be
  idempotent. The cursor offset must **never** run ahead of the child
  high-watermark.
- **No fan-out RBAC gate.** If a child is attached, messages are fanned out
  **regardless** of the producing user's grants. Fan-out is the topic's
  configured behavior — system-internal, not authorized or billed against the
  producer. (Access to *produce to the parent* or *consume from a child* is
  still governed by normal RBAC on those topics.)
- **No backfill.** A child attached to a parent that already has data receives
  only messages produced from the attach point forward.

### 4.1 Lagging / dead children — drop-behind

The parent's retained log **is** the fan-out buffer. If a child falls behind
further than the parent's retention (e.g. its owner was down a long time):

- the cursor **skips forward to the oldest still-retained parent offset**, and
- a metric / alert fires (`fanout_child_dropped_messages`, non-zero = data loss
  for that child).

So fan-out is at-least-once **within the parent's retention window**; beyond
that, a sufficiently-behind child loses the aged-out messages rather than
stalling the parent. This is a deliberate availability-over-completeness choice
for the failure tail.

### 4.2 Minimum retention floor

To stop a brief child outage from silently dropping data, **all topics get a
minimum effective retention of ~1 hour** (not just parents — applied uniformly).
A topic may configure longer retention but not shorter than the floor. This
gives every child at least an hour of outage tolerance before drop-behind can
trigger.

### 4.3 Schema — inherited, and enforced at attach

Children **inherit the parent's schema**. A child receives already-accepted
parent bytes, so validation effectively happens once, at the parent. Fan-out has
no per-message validation gate (§4), so a schema mismatch cannot be reconciled at
runtime without either dropping schema-violating records (data loss) or letting
the child's log violate its own schema (breaks the guarantee for the child's
consumers). Therefore a mismatch is caught as a **configuration error at attach
time**, not a data error.

**Attach rule — the child's schema must be absent or identical to the parent's:**

| Parent | Child | Result |
|---|---|---|
| none | none | ✅ attach; neither validates |
| schema A | none | ✅ attach; child **adopts** A |
| schema A | schema A | ✅ attach |
| schema A | schema B | ❌ reject: child schema differs from parent |
| none | schema B | ❌ reject: cannot attach a validated child to an unvalidated parent |

Equality (not general compatibility) is deliberate: full subset-checking between
arbitrary JSON Schemas is impractical, and Narad's schema model is already
narrow (extend-only evolution), so byte-identical is both sufficient (guarantees
every parent message satisfies the child schema) and cheap.

**Shared while attached.** Because parent and child schemas must stay identical,
an attached child's schema is **parent-managed**: it cannot be independently
changed, and the parent's extend-only schema evolution **propagates to its
children**, so the two never drift. On **detach**, the child keeps its last
schema and becomes independently managed again.

Resolution for a rejected attach: align the schemas (make them the same) or clear
the child's schema first, then re-attach.

## 5. Metadata model (Raft metastore)

Topic record gains:

```
role         : "standalone" | "parent" | "child"
children     : []string      // parent only
parent       : string        // child only
attach_epoch : string        // child only; scopes cursor state to one attach
```

New Raft ops (applied deterministically in the FSM, same pattern as topic CRUD):

- `opAttachChild(parent, child)` — validates and links. Rejects if: either topic
  is missing; parent is already a child; child is already a parent or already
  has a parent; child has children of its own; `len(parent.children) >= 108`.
- `opDetachChild(parent, child)` — unlinks; stops the child's cursors. The child
  becomes `standalone` again and keeps whatever it already received.

The fan-out workers read the parent→children mapping from the **local metastore
replica** (fast, versioned-cache pattern already used for routing), so
membership changes propagate without a hot-path lookup.

## 6. Capacity model (state it loudly)

A parent sustaining `R` msg/s with `C` children generates roughly `R × C`
child-commits/s across the cluster. Batching and owner-spread make this
survivable, but the operator-facing truth is:

> **A parent's sustainable produce rate ≈ cluster capacity ÷ (children + 1).**

The 108-child cap is a **safety rail against runaway amplification**, not a free
allowance. Size the cluster to the *fanned-out* rate, not the produce rate. The
dominant cost remains storage I/O (append + fsync + HWM per child batch), so
larger batches and more nodes are the levers.

## 7. API surface (sketch)

- `POST   /v1/topics/{parent}/children` — attach a child (`{"child": "..."}`).
- `DELETE /v1/topics/{parent}/children/{child}` — detach.
- `GET    /v1/topics/{parent}/children` — list children + per-child fan-out lag.
- Topic describe (`GET /v1/topics/{topic}`) reports `role`, `parent`/`children`.

Attach/detach are admin-or-owner operations on the parent (consistent with topic
alter/delete ownership rules). Normal produce/consume APIs are unchanged.

## 8. Observability

- `narad_fanout_lag_messages{parent,child,partition}` — parent offset − cursor
  offset; the primary health signal.
- `narad_fanout_committed_total{parent,child}` — fanned-out records.
- `narad_fanout_child_dropped_messages{parent,child}` — drop-behind losses;
  **alert on any non-zero rate.**
- `narad_fanout_batch_bytes` / records histograms — batch effectiveness.

## 9. Phased implementation plan

1. **Metadata** — roles + `opAttachChild`/`opDetachChild` + invariants in the
   metastore; the 1-hour retention floor for all topics. Unit tests for every
   invariant (exclusivity, no re-parenting, cap, single-parent).
2. **Cursor engine** — per-`(child, parentPartition)` cursor: read parent slab →
   re-key → batch-commit to child → advance offset; fill-or-linger batching;
   commit-before-advance. Owner-driven placement. Drop-behind on retention.
3. **API + wiring** — attach/detach/list endpoints; child cursors spawned on
   attach and on partition-ownership changes; stopped on detach.
4. **Observability** — the metrics in §8 + alerts.
5. **Tests** — multi-node: attach mid-flow, kill a child owner (siblings
   unaffected), drop-behind under forced retention, duplicate-on-restart,
   per-key ordering within a child, 108-child cap.
6. **Docs + rollout** — README section, capacity guidance, devstack soak.

## 10. Open questions / future

- **Backpressure** if a child owner is healthy but slow: cursors naturally slow
  (lag grows) until drop-behind; is an explicit rate cap per parent worthwhile?
- ~~Detach semantics on a busy parent~~ — resolved: the cursor is cancelled,
  an in-flight batch either completes or is dropped (commit-before-advance
  keeps this safe), the cursor file is removed, and the attach epoch
  guarantees no replay on re-attach.
- **Fairness** across 108 children sharing a node's fsync budget — round-robin
  cursor scheduling vs weighted; revisit under soak.

(Schema handling is resolved — see §4.3: inherited, equality-gated at attach,
parent-managed while attached.)

## 11. Implementation notes (deltas from the original sketch)

- **Cursor placement (§3.2).** The sketch said "the owner of a child partition
  runs the cursors that feed that partition", but a `(child, parentPartition)`
  cursor re-keys records across *many* child partitions, so "the child write is
  local" and "a single commit batch" cannot both hold for multi-partition
  children. The implementation keeps the sketch's cursor model (state per
  `(child, parentPartition)`, exactly the 648-record math in §3.1) and places
  each cursor on the **parent** partition's owner instead: reads are always
  local, no new read RPC was needed, and child writes reuse the dispatcher's
  local/remote batched commit paths.
- **Keys in the log.** Committed partition logs historically stored only the
  payload, so the produce key needed for re-keying was unrecoverable. Records
  are now stored in a tiny versioned envelope (`[v1][keyLen][key][payload]`);
  each partition log stamps a `keyed.from` marker at first open so
  pre-envelope records still read as bare payloads. Consumers now receive
  `key` on consumed messages as a side benefit. Downgrading a binary after
  keyed records were written is not supported.
- **Attach epochs.** Every attach generates a random epoch, stored on the
  child's topic record and in each cursor file. Cursor state is scoped to the
  epoch, which is what makes "no replay on re-attach" robust even when a node
  was down across the detach/re-attach.
- **Deletes dissolve links.** Deleting a parent detaches all children (they
  keep data + schema, become standalone); deleting a child unlinks it from its
  parent. A parent whose last child detaches reverts to standalone.
- **Schema equality is full-history.** The attach gate compares the entire
  version history byte-for-byte (adoption copies it wholesale), and parent
  schema puts propagate to children inside the same Raft apply, so histories
  cannot drift while attached.
- **Config updates cannot clobber links.** `opUpdateTopic` preserves
  role/children/parent/epoch from the stored record; those fields change only
  through attach/detach/delete.

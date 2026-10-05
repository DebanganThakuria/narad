---
description: "Learn how rebalance and decommission move a partition between nodes: a verbatim copy, then an ownership switch that loses no record."
search:
  boost: 0.5
---

# Rebalance and decommission

Learn how rebalance and decommission move a partition between nodes: a verbatim copy, then an ownership switch that loses no record.

!!! abstract "In short"
    - A partition's data lives only on its owner's disk, so moving it means copying it. Rebalance and decommission run the same move.
    - The leader only writes where each partition should go. Each destination node copies its partitions itself, then switches ownership with a Raft compare-and-swap.
    - The copy runs while produce and consume continue. Only the last few MiB are copied under a freeze, which usually lasts milliseconds.
    - During the freeze the source takes no new commits and hands out no new messages for that partition. Leases still out at the handover are delivered again by the new owner.
    - If the source dies for good mid-move, the destination promotes its copy after 2 minutes, but only if the copy holds everything the source had made visible.
    - A move copies only committed records, keeps each segment's age, and never freezes the source forever: a copy that cannot be verified is retried once from scratch, then reported as blocked.

Narad has no follower replication: a partition's data lives only on its owner's disk. So moving a partition means *physically copying it* to another node and switching over without losing a record. Rebalance (spreading partitions onto a new node) and [decommission](../reference/glossary.md#decommission) (draining a node) use the same machine: **relocate a partition, verbatim, from one owner to another.**

The design principle is the one that runs fan-out and assignment: **each node looks after its own partitions.** The controller (the Raft leader) writes the *desired* ownership into Raft, and each node runs a local reconcile loop that moves its own partitions toward it. No component directs the others step by step.

## Owner and target {#owner-target}

Every partition assignment carries two fields:

- **Owner**: the node that serves the partition *now*; produce and consume land here. See [owner](../reference/glossary.md#owner).
- **Target**: the node where it *should* end up; empty when the partition is where it belongs. See [target](../reference/glossary.md#target).

The controller's only job is policy: set `Target` to balance the partition count across the live nodes. The nodes do the work. The ownership change is a Raft compare-and-swap: one atomic entry, with no split-brain.

Each node's move runner looks for partitions targeted at it once a second. Each pass lists every topic and reads its assignments, so it is gated the way the fan-out reconciler is (see [Fan-out engine](fanout-engine.md#where-the-work-runs)). A tick skips the read while the replica's domain versions have not moved, and runs it anyway after a failed or unfinished pass, after a move worker exits, and at least every 30 s. A new target still starts its worker on the next tick. The stale-copy sweep keeps its own count of ticks, skipped or not, and runs every 30th.

<figure class="nr-dia nr-dia--doc" id="fig-rebalance-owner-target">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/rebalance-owner-target.html"
</div>
<figcaption>The controller only sets <code>Target</code>. The destination copies the partition and proposes the flip, which applies only if nothing changed in the meantime.</figcaption>
</figure>

## Partition move: copy, then freeze {#move}

A partition can be gigabytes. Freezing produce for the whole copy would be an outage, so the copy runs in **two phases**, and the freeze covers only the small tail:

<figure class="nr-dia nr-dia--doc" id="fig-rebalance-move-timeline">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/rebalance-move-timeline.html"
</div>
<figcaption>The bulk copy runs while the partition keeps serving; only the last tail is copied under the freeze. No client request is refused: produces still get <code>202</code>, and consumes of this partition pause for the freeze. Not to scale.</figcaption>
</figure>

**CatchUp** streams the source's segments (the sealed files plus the growing active tail) while produce keeps flowing, repeating to shrink the tail that is not yet copied. Once it is within `lagBytes` of the live tail it stops, and **PrepareHandoff** freezes the source. The freeze lasts milliseconds, because Finalize has only the last few MiB to drain.

### Hot partitions: pre-copy, then stop-and-copy {#hot-partitions}

CatchUp is a **pre-copy, then stop-and-copy** cutover, the same shape as live migration of a virtual machine. Let `W` be the partition's write rate and `B` the copy bandwidth. Each pass copies what was written since the last one, so consecutive deltas scale by `W/B`:

- **`W < B`** (the normal case: one partition's writes are far below network and disk copy speed): deltas shrink geometrically, CatchUp converges, and the freeze is tiny.
- **`W ≥ B`** (a very hot partition, or a slow copy link): deltas stay flat or grow, so the tail would never shrink below `lagBytes`.

CatchUp is therefore **bounded**: after a capped number of passes, or once the tail stops shrinking, it stops pre-copying and freezes anyway. The freeze then does a **stop-and-copy** of whatever tail remains. That is always safe and always ends, because the freeze *stops the writes*: `Finalize` drains a now-fixed tail at full bandwidth with nothing competing.

The move never hangs. A partition the copy cannot keep up with just gets a **longer freeze** (bounded by the remaining tail divided by the bandwidth), logged as a non-converged cutover. Because the tail only grows while the copy chases a moving target, stopping sooner is what keeps that freeze *smallest*.

The freeze stops *both* produce commits and new consume reservations on the source:

- **Commits are refused**, so the ingress dispatcher keeps the records and retries them, and they land at the new owner after the flip.
- **New reservations stop** for that partition (see [Handoff freeze on the source](#handoff-freeze)).

That is the no-loss guarantee: once the destination captures the final tail, no record can land behind it on the source. Anything in flight is delivered again by the new owner, and at-least-once absorbs the small burst of duplicates. If the destination dies mid-handoff, the freeze **lifts on its own when its TTL runs out**; no coordinator is needed to clean up.

### Freeze fencing and re-arming {#freeze-fence}

The TTL that makes a dead destination harmless would also make a *slow* destination dangerous. The freeze lasts 30 s, and a non-converged stop-and-copy can drain for longer. If the freeze silently lapsed mid-drain, the source would take commits again, `Finalize` would still stop on a quiet pass, and everything committed on the source between that pass and the flip would be stranded on a copy the sweep later deletes. Two things close that hole:

- **Re-arm.** `PrepareHandoff` returns a **freeze token**. While `Finalize` drains, the destination presents the token every `FreezeTTL/4`, on its own timer, so a single long pass cannot outlive the TTL either. Presenting a token *extends* the freeze that token names; it never arms a new one.
- **Fence.** Right before the install and the flip, the destination presents the token once more. The source refuses (`409`) if that freeze is no longer active, which is exactly the case where commits may have landed behind the copy. The refused destination does not flip: it freezes again under a fresh token, drains again, and only then proposes the flip. The fence also checks that the high watermark the source reports now equals the one the copy reproduced, and renews the TTL, so the install and the Raft round trip run under a fresh freeze.

A source running an older release returns no token. The destination then behaves as before (re-arms without a fence, and logs a warning), so a rolling upgrade never stalls a move.

Two details make the reported high watermark *final* rather than merely current. `PrepareHandoff` reads the transfer info while holding the partition's **produce lock**, so a commit that passed the freeze gate a moment earlier and is in the middle of its fsync finishes first and is included. A commit that passed the gate but had not reached the lock yet checks the gate again once it holds the lock, finds the freeze, and is refused with nothing appended; it retries at the new owner, so it cannot land after the reported high watermark either. And `Finalize` stops only after **two consecutive passes that copy nothing and report the same high watermark**: a single quiet pass could see the segment before an append and the high watermark after it.

The copy reproduces the source's *exact* high watermark, which may be below the physical record count (a [hidden tail](../reference/glossary.md#hidden-tail)): a reopened copy must never expose records the source had not made visible. The verify step reopens the staged copy and confirms it recovers into a log that ends exactly at that high watermark before the flip.

### Committed bytes only {#committed-only}

A partition's active segment holds more than its committed records: the frames a commit has written and not yet made visible, and the hidden tail a failed commit or a crash leaves past the high watermark. A failed commit truncates those frames and hands their offsets to other records. So the source lists the active segment only up to its committed boundary, the first frame at or above the high watermark, and while the partition's log is open a segment read never goes past it (unreleased). For a closed partition the boundary is found by walking the segment's frame headers up to the high watermark its crash recovery reports. Sealed segments are listed as they are: everything in them is below their successor's base offset.

The destination never stages more bytes than a listing reported. It cuts a staged segment back when the source lists it shorter than the copy holds, and cuts a staged copy that recovers past the high watermark back to it before the verify, which then requires the copy to end exactly at the high watermark. Those two cuts only matter for a source on an older release, which lists file sizes (see the upgrade notes in the changelog). Releases through v3.0.1 accepted any copy that reached the high watermark, so a copy could pass verification holding records the source never committed, at offsets the source later committed other records at.

### A copy that cannot be verified {#unverifiable-copy}

A staged copy that fails verification after a drain under a freeze that held (it recovers short of the high watermark, past it, or not at all) is thrown away, and the partition is copied once more from scratch (unreleased). Draining again cannot fix a copy that fails a second time, so the worker then stops: it logs `move: staged copy cannot be verified; not freezing the source again` at error, counts the move in `narad_moves_blocked{reason="copy_unverifiable"}`, and never freezes the source again. The source's last freeze lapses on its 30 s TTL, and the partition keeps serving from the source. The move stays in flight, holding one of the `MaxInFlightMoves` slots, until it is aborted or re-planned, or the destination restarts, which tries once more ([Troubleshooting](../operate/troubleshooting.md#log-move-unverifiable)). Releases through v3.0.1 froze the source again every 2 s, forever, for a copy that could never pass.

A sealed segment with a damaged frame that the source tolerates (it reads the record as corrupt and consume skips it) is not such a failure. The copy takes the segment byte for byte, the verify does not prove sealed segments, and the new owner recovers it the same way the source does.

### Segment age {#segment-age}

Retention and the cold retention walk judge a segment by its file's modification time. A transfer listing carries each segment's modification time and the source's clock at the listing, and once the staged copy is complete the destination stamps each segment with the same age on its own clock: the listing time minus the modification time, taken back from when the listing arrived (unreleased). Clock skew between the two nodes does not shift retention; a copy only looks younger by the listing's round trip. A time more than an hour in the future is ignored, and a listing from an older source carries neither field; such a copy keeps the time it was written. Releases through v3.0.1 always did that, so every move kept the moved records a full retention period longer. A power loss right after the move can undo the stamp, which only restores that behaviour.

### Handoff freeze on the source {#handoff-freeze}

`PrepareHandoff` freezes produce commits for the partition (commits are refused and retry at the new owner) and consume: the source hands out no new reservations for the partition until the flip, or until the freeze lapses. Acks, extends and nacks of leases already handed out keep working.

Before it reports the transfer info, the source waits up to 500 ms (or a quarter of the freeze TTL, if that is shorter) for the leases already out to be acked or released. It then reads the transfer info under the partition's produce lock, so an in-flight commit is either fully in it or refused. The committed offset it reports is the in-memory frontier, which is ahead of the one in the consumer files by about a tick of the offset committer (100 ms, see [Ack persistence](consume-path.md#how-acks-reach-the-disk)), and of `consumer.offset` alone by up to about 30 s plus the durability interval.

Before this, the copied frontier was the file's value, and the acks that landed in the last flush interval, or during the copy, were delivered again by the new owner: every move of a freshly created child topic onto its parent's node produced a few redeliveries of acked messages. A lease still out when the 500 ms bound is reached is delivered again by the new owner, as before (at-least-once). The freeze it arms is fenced by the token it returns (see [Freeze fencing](#freeze-fence)). Presenting the token again (`PrepareHandoff` with a token on the wire) extends that freeze, or fails with `409` if it lapsed.

### Client view during a move {#client-view}

**No client request is refused, at any point.** The freeze is internal to one partition. From the outside:

- **Produce keeps succeeding.** Every produce is accepted into the receiving node's ingress WAL and answered `202`, as always. The dispatcher's commits to the moving partition are refused for the length of the freeze, so it keeps those records and retries them about once a second; they land at the new owner after the flip, on the same partition. Only a freeze that outlasts the 3 s reroute grace sends them to a live sibling partition instead, with the usual seam in per-key order (see [Produce path](produce-path.md#dispatch)).
- **Consume pauses on this partition for the freeze.** The source hands out no new messages from the moving partition while the freeze holds, usually milliseconds; other partitions keep serving. Acks, extends and nacks of messages already handed out keep working. After the flip, consumes of that partition route to the new owner.
- **The consumer position moves with the partition.** The committed consumer offset is copied along with the data, so the new owner resumes where the old one left off. In-flight reservations are *not* transferred: anything unacked at the flip, plus an ack that lands on the source in the milliseconds after the final capture, is **delivered again** by the new owner. Duplicates, never gaps: the standard at-least-once contract.
- **The listing never runs ahead of the copy.** The source reads the consumer frontier and the fan-out cursors before the high watermark, and the high watermark before the segments, so a listing never carries a position past what it copies. The destination clamps anyway (unreleased): it installs a committed offset of at most HWM-1 and keeps only acked-ahead offsets below the high watermark, logging a warning when it had to clamp the frontier, and lowers a fan-out cursor past the high watermark to it without a log line. A source on an older release costs redeliveries, never a skipped record.
- **Old consumer state is dropped.** A destination that owned the partition before may still hold that ownership's in-memory consumer state. It drops that state before it installs the copy and again after, so no ack of the old state can reach the copy. A move target never creates consumer state for the partition before the flip, because only a node that owns a partition reserves from it.
- **Fan-out cursors move with the partition.** A parent partition's `fanout-<child>.offset` files travel as **sidecars** of the transfer info, and are installed into the copy last, after the final segment pass (the source's cursors keep advancing until the flip, since fan-out only reads). The new owner's cursor resumes from the copied position, and whatever the source fanned out between the copy and the flip is fanned out again. A cursor file is overwritten in place as its cursor advances, so the source checks each file's checksum before shipping it (reading one caught in the middle of a write again, never shipping an unverified one), and the destination checks it again before installing it. Without this, the new owner anchored at the tail and skipped the child's backlog, which for a delay child is its entire pending window.

Should a cursor file still be missing after a move, for a link that already existed when the partition was installed (a source on an older release that did not ship sidecars, or a lost file), the new owner refuses to anchor at the tail over data. Every installed copy carries a `move.marker` naming the child links that existed at install time, with their attach epochs. A cursor for such a link that finds no file resumes from the partition's **oldest retained offset** instead of the tail. A link attached after the install is a fresh attach and keeps the no-backfill contract.

So the freeze protects **durability** (no write may land behind the final copy), and **redelivery** protects consume correctness. A move never costs availability.

## Ownership flip: compare-and-swap {#flip}

`CompleteMove` sets `Owner := Target` **only if** the owner is still the source and the target is still this node. A re-plan that retargeted the move, or a competing worker, makes the compare-and-swap fail. This single guarded entry is what keeps the whole scheme free of split-brain. A retried flip of a partition that already flipped to this node is answered as done (unreleased).

An error from the flip is not proof that it failed (unreleased). A leader that loses its leadership while committing the entry answers with an error although the next leader can still commit it, and a forwarded flip's reply can be lost after the leader applied it. Releases through v3.0.1 removed the installed copy on any error; when the flip had in fact committed, the new owner served the partition from an empty log, and the old owner's sweep then deleted the only other copy. Now the destination keeps the install and resolves the outcome with the leader:

- It reads the assignment as the leader has it: behind a Raft barrier when it leads, else from the leader, which answers `GetAssignment` only behind a barrier and marks the answer as a leader read (a follower answers `421`).
- Owner is this node: the flip committed, and the move is done.
- Owner is still the source and the target still this node: the flip can still commit. The destination proposes it again under the source's re-armed freeze, as long as the freeze holds and the source's high watermark still matches the copy, for at most 2 minutes. Past that it lets the freeze lapse and stops proposing.
- Anything else, from a leader read: the flip cannot commit. The leader answers a refused flip with `409`, or `404` for a partition with no assignment, and any other failure with `503`, so the destination can tell a refusal from an outcome it does not know.

The destination takes the copy off the partition's path only once a leader read confirms the flip did not commit, and only a read that started at least 15 s after the last proposal whose outcome was unknown counts (a proposal can still be on its way into the leader's log for that long, and a read started earlier may carry a barrier taken before it arrived). It renames the copy back to staging; what happens to it then depends on why the flip did not commit:

- The leader refused the flip (the move was re-planned or aborted, or the topic was deleted). The flip can never commit, so the worker ends and removes its staging copy, keeping it only if this node owns the partition by then. A re-plan back onto this node starts a fresh worker, which copies from scratch.
- The flip only stopped being proposable as it was (the source's freeze lapsed, its high watermark moved, or the 2-minute limit passed), and the leader confirms it did not commit. The same worker resumes from the staged copy: it fetches only what it lacks, drains the source again under a fresh freeze and proposes the flip again.

A leader that cannot be asked, or an older leader whose answers are not marked as leader reads, leaves the install in place: such answers can confirm a flip but never undo one. If the source dies while a flip is unconfirmed, the installed copy is flipped as a [force-promote](#what-if-the-source-dies-mid-move) once the source has been dead long enough. A move that ends without a flip removes its staging copy, unless the partition's owner (the move's source) reads dead or has no member record and the copy holds records: that copy may be the only one left, so it is set aside to `.moves/<topic>-<N>.quarantine` and logged at error level (unreleased) ([Troubleshooting](../operate/troubleshooting.md#log-move-dead-source-staging)). If this node owns the partition by then, the copy is kept when it may hold records the partition's path lacks (the move had taken its install off the path, or no copy installed from the source is there); that case is logged at error level and needs an operator ([Troubleshooting](../operate/troubleshooting.md#log-move-keeping-staging)). When the path holds a copy installed from the source and the move took nothing off it, the flip that committed was an earlier attempt's (a restart cancelled that worker with its flip pending), and the staging copy, a later attempt's re-copy, is removed.

The install meets whatever is at the partition's path: usually nothing, or this node's own copy from an earlier ownership that its stale-copy sweep has not judged yet (a rebalance can plan the partition back before the sweep runs, and from then on the sweep skips it). That copy can hold records nobody else has, such as records this node committed past a force-promote while it was cut off, so the install quarantines it whenever it holds unexpired records, logged at error level, and replaces only an empty one. Comparing it with the incoming copy cannot prove it redundant: a copy left past a force-promote holds other records at offsets the incoming copy also holds ([Troubleshooting](../operate/troubleshooting.md#log-move-install-set-aside)) (unreleased).

The install and the rename back act on the partition's path, and a flip is also rejected when the topic was deleted under the move. If the name was recreated meanwhile, and this node already opened the successor's partition there, moving whatever the path names would take the successor's records and consumer state with it. So the install's swap and the rename back both run under the topic's open guard with the partition's log closed.

For a topic with an incarnation id, they run only while the topic directory's marker still names the incarnation the move prepared it for, and the rename back also checks that the path still names the directory the install put there. A refused install fails that attempt, and the move retries. A refused rename back moves nothing: the copy went with its incarnation's directory when that was quarantined, and the sweep reclaims it.

Before the install, the move prepares the topic directory for the incarnation its replica named when the move read the topic record: it sets aside a deleted incarnation's directory found under the name, and stamps the marker. The preparation re-reads the record under the topic's open guard and refuses unless the record still names that incarnation (unreleased). A delete and recreate between the read and the guard would otherwise have it set the successor's live directory aside as a deleted incarnation's and stamp the deleted id on a fresh one, and the successor would reopen without its records. A refused preparation fails that attempt and logs a warning once per move; the move retries until the replica catches up or a re-plan cancels it.

## Source failure mid-move {#what-if-the-source-dies-mid-move}

The destination worker holds one copy session and **retries**; it does not give up when a copy attempt fails. If the source is briefly unreachable (a pod restart), the next attempt resumes the copy, and the move completes normally once the source is back. With no replication, waiting for the source is the safe default: the source's disk holds the authoritative partition.

If the source stays dead past `ForcePromoteAfter` (2 minutes by default, long enough to rule out a restart), the destination [force-promotes](../reference/glossary.md#force-promote) the copy it already holds. Dead that long means both: the leader's last heartbeat stamp for the source is older than `ForcePromoteAfter`, and the destination has itself watched the source read dead for that long, on its own monotonic clock (unreleased). The stamp is another node's wall clock, and after a leaderless period, or with skewed clocks, it can be old the moment a destination first looks; through v3.0.1 such a destination promoted its copy at once. The destination's clock starts when its move worker first reads the source dead, and starts again when the source reads alive or the worker restarts, so a destination restart can add up to 2 minutes. A force-promote skips the freeze (a dead source is not writing) and flips ownership to itself. Force-promote is strictly gated, so it can never expose a truncated partition:

- the session must have reached the source at least once; otherwise the destination has no idea what the source exposed, so it refuses;
- the staged copy must recover to an offset **at or above the source's last-known high watermark**: the destination copied everything the source had made *visible*. If the source died before the copy caught up, promoting would drop visible records, so it refuses and keeps waiting.

A destination that cannot promote keeps waiting for the source. It logs `move: the source is dead and this node's copy is behind its last high watermark, so it cannot force-promote` once at error each time the source dies, with the copy's next offset and the source's last high watermark, and counts the move in `narad_moves_blocked{reason="source_dead_copy_behind"}` until the source returns (unreleased; through v3.0.1 it logged a warning every 2 s). A copy that reaches the high watermark but fails verification is counted as `copy_unverifiable` instead ([Troubleshooting](../operate/troubleshooting.md#log-move-dead-source-behind)).

<figure class="nr-dia nr-dia--doc" id="fig-rebalance-force-promote">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--lilac">
--8<-- "diagrams/rebalance-force-promote.html"
</div>
<figcaption>After 2 minutes the destination promotes its copy only if the copy reaches the high watermark the source last reported; otherwise it keeps waiting.</figcaption>
</figure>

Force-promote works from the last pre-copy listing, taken while produce and consume were live, and installs its positions clamped to the promoted high watermark, so the new owner never starts with a frontier past its own log end. Releases through v3.0.1 read the high watermark before the frontier and did not clamp. A record committed, delivered and acked between the two reads put the frontier at or above the high watermark, and after a force-promote every record the new owner committed below that frontier was readable but never delivered. A source asked to list a partition it has not opened since a crash opens it first, so the high watermark it reports is the recovered boundary, never 0, which would let the gate accept a partial copy.

Records the source committed between the destination's last successful read and the source's death live only on the source's disk. If the source really crashed for good, they are lost regardless; force-promote recovers everything that can be recovered, and turns a stalled move into a completed one.

"Dead" is a heartbeat verdict, though, not a post-mortem. A source cut off by a long network partition keeps serving its local consumers, and whatever dispatchers can still reach it, and its copy ends up **ahead** of the position the destination promoted at. When that source returns, its stale-copy sweep must not delete the only copy of those records. So the install writes a **move marker** (`move.marker`) into the partition directory, recording the promoted high watermark and whether it was a force-promote. The owner reports it in its transfer info, and the old owner's sweep asks the new owner for it before reclaiming.

A local copy whose recovered next offset is **ahead** of the promoted high watermark is [quarantined](../reference/glossary.md#quarantine) (renamed to `<partition>.quarantine`) and logged at error level for an operator to reconcile; a copy at or behind it is reclaimed as before. The acks such a source took from its own consumers past the promoted high watermark stay in its copy, which the sweep quarantines. They never reach the new owner, whose records at those offsets are different ones.

The destination makes the copy durable before it proposes the flip (unreleased): it syncs each sealed segment as its last chunk lands, every other file of the copy before it writes the marker, the marker and the staging directory after, and the directories the install renames the copy into, and the marker records when the copy became durable. A marker without that stamp comes from an older destination, whose copy may still be in its page cache shortly after the install, so the old owner's sweep leaves such a copy alone until the install is 5 minutes old.

The new owner can also hold **less** than the move gave it: it came back on an empty volume under the same node ID, or lost or shortened a segment that never reached its disk. So the sweep compares the new owner's segment listing with its own copy before reclaiming (unreleased). The position the owner vouches for is its high watermark, never more than the marker's promoted one. The sweep quarantines its copy, without recovering it first, when the copy holds records retention has not expired and:

- the owner lists no records;
- the owner holds records but no move marker, so they did not come from this copy (every release since v2.2.0 writes a marker on install);
- the owner's move marker records a move from another node: the partition moved on again before this sweep ran, so the marker vouches for the records that node gave the owner, not for this copy's (past a force-promote away from this node, the records it committed while cut off never left it);
- a segment below the vouched position is missing from the owner's listing, or a sealed one is shorter there.

Otherwise it reclaims with a guard at the vouched position. It defers only while the owner cannot be asked or its own copy cannot be listed. Releases through v3.0.1 reclaimed without any check when the owner reported no marker.

A destination can also be left with a copy that came **from** the owner: an install whose flip never committed, because the destination died with its flip unconfirmed and the controller cleared the target ([below](#target-failure)), or because its worker was cancelled with the flip unconfirmed and the move was re-planned. The sweep does not trust a copy's own move marker, which survives every later move of the partition and so cannot tell such an install from a copy this node served. The install is judged like any other copy: it is quarantined, and logged at error level, unless the owner vouches for it at its own marker's position (unreleased). The owner did not get the partition from the install, so it usually cannot, and the install is quarantined. Its records are usually also on the owner; check the owner before treating them as the only copy.

The reclaim closes the partition's log and drops its consumer state, then does its recovery, quarantine or removal under the topic's open guard, and only while the topic directory's marker is absent or names the incarnation the reclaim read from the metastore. A delete and recreate landing in between, with this node opening the successor's partition under the path, refuses the reclaim instead of removing the successor's directory; an old incarnation's copy left that way is quarantined and reclaimed by the stale-incarnation sweep. The reclaim reads that incarnation before it checks that the partition is owned elsewhere (unreleased). Read after it, a delete and recreate between the two reads, with the successor's partition placed on this node, paired the deleted incarnation's assignment with the successor's incarnation, so the marker check passed on the successor's own directory and the reclaim removed or quarantined it.

The same sweep removes the directory of a deleted topic whose purge never reached this node (it was down, marked dead or lagging when the delete committed), which used to stay until the node restarted (unreleased). It acts only on a directory carrying an incarnation marker whose topic the node's replica no longer knows, and only once the leader confirms that incarnation gone. The removal re-checks under the topic's guard that the local record is still absent and the marker unchanged, so a recreate the node served in the meantime keeps its directory. A directory without a marker is only counted in `narad_orphan_topic_dirs` and removed by the startup sweep, which runs before the node accepts topic creates. A node whose startup sweep was skipped, because its replica took longer than 60 s to catch up, runs this reclaim once the replica is current.

Every copy a sweep, a reclaim or an install sets aside instead of deleting is counted in `narad_quarantined_copies` and `narad_quarantined_bytes`, and listed at error level when the node starts ([Troubleshooting](../operate/troubleshooting.md#quarantined-copies)) (unreleased).

## Target failure mid-move {#target-failure}

Only the destination aborts a move, and a dead destination cannot. Left alone, moves aimed at a node that died would sit in flight forever, holding the `MaxInFlightMoves` budget and stopping every later rebalance and decommission (`narad cluster moves` would list them indefinitely).

The controller's rebalance pass therefore clears the target of any in-flight move whose target member is gone from the membership, has been dead longer than `DeadTargetAbortAfter` (2 minutes by default, so a restarting pod still finishes its copy), or is out of the Raft configuration (neither a voter nor a [non-voter still waiting for promotion](cluster-lifecycle.md#join-promotion)) while dead or draining. The partition never left its owner, so clearing the target is safe at any point. A destination that comes back runs no worker for the move, because the target no longer names it. An install it left at the partition's path, with its flip unconfirmed, is a copy of the owner's records; its stale-copy sweep quarantines it and logs it at error level unless the owner vouches for it at its own marker's position, and an operator decides what to do with it ([above](#what-if-the-source-dies-mid-move)). A staging copy it left under `.moves` stays until a later move of the same partition onto it clears it. The freed budget is used in the same pass.

## Move planner {#planner}

On each tick the leader computes the fewest moves that balance the count of owned partitions. With T movable partitions over R receiving nodes, balance gives each receiver `floor(T/R)` or `ceil(T/R)`. The plan moves exactly the surplus above that capacity, and the nodes already holding the most keep the `ceil` slots, so a partition on a node within its share is never touched.

<figure class="nr-dia nr-dia--doc" id="fig-rebalance-planner">
<div class="nr-dia__frame nr-plate nr-tint nr-tint--butter">
--8<-- "diagrams/rebalance-planner.html"
</div>
<figcaption>With 18 partitions over 4 nodes each share is 4 or 5, and only the four partitions above those shares move, all to the new node.</figcaption>
</figure>

Two properties make it safe to recompute every tick:

- **Idempotent under moves in flight.** A partition in the middle of a move is counted at its *destination* and excluded from the movable pool, so a plan computed while moves run already accounts for them and reaches a fixed point. The planner recomputes and converges; it never oscillates.
- **Bounded concurrency.** The plan tops the count of moves in flight up to `MaxInFlightMoves` (8 by default) each tick, so a large rebalance drains gradually instead of copying every partition at once.

Planning runs under a **mutex** and after a **Raft barrier**: a membership change landing in the middle of a computation cannot race two passes, and a freshly elected leader never plans against a stale state machine. Partitions of a dead owner stay where they are: their data lives only on the dead node's disk, so they wait for it to return. Anti-affinity is a *preference*: a fan-out child is steered off its parent's node when a balanced alternative exists, but balance always wins. So a replica child's copies can end up on one node after a rebalance.

## Decommission as rebalance {#decommission}

Marking a node **draining** (`POST /v1/cluster/members/{id}/decommission`) removes it from the planner's set of receiving nodes while it stays a live owner. The same minimal-movement algorithm then sheds every partition it owns onto the others. The drain flag survives a new registration, so a node that restarts in the middle of a decommission stays draining.

Once a draining node owns nothing, the controller removes it from the Raft configuration. A voter's removal sits behind two guards:

- **MinVoters** (3 by default): a node is never removed if that would drop the cluster below a quorum-safe size.
- **Leader moves off first**: a node cannot be cleanly removed from its own Raft configuration while it leads, so if the drained node is the current leader, the controller transfers leadership away, and the new leader finishes the removal.

A node that joined and was never promoted is a [non-voter](cluster-lifecycle.md#join-promotion): it has no vote and cannot lead, so it is removed without either guard.

Rebalance starts on its own when a node joins; decommission is started by an operator. The commands and the safe order of steps are in [Scale out and in](../operate/scaling.md#decommission).

## Move constants {#constants}

| Constant | Value |
|---|---|
| Move runner tick | 1s; a full pass at least every 30s |
| Freeze TTL | 30s, re-armed every quarter of it while `Finalize` drains |
| Lease drain before the frontier is read | up to 500ms, or a quarter of the freeze TTL if shorter |
| Force-promote after (`ForcePromoteAfter`) | 2 minutes, by the leader's heartbeat stamp and the destination's own clock |
| Clear the target of a dead destination (`DeadTargetAbortAfter`) | 2 minutes |
| Moves in flight (`MaxInFlightMoves`) | 8 |
| Minimum voters before a removal (`MinVoters`) | 3 |
| Stale-copy sweep | every 30th move-runner tick |

## Next steps

- [Scale out and in](../operate/scaling.md): add and remove nodes, and watch moves as they run.
- [Delivery contract](delivery-contract.md#failure-matrix): what a move, or a source that dies mid-move, does to your messages.

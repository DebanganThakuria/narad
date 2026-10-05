package cluster

// The stale-copy sweep — the source-side epilogue of a partition move.
// After the ownership flip the OLD owner still holds the partition's
// directory on disk: harmless (nothing routes to a non-owner) but wasted
// disk, forever. This sweep reclaims it.
//
// Deleting partition data is the most dangerous operation in the broker —
// without replication a wrong delete destroys the only copy — so the sweep
// follows the same discipline as every destructive reconciler here (fan-out
// cursor cleanup, orphan sweeps): act only on an AppliedCaughtUp replica,
// require the LEADER to confirm the local view (self-leader confirms
// through a Raft barrier), and let the engine re-verify affirmatively
// before touching the filesystem. Any doubt defers to the next pass —
// deferring is free, deleting is not.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// moveSweepEvery is how many reconcile ticks (Reconcile calls) elapse
// between stale-copy sweeps, counted whether or not the tick then skips
// its pass (see reconcileGate). The sweep stats partition dirs and may
// RPC the leader, so it runs at a fraction of the reconcile cadence; a
// stale copy sitting on disk a few extra minutes costs nothing.
const moveSweepEvery = 30

// sweepStaleCopies reclaims local partition directories whose partitions
// now live on other nodes (a completed move relocated them away).
func (r *MoveRunner) sweepStaleCopies(ctx context.Context) {
	if r.selfID == "" || r.reclaimer == nil {
		return
	}
	// A replica that has not proven itself current must not act: a stale
	// view could show a partition "owned elsewhere" that this node in fact
	// owns. AppliedCaughtUp requires fresh leader contact, so the view
	// below is at most seconds old — and the leader confirmation closes
	// the remaining window.
	if !r.store.AppliedCaughtUp() {
		return
	}
	topics, _, err := r.store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return
	}
	r.sweepMu.Lock()
	r.sweepStaleIncarnations(ctx, topics)
	// Plain directories of topics this replica no longer knows: a purge
	// that never reached this node.
	r.reclaimOrphanTopicDirs(ctx)
	r.sweepMu.Unlock()
	for _, t := range topics {
		if r.localDirIsOtherIncarnation(t) {
			// The local directory is a deleted incarnation's, not this
			// topic's: its partitions are not stale copies of anything
			// and must not be compared against the new owner's positions.
			// sweepStaleIncarnations sets it aside once the leader confirms.
			continue
		}
		assignments, err := r.store.ListAssignments(t.Name)
		if err != nil {
			continue
		}
		for _, a := range assignments {
			if a.OwnerID == "" || a.OwnerID == r.selfID || a.TargetID == r.selfID {
				continue // unassigned, ours, or becoming ours — never touch
			}
			dir := storage.TopicPartitionDir(r.dataDir, t.Name, a.Partition)
			if _, err := os.Stat(dir); err != nil {
				continue // no local copy; nothing to reclaim
			}
			if !r.assignmentAwayConfirmedByLeader(ctx, t.Name, a.Partition) {
				continue
			}
			// Learn what the new owner holds before deleting anything: a
			// force-promote leaves the old owner's copy AHEAD of the
			// promoted position when the old owner kept committing
			// through a network partition, and a new owner that rolled
			// back its install, lost its volume or lost unsynced segments
			// holds less than the move gave it. Records it cannot vouch
			// for exist nowhere else (see ownerReclaimGuard). An owner
			// that cannot be asked defers the sweep.
			retention := time.Duration(t.RetentionMs) * time.Millisecond
			guard, ok := r.promotedPosition(ctx, a.OwnerID, t.Name, a.Partition, dir, retention)
			if !ok {
				continue
			}
			err := r.reclaim(ctx, t.Name, a.Partition, guard)
			if errors.Is(err, messaging.ErrPartitionQuarantined) {
				if guard.SetAside != "" {
					r.logger.Error("move: stale partition copy QUARANTINED, not deleted: the new owner cannot vouch for it; operator action required",
						"topic", t.Name, "partition", a.Partition, "owner", a.OwnerID, "reason", guard.SetAside, "err", err)
					continue
				}
				r.logger.Error("move: stale partition copy QUARANTINED, not deleted: it holds records past the hwm the partition was promoted at on its new owner; operator action required",
					"topic", t.Name, "partition", a.Partition, "owner", a.OwnerID, "promoted_hwm", guard.PromotedHWM, "err", err)
				continue
			}
			if err != nil {
				r.logger.Warn("move: reclaim stale copy", "topic", t.Name, "partition", a.Partition, "err", err)
				continue
			}
			r.logger.Info("move: reclaimed stale partition copy left by a completed move",
				"topic", t.Name, "partition", a.Partition, "owner", a.OwnerID)
		}
	}
}

// assignmentAwayConfirmedByLeader reports whether the LEADER confirms the
// partition is owned by another node with no move targeting this one — the
// bar for deleting the local copy. The local replica alone is not enough:
// a stale view missing a flip TO this node would otherwise delete data
// this node is about to serve. Mirrors leaderTopicView's discipline: a
// self-leader confirms through a Raft barrier; a follower asks the leader
// over peer RPC; anything unconfirmed is a "no".
func (r *MoveRunner) assignmentAwayConfirmedByLeader(ctx context.Context, topicName string, partition int) bool {
	leaderID := r.store.LeaderID()
	if leaderID == "" {
		return false
	}
	var a metastore.Assignment
	if leaderID == r.selfID {
		if err := r.store.Barrier(); err != nil {
			return false
		}
		var err error
		a, err = r.store.GetAssignment(topicName, partition)
		if err != nil {
			return false
		}
	} else {
		m, err := r.store.GetMember(leaderID)
		if err != nil || m.Addr == "" {
			return false
		}
		a, err = r.peer.GetAssignment(ctx, m.Addr, topicName, partition)
		if err != nil {
			return false
		}
	}
	return a.OwnerID != "" && a.OwnerID != r.selfID && a.TargetID != r.selfID
}

// promotedPosition asks the partition's current owner for its transfer
// info, compares it with the local copy in dir, and returns the reclaim
// guard (ownerReclaimGuard): always KNOWN, at the position the owner
// vouches for, or set aside when the owner cannot vouch for the copy.
// A local copy that is an install which never flipped here is judged
// the same way: the owner's marker records how the owner got the
// partition, not where this copy came from, so the sweep never trusts a
// copy's own marker to relax the guard; such a copy is quarantined
// unless the owner vouches for it like any other. ok=false
// defers the sweep, which is free, and happens only when the owner could
// not be asked (unknown, no address, RPC failed) or the local copy could
// not be listed. retention is the topic's age bound (zero keeps
// forever).
func (r *MoveRunner) promotedPosition(ctx context.Context, ownerID, topicName string, partition int, dir string, retention time.Duration) (guard messaging.ReclaimGuard, ok bool) {
	m, err := r.store.GetMember(ownerID)
	if err != nil || m.Addr == "" {
		return messaging.ReclaimGuard{}, false
	}
	info, err := r.peer.ListPartitionSegments(ctx, m.Addr, topicName, partition)
	if err != nil {
		r.logger.Warn("move: sweep could not read the new owner's transfer info; deferring reclaim",
			"topic", topicName, "partition", partition, "owner", ownerID, "err", err)
		return messaging.ReclaimGuard{}, false
	}
	local, err := listLocalSegments(dir)
	if err != nil {
		r.logger.Warn("move: sweep could not list the local stale copy; deferring reclaim",
			"topic", topicName, "partition", partition, "err", err)
		return messaging.ReclaimGuard{}, false
	}
	now := time.Now()
	if unsyncedInstallTooRecent(info.MoveMarker, now) {
		r.logger.Info("move: the new owner's copy was installed by a release that does not sync it before the flip; deferring the reclaim until its writeback window has passed",
			"topic", topicName, "partition", partition, "owner", ownerID,
			"installed_at", time.UnixMilli(info.MoveMarker.InstalledAtUnixMs), "window", moveUnsyncedCopyWriteback)
		return messaging.ReclaimGuard{}, false
	}
	return ownerReclaimGuard(info, local, r.selfID, retention, now), true
}

// reclaim runs the guarded reclaim. A broker that cannot honor the guard
// is refused, never called: the sweep does not delete a copy it could not
// compare with what the new owner holds.
func (r *MoveRunner) reclaim(ctx context.Context, topicName string, partition int, guard messaging.ReclaimGuard) error {
	if g, ok := r.reclaimer.(guardedReclaimer); ok {
		return g.ReclaimMovedPartitionGuarded(ctx, topicName, partition, guard)
	}
	return fmt.Errorf("reclaimer %T cannot compare the local copy with the new owner's position (it lacks ReclaimMovedPartitionGuarded); keeping the copy", r.reclaimer)
}

// localDirIsOtherIncarnation reports whether topics/<t.Name> on this
// node carries a marker for an incarnation other than t's. Unreadable
// or unmarked directories, and records without an ID, report false.
func (r *MoveRunner) localDirIsOtherIncarnation(t topic.Topic) bool {
	if t.ID == "" {
		return false
	}
	marker, marked, err := storage.ReadTopicIncarnation(storage.TopicDir(r.dataDir, t.Name))
	return err == nil && marked && marker != t.ID
}

// sweepStaleIncarnations is the periodic half of the incarnation
// reconciliation (the startup orphan sweep is the other). Two cases:
//
//   - topics/<name> carries the marker of an incarnation other than the
//     live topic's: the name was deleted and recreated while this node
//     was down (or the purge never reached it), and this node owns none
//     of the new incarnation's partitions, so no open will ever
//     quarantine it. Once the LEADER confirms the live incarnation, the
//     directory is set aside exactly as an open would (never deleted
//     here).
//   - topics/<name>.stale-<id>: a quarantined directory. Reclaimed once
//     the leader confirms that incarnation is gone (the record is absent
//     or carries a different ID). Only quarantined directories are
//     removed by path here: a plain directory can be created by a
//     concurrent lazy open, which the startup sweep excludes with the
//     create gate and this sweep cannot.
//
// Plain directories of topics this replica no longer knows are the
// caller's third half (reclaimOrphanTopicDirs): purged under the topic's
// guard, never by path. Every failure to confirm defers to the next pass.
func (r *MoveRunner) sweepStaleIncarnations(ctx context.Context, topics []topic.Topic) {
	keeper, hasKeeper := r.reclaimer.(incarnationKeeper)
	for _, t := range topics {
		if !hasKeeper || !r.localDirIsOtherIncarnation(t) {
			continue
		}
		leaderRec, absent, ok := leaderTopicView(ctx, r.store, r.peer, r.selfID, t.Name, r.logger)
		if !ok || absent || leaderRec.ID == "" {
			continue
		}
		// EnsureTopicIncarnation refuses unless the local record carries
		// the leader's ID too (this replica can lag a recreate the leader
		// already applied): the next pass tries again.
		if err := keeper.EnsureTopicIncarnation(t.Name, leaderRec.ID); err != nil {
			r.logger.Warn("move: set aside stale incarnation directory; will retry on the next sweep", "topic", t.Name, "err", err)
		}
	}
	r.reclaimQuarantinedTopicDirs(ctx)
}

// reclaimQuarantinedTopicDirs removes topics/<name>.stale-<id>
// directories (and their numbered variants) once the leader confirms the
// incarnation id is gone: the name is absent, or live as another
// incarnation. Nothing opens a quarantined directory, so it is removed by
// path; every other directory is kept.
func (r *MoveRunner) reclaimQuarantinedTopicDirs(ctx context.Context) {
	removed, err := runtime.SweepOrphanTopicDirs(r.dataDir, func(c runtime.OrphanCandidate) bool {
		if !c.Quarantined {
			return true
		}
		leaderRec, absent, ok := leaderTopicView(ctx, r.store, r.peer, r.selfID, c.Topic, r.logger)
		if !ok {
			return true
		}
		return !absent && !(leaderRec.ID != "" && leaderRec.ID != c.Incarnation)
	}, r.logger)
	if err != nil {
		r.logger.Warn("move: quarantined topic directory sweep", "err", err)
	}
	if len(removed) > 0 {
		r.logger.Info("move: reclaimed quarantined topic directories of deleted incarnations", "count", len(removed), "dirs", removed)
	}
}

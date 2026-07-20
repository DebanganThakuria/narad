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
	"os"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// moveSweepEvery is how many reconcile passes elapse between stale-copy
// sweeps. The sweep stats partition dirs and may RPC the leader, so it
// runs at a fraction of the reconcile cadence; a stale copy sitting on
// disk a few extra minutes costs nothing.
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
	for _, t := range topics {
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
			if err := r.reclaimer.ReclaimMovedPartition(ctx, t.Name, a.Partition); err != nil {
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

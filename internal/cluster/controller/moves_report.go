package controller

import (
	"maps"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// Reasons the leader sees an in-flight move as blocked on a node (the
// reason label of narad_moves_blocked, beside the reasons each
// destination reports for its own workers).
const (
	// MoveBlockedSourceDead: the partition's owner, the move's source,
	// is dead or no longer a member. The copy cannot catch up; the
	// destination force-promotes it only once it has seen the source
	// dead long enough and the copy holds everything the source had.
	MoveBlockedSourceDead = "source_dead"
	// MoveBlockedTargetDead: the move's destination is dead. The leader
	// clears the target once it has been dead past the dead-target
	// bound.
	MoveBlockedTargetDead = "target_dead"
)

// LeaderMoveBlockedReasons lists the reasons BlockedMoves counts.
var LeaderMoveBlockedReasons = []string{MoveBlockedSourceDead, MoveBlockedTargetDead}

// BlockedMoves returns how many in-flight moves the leader sees blocked
// on a dead node, by reason, as of its last rebalance pass. Nil when this
// node does not lead or nothing is blocked.
func (c *Controller) BlockedMoves() map[string]int {
	p := c.blockedMoves.Load()
	if p == nil {
		return nil
	}
	return maps.Clone(*p)
}

// noteBlockedMoves counts the in-flight moves (those the pass did not
// just clear) whose source or target is dead, publishes the counts, and
// logs each such move at error once per term.
func (c *Controller) noteBlockedMoves(t *leaderTerm, inFlight []metastore.Assignment, aborted map[string]bool, members []metastore.Member) {
	status := make(map[string]metastore.MemberStatus, len(members))
	for _, m := range members {
		status[m.ID] = m.Status
	}
	counts := map[string]int{}
	for _, a := range inFlight {
		if aborted[moveKey(a)] {
			continue
		}
		ownerStatus, ownerKnown := status[a.OwnerID]
		var reason, msg string
		switch {
		case !ownerKnown || ownerStatus == metastore.MemberDead:
			reason, msg = MoveBlockedSourceDead, "controller: move blocked: its source is dead; it finishes only if the destination can force-promote a complete copy, or once the source returns; abort it with narad cluster moves abort to keep the partition where it is"
		case status[a.TargetID] == metastore.MemberDead:
			reason, msg = MoveBlockedTargetDead, "controller: move blocked: its destination is dead; the leader clears the target if it stays dead past the dead-target bound"
		default:
			continue
		}
		counts[reason]++
		key := reason + " " + moveKey(a)
		t.mu.Lock()
		if t.movesLogged == nil {
			t.movesLogged = map[string]bool{}
		}
		first := !t.movesLogged[key]
		t.movesLogged[key] = true
		t.mu.Unlock()
		if first {
			c.logger().Error(msg, "topic", a.Topic, "partition", a.Partition, "source", a.OwnerID, "target", a.TargetID, "reason", reason)
		}
	}
	c.publish(t, func() { c.blockedMoves.Store(&counts) })
}

// noteColocated sets narad_colocated_child_partitions: the fan-out child
// partitions whose owner also owns the parent's same-index partition, so
// both copies sit on one disk. The planner only avoids this when it can;
// the gauge says how often it could not. Nothing repairs it.
func (c *Controller) noteColocated(t *leaderTerm, byName map[string]topic.Topic, owners map[string]map[int]string) {
	n := 0
	for name, tp := range byName {
		if tp.Parent == "" {
			continue
		}
		parent := owners[tp.Parent]
		for p, owner := range owners[name] {
			if po, ok := parent[p]; ok && owner != "" && po == owner {
				n++
			}
		}
	}
	c.publish(t, func() { c.m.setColocated(n) })
}

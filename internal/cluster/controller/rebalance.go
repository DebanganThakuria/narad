package controller

// reconcileRebalance is the leader's auto-rebalance pass. It reads the
// settled cluster placement, asks the planner for the minimal moves that
// balance partition count across the live nodes, and records them as desired
// state (Assignment.TargetID) for the destination nodes to act on. It never
// copies or serves anything itself — the move workers on each node do the
// work; the controller only declares intent.
//
// Everything here is level-triggered and idempotent: it tops the in-flight
// move count up to MaxInFlightMoves each pass and re-plans from scratch, so a
// node that joins (or dies) mid-rebalance is simply reflected in the next
// pass. That, plus the barrier'd read and the planning mutex, is why a
// membership change during planning cannot corrupt anything — the worst case
// is a plan against a snapshot one tick stale, which the next tick corrects.

import (
	"context"
	"fmt"
	"slices"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func (c *Controller) reconcileRebalance(ctx context.Context) {
	if !c.store.IsLeader() || c.cfg.MaxInFlightMoves <= 0 {
		return
	}
	c.planMu.Lock()
	defer c.planMu.Unlock()

	// A consistent snapshot: barrier so this FSM reflects every committed
	// entry before we read placement and decide moves. A freshly elected
	// leader may otherwise plan against a stale applied state.
	if err := c.store.Barrier(); err != nil {
		return
	}

	members, err := c.store.ListMembers()
	if err != nil {
		return
	}
	// A draining member is still alive — it serves and answers copy RPCs, so
	// it counts as a live owner (its partitions are movable) — but it must
	// not RECEIVE partitions. Excluding it from the receiver set is exactly
	// what makes the planner shed everything it owns onto the others.
	alive := metastore.AliveMembers(members)
	aliveSet := make(map[string]bool, len(alive))
	receivers := make([]string, 0, len(alive))
	for _, m := range alive {
		aliveSet[m.ID] = true
		if !m.Draining {
			receivers = append(receivers, m.ID)
		}
	}
	if len(receivers) == 0 {
		return // nowhere to place partitions (every live node is draining)
	}

	topics, _, err := c.store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return
	}

	in, parentOwners, byName, inFlight, ok := c.buildPlanInput(topics, receivers, aliveSet)
	if !ok {
		return
	}
	in.Avoid = antiAffinityAvoid(byName, parentOwners)
	t := c.term(ctx)
	c.noteColocated(t, byName, parentOwners)

	// Moves aimed at a node that is gone never finish on their own: only
	// the destination aborts a move, and a dead destination cannot. Clear
	// them so they stop pinning the budget; the partition stays with its
	// owner (untouched by the move) and is re-planned like any other.
	aborted := c.abortDeadTargetMoves(ctx, inFlight, members)
	c.noteBlockedMoves(t, inFlight, aborted, members)

	budget := c.cfg.MaxInFlightMoves - (len(inFlight) - len(aborted))
	if budget <= 0 {
		return // already at the in-flight cap; let running moves finish
	}

	moves := PlanRebalance(in)
	var set []string
	for _, m := range moves {
		if budget <= 0 {
			break
		}
		if err := c.store.SetAssignmentTarget(ctx, m.Partition.Topic, m.Partition.Partition, m.To); err != nil {
			continue
		}
		set = append(set, fmt.Sprintf("%s/%d %s->%s", m.Partition.Topic, m.Partition.Partition, m.From, m.To))
		budget--
	}
	if len(set) > 0 {
		c.logger().Info("controller: move targets set", "count", len(set), "moves", set)
	}
}

// buildPlanInput reads every topic's assignments once and assembles the
// planner's view: effective load (in-flight partitions counted at their
// target), the movable settled partitions, the parent-owner map for
// anti-affinity, and the in-flight moves. ok is false on a transient
// read failure; better to skip a pass than plan from a partial view.
func (c *Controller) buildPlanInput(
	topics []topic.Topic, receivers []string, aliveSet map[string]bool,
) (in PlanInput, parentOwners map[string]map[int]string, byName map[string]topic.Topic, inFlight []metastore.Assignment, ok bool) {
	load := make(map[string]int, len(receivers))
	for _, r := range receivers {
		load[r] = 0
	}
	movable := map[string][]PartitionRef{}
	parentOwners = map[string]map[int]string{}
	byName = make(map[string]topic.Topic, len(topics))

	for _, t := range topics {
		byName[t.Name] = t
		assignments, err := c.store.ListAssignments(t.Name)
		if err != nil {
			return PlanInput{}, nil, nil, nil, false
		}
		owners := make(map[int]string, len(assignments))
		for _, a := range assignments {
			owners[a.Partition] = a.OwnerID
			if a.TargetID != "" {
				inFlight = append(inFlight, a)
				// Level-triggered: count an in-flight partition at its
				// destination and leave it OUT of the movable pool, so the
				// planner never re-plans a move already running.
				if aliveSet[a.TargetID] {
					load[a.TargetID]++
				}
				continue
			}
			// Settled: count at its owner and (if the owner is alive) make it
			// movable. A partition on a dead owner is stuck — its data lives
			// only there — so it is neither counted nor moved until the owner
			// returns.
			if aliveSet[a.OwnerID] {
				load[a.OwnerID]++
				movable[a.OwnerID] = append(movable[a.OwnerID], PartitionRef{Topic: t.Name, Partition: a.Partition})
			}
		}
		parentOwners[t.Name] = owners
	}
	return PlanInput{Load: load, Movable: movable, Receivers: receivers}, parentOwners, byName, inFlight, true
}

// abortDeadTargetMoves clears the target of every in-flight move whose
// destination cannot complete it: the target member is gone from the
// member list, has been dead longer than DeadTargetAbortAfter (on this
// leader's clock, see deadFor), or is out of the Raft configuration
// (neither a voter nor a staged non-voter) while dead or draining (a
// decommissioned node that has not aged out yet). Returns the moves it
// cleared. A briefly dead target (a pod restart) is left alone: its
// worker resumes the copy when it returns. Clearing the target is safe at
// any point of the move: the owner never changed, and a destination that
// comes back finds its guarded CAS refused and discards its staged copy.
func (c *Controller) abortDeadTargetMoves(ctx context.Context, inFlight []metastore.Assignment, members []metastore.Member) map[string]bool {
	aborted := map[string]bool{}
	if len(inFlight) == 0 {
		return aborted
	}
	t := c.term(ctx)
	now := c.clock()
	byID := make(map[string]metastore.Member, len(members))
	for _, m := range members {
		byID[m.ID] = m
	}
	inRaft, err := c.store.Voters()
	if err == nil {
		nonvoters, nerr := c.store.Nonvoters()
		if nerr == nil {
			inRaft = append(inRaft, nonvoters...)
		} else {
			err = nerr
		}
	}
	if err != nil {
		inRaft = nil // unknown: only the membership rules below apply
	}
	for _, a := range inFlight {
		m, known := byID[a.TargetID]
		var why string
		switch {
		case !known:
			why = "the target is not a cluster member"
		case m.Status == metastore.MemberDead && c.deadFor(t, m.LastHeartbeat, c.cfg.DeadTargetAbortAfter, now):
			why = "the target has been dead longer than the dead-target bound"
		case inRaft != nil && !slices.Contains(inRaft, a.TargetID) && (m.Status == metastore.MemberDead || m.Draining):
			why = "the target left the Raft configuration"
		default:
			continue
		}
		if err := c.store.SetAssignmentTarget(ctx, a.Topic, a.Partition, ""); err != nil {
			continue
		}
		c.logger().Warn("controller: move target cleared: "+why,
			"topic", a.Topic, "partition", a.Partition, "owner", a.OwnerID, "target", a.TargetID)
		aborted[moveKey(a)] = true
	}
	return aborted
}

// moveKey names one in-flight move.
func moveKey(a metastore.Assignment) string {
	return fmt.Sprintf("%s/%d->%s", a.Topic, a.Partition, a.TargetID)
}

// antiAffinityAvoid discourages placing a fan-out child's partition on the
// node that owns its parent's same-index partition — the replica pattern's
// whole point (keep the copy off the original's disk). It is a preference:
// the planner honors it only when an equally-balanced alternative exists.
func antiAffinityAvoid(byName map[string]topic.Topic, parentOwners map[string]map[int]string) func(RebalanceMove) bool {
	return func(m RebalanceMove) bool {
		t, ok := byName[m.Partition.Topic]
		if !ok || t.Parent == "" {
			return false
		}
		owners := parentOwners[t.Parent]
		if owners == nil {
			return false
		}
		return owners[m.Partition.Partition] == m.To
	}
}

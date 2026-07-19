package controller

// reconcileDecommission completes a decommission once its placement half is
// done. The draining machinery (Member.Draining → planner excludes it as a
// receiver) sheds every partition off a draining node; this pass watches for
// a draining node that owns nothing left and removes it from the Raft voter
// set, so the pod can be torn down safely.
//
// Two guards, per the design:
//   - MinVoters: never remove a node if doing so would drop the voter count
//     below MinVoters (default 3), so a decommission can't take the cluster
//     below a quorum-safe size.
//   - leader-off-departing: a node cannot be cleanly removed from its own
//     Raft configuration while it leads, so if the drained node is the
//     current leader the controller transfers leadership away and lets the
//     new leader finish the removal on its next pass.
//
// Everything is level-triggered: the pass reads state fresh each tick and is
// a no-op once the node is out of the configuration, so a removal that races
// a leadership change simply completes on a later tick.

import (
	"context"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func (c *Controller) reconcileDecommission(ctx context.Context) {
	if !c.store.IsLeader() {
		return
	}
	c.planMu.Lock()
	defer c.planMu.Unlock()
	if err := c.store.Barrier(); err != nil {
		return
	}

	members, err := c.store.ListMembers()
	if err != nil {
		return
	}
	var draining []string
	for _, m := range members {
		if m.Draining {
			draining = append(draining, m.ID)
		}
	}
	if len(draining) == 0 {
		return
	}

	// A draining node is "drained" when it owns no partition. In-flight moves
	// off it keep it as OwnerID until the CAS flip, so this is only true once
	// every move has completed.
	stillOwns, ok := c.ownersInUse(ctx)
	if !ok {
		return
	}
	for _, id := range draining {
		if stillOwns[id] {
			continue // moves still in flight; wait
		}
		c.removeDrainedNode(id)
	}
}

// ownersInUse reports which nodes currently own at least one partition. ok is
// false on a transient read failure — better to defer removals a tick than
// remove a node whose remaining partitions we failed to see.
func (c *Controller) ownersInUse(ctx context.Context) (map[string]bool, bool) {
	topics, _, err := c.store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return nil, false
	}
	owners := map[string]bool{}
	for _, t := range topics {
		assignments, err := c.store.ListAssignments(t.Name)
		if err != nil {
			return nil, false
		}
		for _, a := range assignments {
			owners[a.OwnerID] = true
		}
	}
	return owners, true
}

// removeDrainedNode removes a fully-drained node from the Raft voter set,
// honoring the MinVoters and leader-off-departing guards.
func (c *Controller) removeDrainedNode(id string) {
	voters, err := c.store.Voters()
	if err != nil {
		return
	}
	if !containsStr(voters, id) {
		return // already out of the configuration; nothing to do
	}
	if len(voters) <= c.cfg.MinVoters {
		return // removal would drop below the quorum-safe floor; leave it
	}
	if c.store.LeaderID() == id {
		// Can't remove the leader from its own config. Hand leadership off;
		// the new leader finishes the removal next tick.
		_ = c.store.TransferLeadership()
		return
	}
	_ = c.store.RemoveServer(id)
}

func containsStr(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

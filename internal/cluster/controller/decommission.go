package controller

// reconcileDecommission completes a decommission once its placement half is
// done. The draining machinery (Member.Draining → planner excludes it as a
// receiver) sheds every partition off a draining node; this pass watches for
// a draining node that owns nothing left and removes it from the Raft
// configuration, so the pod can be torn down safely.
//
// A node is drained only when no assignment names it at all: not as the
// owner, and not as the target of a move still copying TO it. Moves aimed
// at a draining node never help its drain, so the pass clears them (the
// AbortMove compare-and-set) before it looks, and it holds the assignment
// lock from reading placement until the removal is done, so a topic create
// on this leader cannot place a partition on the node in between. Without
// both, a flip or a create landing in that window left a partition owned
// by a removed node that nothing could route to, move off or reassign.
//
// Guards that apply to a voter (a non-voter, a joiner staged and never
// promoted, has no vote and cannot lead, so none applies to it):
//   - CheckVoterRemoval: never remove a voter unless at least MinVoters
//     voters remain (default 3) and the voters left alive are a strict
//     majority of them. Dead draining voters are removed before alive
//     ones, which never lowers the alive count.
//   - leader-off-departing: a node cannot be cleanly removed from its own
//     Raft configuration while it leads, so if the drained node is the
//     current leader the controller transfers leadership away and lets the
//     new leader finish the removal on its next pass.
//
// And for every node: its ingress WAL must have handed every record it
// accepted to its owner (a zero dispatch backlog, read with NodeStatus)
// before it leaves Raft, since a removed node's replica freezes and it can
// never dispatch them after. The backlog counts only once the node itself
// reports that it refuses client produce and is answering none it
// admitted before: a drain flip reaches the node's replica some time
// after it commits, and a produce accepted in between would land in the
// WAL after a zero backlog was read. A node whose release cannot report
// its status (3.0.x) is removed without the check, with a warning, as
// 3.0.x did.
//
// A decommission that cannot progress says why: DecommissionBlockers names
// the reasons, each change is logged once (error when it needs an
// operator, warn when it clears on its own), and
// narad_decommission_blocked{node,reason} is 1 while it holds.
//
// Removal has two halves that must both land: RemoveServer takes the node
// out of the Raft configuration, and RemoveMember deletes its member record
// and tombstones the ID. Without the second half the pod (which the
// StatefulSet keeps running until the operator scales in) re-registers on
// every heartbeat and stays listed alive-and-draining forever; after the
// pod is deleted the record lingers as dead forever. The tombstone makes
// the FSM refuse that re-registration.
//
// Everything is level-triggered: the pass reads state fresh each tick and is
// a no-op once the node is out of the configuration AND forgotten, so a
// removal that races a leadership change simply completes on a later tick
// (a draining member found already outside the configuration just gets
// the second half).

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func (c *Controller) reconcileDecommission(ctx context.Context) {
	if !c.store.IsLeader() {
		return
	}
	c.planMu.Lock()
	defer c.planMu.Unlock()
	t := c.term(ctx)
	if err := c.store.Barrier(); err != nil {
		return
	}
	c.noteBarrier(t)

	members, err := c.store.ListMembers()
	if err != nil {
		return
	}
	draining := drainingMembers(members)
	if len(draining) == 0 {
		c.syncDecomBlocked(t, nil)
		return
	}
	// Ask the draining nodes for their status before taking the
	// assignment lock, so a slow or unreachable node never holds topic
	// creates up for the length of a status call.
	statuses := c.drainingStatuses(ctx, draining)

	// From reading placement to the last removal, no topic create on this
	// leader may place a partition: it could land on a node about to go.
	unlock := c.store.LockAssignments()
	defer unlock()

	usage, ok := c.placementUsage(ctx)
	if !ok {
		return
	}
	drainingSet := make(map[string]bool, len(draining))
	for _, m := range draining {
		drainingSet[m.ID] = true
	}
	if c.abortMovesTo(ctx, usage.inFlight, drainingSet) > 0 {
		// A flip may have landed before its abort: read placement again.
		if usage, ok = c.placementUsage(ctx); !ok {
			return
		}
	}

	blocked := map[string][]Blocker{}
	for _, m := range draining {
		voters, err := c.store.Voters()
		if err != nil {
			return
		}
		leaderID := c.store.LeaderID()
		status := statuses[m.ID]
		if status == nil && c.cfg.NodeStatus != nil && m.ID != leaderID {
			// Not asked this pass (it led when the statuses were read):
			// never remove a node whose backlog was not read.
			status = &NodeStatusResult{Err: errors.New("its status was not read this pass")}
		}
		if bs := DecommissionBlockers(DecommissionView{
			Node: m, Members: members, Voters: voters, LeaderID: leaderID,
			MinVoters: c.cfg.MinVoters, MaxInFlightMoves: c.cfg.MaxInFlightMoves,
			Owned: usage.owned[m.ID], Outbound: usage.outbound[m.ID], Inbound: usage.inbound[m.ID],
			InFlight: len(usage.inFlight), Status: status,
		}); len(bs) > 0 {
			blocked[m.ID] = bs
			if bs[0].Code == BlockedLeaderTransfer {
				// Can't remove the leader from its own config. Hand
				// leadership off; the new leader finishes the removal.
				_ = c.store.TransferLeadership()
			}
			continue
		}
		if usage.owned[m.ID] > 0 {
			continue // moves off it still to run or in flight; wait
		}
		c.removeDrainedNode(ctx, m.ID, members, status)
	}
	c.syncDecomBlocked(t, blocked)
}

// drainingMembers returns the draining members in removal order: dead
// ones first (removing a dead voter never lowers the alive count, and it
// makes the next removal safer), then by ID.
func drainingMembers(members []metastore.Member) []metastore.Member {
	var draining []metastore.Member
	for _, m := range members {
		if m.Draining {
			draining = append(draining, m)
		}
	}
	slices.SortFunc(draining, func(a, b metastore.Member) int {
		aDead, bDead := a.Status == metastore.MemberDead, b.Status == metastore.MemberDead
		switch {
		case aDead && !bDead:
			return -1
		case bDead && !aDead:
			return 1
		}
		return strings.Compare(a.ID, b.ID)
	})
	return draining
}

// placement is who the assignments name: how many partitions each node
// owns, how many of those are moving off it, how many moves aim at it,
// and the in-flight moves.
type placement struct {
	owned    map[string]int
	outbound map[string]int
	inbound  map[string]int
	inFlight []metastore.Assignment
}

// placementUsage reads every topic's assignments. ok is false on a
// transient read failure: better to defer removals a tick than remove a
// node whose remaining partitions we failed to see.
func (c *Controller) placementUsage(ctx context.Context) (placement, bool) {
	topics, _, err := c.store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return placement{}, false
	}
	u := placement{owned: map[string]int{}, outbound: map[string]int{}, inbound: map[string]int{}}
	for _, t := range topics {
		assignments, err := c.store.ListAssignments(t.Name)
		if err != nil {
			return placement{}, false
		}
		for _, a := range assignments {
			u.owned[a.OwnerID]++
			if a.TargetID != "" {
				u.outbound[a.OwnerID]++
				u.inbound[a.TargetID]++
				u.inFlight = append(u.inFlight, a)
			}
		}
	}
	return u, true
}

// abortMovesTo clears, through the AbortMove compare-and-set, every
// in-flight move whose target is in targets, and returns how many it
// cleared. The owner never changed, so the partition stays where its data
// is; a destination that still finishes its copy finds its flip refused.
func (c *Controller) abortMovesTo(ctx context.Context, inFlight []metastore.Assignment, targets map[string]bool) int {
	cleared := 0
	for _, a := range inFlight {
		if !targets[a.TargetID] {
			continue
		}
		if err := c.store.AbortMove(ctx, a.Topic, a.Partition, a.TargetID); err != nil {
			c.logger().Warn("controller: clearing a move aimed at a draining node failed; retrying next pass",
				"topic", a.Topic, "partition", a.Partition, "target", a.TargetID, "err", err)
			continue
		}
		c.logger().Info("controller: move target cleared: the target is being decommissioned",
			"topic", a.Topic, "partition", a.Partition, "owner", a.OwnerID, "target", a.TargetID)
		cleared++
	}
	return cleared
}

// nodeStatusTimeout bounds one NodeStatus call, and nodeStatusConcurrency
// how many run at once.
const (
	nodeStatusTimeout     = 2 * time.Second
	nodeStatusConcurrency = 4
)

// drainingStatuses asks every draining node but the leader itself (which
// hands leadership off before anything else) for its status. A node
// marked dead is asked too: one that still answers can be checked. Nil
// when NodeStatus is not configured: no status reasons apply then.
func (c *Controller) drainingStatuses(ctx context.Context, draining []metastore.Member) map[string]*NodeStatusResult {
	if c.cfg.NodeStatus == nil {
		return nil
	}
	leaderID := c.store.LeaderID()
	out := make(map[string]*NodeStatusResult, len(draining))
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, nodeStatusConcurrency)
	)
	for _, m := range draining {
		if m.ID == leaderID {
			continue
		}
		if strings.TrimSpace(m.Addr) == "" {
			out[m.ID] = &NodeStatusResult{Err: errors.New("it has no node RPC address on record")}
			continue
		}
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			sctx, cancel := context.WithTimeout(ctx, nodeStatusTimeout)
			st, err := c.cfg.NodeStatus(sctx, m.Addr)
			cancel()
			if err != nil && m.Status == metastore.MemberDead && !errors.Is(err, ErrNodeStatusUnsupported) {
				err = errors.New("it is dead and does not answer: " + err.Error())
			}
			mu.Lock()
			out[m.ID] = &NodeStatusResult{Status: st, Err: err}
			mu.Unlock()
		})
	}
	wg.Wait()
	return out
}

// removeDrainedNode removes a fully-drained node from the Raft
// configuration, then forgets its member record. A voter is removed only
// when CheckVoterRemoval and the leader-off-departing guard allow. A
// non-voter (a joiner that was staged and never promoted) is removed
// without them: it carries no quorum weight and cannot be the leader. The
// record is deleted only once the node is out of the configuration: it is
// the tombstone that stops the departed pod's heartbeats from resurrecting
// it. The caller has already read the node's dispatch backlog.
func (c *Controller) removeDrainedNode(ctx context.Context, id string, members []metastore.Member, status *NodeStatusResult) {
	voters, err := c.store.Voters()
	if err != nil {
		return
	}
	if containsStr(voters, id) {
		if err := CheckVoterRemoval(voters, members, id, c.cfg.MinVoters); err != nil {
			return // DecommissionBlockers reports why
		}
		if c.store.LeaderID() == id {
			// Can't remove the leader from its own config. Hand leadership off;
			// the new leader finishes the removal next tick.
			_ = c.store.TransferLeadership()
			return
		}
		c.warnBacklogUnchecked(id, status)
		if err := c.store.RemoveServer(id); err != nil {
			c.logger().Warn("controller: removing a drained voter from Raft failed; retrying next pass", "node", id, "err", err)
			return // still a voter; retry next tick
		}
		c.logger().Warn("controller: decommissioned node removed from the Raft voters", "node", id, "voters_before", voters)
	} else {
		nonvoters, err := c.store.Nonvoters()
		if err != nil {
			return
		}
		if containsStr(nonvoters, id) {
			c.warnBacklogUnchecked(id, status)
			if err := c.store.RemoveServer(id); err != nil {
				c.logger().Warn("controller: removing a drained non-voter from Raft failed; retrying next pass", "node", id, "err", err)
				return // still a non-voter; retry next tick
			}
			c.logger().Warn("controller: decommissioned node removed from the Raft non-voters", "node", id)
		}
	}
	if err := c.store.RemoveMember(ctx, id, c.clock().Unix()); err != nil {
		c.logger().Warn("controller: forgetting a decommissioned member failed; retrying next pass", "node", id, "err", err)
		return
	}
	c.logger().Info("controller: decommissioned member removed", "node", id)
}

// warnBacklogUnchecked says that a node is leaving Raft without its
// dispatch backlog read, because its release cannot report it.
func (c *Controller) warnBacklogUnchecked(id string, status *NodeStatusResult) {
	if status != nil && errors.Is(status.Err, ErrNodeStatusUnsupported) {
		c.logger().Warn("controller: removing a drained node whose release cannot report its dispatch backlog; it was not checked (records only its ingress WAL holds are lost if its volume is deleted before it dispatches them)",
			"node", id)
	}
}

// syncDecomBlocked exports this pass's blocked decommissions: it logs
// each reason that is new for its node (error when it needs an operator,
// warn when it clears on its own), sets its series, and deletes the
// series of reasons that no longer hold.
func (c *Controller) syncDecomBlocked(t *leaderTerm, blocked map[string][]Blocker) {
	t.mu.Lock()
	prev := t.blocked
	next := make(map[string]map[string]bool, len(blocked))
	for node, bs := range blocked {
		next[node] = make(map[string]bool, len(bs))
		for _, b := range bs {
			next[node][b.Code] = true
		}
	}
	t.blocked = next
	t.mu.Unlock()

	c.publish(t, func() {
		for node, reasons := range prev {
			for reason := range reasons {
				if !next[node][reason] {
					c.m.clearDecomBlocked(node, reason)
				}
			}
		}
		for node, reasons := range next {
			for reason := range reasons {
				c.m.setDecomBlocked(node, reason)
			}
		}
	})
	for node, reasons := range prev {
		if _, still := next[node]; !still && len(reasons) > 0 {
			c.logger().Info("controller: decommission no longer blocked", "node", node)
		}
	}
	for node, bs := range blocked {
		for _, b := range bs {
			if prev[node][b.Code] {
				continue
			}
			if stallReasons[b.Code] {
				c.logger().Error("controller: decommission blocked", "node", node, "reason", b.Code, "detail", b.Message)
			} else {
				c.logger().Warn("controller: decommission waiting", "node", node, "reason", b.Code, "detail", b.Message)
			}
		}
	}
}

func containsStr(ss []string, s string) bool {
	return slices.Contains(ss, s)
}

package controller

import (
	"context"
	"time"
)

// leadershipResyncInterval bounds how long a missed or coalesced
// LeaderCh edge can leave the leader loop out of sync with the actual
// Raft leadership state.
const leadershipResyncInterval = 5 * time.Second

// Run watches for Raft leadership transitions and drives controller logic.
// It blocks until ctx is cancelled.
//
// hashicorp/raft's LeaderCh is best-effort (a capacity-1 channel with
// non-blocking sends), so a rapid flap can coalesce or drop an edge. To
// avoid a node that is the Raft leader but silently never reconciles or
// reaps dead members, a low-frequency timer reconciles the leader loop
// against a fresh IsLeader() check, self-healing any missed edge.
func (c *Controller) Run(ctx context.Context) {
	leaderCh := c.store.LeaderCh()

	var cancel context.CancelFunc
	syncLeadership := func(isLeader bool) {
		if isLeader && cancel == nil {
			cancel = c.startLeaderLoop(ctx)
			return
		}
		if !isLeader && cancel != nil {
			cancel()
			cancel = nil
		}
	}

	syncLeadership(c.store.IsLeader())

	ticker := time.NewTicker(leadershipResyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if cancel != nil {
				cancel()
			}
			return
		case isLeader := <-leaderCh:
			// On an explicit transition, restart the loop so leader state
			// (and its context) is rebuilt cleanly.
			if cancel != nil {
				cancel()
				cancel = nil
			}
			syncLeadership(isLeader)
		case <-ticker.C:
			syncLeadership(c.store.IsLeader())
		}
	}
}

func (c *Controller) startLeaderLoop(ctx context.Context) context.CancelFunc {
	leaderCtx, cancel := context.WithCancel(ctx)
	go c.runAsLeader(leaderCtx)
	return cancel
}

// runAsLeader performs an immediate reconciliation then loops on a ticker
// until ctx is cancelled (i.e. leadership is lost or node is shutting down).
func (c *Controller) runAsLeader(ctx context.Context) {
	c.reconcileAssignments(ctx)
	c.checkHeartbeats(ctx)

	ticker := time.NewTicker(c.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.reconcileAssignments(ctx)
			c.checkHeartbeats(ctx)
		}
	}
}

package controller

import (
	"context"
	"time"
)

// Run watches for Raft leadership transitions and drives controller logic.
// It blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	leaderCh := c.store.LeaderCh()

	var cancel context.CancelFunc
	if c.store.IsLeader() {
		cancel = c.startLeaderLoop(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			if cancel != nil {
				cancel()
			}
			return
		case isLeader := <-leaderCh:
			if cancel != nil {
				cancel()
				cancel = nil
			}
			if isLeader {
				cancel = c.startLeaderLoop(ctx)
			}
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

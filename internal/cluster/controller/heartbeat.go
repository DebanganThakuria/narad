package controller

import (
	"context"
	"errors"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// checkHeartbeats marks any alive member whose heartbeat has expired as
// dead. LastHeartbeat is stamped by the leader when it applies a
// member registration (cluster.RPCServer.handleRegisterMember, or the
// leader's own local registration). The member heartbeater itself lives
// in cmd/narad (runMemberHeartbeater), which forwards to the leader when
// this node is not it.
//
// "Expired" is judged on this leader's clock (see deadFor): the stamps
// it inherits say nothing about a leaderless stretch before its
// election, so it gives every member one full DeadTimeout from the
// moment its leader loop started. It reads the member table only after
// a Barrier has succeeded in this term, so a freshly elected leader
// never judges members from an FSM still applying their fresher
// heartbeats. The dead-marking breaker (applyBreaker) then refuses a
// verdict that would leave most voters dead.
func (c *Controller) checkHeartbeats(ctx context.Context) {
	if !c.store.IsLeader() {
		return
	}
	t := c.term(ctx)
	if err := c.termBarrier(t); err != nil {
		c.logger().Debug("controller: heartbeat pass skipped: barrier failed", "err", err)
		return
	}
	members, err := c.store.ListMembers()
	if err != nil {
		return
	}
	now := c.clock()
	c.noteStatusChanges(t, members, now)

	var verdict []metastore.Member
	for _, m := range members {
		if m.Status == metastore.MemberDead {
			continue
		}
		if c.deadFor(t, m.LastHeartbeat, c.cfg.DeadTimeout, now) {
			verdict = append(verdict, m)
		}
	}
	for _, m := range c.applyBreaker(t, members, verdict) {
		// The mark carries the heartbeat it was decided from: one that
		// committed after this read wins, and the member stays alive.
		err := c.store.MarkMemberDeadObserved(ctx, m.ID, m.LastHeartbeat)
		if errors.Is(err, metastore.ErrMemberHeartbeatNewer) {
			continue
		}
		if err != nil {
			c.logger().Warn("controller: mark member dead failed; retrying next pass", "member", m.ID, "err", err)
			continue
		}
		c.logger().Warn("controller: member marked dead",
			"member", m.ID, "silent_for", now.Sub(time.Unix(m.LastHeartbeat, 0)).Round(time.Second), "dead_timeout", c.cfg.DeadTimeout)
		t.mu.Lock()
		if t.statuses == nil {
			t.statuses = map[string]metastore.MemberStatus{}
		}
		t.statuses[m.ID] = metastore.MemberDead
		t.mu.Unlock()
	}
}

// noteStatusChanges logs, once each, a member this term finds dead that
// it did not mark itself, and a member seen alive again after being dead.
func (c *Controller) noteStatusChanges(t *leaderTerm, members []metastore.Member, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.statuses == nil {
		t.statuses = make(map[string]metastore.MemberStatus, len(members))
	}
	for _, m := range members {
		prev, seen := t.statuses[m.ID]
		t.statuses[m.ID] = m.Status
		switch {
		case !seen && m.Status == metastore.MemberDead:
			c.logger().Info("controller: member is dead", "member", m.ID,
				"last_heartbeat_age", now.Sub(time.Unix(m.LastHeartbeat, 0)).Round(time.Second))
		case seen && prev == metastore.MemberDead && m.Status != metastore.MemberDead:
			c.logger().Info("controller: member alive again", "member", m.ID)
		}
	}
}

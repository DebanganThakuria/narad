package controller

import (
	"context"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// checkHeartbeats marks any alive member whose heartbeat has expired as
// dead. LastHeartbeat is stamped by the leader when it applies a
// member registration (cluster.RPCServer.handleRegisterMember, or the
// leader's own local registration), so this comparison is between two
// readings of the same clock. The member heartbeater itself lives in
// cmd/narad (runMemberHeartbeater), which forwards to the leader when
// this node is not it.
func (c *Controller) checkHeartbeats(ctx context.Context) {
	if !c.store.IsLeader() {
		return
	}
	members, err := c.store.ListMembers()
	if err != nil {
		return
	}
	threshold := time.Now().Unix() - int64(c.cfg.DeadTimeout.Seconds())
	for _, m := range members {
		if m.Status == metastore.MemberDead {
			continue
		}
		if m.LastHeartbeat < threshold {
			c.store.MarkMemberDead(ctx, m.ID) //nolint:errcheck
		}
	}
}

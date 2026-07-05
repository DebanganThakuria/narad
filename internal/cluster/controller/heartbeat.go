package controller

import (
	"context"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// Heartbeater runs a background loop that upserts this pod's Member record
// into the metastore on every tick. Using RegisterMember (not Heartbeat)
// means the first tick also handles initial registration, and a pod that
// was marked dead gets resurrected automatically when it comes back.
type Heartbeater struct {
	store    *metastore.Store
	member   metastore.Member
	interval time.Duration
}

// NewHeartbeater creates a Heartbeater. interval should be well below the
// controller's DeadTimeout (e.g. DeadTimeout/4).
func NewHeartbeater(store *metastore.Store, m metastore.Member, interval time.Duration) *Heartbeater {
	if interval == 0 {
		interval = 5 * time.Second
	}
	return &Heartbeater{store: store, member: m, interval: interval}
}

// Run upserts the member record immediately then ticks until ctx is cancelled.
// Errors from RegisterMember are silently retried on the next tick — the pod
// may not have joined the Raft cluster yet when Run is first called.
func (h *Heartbeater) Run(ctx context.Context) {
	send := func() {
		m := h.member
		m.LastHeartbeat = time.Now().Unix()
		h.store.RegisterMember(ctx, m) //nolint:errcheck
	}
	send()
	t := time.NewTicker(h.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			send()
		}
	}
}

// checkHeartbeats marks any alive member whose heartbeat has expired as dead.
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

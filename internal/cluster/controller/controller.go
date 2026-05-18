// Package controller runs partition assignment and member health monitoring
// on the Raft leader. Only the leader runs controller logic; any leadership
// change cancels the running loops and (on promotion) starts fresh ones.
package controller

import (
	"context"
	"sort"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// Config holds tunables for the controller. Zero values use safe defaults.
type Config struct {
	// ReconcileInterval controls how often the leader checks for unassigned
	// partitions and dead members. Default: 10s.
	ReconcileInterval time.Duration
	// DeadTimeout is how long a member can go without a heartbeat before the
	// controller marks it dead. Default: 30s.
	DeadTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = 10 * time.Second
	}
	if c.DeadTimeout == 0 {
		c.DeadTimeout = 30 * time.Second
	}
	return c
}

// Controller drives cluster-level decisions: partition assignment and
// member liveness. It must be started with Run and stopped via context.
type Controller struct {
	store *metastore.Store
	cfg   Config
}

// New creates a Controller. Call Run to start it.
func New(store *metastore.Store, cfg Config) *Controller {
	return &Controller{store: store, cfg: cfg.withDefaults()}
}

// Run watches for Raft leadership transitions and drives controller logic.
// It blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context) {
	leaderCh := c.store.LeaderCh()

	var cancel context.CancelFunc
	// Handle the case where we're already the leader when Run is called
	// (single-node bootstrap: the node becomes leader before Run starts).
	if c.store.IsLeader() {
		var lCtx context.Context
		lCtx, cancel = context.WithCancel(ctx)
		go c.runAsLeader(lCtx)
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
				var lCtx context.Context
				lCtx, cancel = context.WithCancel(ctx)
				go c.runAsLeader(lCtx)
			}
		}
	}
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

// reconcileAssignments assigns any partitions that have no owner. It never
// moves existing assignments — without replication, data lives only on the
// current owner's disk.
func (c *Controller) reconcileAssignments(ctx context.Context) {
	if !c.store.IsLeader() {
		return
	}

	members, err := c.store.ListMembers()
	if err != nil {
		return
	}
	active := aliveMembers(members)
	if len(active) == 0 {
		return
	}

	topics, _, err := c.store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		return
	}

	// Count partitions currently owned by each active member across all topics.
	counts := make(map[string]int, len(active))
	for _, m := range active {
		counts[m.ID] = 0
	}
	for _, t := range topics {
		existing, _ := c.store.ListAssignments(t.Name)
		for _, a := range existing {
			counts[a.OwnerID]++ // counts dead-pod assignments too, that's intentional
		}
	}

	for _, t := range topics {
		c.assignTopic(ctx, t.Name, t.Partitions, t.ReplicationFactor, active, counts)
	}
}

// assignTopic assigns partitions of topicName that are missing an alive owner.
func (c *Controller) assignTopic(ctx context.Context, topicName string, numPartitions int, replicationFactor int, active []metastore.Member, counts map[string]int) {
	if replicationFactor < 2 || len(active) < replicationFactor {
		return
	}
	existing, _ := c.store.ListAssignments(topicName)
	assigned := make(map[int]bool, len(existing))
	for _, a := range existing {
		owner, err := c.store.GetMember(a.OwnerID)
		if err == nil && owner.Status == metastore.MemberAlive {
			assigned[a.Partition] = true
			continue
		}
		if a.FollowerID == "" {
			continue
		}
		follower, err := c.store.GetMember(a.FollowerID)
		if err != nil || follower.Status != metastore.MemberAlive {
			continue
		}
		if err := c.store.AssignPartition(ctx, topicName, a.Partition, a.FollowerID, a.OwnerID); err != nil {
			continue
		}
		counts[a.FollowerID]++
		counts[a.OwnerID]++
		assigned[a.Partition] = true
	}

	for p := 0; p < numPartitions; p++ {
		if assigned[p] {
			continue
		}
		sort.Slice(active, func(i, j int) bool {
			return counts[active[i].ID] < counts[active[j].ID]
		})
		owner := active[0].ID
		follower := active[1].ID
		if err := c.store.AssignPartition(ctx, topicName, p, owner, follower); err != nil {
			continue
		}
		counts[owner]++
		counts[follower]++
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

// aliveMembers filters a member list to those with MemberAlive status.
func aliveMembers(members []metastore.Member) []metastore.Member {
	out := make([]metastore.Member, 0, len(members))
	for _, m := range members {
		if m.Status == metastore.MemberAlive {
			out = append(out, m)
		}
	}
	return out
}

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

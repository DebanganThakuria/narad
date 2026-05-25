// Package controller runs partition assignment and member health monitoring
// on the Raft leader. Only the leader runs controller logic; any leadership
// change cancels the running loops and (on promotion) starts fresh ones.
package controller

import (
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

// Heartbeater runs a background loop that upserts this pod's Member record
// into the metastore on every tick. Using RegisterMember (not Heartbeat)
// means the first tick also handles initial registration, and a pod that
// was marked dead gets resurrected automatically when it comes back.
type Heartbeater struct {
	store    *metastore.Store
	member   metastore.Member
	interval time.Duration
}

// New creates a Controller. Call Run to start it.
func New(store *metastore.Store, cfg Config) *Controller {
	return &Controller{store: store, cfg: cfg.withDefaults()}
}

// NewHeartbeater creates a Heartbeater. interval should be well below the
// controller's DeadTimeout (e.g. DeadTimeout/4).
func NewHeartbeater(store *metastore.Store, m metastore.Member, interval time.Duration) *Heartbeater {
	if interval == 0 {
		interval = 5 * time.Second
	}
	return &Heartbeater{store: store, member: m, interval: interval}
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

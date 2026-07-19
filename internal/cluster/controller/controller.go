// Package controller runs partition assignment and member health monitoring
// on the Raft leader. Only the leader runs controller logic; any leadership
// change cancels the running loops and (on promotion) starts fresh ones.
package controller

import (
	"context"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// controllerStore is the slice of the metastore the controller uses.
// *metastore.Store implements it; tests substitute fakes.
type controllerStore interface {
	IsLeader() bool
	LeaderCh() <-chan bool
	Barrier() error
	ListMembers() ([]metastore.Member, error)
	ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error)
	ListAssignments(topicName string) ([]metastore.Assignment, error)
	AssignPartition(ctx context.Context, topicName string, partition int, ownerID string) error
	SetAssignmentTarget(ctx context.Context, topicName string, partition int, targetID string) error
	MarkMemberDead(ctx context.Context, podID string) error
}

// Config holds tunables for the controller. Zero values use safe defaults.
type Config struct {
	// ReconcileInterval controls how often the leader checks for unassigned
	// partitions and dead members. Default: 10s.
	ReconcileInterval time.Duration
	// DeadTimeout is how long a member can go without a heartbeat before the
	// controller marks it dead. Default: 30s.
	DeadTimeout time.Duration
	// MaxInFlightMoves caps how many partition moves may be running at once
	// across the cluster. The rebalance planner tops up to this bound each
	// tick (level-triggered), so a large rebalance drains gradually rather
	// than copying every partition at once. Default: 8. Zero disables
	// auto-rebalance entirely.
	MaxInFlightMoves int
}

func (c Config) withDefaults() Config {
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = 10 * time.Second
	}
	if c.DeadTimeout == 0 {
		c.DeadTimeout = 30 * time.Second
	}
	if c.MaxInFlightMoves == 0 {
		c.MaxInFlightMoves = 8
	}
	return c
}

// Controller drives cluster-level decisions: partition assignment and
// member liveness. It must be started with Run and stopped via context.
type Controller struct {
	store controllerStore
	cfg   Config

	// planMu serializes rebalance planning so a membership change that lands
	// mid-computation cannot race a plan. A plan that used the pre-change
	// snapshot is still safe — the level-triggered planner re-converges on
	// the next tick — but the mutex keeps any two planning passes from
	// interleaving their reads and target writes.
	planMu sync.Mutex
}

// New creates a Controller. Call Run to start it.
func New(store *metastore.Store, cfg Config) *Controller {
	return &Controller{store: store, cfg: cfg.withDefaults()}
}

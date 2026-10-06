// Package controller runs partition assignment and member health monitoring
// on the Raft leader. Only the leader runs controller logic; any leadership
// change cancels the running loops and (on promotion) starts fresh ones.
package controller

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// controllerStore is the slice of the metastore the controller uses.
// *metastore.Store implements it; tests substitute fakes.
type controllerStore interface {
	IsLeader() bool
	LeaderCh() <-chan bool
	Barrier() error
	// LeaderBarrier barriers once per leadership term (see
	// metastore.Store.LeaderBarrier); nil on a follower.
	LeaderBarrier(ctx context.Context) error
	ListMembers() ([]metastore.Member, error)
	RoutingMembersVersion() uint64
	ListTopics(ctx context.Context, opts metastore.ListOptions) ([]topic.Topic, string, error)
	GetTopic(ctx context.Context, name string) (topic.Topic, error)
	ListAssignments(topicName string) ([]metastore.Assignment, error)
	LockAssignments() (unlock func())
	AssignPartition(ctx context.Context, topicName string, partition int, ownerID string) error
	// AssignPartitionIfAbsent is the insert-only placement; it returns
	// metastore.ErrEntryTypeNotYetUsable while some member does not
	// apply it, and the controller uses AssignPartition then.
	AssignPartitionIfAbsent(ctx context.Context, topicName string, partition int, ownerID, expectID string) error
	OrphanAssignments() ([]metastore.Assignment, error)
	PruneAssignment(ctx context.Context, topicName string, partition int) error
	SetAssignmentTarget(ctx context.Context, topicName string, partition int, targetID string) error
	AbortMove(ctx context.Context, topicName string, partition int, expectedTarget string) error
	MarkMemberDeadObserved(ctx context.Context, podID string, observed int64) error
	Voters() ([]string, error)
	Nonvoters() ([]string, error)
	RemoveServer(id string) error
	RemoveMember(ctx context.Context, podID string, at int64) error
	TransferLeadership() error
	LeaderID() string
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
	// MinVoters is the smallest Raft voter count decommission will leave
	// behind: a drained node is removed only if doing so keeps at least this
	// many voters, so a decommission can never drop the cluster below a
	// quorum-safe size. Default: 3.
	MinVoters int
	// MemberSettleDelay is how long the leader waits, after a member turns
	// alive, for more members to arrive before it runs an out-of-cycle
	// assignment and rebalance pass. It does not wait once every Raft
	// voter is alive. Without the pass, partitions of a topic created
	// before any member registered stayed unowned until the next
	// ReconcileInterval tick. Default: 1s.
	MemberSettleDelay time.Duration
	// DeadTargetAbortAfter is how long a move's TARGET node may stay dead
	// before the controller clears the target. Only the destination itself
	// aborts a move, and a dead destination cannot, so without this bound
	// moves aimed at a node that died pin the in-flight budget forever and
	// stop every later rebalance and decommission. Generous, so a pod that
	// restarts finishes its copy instead of being re-planned. Measured on
	// the leader's own clock like DeadTimeout. Default: 2m.
	DeadTargetAbortAfter time.Duration
	// Logger receives the controller's transitions: leadership, members
	// marked dead or seen alive again, move targets set and cleared,
	// voters and members removed, and why a decommission is blocked.
	// Nil logs nothing.
	Logger *slog.Logger
	// Registerer receives the controller's leader-only metrics
	// (narad_decommission_blocked, narad_dead_marking_refused,
	// narad_colocated_child_partitions). Nil registers none.
	Registerer prometheus.Registerer
	// NodeStatus asks the member at addr (Member.Addr) for its status
	// over node RPC. Decommission reads a draining node's dispatch
	// backlog from it before taking the node out of Raft. Nil skips that
	// check, as before node status existed.
	NodeStatus func(ctx context.Context, addr string) (NodeStatus, error)
}

// DefaultMinVoters is the MinVoters a zero Config uses. The decommission
// preflight on every node applies the same floor.
const DefaultMinVoters = 3

// DefaultMaxInFlightMoves is the MaxInFlightMoves a zero Config uses.
const DefaultMaxInFlightMoves = 8

func (c Config) withDefaults() Config {
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = 10 * time.Second
	}
	if c.DeadTimeout == 0 {
		c.DeadTimeout = 30 * time.Second
	}
	if c.MaxInFlightMoves == 0 {
		c.MaxInFlightMoves = DefaultMaxInFlightMoves
	}
	if c.MinVoters == 0 {
		c.MinVoters = DefaultMinVoters
	}
	if c.MemberSettleDelay == 0 {
		c.MemberSettleDelay = time.Second
	}
	if c.DeadTargetAbortAfter == 0 {
		c.DeadTargetAbortAfter = 2 * time.Minute
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
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

	// now is the controller's clock; nil reads time.Now. Tests set it.
	now func() time.Time
	// m is the controller's metrics; nil (no Registerer) records nothing.
	m *metrics

	// termMu guards defaultTerm and activeTerm, and orders the writes
	// of leader-only metrics against a term change.
	termMu sync.Mutex
	// defaultTerm is the leader state of passes run outside a leader
	// loop (tests call the passes directly).
	defaultTerm *leaderTerm
	// activeTerm is the running leader loop's term; nil when this node
	// does not lead.
	activeTerm *leaderTerm

	// blockedMoves is the leader's count of in-flight moves blocked on a
	// dead or departed node, by reason (BlockedMoves); nil when none or
	// when this node does not lead.
	blockedMoves atomic.Pointer[map[string]int]

	// orphansLogged holds the orphan assignment rows already logged as
	// waiting for the prune entry type (orphans.go), so each is logged
	// once.
	orphanMu      sync.Mutex
	orphansLogged map[string]bool
}

// New creates a Controller. Call Run to start it.
func New(store *metastore.Store, cfg Config) *Controller {
	cfg = cfg.withDefaults()
	return &Controller{store: store, cfg: cfg, m: newMetrics(cfg.Registerer)}
}

// clock reads the controller's clock.
func (c *Controller) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// logger returns the configured logger, or one that discards.
func (c *Controller) logger() *slog.Logger {
	if c.cfg.Logger != nil {
		return c.cfg.Logger
	}
	return discardLogger
}

var discardLogger = slog.New(slog.DiscardHandler)

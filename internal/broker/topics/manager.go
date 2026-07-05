// Package topics owns the topic-CRUD surface of the broker. The
// Manager type is embedded by the broker facade so its methods are
// promoted onto the Broker interface — callers (the HTTP handlers,
// CLI, tests) use the topic-management API through the broker
// without knowing the package split exists.
//
// Files:
//
//   - manager.go: Manager struct, Config, CreateOpts, error sentinels, constructor.
//   - create.go:  CreateTopic and the startup create gate (ArmCreateGate/ReleaseCreateGate).
//   - update.go:  IncreaseTopicPartitions, UpdateTopicRetention, UpdateTopicCaps, UpdateTopicSchema.
//   - delete.go:  DeleteTopic.
//   - query.go:   GetTopic, GetTopicDetails, ListTopics.
//
// Cross-package state: Manager holds a *runtime.Logs for closing
// cached partition logs on retention update and topic delete, and a
// *consumer.InFlight for dropping in-flight reservations on delete.
package topics

import (
	"context"
	"log/slog"
	"sync"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// Aliases of the shared broker error sentinels for ergonomic local
// use; the broker package re-exports the underlying errs.* values
// publicly.
var (
	ErrNotFound      = errs.ErrTopicNotFound
	ErrAlreadyExists = errs.ErrTopicAlreadyExists
	ErrInvalid       = errs.ErrInvalidArgument
)

// Config supplies create-time defaults and bounds. Same fields as the
// broker-level TopicConfig; the broker constructs this struct when
// wiring the Manager so topics doesn't import broker.
type Config struct {
	DefaultPartitions                int
	MaxPartitions                    int
	DefaultRetentionMs               int64
	DefaultVisibilityTimeoutMs       int64
	DefaultMaxInFlightPerPartition   int64
	DefaultMaxAckedAheadPerPartition int64
}

// CreateOpts is the input for CreateTopic. Zero values for any
// policy field inherit the matching default from Config.
type CreateOpts struct {
	Name                      string
	Partitions                int
	RetentionMs               int64
	VisibilityTimeoutMs       int64
	MaxInFlightPerPartition   int64
	MaxAckedAheadPerPartition int64
	Schema                    []byte
}

// PartitionAssigner assigns a topic partition range to cluster members.
// It is injected so topic CRUD can use assignment capability without
// depending directly on the concrete metastore store type.
type PartitionAssigner interface {
	AssignNewPartitions(ctx context.Context, topicName string, fromPartition, toPartition int) error
}

// Manager handles every topic-CRUD operation. Constructed once at
// broker startup; safe for concurrent use.
type Manager struct {
	dataDir   string
	metastore metastore.Metastore
	assigner  PartitionAssigner
	schemas   schema.Registry
	offsets   *consumer.InFlight
	logs      *runtime.Logs
	cfg       Config
	logger    *slog.Logger
	// selfID is this node's cluster ID, used to resolve partition
	// ownership for stat queries. Empty means "no cluster identity"
	// (tests / embedded use): the manager then treats every partition
	// as locally owned.
	selfID string

	topicLocksMu sync.Mutex
	topicLocks   map[string]*topicLock

	// createGate, when non-nil, blocks CreateTopic until the channel is
	// closed. It is nil (open) by default so tests and embedded users
	// are unaffected; serve.go arms it via ArmCreateGate before the
	// cluster RPC listener starts and releases it once the startup
	// orphan sweep has finished. See create.go.
	createGateMu sync.Mutex
	createGate   chan struct{}
}

// topicLock is a refcounted per-topic-name mutex. Refcounting lets
// lockTopicName delete idle entries so topic churn doesn't grow the
// map forever.
type topicLock struct {
	mu   sync.Mutex
	refs int
}

// lockTopicName serializes topic mutations (create, update, delete,
// purge) for a single name. Every mutation is a read-modify-write of
// the full Topic record over a blind-overwrite metastore UpdateTopic,
// so unserialized concurrent updates would lose writes (e.g. a
// retention update racing a partition increase could silently shrink
// the partition count back). It also keeps delete→recreate ordered:
// the purge finishes before a recreate of the same name can start.
func (m *Manager) lockTopicName(name string) (unlock func()) {
	m.topicLocksMu.Lock()
	l := m.topicLocks[name]
	if l == nil {
		l = &topicLock{}
		m.topicLocks[name] = l
	}
	l.refs++
	m.topicLocksMu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		m.topicLocksMu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(m.topicLocks, name)
		}
		m.topicLocksMu.Unlock()
	}
}

// NewManager wires a Manager. dataDir is the topic directory root
// (used by CreateTopic/DeleteTopic to mkdir/rmdir on disk). selfID is
// this node's cluster ID (may be empty when there is no cluster
// identity, in which case every partition is treated as locally owned).
func NewManager(
	dataDir string,
	ms metastore.Metastore,
	assigner PartitionAssigner,
	schemas schema.Registry,
	offsets *consumer.InFlight,
	logs *runtime.Logs,
	cfg Config,
	logger *slog.Logger,
	selfID string,
) *Manager {
	return &Manager{
		dataDir:    dataDir,
		metastore:  ms,
		assigner:   assigner,
		schemas:    schemas,
		offsets:    offsets,
		logs:       logs,
		cfg:        cfg,
		logger:     logger,
		selfID:     selfID,
		topicLocks: map[string]*topicLock{},
	}
}

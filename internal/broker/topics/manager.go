// Package topics owns the topic-CRUD surface of the broker. The
// Manager type is embedded by the broker facade so its methods are
// promoted onto the Broker interface — callers (the HTTP handlers,
// CLI, tests) use the topic-management API through the broker
// without knowing the package split exists.
//
// Files:
//
//   - manager.go: Manager struct, Config, CreateOpts, error sentinels, constructor.
//   - create.go:  CreateTopic.
//   - update.go:  IncreaseTopicPartitions, UpdateTopicRetention, UpdateTopicCaps, UpdateTopicSchema.
//   - delete.go:  DeleteTopic.
//   - query.go:   GetTopic, GetTopicDetails, ListTopics.
//
// Cross-package state: Manager holds a *runtime.Logs for closing
// cached partition logs on retention update and topic delete, and a
// *consumer.InFlight for dropping in-flight reservations on delete.
package topics

import (
	"log/slog"

	"github.com/debanganthakuria/narad/internal/broker/errs"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/schema"
)

// Aliases of the shared broker error sentinels for ergonomic local
// use; the broker package re-exports the underlying errs.* values
// publicly.
var (
	ErrNotFound      = errs.TopicNotFound
	ErrAlreadyExists = errs.TopicAlreadyExists
	ErrInvalid       = errs.InvalidArgument
)

// Config supplies create-time defaults and bounds. Same fields as the
// broker-level TopicConfig; the broker constructs this struct when
// wiring the Manager so topics doesn't import broker.
type Config struct {
	DefaultPartitions                int
	MaxPartitions                    int
	DefaultReplicationFactor         int
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
	ReplicationFactor         int
	RetentionMs               int64
	VisibilityTimeoutMs       int64
	MaxInFlightPerPartition   int64
	MaxAckedAheadPerPartition int64
}

// Manager handles every topic-CRUD operation. Constructed once at
// broker startup; safe for concurrent use.
type Manager struct {
	dataDir   string
	metastore metastore.Metastore
	schemas   schema.Registry
	offsets   *consumer.InFlight
	logs      *runtime.Logs
	cfg       Config
	logger    *slog.Logger
}

// NewManager wires a Manager. dataDir is the topic directory root
// (used by CreateTopic/DeleteTopic to mkdir/rmdir on disk).
func NewManager(
	dataDir string,
	ms metastore.Metastore,
	schemas schema.Registry,
	offsets *consumer.InFlight,
	logs *runtime.Logs,
	cfg Config,
	logger *slog.Logger,
) *Manager {
	return &Manager{
		dataDir:   dataDir,
		metastore: ms,
		schemas:   schemas,
		offsets:   offsets,
		logs:      logs,
		cfg:       cfg,
		logger:    logger,
	}
}

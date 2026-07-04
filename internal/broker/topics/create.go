package topics

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// topicNamePattern is the set of allowed topic names. Restricting to a
// single path-safe segment is load-bearing: the name becomes a directory
// under dataDir/topics, so "/" would nest (and the startup orphan sweep
// would delete the nested dirs), ".." would resolve the topic dir to the
// data dir itself, and "." to the topics root — either of which would
// make DeleteTopic/PurgeTopic os.RemoveAll far more than one topic.
var topicNamePattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)

// validateTopicName rejects names that are unsafe as on-disk directory
// names. "." and ".." match the allowed character set but are path
// traversals, so they are rejected explicitly.
func validateTopicName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: name required", ErrInvalid)
	}
	if name == "." || name == ".." {
		return fmt.Errorf("%w: topic name must not be %q", ErrInvalid, name)
	}
	if !topicNamePattern.MatchString(name) {
		return fmt.Errorf("%w: topic name must match %s", ErrInvalid, topicNamePattern)
	}
	return nil
}

// ArmCreateGate blocks subsequent CreateTopic calls until
// ReleaseCreateGate is called. The gate exists to close a startup race:
// the startup orphan sweep (runtime.SweepOrphanTopicDirs) removes topic
// directories whose topic is absent from the metastore, so a create that
// lands its directory while the sweep is still walking — e.g. a
// peer-forwarded create arriving over the cluster RPC listener before
// startup reconciliation finishes — could have that live directory
// deleted as an orphan. The gate is open by default (constructors don't
// arm it), so only processes that run the sweep need to participate.
// Arming an already-armed gate is a no-op; re-arming after release
// installs a fresh gate.
func (m *Manager) ArmCreateGate() {
	m.createGateMu.Lock()
	defer m.createGateMu.Unlock()
	if m.createGate == nil {
		m.createGate = make(chan struct{})
	}
}

// ReleaseCreateGate opens the create gate armed by ArmCreateGate,
// unblocking any waiting CreateTopic calls. It is idempotent and safe to
// call when the gate was never armed, so callers can release
// unconditionally on every path (success, failure, shutdown).
func (m *Manager) ReleaseCreateGate() {
	m.createGateMu.Lock()
	defer m.createGateMu.Unlock()
	if m.createGate != nil {
		close(m.createGate)
		m.createGate = nil
	}
}

// waitCreateGate blocks until the create gate is open or ctx is done.
// The wait is bounded in practice: startup reconciliation caps its
// metastore catch-up wait (~60s) and the gate is released as soon as it
// returns, on every path.
func (m *Manager) waitCreateGate(ctx context.Context) error {
	m.createGateMu.Lock()
	gate := m.createGate
	m.createGateMu.Unlock()
	if gate == nil {
		return nil
	}
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("topics: create topic aborted while waiting for startup reconciliation: %w", ctx.Err())
	}
}

// CreateTopic registers a new topic and prepares its on-disk
// directory. Partition log files are opened lazily on first use.
//
// Zero values for any policy field inherit the matching default from
// Config. Negative values and partitions exceeding Config.MaxPartitions
// are rejected.
//
// If the startup create gate is armed (see ArmCreateGate), CreateTopic
// waits for it to open before taking the per-name lock or touching disk.
func (m *Manager) CreateTopic(ctx context.Context, opts CreateOpts) (topic.Topic, error) {
	if err := validateTopicName(opts.Name); err != nil {
		return topic.Topic{}, err
	}
	// Wait before lockTopicName so a gated create doesn't stall
	// deletes/updates of the same name behind the gate.
	if err := m.waitCreateGate(ctx); err != nil {
		return topic.Topic{}, err
	}
	unlock := m.lockTopicName(opts.Name)
	defer unlock()

	partitions := opts.Partitions
	if partitions == 0 {
		partitions = m.cfg.DefaultPartitions
	}
	if partitions < 3 {
		return topic.Topic{}, fmt.Errorf("%w: partitions must be >= 3 (0 = use default)", ErrInvalid)
	}
	if maximum := m.cfg.MaxPartitions; maximum > 0 && partitions > maximum {
		return topic.Topic{}, fmt.Errorf("%w: partitions (%d) exceeds topic.max_partitions (%d)",
			ErrInvalid, partitions, maximum)
	}

	retentionMs := opts.RetentionMs
	if retentionMs == 0 {
		retentionMs = m.cfg.DefaultRetentionMs
	}
	if retentionMs < 0 {
		return topic.Topic{}, fmt.Errorf("%w: retention_ms must be >= 0 (0 = use default)", ErrInvalid)
	}

	visibilityMs := opts.VisibilityTimeoutMs
	if visibilityMs == 0 {
		visibilityMs = m.cfg.DefaultVisibilityTimeoutMs
	}
	if visibilityMs < 0 {
		return topic.Topic{}, fmt.Errorf("%w: visibility_timeout_ms must be >= 0 (0 = use default)", ErrInvalid)
	}

	maxIF := opts.MaxInFlightPerPartition
	if maxIF == 0 {
		maxIF = m.cfg.DefaultMaxInFlightPerPartition
	}
	if maxIF < 0 {
		return topic.Topic{}, fmt.Errorf("%w: max_in_flight_per_partition must be >= 0 (0 = use default)", ErrInvalid)
	}

	maxAA := opts.MaxAckedAheadPerPartition
	if maxAA == 0 {
		maxAA = m.cfg.DefaultMaxAckedAheadPerPartition
	}
	if maxAA < 0 {
		return topic.Topic{}, fmt.Errorf("%w: max_acked_ahead_per_partition must be >= 0 (0 = use default)", ErrInvalid)
	}
	if len(opts.Schema) > 0 {
		if err := m.schemas.ValidateDefinition(ctx, opts.Name, opts.Schema); err != nil {
			return topic.Topic{}, fmt.Errorf("%w: %w", ErrInvalid, err)
		}
	}

	t := topic.Topic{
		Name:                      opts.Name,
		Partitions:                partitions,
		RetentionMs:               retentionMs,
		VisibilityTimeoutMs:       visibilityMs,
		MaxInFlightPerPartition:   maxIF,
		MaxAckedAheadPerPartition: maxAA,
		CreatedAt:                 time.Now().Unix(),
	}

	dir, err := m.topicDir(opts.Name)
	if err != nil {
		return topic.Topic{}, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return topic.Topic{}, fmt.Errorf("topics: create topic dir: %w", err)
	}

	if err := m.metastore.CreateTopic(ctx, t); err != nil {
		if errors.Is(err, errs.ErrAlreadyExists) {
			return topic.Topic{}, ErrAlreadyExists
		}
		return topic.Topic{}, err
	}
	if len(opts.Schema) > 0 {
		if err := m.createInitialSchema(ctx, opts.Name, opts.Schema); err != nil {
			return topic.Topic{}, m.rollbackCreatedTopic(ctx, opts.Name, err)
		}
	}
	if m.assigner != nil {
		if err := m.assigner.AssignNewPartitions(ctx, opts.Name, 0, partitions); err != nil {
			m.logger.Warn("topic created without immediate partition assignment", "topic", opts.Name, "err", err)
		}
	}

	m.logger.Info("topic created",
		"topic", opts.Name,
		"partitions", partitions,
		"retention_ms", retentionMs,
		"visibility_timeout_ms", visibilityMs,
		"max_in_flight_per_partition", maxIF,
		"max_acked_ahead_per_partition", maxAA)

	return t, nil
}

func (m *Manager) rollbackCreatedTopic(ctx context.Context, topicName string, cause error) error {
	var rollbackErrs []error
	if err := m.metastore.DeleteTopic(ctx, topicName); err != nil && !errors.Is(err, errs.ErrNotFound) {
		rollbackErrs = append(rollbackErrs, fmt.Errorf("delete topic metadata: %w", err))
	}
	if err := m.purgeTopicLocked(ctx, topicName); err != nil {
		rollbackErrs = append(rollbackErrs, fmt.Errorf("purge topic data: %w", err))
	}
	if len(rollbackErrs) > 0 {
		return fmt.Errorf("%w; rollback failed: %v", cause, errors.Join(rollbackErrs...))
	}
	return cause
}

func (m *Manager) createInitialSchema(ctx context.Context, topicName string, rawSchema []byte) error {
	const version = 1
	if err := m.metastore.PutSchema(ctx, topicName, version, rawSchema); err != nil {
		return fmt.Errorf("topics: persist schema: %w", err)
	}
	if err := m.schemas.Load(ctx, topicName, version, rawSchema); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return nil
}

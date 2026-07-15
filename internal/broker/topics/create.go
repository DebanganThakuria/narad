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
	if err := m.resolveCreateAsChild(ctx, &opts); err != nil {
		return topic.Topic{}, err
	}
	// Wait before lockTopicName so a gated create doesn't stall
	// deletes/updates of the same name behind the gate.
	if err := m.waitCreateGate(ctx); err != nil {
		return topic.Topic{}, err
	}
	unlock := m.lockTopicName(opts.Name)
	defer unlock()

	t, err := m.topicFromOpts(opts)
	if err != nil {
		return topic.Topic{}, err
	}
	if len(opts.Schema) > 0 {
		if err := m.schemas.ValidateDefinition(ctx, opts.Name, opts.Schema); err != nil {
			return topic.Topic{}, fmt.Errorf("%w: %w", ErrInvalid, err)
		}
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
	// Attach BEFORE partition assignment: placement reads the parent
	// link to keep the child's partitions off the parent's nodes. A
	// failed attach rolls the create back so no half-linked topic
	// survives.
	if opts.Parent != "" {
		if err := m.metastore.AttachChild(ctx, opts.Parent, opts.Name, opts.FanoutDelayMs); err != nil {
			if errors.Is(err, errs.ErrNotFound) {
				err = fmt.Errorf("%w: %v", ErrNotFound, err)
			}
			return topic.Topic{}, m.rollbackCreatedTopic(ctx, opts.Name, err)
		}
	}
	if m.assigner != nil {
		if err := m.assigner.AssignNewPartitions(ctx, opts.Name, 0, t.Partitions); err != nil {
			m.logger.Warn("topic created without immediate partition assignment", "topic", opts.Name, "err", err)
		}
	}
	if opts.Parent != "" {
		// Return the attached view (role, parent, attach epoch, delay),
		// not the pre-attach record.
		if attached, err := m.GetTopic(ctx, opts.Name); err == nil {
			t = attached
		}
	}

	m.logger.Info("topic created",
		"topic", opts.Name,
		"partitions", t.Partitions,
		"retention_ms", t.RetentionMs,
		"visibility_timeout_ms", t.VisibilityTimeoutMs,
		"max_in_flight_per_partition", t.MaxInFlightPerPartition,
		"max_acked_ahead_per_partition", t.MaxAckedAheadPerPartition)

	return t, nil
}

// resolveCreateAsChild validates the Parent/FanoutDelayMs pair and,
// for a create-as-child, defaults Partitions to the parent's count —
// matching counts make the anti-affine per-key guarantee exact. It
// runs before anything is written, so every failure is a clean 4xx.
func (m *Manager) resolveCreateAsChild(ctx context.Context, opts *CreateOpts) error {
	if opts.Parent == "" {
		if opts.FanoutDelayMs != 0 {
			return fmt.Errorf("%w: fanout_delay_ms requires parent", ErrInvalid)
		}
		return nil
	}
	if err := validateTopicName(opts.Parent); err != nil {
		return err
	}
	if opts.Parent == opts.Name {
		return fmt.Errorf("%w: a topic cannot be its own parent", ErrInvalid)
	}
	if opts.FanoutDelayMs < 0 {
		return fmt.Errorf("%w: fanout_delay_ms must be >= 0", ErrInvalid)
	}
	if opts.FanoutDelayMs > topic.MaxFanoutDelayMs {
		return fmt.Errorf("%w: fanout_delay_ms (%d) exceeds the maximum of %d (1 year)",
			ErrInvalid, opts.FanoutDelayMs, topic.MaxFanoutDelayMs)
	}
	parent, err := m.GetTopic(ctx, opts.Parent)
	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, errs.ErrNotFound) {
			return fmt.Errorf("%w: parent topic %q", ErrNotFound, opts.Parent)
		}
		return err
	}
	if opts.Partitions == 0 {
		opts.Partitions = parent.Partitions
	}
	return nil
}

// topicFromOpts resolves CreateOpts against the configured defaults
// and bounds, returning the fully populated record to persist. Zero
// policy fields inherit their Config default; negative values are
// rejected.
func (m *Manager) topicFromOpts(opts CreateOpts) (topic.Topic, error) {
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

	retentionMs, err := defaultedNonNegative(opts.RetentionMs, m.cfg.DefaultRetentionMs, "retention_ms")
	if err != nil {
		return topic.Topic{}, err
	}
	if err := checkRetentionFloor(retentionMs); err != nil {
		return topic.Topic{}, err
	}
	visibilityMs, err := defaultedNonNegative(opts.VisibilityTimeoutMs, m.cfg.DefaultVisibilityTimeoutMs, "visibility_timeout_ms")
	if err != nil {
		return topic.Topic{}, err
	}
	maxInFlight, err := defaultedNonNegative(opts.MaxInFlightPerPartition, m.cfg.DefaultMaxInFlightPerPartition, "max_in_flight_per_partition")
	if err != nil {
		return topic.Topic{}, err
	}
	maxAckedAhead, err := defaultedNonNegative(opts.MaxAckedAheadPerPartition, m.cfg.DefaultMaxAckedAheadPerPartition, "max_acked_ahead_per_partition")
	if err != nil {
		return topic.Topic{}, err
	}

	return topic.Topic{
		Name:                      opts.Name,
		Partitions:                partitions,
		RetentionMs:               retentionMs,
		VisibilityTimeoutMs:       visibilityMs,
		MaxInFlightPerPartition:   maxInFlight,
		MaxAckedAheadPerPartition: maxAckedAhead,
		CreatedAt:                 time.Now().Unix(),
		Owner:                     opts.Owner,
	}, nil
}

// defaultedNonNegative substitutes def when v is zero and rejects
// negative values; field names the offender in the error message.
func defaultedNonNegative(v, def int64, field string) (int64, error) {
	if v == 0 {
		v = def
	}
	if v < 0 {
		return 0, fmt.Errorf("%w: %s must be >= 0 (0 = use default)", ErrInvalid, field)
	}
	return v, nil
}

// checkRetentionFloor rejects a resolved retention below the uniform
// one-hour minimum. The retained log is the fan-out buffer for lagging
// children, so the floor guarantees at least an hour of child outage
// tolerance before drop-behind can lose messages. Zero (keep forever)
// passes: it can only arrive here via a keep-forever configured
// default, which is above any floor.
func checkRetentionFloor(retentionMs int64) error {
	if retentionMs != 0 && retentionMs < topic.MinRetentionMs {
		return fmt.Errorf("%w: retention_ms (%d) is below the minimum of %d (1 hour)",
			ErrInvalid, retentionMs, topic.MinRetentionMs)
	}
	return nil
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

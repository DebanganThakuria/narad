package cluster

// FanoutRunner drives parent→child topic fan-out (Model C: materialize
// the parent, then tail its committed log). One cursor exists per
// (child, parentPartition); it runs on the node that OWNS the parent
// partition, so the slab read is always local and the cursor's
// persisted offset lives in the parent partition's directory, in the
// same durability domain as the log it tails. Child writes are
// re-keyed per record with the child's partitioner and committed in
// per-partition batches through the same local/remote commit paths the
// produce dispatcher uses.
//
// The runner polls the local metastore replica and diffs the desired
// cursor set against the running one: cursors spawn on attach (and on
// parent partition growth) and stop on detach, topic delete, or
// ownership change. A cursor key includes the link's attach epoch, so
// a detach followed by a re-attach always starts a fresh cursor at the
// parent's tail — never resuming (and replaying) the dead one.

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
)

const (
	defaultFanoutMaxBatchRecords   = 4096
	defaultFanoutMaxBatchBytes     = 4 << 20 // 4 MiB
	defaultFanoutLinger            = 25 * time.Millisecond
	defaultFanoutReconcileInterval = time.Second
	defaultFanoutLongPollWait      = time.Second
	defaultFanoutRetryBackoff      = time.Second

	// defaultFanoutDueWakeCap bounds how long a delay cursor sleeps
	// waiting for its head record to become due, so lag gauges and the
	// cursor's view of metadata stay reasonably fresh even under very
	// long delays. Context cancellation (detach/shutdown) wakes the
	// sleep immediately regardless.
	defaultFanoutDueWakeCap = 30 * time.Second

	// fanoutCursorFileSweepEvery is how many reconcile passes elapse
	// between sweeps for orphaned cursor files (links that dissolved
	// while this node was down). The sweep walks partition directories,
	// so it runs at a fraction of the reconcile cadence; live-link
	// bookkeeping doesn't depend on it.
	fanoutCursorFileSweepEvery = 30
)

// FanoutConfig holds the cursor engine tunables. Zero values use the
// defaults above. Batch size trades latency for throughput: bigger
// batches mean fewer fsyncs on the child but records wait until the
// batch fills or lingers.
type FanoutConfig struct {
	MaxBatchRecords   int
	MaxBatchBytes     int64
	Linger            time.Duration
	ReconcileInterval time.Duration
}

func (c FanoutConfig) withDefaults() FanoutConfig {
	if c.MaxBatchRecords <= 0 {
		c.MaxBatchRecords = defaultFanoutMaxBatchRecords
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = defaultFanoutMaxBatchBytes
	}
	if c.Linger < 0 {
		c.Linger = 0
	} else if c.Linger == 0 {
		c.Linger = defaultFanoutLinger
	}
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = defaultFanoutReconcileInterval
	}
	return c
}

// fanoutBroker is the slice of the local broker the runner needs: the
// local slab read from a parent partition and the local child commit.
type fanoutBroker interface {
	ReadFanoutSlab(ctx context.Context, topicName string, partitionIdx int, opts topic.FanoutReadOpts) (topic.FanoutSlab, error)
	CommitAcceptedProduceBatch(ctx context.Context, records []ingress.ProduceRecord) ([]int64, error)
}

// fanoutCursorKey identifies one running cursor. The epoch scopes the
// cursor to one attachment of the child.
type fanoutCursorKey struct {
	parent    string
	partition int
	child     string
	epoch     string
	// delayMs is the child's fan-out delay. Immutable per epoch (a
	// re-attach changes both), so carrying it here means the cursor
	// never has to re-resolve it — and can never mistakenly run
	// ungated after a transient metadata read failure.
	delayMs int64
}

type fanoutCursorHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// FanoutRunner owns every fan-out cursor whose parent partition this
// node owns. Construct with NewFanoutRunner and drive with Run.
type FanoutRunner struct {
	store       *metastore.Store
	selfID      string
	dataDir     string
	broker      fanoutBroker
	peer        peerClient
	partitioner partition.Manager
	metrics     *metrics.Metrics
	logger      *slog.Logger
	cfg         FanoutConfig

	mu      sync.Mutex
	cursors map[fanoutCursorKey]*fanoutCursorHandle
	wg      sync.WaitGroup

	reconcilePasses int
}

// NewFanoutRunner wires a runner. selfID may be empty (single-process /
// test use), in which case every partition is treated as locally owned.
// metrics may be nil.
func NewFanoutRunner(
	store *metastore.Store,
	selfID string,
	dataDir string,
	broker fanoutBroker,
	peer peerClient,
	partitioner partition.Manager,
	m *metrics.Metrics,
	logger *slog.Logger,
	cfg FanoutConfig,
) *FanoutRunner {
	if logger == nil {
		logger = slog.Default()
	}
	return &FanoutRunner{
		store:       store,
		selfID:      selfID,
		dataDir:     dataDir,
		broker:      broker,
		peer:        peer,
		partitioner: partitioner,
		metrics:     m,
		logger:      logger,
		cfg:         cfg.withDefaults(),
		cursors:     map[fanoutCursorKey]*fanoutCursorHandle{},
	}
}

// Run reconciles the cursor set until ctx is cancelled, then stops and
// drains every cursor.
func (r *FanoutRunner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ReconcileInterval)
	defer ticker.Stop()
	for {
		r.Reconcile(ctx)
		select {
		case <-ctx.Done():
			r.mu.Lock()
			for _, h := range r.cursors {
				h.cancel()
			}
			r.mu.Unlock()
			r.wg.Wait()
			return
		case <-ticker.C:
		}
	}
}

// Reconcile performs one pass: compute the desired cursor set from the
// local metastore replica, stop cursors that no longer belong here,
// and spawn the missing ones. Exported so tests (and the e2e harness)
// can drive passes directly.
func (r *FanoutRunner) Reconcile(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	topics, _, err := r.store.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		r.logger.Error("fanout: list topics", "err", err)
		return
	}
	byName := make(map[string]topic.Topic, len(topics))
	for _, t := range topics {
		byName[t.Name] = t
	}

	desired := map[fanoutCursorKey]struct{}{}
	for _, t := range topics {
		if !t.IsChild() || t.Parent == "" {
			continue
		}
		parent, ok := byName[t.Parent]
		if !ok {
			continue
		}
		for p := range parent.Partitions {
			if !r.ownsPartition(parent.Name, p) {
				continue
			}
			desired[fanoutCursorKey{parent: parent.Name, partition: p, child: t.Name, epoch: t.AttachEpoch, delayMs: t.FanoutDelayMs}] = struct{}{}
		}
	}

	r.mu.Lock()
	// Stop cursors that should no longer run here. A cancelled cursor
	// finishes draining in the background; its map entry (and, for a
	// dissolved link, its offset file) is reaped on a later pass once
	// done — never while it might still write. A cursor that exited on
	// its own while still desired (e.g. a transient cursor-file write
	// failure stopped it) is reaped too, so the spawn loop below
	// respawns it from its last persisted offset.
	draining := map[fanoutCursorKey]bool{}
	for key, h := range r.cursors {
		if _, want := desired[key]; want {
			select {
			case <-h.done:
				delete(r.cursors, key)
			default:
			}
			continue
		}
		h.cancel()
		select {
		case <-h.done:
			delete(r.cursors, key)
			r.cleanUpStoppedCursor(key, byName)
		default:
			draining[key] = true
		}
	}
	// Spawn missing cursors — but never two cursors over the same
	// offset file: if a predecessor with a different epoch is still
	// draining, wait for a later pass.
	for key := range desired {
		if _, running := r.cursors[key]; running {
			continue
		}
		if fanoutCursorFileBusy(draining, key) {
			continue
		}
		cursorCtx, cancel := context.WithCancel(ctx)
		h := &fanoutCursorHandle{cancel: cancel, done: make(chan struct{})}
		r.cursors[key] = h
		r.wg.Add(1)
		go func(key fanoutCursorKey) {
			defer r.wg.Done()
			defer close(h.done)
			r.runCursor(cursorCtx, key)
		}(key)
	}
	r.mu.Unlock()

	r.reconcilePasses++
	if r.reconcilePasses%fanoutCursorFileSweepEvery == 1 {
		r.sweepOrphanCursorFiles(byName)
	}
}

// cleanUpStoppedCursor removes the offset file and metric series of a
// cursor whose link no longer exists. A cursor stopped for any other
// reason (e.g. the runner shutting down) keeps its file so a restart
// resumes where it left off.
func (r *FanoutRunner) cleanUpStoppedCursor(key fanoutCursorKey, byName map[string]topic.Topic) {
	child, ok := byName[key.child]
	if ok && child.IsChild() && child.Parent == key.parent && child.AttachEpoch == key.epoch {
		return // link still live; cursor stopped for another reason
	}
	dir := storage.TopicPartitionDir(r.dataDir, key.parent, key.partition)
	if err := storage.RemoveFanoutCursor(dir, key.child); err != nil {
		r.logger.Warn("fanout: remove cursor file", "parent", key.parent, "partition", key.partition, "child", key.child, "err", err)
	}
	if r.metrics != nil {
		r.metrics.FanoutLagMessages.DeletePartialMatch(map[string]string{"parent": key.parent, "child": key.child})
		r.metrics.FanoutDueLagSeconds.DeletePartialMatch(map[string]string{"parent": key.parent, "child": key.child})
	}
}

// sweepOrphanCursorFiles removes cursor files whose parent→child link
// dissolved while this node was not running (detach or delete applied
// elsewhere). Without the sweep a later re-attach could resume — and
// replay — a cursor from a previous attachment.
func (r *FanoutRunner) sweepOrphanCursorFiles(byName map[string]topic.Topic) {
	// A freshly restarted replica can present a stale topic view (old
	// snapshot, catch-up in flight); deleting cursor state against it
	// silently rewinds fan-out to a tail anchor. Cursor-file hygiene is
	// an optimization — skip it until the replica is provably current.
	if !r.store.AppliedCaughtUp() {
		return
	}
	for _, t := range byName {
		for p := range t.Partitions {
			if !r.ownsPartition(t.Name, p) {
				continue
			}
			dir := storage.TopicPartitionDir(r.dataDir, t.Name, p)
			children, err := storage.ListFanoutCursorChildren(dir)
			if err != nil {
				r.logger.Warn("fanout: sweep cursor files", "topic", t.Name, "partition", p, "err", err)
				continue
			}
			for _, childName := range children {
				child, ok := byName[childName]
				if ok && child.IsChild() && child.Parent == t.Name {
					continue // live link; the cursor owns this file
				}
				r.mu.Lock()
				busy := fanoutCursorFileBusyAny(r.cursors, t.Name, p, childName)
				r.mu.Unlock()
				if busy {
					continue
				}
				if err := storage.RemoveFanoutCursor(dir, childName); err != nil {
					r.logger.Warn("fanout: remove orphan cursor file", "parent", t.Name, "partition", p, "child", childName, "err", err)
				} else {
					r.logger.Info("fanout: removed orphan cursor file", "parent", t.Name, "partition", p, "child", childName)
				}
			}
		}
	}
}

// fanoutCursorFileBusy reports whether a draining cursor shares key's
// offset file (same parent partition and child, any epoch).
func fanoutCursorFileBusy(draining map[fanoutCursorKey]bool, key fanoutCursorKey) bool {
	for k := range draining {
		if k.parent == key.parent && k.partition == key.partition && k.child == key.child {
			return true
		}
	}
	return false
}

func fanoutCursorFileBusyAny(cursors map[fanoutCursorKey]*fanoutCursorHandle, parent string, partitionIdx int, child string) bool {
	for k := range cursors {
		if k.parent == parent && k.partition == partitionIdx && k.child == child {
			return true
		}
	}
	return false
}

// ownsPartition reports whether this node owns the partition. An empty
// selfID (no cluster identity: tests, embedded use) owns everything.
func (r *FanoutRunner) ownsPartition(topicName string, partitionIdx int) bool {
	if r.selfID == "" {
		return true
	}
	a, err := r.store.GetAssignment(topicName, partitionIdx)
	return err == nil && a.OwnerID == r.selfID
}

func fanoutPartitionLabel(p int) string { return strconv.Itoa(p) }

// Package runtime owns the broker's mutable per-process state: the
// lazy partition-log map, snapshot reader, and lifecycle hooks.
//
// Logs is the single owner of the map from (topic, partition) to
// *storage.Log. Every other broker subpackage that needs to read or
// write a partition's log goes through Logs — there is no sharing of
// the underlying map. CloseTopic / CloseAll are the only paths that
// retire entries; UpdateTopicRetention and DeleteTopic call
// CloseTopic so the next access reopens with fresh options.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// Logs is the partition-log manager. All access is concurrency-safe;
// the map is RWMutex-guarded for the lazy-open fast path. The produce
// serialization mutexes live in produce_lock.go.
type Logs struct {
	dataDir     string
	storageOpts storage.Options
	metastore   metastore.Metastore
	// versions, when the metastore exposes per-topic versions, lets
	// the Get fast path notice that a topic's record changed under an
	// open log (a delete plus a recreate applied locally) and re-check
	// the incarnation before serving it. Nil for metastores without
	// versions (tests): the fast path then trusts the open entry.
	versions topicVersioner
	metrics  *metrics.Metrics
	logger   *slog.Logger

	mu   sync.RWMutex
	logs map[string]*logEntry

	// guards serializes the slow path (opening a partition log, which
	// may adopt, verify or quarantine the topic directory) against a
	// purge of the same topic, per topic name. A purge holds its
	// topic's guard from closing the logs through unlinking the
	// directory, so no Get can open a log in a directory that is about
	// to disappear, and a Get that waited sees the directory gone.
	// Lock order: guard, then mu.
	guardMu sync.Mutex
	guards  map[string]*topicGuard

	// retired, when set, is called after a topic incarnation's local
	// state has been retired (its directory purged or quarantined) so
	// the owner of the per-topic in-memory state (consumer reservations,
	// loaded schemas) drops it too: that state belongs to the retired
	// incarnation and must not leak into a same-named successor.
	retired func(topicName string)

	// opened, when set, is called with every partition log just after it
	// is opened, so an owner of per-topic delivery state can install its
	// hooks on it (see storage.Log.SetWakeNotifier). Called under the
	// topic guard and mu, so it must not block or reopen a log.
	opened func(topicName string, idx int, l *storage.Log)

	produceMu   sync.Mutex
	produceSync map[string]*sync.Mutex

	// coldDefer holds partitions the cold-retention walk opened and found
	// nothing to reap in, keyed by keyOf, with the time before which the
	// walk leaves them alone.
	coldMu    sync.Mutex
	coldDefer map[string]time.Time
}

// topicVersioner is the optional metastore capability the Get fast
// path uses to detect a topic record change; *metastore.Store has it.
type topicVersioner interface {
	TopicVersion(name string) uint64
}

// logEntry pairs an open log with the time of its last real use. Get
// stamps it; Peek deliberately does not — observation (metrics polls)
// must never keep an idle log warm, or idle eviction could never fire.
type logEntry struct {
	log        *storage.Log
	lastAccess atomic.Int64 // unix nanoseconds of the last Get
	// incarnation is the topic ID the log was opened under (empty for
	// a record without one) and version the topic's metadata version
	// observed at that time. A Get whose live version differs
	// re-reads the record: a different incarnation means the entry
	// serves a deleted topic's directory and must be retired.
	incarnation string
	version     atomic.Uint64
	// walkOwned is set by the cold-retention walk in the critical section
	// that installs this entry for a sweep, and cleared by any Get since
	// (stamp). The walk closes the log only while it is still set, so a
	// consumer that opened the log after it is never cut off mid-read.
	walkOwned atomic.Bool
}

func (e *logEntry) stamp() {
	e.walkOwned.Store(false)
	e.lastAccess.Store(time.Now().UnixNano())
}

// NewLogs constructs a partition-log manager. metastore is consulted
// at lazy-open time to fold the topic's RetentionMs into the storage
// options; metrics may be nil for tests that don't care.
func NewLogs(dataDir string, storageOpts storage.Options, ms metastore.Metastore, m *metrics.Metrics) *Logs {
	g := &Logs{
		dataDir:     dataDir,
		storageOpts: storageOpts,
		metastore:   ms,
		metrics:     m,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		logs:        make(map[string]*logEntry),
		guards:      make(map[string]*topicGuard),
		produceSync: make(map[string]*sync.Mutex),
	}
	if v, ok := ms.(topicVersioner); ok {
		g.versions = v
	}
	return g
}

// SetLogger sets the logger used for incarnation events (a quarantined
// directory is logged at error level). The default discards.
func (g *Logs) SetLogger(l *slog.Logger) {
	if l != nil {
		g.logger = l
	}
}

// DataDir returns the topic-directory root the log map serves from.
func (g *Logs) DataDir() string { return g.dataDir }

// Get returns the storage.Log for (topic, partition), opening the
// underlying file lazily on first access. Per-topic retention is
// folded into Options at open time. Cap and visibility-timeout
// changes do NOT require reopening; only retention does.
//
// The directory the log opens in must belong to the topic's CURRENT
// incarnation: the topic directory's marker is compared with the
// metastore record's ID, and a directory left behind by a deleted
// same-named topic is quarantined instead of served (see
// ensureIncarnationLocked). An already-open log is re-checked whenever
// the topic's metadata version moves, so a delete plus recreate applied
// while the log was open retires it rather than serving the old data.
func (g *Logs) Get(topicName string, idx int) (*storage.Log, error) {
	key := keyOf(topicName, idx)

	g.mu.RLock()
	if e, ok := g.logs[key]; ok && g.entryCurrent(topicName, e) {
		e.stamp()
		g.mu.RUnlock()
		return e.log, nil
	}
	g.mu.RUnlock()

	unlock := g.lockTopic(topicName)
	defer unlock()
	g.mu.Lock()
	l, quarantined, err := g.openLocked(topicName, idx, key)
	g.mu.Unlock()
	if quarantined {
		// A deleted incarnation's directory was set aside: drop the
		// in-memory state that belonged to it (outside mu, under the
		// guard).
		g.notifyRetired(topicName)
	}
	return l, err
}

// openLocked is Get's slow path: re-validate or open the (topic, idx)
// log under the current incarnation. Caller holds the topic's guard and
// mu (write). quarantined reports that a directory of another
// incarnation was set aside on the way.
func (g *Logs) openLocked(topicName string, idx int, key string) (l *storage.Log, quarantined bool, err error) {
	var version uint64
	if g.versions != nil {
		// Read the version BEFORE the record: a change that lands
		// between the two is caught by the next Get's re-check.
		version = g.versions.TopicVersion(topicName)
	}
	opts := g.storageOpts
	var incarnation string
	if g.metastore != nil {
		t, err := g.metastore.GetTopic(context.Background(), topicName)
		switch {
		case err == nil:
			opts.Retention = retentionFromTopic(t.RetentionMs, opts.Retention.CheckInterval)
			incarnation = t.ID
		case errors.Is(err, errs.ErrNotFound):
			// Refuse to (re)create a partition log for a topic that the
			// local metastore no longer knows about. This is the guard
			// that stops a deleted topic from being resurrected by a
			// late produce-dispatch or consume that lazily opens a log
			// after the topic's files were purged. The delete path waits
			// for the local replica to reflect the deletion before
			// purging, so by purge time this branch is authoritative.
			return nil, false, errs.ErrTopicNotFound
		default:
			return nil, false, fmt.Errorf("broker/runtime: lookup topic for retention: %w", err)
		}
	}
	if e, ok := g.logs[key]; ok {
		if e.incarnation == incarnation {
			// The record changed (an alter, or a version bump) but the
			// incarnation did not: the open log is still the right one.
			e.version.Store(version)
			e.stamp()
			return e.log, false, nil
		}
		// The topic was deleted and recreated while its logs were
		// open: every open log under the name belongs to the old
		// incarnation and must go before the directory is checked.
		g.closeTopicLocked(topicName)
	}
	quarantined, err = g.ensureIncarnationLocked(topicName, incarnation)
	if err != nil {
		return nil, quarantined, err
	}
	if g.metrics != nil {
		opts.Metrics = g.metrics.StorageRecorder(topicName, idx)
	}

	partitionDir := storage.TopicPartitionDir(g.dataDir, topicName, idx)
	l, err = storage.NewLog(partitionDir, opts)
	if err != nil {
		return nil, quarantined, fmt.Errorf("broker/runtime: open partition log %s: %w", partitionDir, err)
	}
	e := &logEntry{log: l, incarnation: incarnation}
	e.version.Store(version)
	e.stamp()
	g.logs[key] = e
	if g.opened != nil {
		g.opened(topicName, idx, l)
	}
	return l, quarantined, nil
}

// SetOpened registers a hook invoked with every partition log just
// after it is opened. Call before serving; a nil fn clears it.
func (g *Logs) SetOpened(fn func(topicName string, idx int, l *storage.Log)) {
	g.opened = fn
}

// entryCurrent reports whether an open entry can be served without
// consulting the metastore: true when versions are unavailable or the
// topic's version has not moved since the entry was (re)validated.
func (g *Logs) entryCurrent(topicName string, e *logEntry) bool {
	if g.versions == nil {
		return true
	}
	return e.version.Load() == g.versions.TopicVersion(topicName)
}

// Peek returns the already-open log for (topic, idx) without lazily
// opening one. Read-only observers (the metrics snapshotter) use it so a
// poll never creates a partition directory or resurrects a log that a
// concurrent topic delete just retired.
func (g *Logs) Peek(topicName string, idx int) (*storage.Log, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	e, ok := g.logs[keyOf(topicName, idx)]
	if !ok || e.walkOwned.Load() {
		// A log the cold-retention walk opened for a sweep is not open
		// to observers: it was closed a moment ago and will be closed
		// again in milliseconds, and a reader that found it here would be
		// cut off mid-read.
		return nil, false
	}
	return e.log, true
}

// CloseTopic flushes and closes every cached log under the given
// topic. Subsequent Get calls reopen with whatever options reflect
// the current metastore record. Returns the first close error, if
// any — remaining logs are still removed from the map so retries
// pick up clean state.
func (g *Logs) CloseTopic(topicName string) error {
	prefix := topicName + "/"
	g.mu.Lock()
	firstErr := g.closeTopicLocked(topicName)
	g.mu.Unlock()

	// Retire the topic's produce-serialization mutexes too; otherwise
	// topic churn leaks one entry per (topic, partition) forever. Each
	// entry is deleted only while holding its mutex (retireProduceMutex),
	// so a produce commit mid-critical-section finishes before its mutex
	// disappears from the map — combined with lockProduce's revalidation
	// this keeps produce mutual exclusion intact even when CloseTopic
	// runs against a LIVE topic (e.g. UpdateTopicRetention).
	g.retireProduceEntries(func(k string) bool { return strings.HasPrefix(k, prefix) })
	return firstErr
}

// CloseAll flushes and closes every cached log. Called on broker
// shutdown.
func (g *Logs) CloseAll() error {
	g.mu.Lock()
	var firstErr error
	for k, e := range g.logs {
		if err := e.log.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(g.logs, k)
	}
	g.mu.Unlock()

	g.retireProduceEntries(func(string) bool { return true })
	return firstErr
}

// closeTopicLocked closes and drops every open log under topicName.
// Caller holds mu (write). Produce mutexes are NOT retired here: that
// takes each mutex, which a produce commit inside Get may hold while
// waiting for mu.
func (g *Logs) closeTopicLocked(topicName string) error {
	prefix := topicName + "/"
	var firstErr error
	for k, e := range g.logs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if err := e.log.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(g.logs, k)
	}
	return firstErr
}

func keyOf(topicName string, idx int) string {
	return topicName + "/" + strconv.Itoa(idx)
}

// retentionFromTopic folds a topic's retention into storage options.
// The create/alter paths enforce the one-hour retention floor, so a
// stored record's retention is either zero (keep forever) or at least
// topic.MinRetentionMs.
func retentionFromTopic(r int64, checkInterval time.Duration) storage.RetentionConfig {
	return storage.RetentionConfig{
		MaxAge:        time.Duration(r) * time.Millisecond,
		CheckInterval: checkInterval,
	}
}

// ClosePartition flushes and closes one partition's cached log (a no-op
// when it is not open) and retires its produce mutex. Used when a
// partition's local data is reclaimed after a rebalance moved it to
// another node — the log must be closed before its files are deleted.
func (g *Logs) ClosePartition(topicName string, idx int) error {
	key := keyOf(topicName, idx)
	g.mu.Lock()
	var err error
	if e, ok := g.logs[key]; ok {
		err = e.log.Close()
		delete(g.logs, key)
	}
	g.mu.Unlock()
	g.retireProduceEntries(func(k string) bool { return k == key })
	return err
}

// ReplacePartitionDir closes the partition's open log, if any, and runs
// swap (which replaces the partition directory on disk) while holding
// the topic's guard, so no Get can open or serve the directory that is
// being replaced. The move runner installs a copied partition through
// it: a node that sourced the same partition moments earlier still had
// its old log open, the install renamed the copy over that directory
// underneath the open handle, and every later read served the stale
// files while every later write went to unlinked inodes; records
// committed on the transient owner were never seen and records
// committed after the install were lost when the handle closed (the
// child-topic gap on every join-then-decommission cycle). The next Get
// opens the installed directory under the current incarnation.
func (g *Logs) ReplacePartitionDir(topicName string, idx int, swap func() error) error {
	unlock := g.lockTopic(topicName)
	defer unlock()
	if err := g.ClosePartition(topicName, idx); err != nil {
		return fmt.Errorf("broker/runtime: close %s/%d before replacing its directory: %w", topicName, idx, err)
	}
	return swap()
}

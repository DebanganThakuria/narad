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

	produceMu   sync.Mutex
	produceSync map[string]*sync.Mutex
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
}

func (e *logEntry) stamp() { e.lastAccess.Store(time.Now().UnixNano()) }

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
	defer g.mu.Unlock()

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
			return nil, errs.ErrTopicNotFound
		default:
			return nil, fmt.Errorf("broker/runtime: lookup topic for retention: %w", err)
		}
	}
	if e, ok := g.logs[key]; ok {
		if e.incarnation == incarnation {
			// The record changed (an alter, or a version bump) but the
			// incarnation did not: the open log is still the right one.
			e.version.Store(version)
			e.stamp()
			return e.log, nil
		}
		// The topic was deleted and recreated while its logs were
		// open: every open log under the name belongs to the old
		// incarnation and must go before the directory is checked.
		g.closeTopicLocked(topicName)
	}
	if err := g.ensureIncarnationLocked(topicName, incarnation); err != nil {
		return nil, err
	}
	if g.metrics != nil {
		opts.Metrics = g.metrics.StorageRecorder(topicName, idx)
	}

	partitionDir := storage.TopicPartitionDir(g.dataDir, topicName, idx)
	l, err := storage.NewLog(partitionDir, opts)
	if err != nil {
		return nil, fmt.Errorf("broker/runtime: open partition log %s: %w", partitionDir, err)
	}
	e := &logEntry{log: l, incarnation: incarnation}
	e.version.Store(version)
	e.stamp()
	g.logs[key] = e
	return l, nil
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
	if !ok {
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

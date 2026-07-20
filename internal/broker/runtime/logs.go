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
	metrics     *metrics.Metrics

	mu   sync.RWMutex
	logs map[string]*logEntry

	produceMu   sync.Mutex
	produceSync map[string]*sync.Mutex
}

// logEntry pairs an open log with the time of its last real use. Get
// stamps it; Peek deliberately does not — observation (metrics polls)
// must never keep an idle log warm, or idle eviction could never fire.
type logEntry struct {
	log        *storage.Log
	lastAccess atomic.Int64 // unix nanoseconds of the last Get
}

func (e *logEntry) stamp() { e.lastAccess.Store(time.Now().UnixNano()) }

// NewLogs constructs a partition-log manager. metastore is consulted
// at lazy-open time to fold the topic's RetentionMs into the storage
// options; metrics may be nil for tests that don't care.
func NewLogs(dataDir string, storageOpts storage.Options, ms metastore.Metastore, m *metrics.Metrics) *Logs {
	return &Logs{
		dataDir:     dataDir,
		storageOpts: storageOpts,
		metastore:   ms,
		metrics:     m,
		logs:        make(map[string]*logEntry),
		produceSync: make(map[string]*sync.Mutex),
	}
}

// DataDir returns the topic-directory root the log map serves from.
func (g *Logs) DataDir() string { return g.dataDir }

// Get returns the storage.Log for (topic, partition), opening the
// underlying file lazily on first access. Per-topic retention is
// folded into Options at open time. Cap and visibility-timeout
// changes do NOT require reopening; only retention does.
func (g *Logs) Get(topicName string, idx int) (*storage.Log, error) {
	key := keyOf(topicName, idx)

	g.mu.RLock()
	if e, ok := g.logs[key]; ok {
		e.stamp()
		g.mu.RUnlock()
		return e.log, nil
	}
	g.mu.RUnlock()

	g.mu.Lock()
	defer g.mu.Unlock()
	if e, ok := g.logs[key]; ok {
		e.stamp()
		return e.log, nil
	}

	opts := g.storageOpts
	if g.metastore != nil {
		t, err := g.metastore.GetTopic(context.Background(), topicName)
		switch {
		case err == nil:
			opts.Retention = retentionFromTopic(t.RetentionMs, opts.Retention.CheckInterval)
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
	if g.metrics != nil {
		opts.Metrics = g.metrics.StorageRecorder(topicName, idx)
	}

	partitionDir := storage.TopicPartitionDir(g.dataDir, topicName, idx)
	l, err := storage.NewLog(partitionDir, opts)
	if err != nil {
		return nil, fmt.Errorf("broker/runtime: open partition log %s: %w", partitionDir, err)
	}
	e := &logEntry{log: l}
	e.stamp()
	g.logs[key] = e
	return l, nil
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

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
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
)

// Logs is the partition-log manager. All access is concurrency-safe;
// the map is RWMutex-guarded for the lazy-open fast path.
type Logs struct {
	dataDir     string
	storageOpts storage.Options
	metastore   metastore.Metastore
	metrics     *metrics.Metrics

	mu   sync.RWMutex
	logs map[string]*storage.Log
}

// NewLogs constructs a partition-log manager. metastore is consulted
// at lazy-open time to fold the topic's RetentionMs into the storage
// options; metrics may be nil for tests that don't care.
func NewLogs(dataDir string, storageOpts storage.Options, ms metastore.Metastore, m *metrics.Metrics) *Logs {
	return &Logs{
		dataDir:     dataDir,
		storageOpts: storageOpts,
		metastore:   ms,
		metrics:     m,
		logs:        make(map[string]*storage.Log),
	}
}

// Get returns the storage.Log for (topic, partition), opening the
// underlying file lazily on first access. Per-topic retention is
// folded into Options at open time. Cap and visibility-timeout
// changes do NOT require reopening; only retention does.
func (g *Logs) Get(topicName string, idx int) (*storage.Log, error) {
	key := keyOf(topicName, idx)

	g.mu.RLock()
	if l, ok := g.logs[key]; ok {
		g.mu.RUnlock()
		return l, nil
	}
	g.mu.RUnlock()

	g.mu.Lock()
	defer g.mu.Unlock()
	if l, ok := g.logs[key]; ok {
		return l, nil
	}

	opts := g.storageOpts
	if t, err := g.metastore.GetTopic(context.Background(), topicName); err == nil {
		opts.Retention = retentionFromTopic(t.RetentionMs, opts.Retention.CheckInterval)
	} else if !errors.Is(err, errs.ErrNotFound) {
		return nil, fmt.Errorf("broker/runtime: lookup topic for retention: %w", err)
	}
	if g.metrics != nil {
		opts.Metrics = g.metrics.StorageRecorder(topicName, idx)
	}

	partitionDir := filepath.Join(g.dataDir, "topics", topicName, fmt.Sprintf("p%05d", idx))
	l, err := storage.NewLog(partitionDir, opts)
	if err != nil {
		return nil, fmt.Errorf("broker/runtime: open partition log %s: %w", partitionDir, err)
	}
	g.logs[key] = l
	return l, nil
}

// CloseTopic flushes and closes every cached log under the given
// topic. Subsequent Get calls reopen with whatever options reflect
// the current metastore record. Returns the first close error, if
// any — remaining logs are still removed from the map so retries
// pick up clean state.
func (g *Logs) CloseTopic(topicName string) error {
	prefix := topicName + "/"
	g.mu.Lock()
	defer g.mu.Unlock()
	var firstErr error
	for k, l := range g.logs {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(g.logs, k)
	}
	return firstErr
}

// CloseAll flushes and closes every cached log. Called on broker
// shutdown.
func (g *Logs) CloseAll() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	var firstErr error
	for k, l := range g.logs {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(g.logs, k)
	}
	return firstErr
}

func keyOf(topicName string, idx int) string {
	return topicName + "/" + strconv.Itoa(idx)
}

func retentionFromTopic(r int64, checkInterval time.Duration) storage.RetentionConfig {
	return storage.RetentionConfig{
		MaxAge:        time.Duration(r) * time.Millisecond,
		CheckInterval: checkInterval,
	}
}

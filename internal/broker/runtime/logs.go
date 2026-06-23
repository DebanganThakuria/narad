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

	produceMu   sync.Mutex
	produceSync map[string]*sync.Mutex
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
		produceSync: make(map[string]*sync.Mutex),
	}
}

func (g *Logs) lockProduce(topicName string, idx int) func() {
	key := keyOf(topicName, idx)

	g.produceMu.Lock()
	mu, ok := g.produceSync[key]
	if !ok {
		mu = &sync.Mutex{}
		g.produceSync[key] = mu
	}
	g.produceMu.Unlock()

	mu.Lock()
	return mu.Unlock
}

func (g *Logs) dropProduceLock(topicName string, idx int) {
	g.produceMu.Lock()
	delete(g.produceSync, keyOf(topicName, idx))
	g.produceMu.Unlock()
}

func (g *Logs) WithProduceLock(topicName string, idx int, fn func(*storage.Log) error) error {
	unlock := g.lockProduce(topicName, idx)
	defer unlock()

	log, err := g.Get(topicName, idx)
	if err != nil {
		return err
	}
	return fn(log)
}

func (g *Logs) WithProduceLockResult(topicName string, idx int, fn func(*storage.Log) (int64, error)) (int64, error) {
	unlock := g.lockProduce(topicName, idx)
	defer unlock()

	log, err := g.Get(topicName, idx)
	if err != nil {
		return 0, err
	}
	return fn(log)
}

func (g *Logs) WithProduceLockValue(topicName string, idx int, fn func(*storage.Log) (int64, int, error)) (int64, int, error) {
	waitStart := time.Now()
	unlock := g.lockProduce(topicName, idx)
	g.observeProduce("lock_wait", "ok", time.Since(waitStart))
	defer unlock()

	openStart := time.Now()
	log, err := g.Get(topicName, idx)
	if err != nil {
		g.observeProduce("log_open", "error", time.Since(openStart))
		return 0, 0, err
	}
	g.observeProduce("log_open", "ok", time.Since(openStart))
	return fn(log)
}

func (g *Logs) ProduceSyncCount() int {
	g.produceMu.Lock()
	defer g.produceMu.Unlock()
	return len(g.produceSync)
}

func (g *Logs) DropProduceSync(topicName string, idx int) {
	g.dropProduceLock(topicName, idx)
}

func (g *Logs) observeProduce(stage, outcome string, duration time.Duration) {
	if g.metrics == nil {
		return
	}
	g.metrics.ObserveHotPathStage("broker_runtime", "produce", stage, outcome, duration)
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
	if g.metastore != nil {
		if t, err := g.metastore.GetTopic(context.Background(), topicName); err == nil {
			opts.Retention = retentionFromTopic(t.RetentionMs, opts.Retention.CheckInterval)
		} else if !errors.Is(err, errs.ErrNotFound) {
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
	g.logs[key] = l
	g.setActiveLogCountLocked()
	return l, nil
}

// Peek returns the already-open log for (topic, idx) without lazily
// opening one. Read-only observers (the metrics snapshotter) use it so a
// poll never creates a partition directory or resurrects a log that a
// concurrent topic delete just retired.
func (g *Logs) Peek(topicName string, idx int) (*storage.Log, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	l, ok := g.logs[keyOf(topicName, idx)]
	return l, ok
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
	g.setActiveLogCountLocked()
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
	g.setActiveLogCountLocked()
	return firstErr
}

func keyOf(topicName string, idx int) string {
	return topicName + "/" + strconv.Itoa(idx)
}

func (g *Logs) setActiveLogCountLocked() {
	if g.metrics == nil {
		return
	}
	g.metrics.ActivePartitionLogs.Set(float64(len(g.logs)))
}

func retentionFromTopic(r int64, checkInterval time.Duration) storage.RetentionConfig {
	return storage.RetentionConfig{
		MaxAge:        time.Duration(r) * time.Millisecond,
		CheckInterval: checkInterval,
	}
}

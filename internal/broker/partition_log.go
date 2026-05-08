package broker

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/metastore"
	"github.com/debanganthakuria/narad/internal/storage"
	"github.com/debanganthakuria/narad/internal/topic"
)

// partitionLog returns the storage.Log for a (topic, partition) pair,
// opening the underlying file lazily on first use. The log is
// internally synchronized — callers do not need any further lock.
//
// Per-topic retention is folded into Options at open time. Changing a
// topic's retention requires reopening its partition logs (a known
// v1 limitation).
func (b *impl) partitionLog(topicName string, idx int) (*storage.Log, error) {
	key := topicName + "/" + strconv.Itoa(idx)

	b.mu.RLock()
	if l, ok := b.logs[key]; ok {
		b.mu.RUnlock()
		return l, nil
	}
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	if l, ok := b.logs[key]; ok {
		return l, nil
	}

	opts := b.deps.LogOptions
	if t, err := b.deps.Metastore.GetTopic(context.Background(), topicName); err == nil {
		opts.Retention = retentionFromTopic(t.Retention, opts.Retention.CheckInterval)
	} else if !errors.Is(err, metastore.ErrNotFound) {
		return nil, fmt.Errorf("broker: lookup topic for retention: %w", err)
	}
	opts.Metrics = b.deps.Metrics.StorageRecorder(topicName, idx)

	partitionDir := filepath.Join(b.deps.DataDir, "topics", topicName, fmt.Sprintf("p%05d", idx))

	l, err := storage.NewLog(partitionDir, opts)
	if err != nil {
		return nil, fmt.Errorf("broker: open partition log %s: %w", partitionDir, err)
	}
	b.logs[key] = l
	return l, nil
}

func retentionFromTopic(r topic.Retention, checkInterval time.Duration) storage.RetentionConfig {
	return storage.RetentionConfig{
		MaxAge:        time.Duration(r.MaxAgeMs) * time.Millisecond,
		MaxBytes:      r.MaxBytes,
		CheckInterval: checkInterval,
	}
}

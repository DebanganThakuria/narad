package runtime

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

const defaultConsumerOffsetCommitInterval = 100 * time.Millisecond

type offsetCommitKey struct {
	topic     string
	partition int
}

type offsetCommit struct {
	key    offsetCommitKey
	offset int64
}

// ConsumerOffsetCommitter batches best-effort consumer offset persistence.
// Ack commits are authoritative in memory; this writer only seeds recovery.
type ConsumerOffsetCommitter struct {
	dataDir  string
	interval time.Duration
	log      *slog.Logger

	mu      sync.Mutex
	pending map[offsetCommitKey]int64

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// NewConsumerOffsetCommitter starts the background flush loop. A
// non-positive interval falls back to the default. Callers must Close
// to stop the loop and flush what's pending.
func NewConsumerOffsetCommitter(dataDir string, interval time.Duration, log *slog.Logger) *ConsumerOffsetCommitter {
	if interval <= 0 {
		interval = defaultConsumerOffsetCommitInterval
	}
	c := &ConsumerOffsetCommitter{
		dataDir:  dataDir,
		interval: interval,
		log:      log,
		pending:  make(map[offsetCommitKey]int64),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go c.run()
	return c
}

// Commit queues an offset for the next flush. Offsets only move
// forward: a smaller offset never overwrites a pending larger one.
func (c *ConsumerOffsetCommitter) Commit(topic string, partition int, offset int64) {
	key := offsetCommitKey{topic: topic, partition: partition}
	c.mu.Lock()
	if current, ok := c.pending[key]; !ok || offset > current {
		c.pending[key] = offset
	}
	c.mu.Unlock()
}

// Close stops the flush loop and performs a final flush. Idempotent.
func (c *ConsumerOffsetCommitter) Close() error {
	c.once.Do(func() { close(c.stop) })
	<-c.done
	return c.flush()
}

func (c *ConsumerOffsetCommitter) run() {
	defer close(c.done)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.flush(); err != nil && c.log != nil {
				c.log.Error("consumer offset batch write failed", "err", err)
			}
		case <-c.stop:
			return
		}
	}
}

// flush drains the pending map and writes each offset. A purged
// partition directory is skipped silently (the topic is gone); any
// other failure re-queues the offset for the next flush so a transient
// error can't lose the recovery seed.
func (c *ConsumerOffsetCommitter) flush() error {
	commits := c.drain()
	var firstErr error
	for _, commit := range commits {
		partitionDir := storage.TopicPartitionDir(c.dataDir, commit.key.topic, commit.key.partition)
		if err := storage.WriteConsumerOffsetIfPartitionDirExists(partitionDir, commit.offset); err != nil {
			if errors.Is(err, storage.ErrPartitionDirMissing) {
				continue
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("write consumer offset %s/%d=%d: %w", commit.key.topic, commit.key.partition, commit.offset, err)
			}
			c.Commit(commit.key.topic, commit.key.partition, commit.offset)
		}
	}
	return firstErr
}

func (c *ConsumerOffsetCommitter) drain() []offsetCommit {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return nil
	}
	commits := make([]offsetCommit, 0, len(c.pending))
	for key, offset := range c.pending {
		commits = append(commits, offsetCommit{key: key, offset: offset})
	}
	clear(c.pending)
	return commits
}

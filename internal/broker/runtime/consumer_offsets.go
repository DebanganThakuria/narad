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

// AheadSource returns, for one partition, the committed frontier, the
// offsets acked out of order above it, and a version that changes
// whenever that set changes; ok=false when the node holds no state for
// the partition. consumer.InFlight.AheadSnapshot is the production
// source.
type AheadSource func(topic string, partition int) (committed int64, offsets []int64, version uint64, ok bool)

// aheadWritten remembers what the last consumer.ahead write for a
// partition carried, so an unchanged set is not rewritten and the next
// write lands in the other slot.
type aheadWritten struct {
	version uint64
	slot    int
	seq     uint64
}

// ConsumerOffsetCommitter batches best-effort consumer offset persistence.
// Ack commits are authoritative in memory; this writer only seeds recovery.
//
// A partition is dirty when its frontier advanced or its acked-ahead
// set changed, and a flush makes it durable with one data sync: to
// consumer.ahead (the offsets acked out of order above the frontier)
// when that set changed, else to consumer.offset (the frontier). The
// consumer.ahead record carries the frontier too, and recovery takes
// the larger of the two files' frontiers, so while acks arrive out of
// order consumer.offset is left behind rather than synced a second
// time; Close brings it level. Every reader of the persisted frontier
// must therefore read both files.
type ConsumerOffsetCommitter struct {
	dataDir  string
	interval time.Duration
	log      *slog.Logger

	mu      sync.Mutex
	pending map[offsetCommitKey]int64
	// lastOffset is the highest frontier made durable per partition,
	// by either file; a flush that finds the same value or a lower one
	// skips the consumer.offset write and its fdatasync. A lower one is
	// not stale data to repair: acks call Commit after dropping the
	// shard lock, so two frontier advances can reach the committer in
	// reverse order, and when the flush that wrote the higher one runs
	// in between, the lower one arrives alone in the next window.
	lastOffset map[offsetCommitKey]int64
	// offsetFile is what this process last wrote to consumer.offset per
	// partition. It trails lastOffset while consumer.ahead carries the
	// frontier.
	offsetFile map[offsetCommitKey]int64
	lastAhead  map[offsetCommitKey]aheadWritten
	ahead      AheadSource

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
		dataDir:    dataDir,
		interval:   interval,
		log:        log,
		pending:    make(map[offsetCommitKey]int64),
		lastOffset: make(map[offsetCommitKey]int64),
		offsetFile: make(map[offsetCommitKey]int64),
		lastAhead:  make(map[offsetCommitKey]aheadWritten),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	go c.run()
	return c
}

// SetAheadSource registers the acked-ahead snapshot source. Without one
// only the frontier is persisted. Call before serving.
func (c *ConsumerOffsetCommitter) SetAheadSource(fn AheadSource) {
	c.mu.Lock()
	c.ahead = fn
	c.mu.Unlock()
}

// Commit queues an offset for the next flush. Offsets only move
// forward: a smaller offset never overwrites a pending larger one, nor
// one a previous flush already wrote. A
// call with the current frontier (no advance) still marks the partition
// dirty, which is how an out-of-order ack reaches the next flush.
func (c *ConsumerOffsetCommitter) Commit(topic string, partition int, offset int64) {
	key := offsetCommitKey{topic: topic, partition: partition}
	c.mu.Lock()
	if current, ok := c.pending[key]; !ok || offset > current {
		c.pending[key] = offset
	}
	c.mu.Unlock()
}

// Forget drops what the committer remembers about a partition, so the
// next flush for it writes both files unconditionally. Call when the
// partition's directory was replaced or removed under the committer (a
// move installed a copy, a reclaim quarantined it).
func (c *ConsumerOffsetCommitter) Forget(topic string, partition int) {
	key := offsetCommitKey{topic: topic, partition: partition}
	c.mu.Lock()
	delete(c.pending, key)
	delete(c.lastOffset, key)
	delete(c.offsetFile, key)
	delete(c.lastAhead, key)
	c.mu.Unlock()
}

// Close stops the flush loop, performs a final flush and brings every
// consumer.offset that consumer.ahead left behind up to the frontier,
// so after a graceful stop the frontier file alone is exact. Idempotent.
func (c *ConsumerOffsetCommitter) Close() error {
	c.once.Do(func() { close(c.stop) })
	<-c.done
	err := c.flush()
	if lerr := c.levelOffsetFiles(); err == nil {
		err = lerr
	}
	return err
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

// flush drains the pending map and writes each dirty partition's files.
// A purged partition directory is skipped silently (the topic is gone);
// any other failure re-queues the partition for the next flush so a
// transient error can't lose the recovery seed.
func (c *ConsumerOffsetCommitter) flush() error {
	commits, ahead := c.drain()
	var firstErr error
	for _, commit := range commits {
		partitionDir := storage.TopicPartitionDir(c.dataDir, commit.key.topic, commit.key.partition)
		if err := c.persist(partitionDir, commit, ahead); err != nil {
			if errors.Is(err, storage.ErrPartitionDirMissing) {
				c.Forget(commit.key.topic, commit.key.partition)
				continue
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("persist consumer state %s/%d: %w", commit.key.topic, commit.key.partition, err)
			}
			c.Commit(commit.key.topic, commit.key.partition, commit.offset)
		}
	}
	return firstErr
}

// persist makes one dirty partition durable, normally with a single
// data sync. consumer.ahead goes first when its set changed; its record
// carries the source's frontier, which is at least the queued commit
// unless that commit outlived its shard, so writeOffset then finds
// nothing above what is durable and skips the second sync. A failed
// consumer.ahead write still lets the frontier through consumer.offset.
func (c *ConsumerOffsetCommitter) persist(partitionDir string, commit offsetCommit, source AheadSource) error {
	var aheadErr error
	if source != nil {
		aheadErr = c.writeAhead(partitionDir, commit.key, source)
		if errors.Is(aheadErr, storage.ErrPartitionDirMissing) {
			return aheadErr
		}
	}
	if err := c.writeOffset(partitionDir, commit); err != nil {
		return err
	}
	return aheadErr
}

// writeOffset persists the frontier unless it is at or below the
// highest one already durable: the frontier never moves backwards, and
// Forget clears the memory whenever the directory is replaced or
// removed.
func (c *ConsumerOffsetCommitter) writeOffset(partitionDir string, commit offsetCommit) error {
	c.mu.Lock()
	last, seen := c.lastOffset[commit.key]
	c.mu.Unlock()
	if seen && commit.offset <= last {
		return nil
	}
	if err := storage.WriteConsumerOffsetIfPartitionDirExists(partitionDir, commit.offset); err != nil {
		return fmt.Errorf("write consumer offset %d: %w", commit.offset, err)
	}
	c.mu.Lock()
	c.lastOffset[commit.key] = commit.offset
	c.offsetFile[commit.key] = commit.offset
	c.mu.Unlock()
	return nil
}

// levelOffsetFiles writes consumer.offset for every partition whose
// latest frontier only consumer.ahead holds, so a reader of the
// frontier file alone (the dispatcher's free-record estimate for a
// partition with no shard, an older binary) sees where consumers got
// to after a graceful stop. Called by Close, after the loop stopped.
func (c *ConsumerOffsetCommitter) levelOffsetFiles() error {
	c.mu.Lock()
	var behind []offsetCommit
	for key, frontier := range c.lastOffset {
		if written, ok := c.offsetFile[key]; !ok || written < frontier {
			behind = append(behind, offsetCommit{key: key, offset: frontier})
		}
	}
	c.mu.Unlock()
	var firstErr error
	for _, commit := range behind {
		partitionDir := storage.TopicPartitionDir(c.dataDir, commit.key.topic, commit.key.partition)
		if err := storage.WriteConsumerOffsetIfPartitionDirExists(partitionDir, commit.offset); err != nil {
			if errors.Is(err, storage.ErrPartitionDirMissing) {
				c.Forget(commit.key.topic, commit.key.partition)
				continue
			}
			if firstErr == nil {
				firstErr = fmt.Errorf("level consumer offset %s/%d: %w", commit.key.topic, commit.key.partition, err)
			}
			continue
		}
		c.mu.Lock()
		c.offsetFile[commit.key] = commit.offset
		c.mu.Unlock()
	}
	return firstErr
}

// writeAhead persists the acked-ahead set unless its version is the one
// last written. Writes alternate slots and carry a rising sequence so
// the previous record survives a torn write. A written record raises
// the durable frontier to the one it carries.
func (c *ConsumerOffsetCommitter) writeAhead(partitionDir string, key offsetCommitKey, source AheadSource) error {
	committed, offsets, version, ok := source(key.topic, key.partition)
	if !ok {
		return nil
	}
	c.mu.Lock()
	last, seen := c.lastAhead[key]
	c.mu.Unlock()
	if seen && last.version == version {
		return nil
	}
	if !seen {
		// First write for this partition in this process: resume from the
		// record on disk so the write lands in the other slot (a torn
		// first write must not destroy the newest record) with a higher
		// seq than it (a clock that stepped back must not make the reader
		// prefer the stale record).
		if rec, ok, err := storage.ReadConsumerAhead(partitionDir); err == nil && ok {
			last = aheadWritten{slot: rec.Slot, seq: rec.Seq}
		}
	}
	if !seen && len(offsets) == 0 {
		// Nothing acked ahead and nothing written by this process yet:
		// a record would only say "empty", which is what a missing file
		// already means. Remember the version so the next change writes.
		last.version = version
		c.mu.Lock()
		c.lastAhead[key] = last
		c.mu.Unlock()
		return nil
	}
	slot := (last.slot + 1) % 2
	seq := max(last.seq+1, uint64(time.Now().UnixNano()))
	if err := storage.WriteConsumerAhead(partitionDir, slot, seq, committed, offsets); err != nil {
		return fmt.Errorf("write consumer ahead (%d offsets): %w", len(offsets), err)
	}
	c.mu.Lock()
	c.lastAhead[key] = aheadWritten{version: version, slot: slot, seq: seq}
	if last, seen := c.lastOffset[key]; !seen || committed > last {
		c.lastOffset[key] = committed
	}
	c.mu.Unlock()
	return nil
}

func (c *ConsumerOffsetCommitter) drain() ([]offsetCommit, AheadSource) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.pending) == 0 {
		return nil, c.ahead
	}
	commits := make([]offsetCommit, 0, len(c.pending))
	for key, offset := range c.pending {
		commits = append(commits, offsetCommit{key: key, offset: offset})
	}
	clear(c.pending)
	return commits, c.ahead
}

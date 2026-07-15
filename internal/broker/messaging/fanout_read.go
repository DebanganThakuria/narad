package messaging

// The fan-out read side: cursors tail a parent partition's committed
// log in large slabs. Runs only on the parent partition's owner — the
// cursor engine is placed there so this read never crosses the network.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// ReadFanoutSlab reads committed keyed records from a locally owned
// partition, starting at opts.FromOffset (or the current committed
// tail when it is topic.FanoutTailOffset), up to opts.MaxRecords /
// opts.MaxBytes of payload. When nothing is committed at FromOffset
// yet and opts.Wait > 0, it long-polls the partition's notify
// broadcast up to Wait.
//
// A positive opts.MaxCommittedAt is the due gate for delay children:
// the read stops at the first record committed after it, reports that
// record's commit time as BlockedUntilUnixMs, and returns immediately
// (no long-poll) — nothing newer can be due either, since records
// commit in time order per partition.
//
// Offsets below the oldest retained offset are reported as
// DroppedBehind; permanently unreadable records inside the retained
// range are skipped and reported as SkippedCorrupt. NextOffset always
// advances past everything returned, dropped, or skipped — but never
// past a record blocked by the due gate. The caller persists it only
// after the records are durably committed downstream.
func (e *Engine) ReadFanoutSlab(ctx context.Context, topicName string, partition int, opts topic.FanoutReadOpts) (topic.FanoutSlab, error) {
	if e.logs == nil {
		return topic.FanoutSlab{}, unavailableError("partition logs")
	}
	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return topic.FanoutSlab{}, err
	}
	if partition < 0 || partition >= t.Partitions {
		return topic.FanoutSlab{}, fmt.Errorf("%w: partition out of range", ErrInvalid)
	}
	if !e.isLocalOwner(topicName, partition) {
		return topic.FanoutSlab{}, ErrNotPartitionOwner
	}
	// Peek first: a caught-up cursor polls this once a second forever,
	// and going through Get would stamp the log as active — an
	// attached-but-silent child would then hold its parent open and
	// idle eviction could never fire. When the log is closed, the
	// durable HWM file (force-synced by Close) answers "is there
	// committed backlog?" without opening anything. Only genuine
	// backlog — or a tail-anchor read, which needs the live tail —
	// opens the log.
	log, open := e.logs.Peek(topicName, partition)
	if !open {
		if slab, served, err := e.fanoutSlabWhileClosed(ctx, topicName, partition, opts); served {
			return slab, err
		}
		var err error
		log, err = e.logs.Get(topicName, partition)
		if err != nil {
			return topic.FanoutSlab{}, err
		}
	}
	if opts.MaxRecords <= 0 {
		opts.MaxRecords = 1
	}

	deadline := time.Now().Add(opts.Wait)
	for {
		// Snapshot the notify channel BEFORE probing so a commit that
		// lands between the probe and the wait still wakes us.
		var notify <-chan struct{}
		if opts.Wait > 0 {
			notify = log.NotifyC()
		}

		slab, err := e.readFanoutSlabOnce(log, opts)
		if err != nil {
			return topic.FanoutSlab{}, err
		}
		if len(slab.Records) > 0 || slab.DroppedBehind > 0 || slab.SkippedCorrupt > 0 {
			return slab, nil
		}
		// Blocked on an undue record: waiting for NEW commits cannot
		// help — the caller sleeps until the head is due instead.
		if slab.BlockedUntilUnixMs > 0 {
			return slab, nil
		}
		remaining := time.Until(deadline)
		if opts.Wait <= 0 || remaining <= 0 {
			return slab, nil
		}
		select {
		case <-ctx.Done():
			return slab, nil
		case <-notify:
		case <-time.After(remaining):
		}
	}
}

// FanoutCursorStats reports the persisted fan-out cursor positions for
// every locally-owned partition of parent, one entry per (child,
// partition) whose cursor has been anchored. Partitions owned by other
// nodes are absent — the API layer merges owners' views.
func (e *Engine) FanoutCursorStats(ctx context.Context, parent string) ([]topic.FanoutCursorStat, error) {
	if e.logs == nil {
		return nil, unavailableError("partition logs")
	}
	t, err := e.getTopic(ctx, parent)
	if err != nil {
		return nil, err
	}
	if !t.IsParent() {
		return nil, nil
	}
	var stats []topic.FanoutCursorStat
	for p := range t.Partitions {
		if !e.isLocalOwner(parent, p) {
			continue
		}
		log, err := e.logs.Get(parent, p)
		if err != nil {
			return nil, err
		}
		hwm := log.HighWatermark()
		dir := storage.TopicPartitionDir(e.logs.DataDir(), parent, p)
		for _, child := range t.Children {
			cur, ok, err := storage.ReadFanoutCursor(dir, child)
			if err != nil || !ok {
				continue // cursor not anchored yet (or unreadable): report nothing
			}
			stats = append(stats, topic.FanoutCursorStat{
				Child:         child,
				Partition:     p,
				NextOffset:    cur.NextOffset,
				HighWatermark: hwm,
			})
		}
	}
	return stats, nil
}

// fanoutLog is the slice of *storage.Log the slab read uses; narrowed
// so drop-behind arithmetic is unit-testable without forcing real
// segment retention.
type fanoutLog interface {
	HighWatermark() int64
	OldestOffset() int64
	ReadKeyed(offset int64) (string, int64, []byte, error)
}

// readFanoutSlabOnce performs one non-blocking slab read.
func (e *Engine) readFanoutSlabOnce(log fanoutLog, opts topic.FanoutReadOpts) (topic.FanoutSlab, error) {
	hwm := log.HighWatermark()
	oldest := log.OldestOffset()

	slab := topic.FanoutSlab{OldestOffset: oldest, HighWatermark: hwm}
	start := opts.FromOffset
	if start == topic.FanoutTailOffset {
		// A fresh cursor starts at the committed tail: no backfill.
		slab.NextOffset = hwm
		return slab, nil
	}
	if start < oldest {
		// The requested range aged out of retention (drop-behind):
		// skip to the oldest record still available. Only offsets that
		// were VISIBLE count as dropped — after a crash the recovered
		// HWM can sit below the cursor, making this difference
		// negative for ranges that never became consumable.
		slab.DroppedBehind = max(0, min(oldest, hwm)-start)
		start = oldest
	}
	slab.NextOffset = start
	if start >= hwm {
		slab.NextOffset = max(start, hwm)
		return slab, nil
	}

	var bytes int64
	offset := start
	for offset < hwm && len(slab.Records) < opts.MaxRecords && bytes < opts.MaxBytes {
		key, committedAt, payload, err := log.ReadKeyed(offset)
		switch {
		case err == nil:
			if opts.MaxCommittedAt > 0 && committedAt > opts.MaxCommittedAt {
				// Due gate: this record (and, by per-partition commit
				// order, everything after it) is not deliverable yet.
				slab.BlockedUntilUnixMs = committedAt
				slab.NextOffset = offset
				return slab, nil
			}
			slab.Records = append(slab.Records, topic.KeyedRecord{
				Key:               key,
				CommittedAtUnixMs: committedAt,
				Payload:           payload,
			})
			bytes += int64(len(key) + len(payload))
		case errors.Is(err, storage.ErrOffsetNotFound):
			// Below the HWM an unreadable offset is either a gap the
			// recovery scan skipped or a segment the reaper deleted
			// mid-read; both mean the record is gone for good.
			if newOldest := log.OldestOffset(); offset < newOldest {
				slab.DroppedBehind += min(newOldest, hwm) - offset
				offset = min(newOldest, hwm)
				continue
			}
			slab.SkippedCorrupt++
		case storage.IsCorrupt(err):
			slab.SkippedCorrupt++
		default:
			return topic.FanoutSlab{}, err
		}
		offset++
	}
	slab.NextOffset = offset
	return slab, nil
}

// fanoutSlabWhileClosed serves a fan-out read against a partition whose
// log is not open, without opening it. served=false means the caller
// must open the log for real: the cursor is behind the durable HWM
// (backlog to drain — correctness over eviction, always), the read is
// a tail-anchor (needs the live tail), or the HWM file is unreadable
// (open for ground truth).
//
// The caught-up case sleeps out the long-poll in short slices,
// re-checking Peek each slice: a produce reopens the log through Get,
// and the next slice notices, falls back to the caller's open path,
// and resumes real long-polling — so the first record after an idle
// period pays at most one slice of extra latency.
func (e *Engine) fanoutSlabWhileClosed(ctx context.Context, topicName string, partition int, opts topic.FanoutReadOpts) (topic.FanoutSlab, bool, error) {
	if opts.FromOffset == topic.FanoutTailOffset {
		return topic.FanoutSlab{}, false, nil
	}
	partitionDir := storage.TopicPartitionDir(e.logs.DataDir(), topicName, partition)
	hwm, _, err := storage.ReadPersistedHighWatermark(partitionDir)
	if err != nil {
		return topic.FanoutSlab{}, false, nil
	}
	if opts.FromOffset < hwm {
		return topic.FanoutSlab{}, false, nil
	}

	const slice = 250 * time.Millisecond
	deadline := time.Now().Add(opts.Wait)
	for opts.Wait > 0 {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		timer := time.NewTimer(min(remaining, slice))
		select {
		case <-ctx.Done():
			timer.Stop()
			return topic.FanoutSlab{NextOffset: opts.FromOffset, HighWatermark: hwm}, true, nil
		case <-timer.C:
		}
		if _, reopened := e.logs.Peek(topicName, partition); reopened {
			return topic.FanoutSlab{}, false, nil
		}
	}
	return topic.FanoutSlab{NextOffset: opts.FromOffset, HighWatermark: hwm}, true, nil
}

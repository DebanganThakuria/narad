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
// partition, starting at fromOffset (or the current committed tail
// when fromOffset is topic.FanoutTailOffset), up to maxRecords /
// maxBytes of payload. When nothing is committed at fromOffset yet and
// wait > 0, it long-polls the partition's notify broadcast up to wait.
//
// Offsets below the oldest retained offset are reported as
// DroppedBehind; permanently unreadable records inside the retained
// range are skipped and reported as SkippedCorrupt. NextOffset always
// advances past everything returned, dropped, or skipped — the caller
// persists it only after the records are durably committed downstream.
func (e *Engine) ReadFanoutSlab(ctx context.Context, topicName string, partition int, fromOffset int64, maxRecords int, maxBytes int64, wait time.Duration) (topic.FanoutSlab, error) {
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
	log, err := e.logs.Get(topicName, partition)
	if err != nil {
		return topic.FanoutSlab{}, err
	}
	if maxRecords <= 0 {
		maxRecords = 1
	}

	deadline := time.Now().Add(wait)
	for {
		// Snapshot the notify channel BEFORE probing so a commit that
		// lands between the probe and the wait still wakes us.
		var notify <-chan struct{}
		if wait > 0 {
			notify = log.NotifyC()
		}

		slab, err := e.readFanoutSlabOnce(log, fromOffset, maxRecords, maxBytes)
		if err != nil {
			return topic.FanoutSlab{}, err
		}
		if len(slab.Records) > 0 || slab.DroppedBehind > 0 || slab.SkippedCorrupt > 0 {
			return slab, nil
		}
		remaining := time.Until(deadline)
		if wait <= 0 || remaining <= 0 {
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

// fanoutLog is the slice of *storage.Log the slab read uses; narrowed
// so drop-behind arithmetic is unit-testable without forcing real
// segment retention.
type fanoutLog interface {
	HighWatermark() int64
	OldestOffset() int64
	ReadKeyed(offset int64) (string, []byte, error)
}

// readFanoutSlabOnce performs one non-blocking slab read.
func (e *Engine) readFanoutSlabOnce(log fanoutLog, fromOffset int64, maxRecords int, maxBytes int64) (topic.FanoutSlab, error) {
	hwm := log.HighWatermark()
	oldest := log.OldestOffset()

	slab := topic.FanoutSlab{OldestOffset: oldest, HighWatermark: hwm}
	start := fromOffset
	if start == topic.FanoutTailOffset {
		// A fresh cursor starts at the committed tail: no backfill.
		slab.NextOffset = hwm
		return slab, nil
	}
	if start < oldest {
		// The requested range aged out of retention (drop-behind):
		// skip to the oldest record still available.
		slab.DroppedBehind = min(oldest, hwm) - start
		start = oldest
	}
	slab.NextOffset = start
	if start >= hwm {
		slab.NextOffset = max(start, hwm)
		return slab, nil
	}

	var bytes int64
	offset := start
	for offset < hwm && len(slab.Records) < maxRecords && bytes < maxBytes {
		key, payload, err := log.ReadKeyed(offset)
		switch {
		case err == nil:
			slab.Records = append(slab.Records, topic.KeyedRecord{Key: key, Payload: payload})
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

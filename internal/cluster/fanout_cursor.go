package cluster

// The per-(child, parentPartition) fan-out cursor loop: read a large
// slab of committed parent records (fill-or-linger), re-key each with
// the child's partitioner, commit per-child-partition batches (local
// or one RPC to the owner), and only then advance the persisted
// offset. Commit-before-advance makes delivery at-least-once: a crash
// mid-flight re-commits the last slab as duplicates, never loses it.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func (r *FanoutRunner) runCursor(ctx context.Context, key fanoutCursorKey) {
	partitionDir := storage.TopicPartitionDir(r.dataDir, key.parent, key.partition)

	next := topic.FanoutTailOffset
	if cur, ok, err := storage.ReadFanoutCursor(partitionDir, key.child); err != nil {
		r.logger.Warn("fanout: read cursor; starting at parent tail",
			"parent", key.parent, "partition", key.partition, "child", key.child, "err", err)
	} else if ok && cur.Epoch == key.epoch {
		next = cur.NextOffset
	}
	if next == topic.FanoutTailOffset {
		// Fresh attachment: fan out from the parent's current committed
		// tail (no backfill), and persist that starting point BEFORE
		// fanning anything so a crash cannot re-anchor at a later tail
		// and silently skip the window in between.
		slab, err := r.broker.ReadFanoutSlab(ctx, key.parent, key.partition, topic.FanoutTailOffset, 1, 1, 0)
		if err != nil {
			r.cursorReadError(key, err)
			return
		}
		next = slab.NextOffset
		if !r.persistCursor(key, partitionDir, next) {
			return
		}
	}

	r.logger.Info("fanout cursor started",
		"parent", key.parent, "partition", key.partition, "child", key.child, "from_offset", next)

	for ctx.Err() == nil {
		batch, batchBytes, newNext, hwm, dropped, err := r.readBatch(ctx, key, next)
		if err != nil {
			if !r.cursorReadRetryable(key, err) {
				return
			}
			if !sleepCtx(ctx, defaultFanoutRetryBackoff) {
				return
			}
			continue
		}
		// Dropped/skipped offsets are recorded only once the cursor has
		// durably advanced past them (below); recording before a failed
		// commit would re-count the same range on every retry.
		if len(batch) == 0 {
			// Nothing to fan out, but the cursor may still advance past
			// a dropped/skipped range; persist so a restart doesn't
			// re-count the same loss.
			if newNext != next {
				next = newNext
				if !r.persistCursor(key, partitionDir, next) {
					return
				}
				if dropped > 0 {
					r.recordDropped(key, dropped)
				}
			}
			r.recordLag(key, hwm-next)
			continue
		}

		if !r.commitBatch(ctx, key, batch) {
			// Commit could not complete: back off and re-read from the
			// unadvanced cursor. Already-committed buckets of this slab
			// will re-commit — the at-least-once duplicate path.
			if !sleepCtx(ctx, defaultFanoutRetryBackoff) {
				return
			}
			continue
		}

		next = newNext
		if !r.persistCursor(key, partitionDir, next) {
			return
		}
		if dropped > 0 {
			r.recordDropped(key, dropped)
		}
		if r.metrics != nil {
			r.metrics.FanoutCommittedTotal.WithLabelValues(key.parent, key.child).Add(float64(len(batch)))
			r.metrics.FanoutBatchRecords.Observe(float64(len(batch)))
			r.metrics.FanoutBatchBytes.Observe(float64(batchBytes))
		}
		r.recordLag(key, hwm-next)
	}
}

// readBatch reads one fill-or-linger batch starting at next: an
// initial long-polled slab, topped up until the batch fills or the
// linger deadline fires. Returns the records, their payload bytes, the
// cursor position after them, the parent HWM observed, and how many
// offsets were lost (aged out or unreadable).
func (r *FanoutRunner) readBatch(ctx context.Context, key fanoutCursorKey, next int64) ([]topic.KeyedRecord, int64, int64, int64, int64, error) {
	slab, err := r.broker.ReadFanoutSlab(ctx, key.parent, key.partition, next,
		r.cfg.MaxBatchRecords, r.cfg.MaxBatchBytes, defaultFanoutLongPollWait)
	if err != nil {
		return nil, 0, 0, 0, 0, err
	}
	records := slab.Records
	var bytes int64
	for _, rec := range records {
		bytes += int64(len(rec.Key) + len(rec.Payload))
	}
	cursor := slab.NextOffset
	hwm := slab.HighWatermark
	dropped := slab.DroppedBehind + slab.SkippedCorrupt

	if len(records) > 0 && r.cfg.Linger > 0 {
		deadline := time.Now().Add(r.cfg.Linger)
		for ctx.Err() == nil && len(records) < r.cfg.MaxBatchRecords && bytes < r.cfg.MaxBatchBytes {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			more, err := r.broker.ReadFanoutSlab(ctx, key.parent, key.partition, cursor,
				r.cfg.MaxBatchRecords-len(records), r.cfg.MaxBatchBytes-bytes, remaining)
			if err != nil {
				// The initial slab is intact; fan it out and surface the
				// error on the next read.
				break
			}
			if len(more.Records) == 0 && more.DroppedBehind == 0 && more.SkippedCorrupt == 0 {
				break // linger expired with nothing new
			}
			records = append(records, more.Records...)
			for _, rec := range more.Records {
				bytes += int64(len(rec.Key) + len(rec.Payload))
			}
			cursor = more.NextOffset
			hwm = more.HighWatermark
			dropped += more.DroppedBehind + more.SkippedCorrupt
		}
	}
	return records, bytes, cursor, hwm, dropped, nil
}

// commitBatch re-keys the batch with the child's partitioner and
// commits one batch per touched child partition. Returns false when
// the batch could not be fully committed — the caller re-reads and
// retries; the cursor never advances past an uncommitted record.
func (r *FanoutRunner) commitBatch(ctx context.Context, key fanoutCursorKey, batch []topic.KeyedRecord) bool {
	child, err := r.store.GetTopic(ctx, key.child)
	if err != nil || !child.IsChild() || child.Parent != key.parent || child.AttachEpoch != key.epoch {
		// The link dissolved (or the child is gone) mid-batch: drop the
		// batch and let the reconciler stop this cursor.
		return false
	}

	now := time.Now().UnixMilli()
	buckets := map[int][]ingress.ProduceRecord{}
	for _, rec := range batch {
		p := r.partitioner.Pick(key.child, rec.Key, child.Partitions)
		buckets[p] = append(buckets[p], ingress.ProduceRecord{
			Topic:           key.child,
			Key:             rec.Key,
			TargetPartition: p,
			Payload:         rec.Payload,
			CreatedAtUnixMs: now,
		})
	}
	for p, records := range buckets {
		if !r.commitBucket(ctx, key, p, records) {
			return false
		}
	}
	return true
}

// commitBucket commits one child-partition batch, retrying transient
// failures a few times before giving the slab back to the read loop.
// Fan-out never reroutes to a sibling partition — that would break
// per-key ordering — so a dead child-partition owner stalls only this
// cursor until drop-behind resolves it.
func (r *FanoutRunner) commitBucket(ctx context.Context, key fanoutCursorKey, childPartition int, records []ingress.ProduceRecord) bool {
	const quickRetries = 3
	for attempt := 1; ctx.Err() == nil; attempt++ {
		err := r.commitBucketOnce(ctx, key.child, childPartition, records)
		if err == nil {
			return true
		}
		r.logger.Warn("fanout: child batch commit failed",
			"parent", key.parent, "parent_partition", key.partition,
			"child", key.child, "child_partition", childPartition,
			"records", len(records), "attempt", attempt, "err", err)
		if attempt >= quickRetries {
			return false
		}
		if !sleepCtx(ctx, time.Duration(attempt)*100*time.Millisecond) {
			return false
		}
	}
	return false
}

func (r *FanoutRunner) commitBucketOnce(ctx context.Context, childTopic string, childPartition int, records []ingress.ProduceRecord) error {
	local, addr, err := r.resolveOwner(childTopic, childPartition)
	if err != nil {
		return err
	}
	if local {
		_, err := r.broker.CommitAcceptedProduceBatch(ctx, records)
		return err
	}
	req := nodewire.CommitProduceBatchRequest{Records: make([]nodewire.CommitProduceRequest, 0, len(records))}
	for _, record := range records {
		req.Records = append(req.Records, nodewire.CommitProduceRequest{
			Topic:           record.Topic,
			Key:             record.Key,
			TargetPartition: record.TargetPartition,
			Payload:         record.Payload,
			CreatedAtUnixMs: record.CreatedAtUnixMs,
		})
	}
	rpcCtx, cancel := context.WithTimeout(ctx, produceCommitRPCTimeout)
	defer cancel()
	res, err := r.peer.CommitProduceBatch(rpcCtx, addr, req)
	if err != nil {
		return err
	}
	if res.Status < http.StatusOK || res.Status >= http.StatusMultipleChoices {
		return fmt.Errorf("fanout: child commit returned status %d", res.Status)
	}
	return nil
}

// resolveOwner locates the child partition's owner: local, or the
// peer address to commit through.
func (r *FanoutRunner) resolveOwner(topicName string, partitionIdx int) (bool, string, error) {
	if r.selfID == "" {
		return true, "", nil
	}
	a, err := r.store.GetAssignment(topicName, partitionIdx)
	if err != nil {
		return false, "", fmt.Errorf("fanout: lookup assignment %s/%d: %w", topicName, partitionIdx, err)
	}
	if a.OwnerID == r.selfID {
		return true, "", nil
	}
	m, err := r.store.GetMember(a.OwnerID)
	if err != nil {
		return false, "", fmt.Errorf("fanout: lookup owner member %q: %w", a.OwnerID, err)
	}
	if m.Status == metastore.MemberDead || m.Addr == "" {
		return false, "", fmt.Errorf("fanout: owner %q of %s/%d is unavailable", a.OwnerID, topicName, partitionIdx)
	}
	return false, m.Addr, nil
}

// persistCursor durably records the cursor position (commit-before-
// advance: call only after the records below next are committed).
// Returns false when the cursor must stop — its parent partition
// directory is gone (topic deleted) or the write failed.
func (r *FanoutRunner) persistCursor(key fanoutCursorKey, partitionDir string, next int64) bool {
	err := storage.WriteFanoutCursorIfPartitionDirExists(partitionDir, key.child,
		storage.FanoutCursor{Epoch: key.epoch, NextOffset: next})
	if err == nil {
		return true
	}
	if errors.Is(err, storage.ErrPartitionDirMissing) {
		r.logger.Info("fanout cursor stopping: parent partition removed",
			"parent", key.parent, "partition", key.partition, "child", key.child)
		return false
	}
	// A stopped cursor is respawned by the reconciler from the last
	// persisted offset; failing to persist only risks duplicates.
	r.logger.Error("fanout: persist cursor",
		"parent", key.parent, "partition", key.partition, "child", key.child, "err", err)
	return false
}

// cursorReadRetryable classifies a slab read error: true means back
// off and retry, false means the cursor should stop (the reconciler
// respawns it if it still belongs here).
func (r *FanoutRunner) cursorReadRetryable(key fanoutCursorKey, err error) bool {
	if errors.Is(err, errs.ErrTopicNotFound) || errors.Is(err, errs.ErrNotFound) ||
		errors.Is(err, errs.ErrNotPartitionOwner) || errors.Is(err, context.Canceled) {
		return false
	}
	r.logger.Warn("fanout: read parent slab",
		"parent", key.parent, "partition", key.partition, "child", key.child, "err", err)
	return true
}

func (r *FanoutRunner) cursorReadError(key fanoutCursorKey, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	r.logger.Warn("fanout: cursor initialization read failed",
		"parent", key.parent, "partition", key.partition, "child", key.child, "err", err)
}

func (r *FanoutRunner) recordDropped(key fanoutCursorKey, dropped int64) {
	r.logger.Warn("fanout: child lost records (drop-behind or unreadable)",
		"parent", key.parent, "partition", key.partition, "child", key.child, "dropped", dropped)
	if r.metrics != nil {
		r.metrics.FanoutChildDroppedMessages.WithLabelValues(key.parent, key.child).Add(float64(dropped))
	}
}

func (r *FanoutRunner) recordLag(key fanoutCursorKey, lag int64) {
	if r.metrics == nil {
		return
	}
	if lag < 0 {
		lag = 0
	}
	r.metrics.FanoutLagMessages.WithLabelValues(key.parent, key.child, fanoutPartitionLabel(key.partition)).Set(float64(lag))
}

// sleepCtx sleeps for d unless ctx is cancelled first; reports whether
// the full sleep elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

package cluster

// The per-(child, parentPartition) fan-out cursor loop: read a large
// slab of committed parent records (fill-or-linger), re-key each with
// the child's partitioner (a keyless record keeps its parent
// partition's index when the partition counts match), commit the
// per-child-partition batches concurrently (local, or one RPC to the
// owner each), and only then advance the persisted offset.
// Commit-before-advance makes delivery at-least-once: a crash
// mid-flight re-commits the last slab as duplicates, never loses it.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/messaging"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func (r *FanoutRunner) runCursor(ctx context.Context, key fanoutCursorKey) {
	partitionDir := storage.TopicPartitionDir(r.dataDir, key.parent, key.partition)
	delayMs := key.delayMs

	next := topic.FanoutTailOffset
	lostByMove := false
	if cur, ok, err := storage.ReadFanoutCursor(partitionDir, key.child); err != nil {
		r.logger.Warn("fanout: read cursor failed; cannot resume",
			"parent", key.parent, "partition", key.partition, "child", key.child, "err", err)
	} else if !ok {
		lostByMove = cursorLostByMove(partitionDir, key)
		r.logger.Warn("fanout: no cursor file; cannot resume (fresh attach, or lost cursor state)",
			"parent", key.parent, "partition", key.partition, "child", key.child, "epoch", key.epoch,
			"link_predates_install", lostByMove)
	} else if cur.Epoch != key.epoch {
		r.logger.Warn("fanout: cursor file epoch mismatch; cannot resume (re-attached link, or stale replica)",
			"parent", key.parent, "partition", key.partition, "child", key.child,
			"file_epoch", cur.Epoch, "epoch", key.epoch)
	} else {
		next = cur.NextOffset
	}
	if next == topic.FanoutTailOffset {
		// Anchoring skips everything before the anchor and OVERWRITES
		// the offset file, so it is destructive on two counts, and this
		// cursor's epoch may have come from a stale replica view. Anchor
		// only on an epoch the leader confirms is the child's live
		// attachment; otherwise defer, leaving the file untouched, and
		// let the reconciler respawn the cursor once the replica catches
		// up.
		if !epochConfirmedByLeader(ctx, r.store, r.peer, r.selfID, key, r.logger) {
			return
		}
		freshAnchor := false
		if key.anchor != topic.FanoutTailOffset {
			// Fresh attachment with a recorded attach point (or a
			// partition newer than the attach, anchor 0): start exactly
			// there, so nothing committed between the attach and this
			// first read is skipped. A lost cursor file lands here too
			// and replays from the attach point: duplicates, never a
			// gap, the at-least-once side of the contract.
			next = key.anchor
			freshAnchor = true
		} else {
			// Attach record without offsets (written before they were
			// recorded): fan out from the parent's current committed
			// tail, the older no-backfill anchor.
			slab, err := r.broker.ReadFanoutSlab(ctx, key.parent, key.partition,
				topic.FanoutReadOpts{FromOffset: topic.FanoutTailOffset, MaxRecords: 1, MaxBytes: 1})
			if err != nil {
				r.cursorReadError(key, err)
				return
			}
			next = slab.NextOffset
			if lostByMove && slab.HighWatermark > 0 {
				// The link existed when a move installed this partition, so
				// the source's cursor file should have arrived with it (the
				// source ran a release that did not ship cursor files, or
				// the file was lost). Tail-anchoring here would skip the
				// child's whole backlog (for a delay child, its entire
				// pending window). Resume from the oldest retained offset
				// instead: duplicates for the child, never a silent gap.
				next = slab.OldestOffset
				r.logger.Warn("fanout: cursor file missing after a partition move for a pre-existing link; resuming from the oldest retained offset instead of the tail (expect duplicates, not loss)",
					"parent", key.parent, "partition", key.partition, "child", key.child,
					"from_offset", next, "tail", slab.HighWatermark)
			}
		}
		// Persist the starting point BEFORE fanning anything so a crash
		// cannot re-anchor later and silently skip the window in
		// between. A recorded anchor may land on a parent partition
		// that has never been produced to and so has no directory yet
		// (a remote owner answered the attach from directory stats
		// without opening the log); the link was just confirmed with
		// the leader, so creating the directory here is safe, whereas
		// refusing would stop the cursor, have the reconciler respawn
		// it, and leave the child never catching up.
		if freshAnchor {
			if err := storage.WriteFanoutCursorCreating(partitionDir, key.child, storage.FanoutCursor{Epoch: key.epoch, NextOffset: next}); err != nil {
				r.logger.Error("fanout: persist first cursor",
					"parent", key.parent, "partition", key.partition, "child", key.child, "err", err)
				return
			}
		} else if !r.persistCursor(key, partitionDir, next) {
			return
		}
	}

	var remoteState *remoteCursor
	if key.remote {
		var forget func()
		remoteState, forget = r.registerRemoteCursor(key)
		defer forget()
	}

	r.logger.Info("fanout cursor started",
		"parent", key.parent, "partition", key.partition, "child", key.child,
		"from_offset", next, "delay_ms", delayMs, "remote", key.remote)

	for ctx.Err() == nil {
		maxRecords, maxBytes := r.cfg.MaxBatchRecords, r.cfg.MaxBatchBytes
		if key.remote {
			var ok bool
			if maxRecords, maxBytes, ok = r.remoteBeforeRead(ctx, key, remoteState, next); !ok {
				return
			}
		}
		batch, batchBytes, newNext, hwm, dropped, blockedUntil, err := r.readBatch(ctx, key, next, delayMs, maxRecords, maxBytes)
		if err != nil {
			if !r.cursorReadRetryable(key, err) {
				return
			}
			if !sleepCtx(ctx, defaultFanoutRetryBackoff) {
				return
			}
			continue
		}
		r.recordDueLag(key, delayMs, batch, blockedUntil)
		// Dropped/skipped offsets are recorded only once the cursor has
		// durably advanced past them (below); recording before a failed
		// commit would re-count the same range on every retry.
		if len(batch) == 0 {
			// Nothing to fan out, but the cursor may still advance past
			// a dropped/skipped range; persist so a restart doesn't
			// re-count the same loss.
			if newNext != next {
				from := next
				next = newNext
				if !r.persistCursor(key, partitionDir, next) {
					return
				}
				if dropped > 0 {
					r.recordDropped(ctx, key, dropped, from, next)
				}
			}
			r.recordLag(key, hwm-next)
			if key.remote {
				r.remoteIdle(ctx, key, remoteState)
				r.remoteAfterCommit(ctx, key, remoteState, next, hwm)
			}
			// Blocked on a record that is not due yet: nothing newer
			// can be due either, so sleep until the head's due time
			// (capped so lag gauges and metadata stay fresh; ctx
			// cancellation wakes the sleep on detach/shutdown).
			if blockedUntil > 0 {
				until := time.Until(time.UnixMilli(blockedUntil + delayMs))
				if until > 0 {
					if !sleepCtx(ctx, min(until, defaultFanoutDueWakeCap)) {
						return
					}
				}
			}
			continue
		}

		switch r.commitSlab(ctx, key, batch) {
		case commitStopped:
			return // stopped mid-commit; the cursor stays at next
		case commitReread:
			// A remote lane could not hold its records across a wait: read
			// the slab again from the unadvanced cursor. The cursor keeps
			// what the target already accepted of it, so only a request
			// cut off in flight is sent again (duplicates, never loss).
			continue
		}

		from := next
		next = newNext
		if !r.persistCursor(key, partitionDir, next) {
			return
		}
		if dropped > 0 {
			r.recordDropped(ctx, key, dropped, from, next)
		}
		if r.metrics != nil {
			r.metrics.FanoutCommittedTotal.WithLabelValues(key.parent, key.child).Add(float64(len(batch)))
			r.metrics.FanoutBatchRecords.Observe(float64(len(batch)))
			r.metrics.FanoutBatchBytes.Observe(float64(batchBytes))
		}
		r.recordLag(key, hwm-next)
		if key.remote {
			r.remoteAfterCommit(ctx, key, remoteState, next, hwm)
		}
	}
}

// remoteAfterCommit refreshes a remote cursor's recovery point once it
// advanced (or found nothing to send): 0 when it reached the high
// watermark, else the age of the next record it has yet to send.
func (r *FanoutRunner) remoteAfterCommit(ctx context.Context, key fanoutCursorKey, cur *remoteCursor, next, hwm int64) {
	if hwm > next {
		r.refreshRemoteLag(ctx, key, cur, next, nil)
	} else {
		var retention int64
		if parent, err := r.store.GetTopic(ctx, key.parent); err == nil {
			retention = parent.RetentionMs
		}
		cur.setLag(0, retention)
	}
	r.publishRemoteState(cur)
}

// cursorLostByMove reports whether the partition directory was installed
// by a move while this exact (child, attach epoch) link already existed:
// the move marker's child snapshot names the link. A cursor file missing
// in that case was lost, not never created, so the cursor must not
// tail-anchor. A link attached after the install (different or absent
// epoch in the marker) is a fresh attach and keeps the no-backfill
// contract.
func cursorLostByMove(partitionDir string, key fanoutCursorKey) bool {
	marker, ok, err := messaging.ReadMoveMarker(partitionDir)
	if err != nil || !ok {
		return false
	}
	epoch, linked := marker.Children[key.child]
	return linked && epoch == key.epoch
}

// readBatch reads one fill-or-linger batch starting at next: an
// initial long-polled slab, topped up until the batch fills or the
// linger deadline fires. For a delay child every read is gated on the
// due cutoff (records committed no later than now - delay). Returns
// the records, their payload bytes, the cursor position after them,
// the parent HWM observed, how many offsets were lost (aged out or
// unreadable), and — when the read stopped at an undue record — that
// record's commit time.
func (r *FanoutRunner) readBatch(ctx context.Context, key fanoutCursorKey, next, delayMs int64, maxRecords int, maxBytes int64) ([]topic.KeyedRecord, int64, int64, int64, int64, int64, error) {
	opts := topic.FanoutReadOpts{
		FromOffset: next,
		MaxRecords: maxRecords,
		MaxBytes:   maxBytes,
		Wait:       defaultFanoutLongPollWait,
	}
	if delayMs > 0 {
		// Clamp so an extreme delay can never yield a non-positive
		// cutoff, which the reader would treat as "no gate".
		opts.MaxCommittedAt = max(time.Now().UnixMilli()-delayMs, 1)
	}
	slab, err := r.broker.ReadFanoutSlab(ctx, key.parent, key.partition, opts)
	if err != nil {
		return nil, 0, 0, 0, 0, 0, err
	}
	records := slab.Records
	var bytes int64
	for _, rec := range records {
		bytes += int64(len(rec.Key) + len(rec.Payload))
	}
	cursor := slab.NextOffset
	hwm := slab.HighWatermark
	dropped := slab.DroppedBehind + slab.SkippedCorrupt
	blockedUntil := slab.BlockedUntilUnixMs

	if len(records) > 0 && blockedUntil == 0 && r.cfg.Linger > 0 {
		deadline := time.Now().Add(r.cfg.Linger)
		for ctx.Err() == nil && len(records) < maxRecords && bytes < maxBytes {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				break
			}
			moreOpts := topic.FanoutReadOpts{
				FromOffset: cursor,
				MaxRecords: maxRecords - len(records),
				MaxBytes:   maxBytes - bytes,
				Wait:       remaining,
			}
			if delayMs > 0 {
				moreOpts.MaxCommittedAt = max(time.Now().UnixMilli()-delayMs, 1)
			}
			more, err := r.broker.ReadFanoutSlab(ctx, key.parent, key.partition, moreOpts)
			if err != nil {
				// The initial slab is intact; fan it out and surface the
				// error on the next read.
				break
			}
			if len(more.Records) == 0 && more.DroppedBehind == 0 && more.SkippedCorrupt == 0 {
				blockedUntil = more.BlockedUntilUnixMs
				break // linger expired (or the due gate closed) with nothing new
			}
			records = append(records, more.Records...)
			for _, rec := range more.Records {
				bytes += int64(len(rec.Key) + len(rec.Payload))
			}
			cursor = more.NextOffset
			hwm = more.HighWatermark
			dropped += more.DroppedBehind + more.SkippedCorrupt
			if more.BlockedUntilUnixMs > 0 {
				blockedUntil = more.BlockedUntilUnixMs
				break
			}
		}
	}
	return records, bytes, cursor, hwm, dropped, blockedUntil, nil
}

// recordDueLag updates the due-lag gauge for delay children: how far
// behind the DUE frontier the cursor is running. Zero when the head of
// the log is not due yet (caught up) or the partition is drained. The
// raw offset-lag gauge stays permanently non-zero for a delay child by
// design; due-lag is the health signal to alert on.
func (r *FanoutRunner) recordDueLag(key fanoutCursorKey, delayMs int64, batch []topic.KeyedRecord, blockedUntil int64) {
	if r.metrics == nil || delayMs <= 0 {
		return
	}
	lagSeconds := 0.0
	if len(batch) > 0 {
		due := batch[0].CommittedAtUnixMs + delayMs
		if behindMs := time.Now().UnixMilli() - due; behindMs > 0 {
			lagSeconds = float64(behindMs) / 1000
		}
	}
	_ = blockedUntil // blocked head ⇒ due frontier not reached ⇒ lag 0
	r.metrics.FanoutDueLagSeconds.WithLabelValues(key.parent, key.child, fanoutPartitionLabel(key.partition)).Set(lagSeconds)
}

// fanoutCommitConcurrency bounds how many child-partition batches of
// one slab commit at once, the produce dispatcher's fan-out
// (defaultProduceDispatchCommitFanout).
const fanoutCommitConcurrency = 16

// fanoutBucket is one child partition's share of a slab, in slab order.
type fanoutBucket struct {
	partition int
	records   []ingress.ProduceRecord
}

// commitOutcome is how a slab commit ended.
type commitOutcome int

const (
	// commitDone: every record is committed; advance the cursor.
	commitDone commitOutcome = iota
	// commitStopped: ctx ended first; the cursor stays where it is.
	commitStopped
	// commitReread: a remote child could not hold the slab's records
	// across a failure (the per-node held budget was full); read the slab
	// again from the unadvanced cursor.
	commitReread
)

// commitBatch is commitSlab for callers that only need to know whether
// every record was committed.
func (r *FanoutRunner) commitBatch(ctx context.Context, key fanoutCursorKey, batch []topic.KeyedRecord) bool {
	return r.commitSlab(ctx, key, batch) == commitDone
}

// commitSlab commits the slab to the child: one batch per touched
// child partition, the partitions concurrently. A partition whose
// commit fails (after commitBucket's quick retries) is retried on its
// own after defaultFanoutRetryBackoff, until it commits. The partitions
// that already committed are never sent again, and the retry works
// from the records in hand rather than a re-read: a re-read from the
// unadvanced cursor can come back longer (linger top-up refills it), so
// it would not line up with what already committed. A remote child's
// slab goes to its target instead (see fanout_remote.go). Returns
// commitDone once every record is committed, commitStopped only if ctx
// ended first, and commitReread for a remote slab that must be read
// again; the cursor never advances past an uncommitted record.
func (r *FanoutRunner) commitSlab(ctx context.Context, key fanoutCursorKey, batch []topic.KeyedRecord) commitOutcome {
	pending := batch
	for {
		var reread bool
		pending, reread = r.commitBatchOnce(ctx, key, pending)
		if reread {
			return commitReread
		}
		if len(pending) == 0 {
			return commitDone
		}
		if !sleepCtx(ctx, defaultFanoutRetryBackoff) {
			return commitStopped
		}
	}
}

// commitBatchOnce makes one pass over records: re-validate the link,
// bucket by child partition under the child's current partition count,
// and commit every bucket. Returns the records whose partition did not
// commit, in slab order (nil when all did). A remote child's records go
// to its target; reread reports a slab that must be read again.
func (r *FanoutRunner) commitBatchOnce(ctx context.Context, key fanoutCursorKey, records []topic.KeyedRecord) (pending []topic.KeyedRecord, reread bool) {
	// The version is read before the record, so a change applied between
	// the two reads leaves the record older than its version, never the
	// other way round: a remote slab then re-reads its stub at once.
	version := r.store.TopicVersion(key.child)
	child, err := r.store.GetTopic(ctx, key.child)
	if err != nil || !child.IsChild() || child.Parent != key.parent || child.AttachEpoch != key.epoch {
		// The link dissolved (or the child is gone) mid-batch: commit
		// nothing and let the reconciler stop this cursor.
		return records, false
	}
	if child.Remote != nil {
		return r.sender().commit(ctx, key, child, version, records)
	}
	keepIndex, ok := r.keylessKeepsIndex(ctx, key, records, child.Partitions)
	if !ok {
		return records, false
	}
	buckets, picks, ok := r.bucketByChildPartition(key, records, child.Partitions, keepIndex)
	if !ok {
		return records, false
	}
	failed := r.commitBuckets(ctx, key, buckets, child.Partitions)
	if failed == nil {
		return nil, false
	}
	return pendingRecords(records, picks, failed), false
}

// keylessKeepsIndex reports whether the keyless records of this slab
// keep their parent partition's index in the child: when the child has
// as many partitions as the parent. That is the replica pattern's shape
// (a child created with parent keeps the inherited count), where
// placement puts child partition p on a different node than parent
// partition p; a keyed record reaches the same index by its key's hash,
// and keeping the index gives a keyless record the same two nodes. It
// also keeps the parent partition's keyless records in their order.
// When the counts differ there is no index to keep and no placement
// promise: keyless records go round-robin over the child's partitions.
// The parent is read only for a slab that holds a keyless record;
// ok=false when that read fails (the cursor retries the slab).
func (r *FanoutRunner) keylessKeepsIndex(ctx context.Context, key fanoutCursorKey, records []topic.KeyedRecord, childPartitions int) (keep, ok bool) {
	keyless := false
	for i := range records {
		if records[i].Key == "" {
			keyless = true
			break
		}
	}
	if !keyless {
		return false, true
	}
	parent, err := r.store.GetTopic(ctx, key.parent)
	if err != nil {
		return false, false
	}
	return parent.Partitions == childPartitions && key.partition < childPartitions, true
}

// bucketByChildPartition re-keys records with the child's partitioner
// into one bucket per touched child partition, each in slab order (a
// key maps to one partition, so its records keep their order). With
// keepIndex, a keyless record goes to the parent partition's index
// instead (see keylessKeepsIndex); otherwise the partitioner places it
// round-robin. The buckets share one backing array; picks[i] is record
// i's partition. ok=false when the partitioner answered outside
// [0, partitions).
func (r *FanoutRunner) bucketByChildPartition(key fanoutCursorKey, records []topic.KeyedRecord, partitions int, keepIndex bool) ([]fanoutBucket, []int, bool) {
	picks := make([]int, len(records))
	counts := make([]int, max(partitions, 0))
	for i, rec := range records {
		p := key.partition
		if rec.Key != "" || !keepIndex {
			p = r.partitioner.Pick(key.child, rec.Key, partitions)
		}
		if p < 0 || p >= partitions {
			r.logger.Error("fanout: partitioner picked a child partition out of range",
				"child", key.child, "partition", p, "partitions", partitions)
			return nil, nil, false
		}
		picks[i] = p
		counts[p]++
	}

	all := make([]ingress.ProduceRecord, len(records))
	pos := make([]int, len(counts))
	buckets := make([]fanoutBucket, 0, min(len(counts), len(records)))
	off := 0
	for p, n := range counts {
		if n == 0 {
			continue
		}
		buckets = append(buckets, fanoutBucket{partition: p, records: all[off : off+n : off+n]})
		pos[p] = off
		off += n
	}
	now := time.Now().UnixMilli()
	for i, rec := range records {
		p := picks[i]
		all[pos[p]] = ingress.ProduceRecord{
			Topic:           key.child,
			Key:             rec.Key,
			TargetPartition: p,
			Payload:         rec.Payload,
			CreatedAtUnixMs: now,
		}
		pos[p]++
	}
	return buckets, picks, true
}

// commitBuckets commits every bucket, up to fanoutCommitConcurrency at
// once, and waits for all of them. The calling goroutine is one of the
// workers, so a slab that touches a single child partition spawns
// nothing. Returns nil when all committed; otherwise failed[p] marks
// each child partition that did not.
func (r *FanoutRunner) commitBuckets(ctx context.Context, key fanoutCursorKey, buckets []fanoutBucket, partitions int) []bool {
	ok := make([]bool, len(buckets))
	var next atomic.Int64
	work := func() {
		for {
			i := int(next.Add(1) - 1)
			if i >= len(buckets) {
				return
			}
			ok[i] = r.commitBucket(ctx, key, buckets[i].partition, buckets[i].records)
		}
	}
	var wg sync.WaitGroup
	for range min(len(buckets), fanoutCommitConcurrency) - 1 {
		wg.Go(work)
	}
	work()
	wg.Wait()
	var failed []bool
	for i, b := range buckets {
		if ok[i] {
			continue
		}
		if failed == nil {
			failed = make([]bool, partitions)
		}
		failed[b.partition] = true
	}
	return failed
}

// pendingRecords returns the records whose child partition failed, in
// slab order, with their payloads copied into one fresh buffer: a retry
// lasts as long as the child owner stays down, and the slab's payloads
// alias the parent log's decoded frames, which they would pin all that
// time.
func pendingRecords(records []topic.KeyedRecord, picks []int, failed []bool) []topic.KeyedRecord {
	n, size := 0, 0
	for i, rec := range records {
		if failed[picks[i]] {
			n++
			size += len(rec.Payload)
		}
	}
	out := make([]topic.KeyedRecord, 0, n)
	arena := make([]byte, 0, size)
	for i, rec := range records {
		if !failed[picks[i]] {
			continue
		}
		start := len(arena)
		arena = append(arena, rec.Payload...)
		rec.Payload = arena[start:len(arena):len(arena)]
		out = append(out, rec)
	}
	return out
}

// commitBucket commits one child-partition batch, retrying transient
// failures a few times before reporting it failed to commitBatch.
// Fan-out never reroutes to a sibling partition (that would break
// per-key ordering), so a dead child-partition owner stalls only this
// cursor, which retries just that partition's records until the owner
// is back.
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
// advance: call only after the records below next are committed), in
// place once the cursor file exists. Returns false when the cursor must
// stop: its parent partition directory is gone (topic deleted) or the
// write failed.
func (r *FanoutRunner) persistCursor(key fanoutCursorKey, partitionDir string, next int64) bool {
	err := storage.AdvanceFanoutCursor(partitionDir, key.child,
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

// recordDropped counts and logs records a cursor passed without
// delivering them (aged out of the parent, or unreadable), between the
// offsets from and to. A remote child's are lost for the other cluster,
// which has no other copy: that is logged at error level with the remote.
func (r *FanoutRunner) recordDropped(ctx context.Context, key fanoutCursorKey, dropped, from, to int64) {
	attrs := []any{"parent", key.parent, "partition", key.partition, "child", key.child,
		"dropped", dropped, "from_offset", from, "to_offset", to}
	if key.remote {
		if r.store != nil {
			if stub, err := r.store.GetTopic(ctx, key.child); err == nil && stub.Remote != nil {
				attrs = append(attrs, "remote", stub.Remote.Name)
			}
		}
		r.logger.Error("fanout: remote child lost records the remote never received (drop-behind: they aged out of the parent before the link sent them)", attrs...)
	} else {
		r.logger.Warn("fanout: child lost records (drop-behind or unreadable)", attrs...)
	}
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

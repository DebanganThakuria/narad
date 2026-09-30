package messaging

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// ErrTopicIncarnationMismatch rejects a commit whose records were
// accepted for another incarnation of the topic than the one this node
// holds under the name: the topic was deleted and recreated while they
// waited in an ingress WAL, or this node's metadata lags the accepting
// node's. Nothing is appended. The rejection is retriable on purpose:
// only the accepting node's dispatcher, with the leader's confirmation,
// may decide that the records belong to a deleted incarnation and drop
// them.
var ErrTopicIncarnationMismatch = errors.New("messaging: records were accepted for another incarnation of the topic")

// CommitAcceptedProduce appends an ingress WAL record to this node's
// partition log and advances the partition high-watermark. It is the
// owner-side visibility step for the WAL-first produce design, and a
// one-record CommitAcceptedProduceBatch.
func (e *Engine) CommitAcceptedProduce(ctx context.Context, record ingress.ProduceRecord) (int64, error) {
	offsets, err := e.CommitAcceptedProduceBatch(ctx, []ingress.ProduceRecord{record})
	if err != nil {
		return 0, err
	}
	return offsets[0], nil
}

// CommitAcceptedProduceBatch commits a batch of ingress WAL records to
// one locally owned topic partition with one append+fsync+verify cycle
// and a single high-watermark advance. Batches that reach the same
// partition while another commit holds its produce lock ride one shared
// cycle (see commitCombined). All records must target the same (topic,
// partition). Returns the assigned offsets in record order.
func (e *Engine) CommitAcceptedProduceBatch(ctx context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	if len(records) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e.logs == nil {
		return nil, unavailableError("partition logs")
	}
	topicName, partition, err := singleBatchTarget(records)
	if err != nil {
		return nil, err
	}

	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return nil, err
	}
	if partition < 0 || partition >= t.Partitions {
		return nil, fmt.Errorf("%w: partition out of range", ErrInvalid)
	}
	// A fast path only: the commit cycle checks the gate again under the
	// produce lock, which is what makes a handoff freeze airtight.
	if !e.isLocalOwner(topicName, partition) || e.isProducePaused(topicName, partition) {
		// A partition frozen for a rebalance handoff rejects commits so
		// nothing lands after the destination captured the final tail;
		// the ingress dispatcher retries and delivers to the new owner.
		return nil, ErrNotPartitionOwner
	}
	incarnation, err := batchIncarnation(topicName, records, t.ID)
	if err != nil {
		return nil, err
	}

	// Records are stored wrapped in the keyed envelope so the produce
	// key and commit time survive the commit (fan-out re-keys parent
	// records with the key, delay children anchor due times to the
	// commit time, and consumers get Message.Key/Timestamp from them).
	// The envelopes are built here, outside the produce lock; the commit
	// cycle raises the time under the lock if another commit stamped a
	// later one first (see commitBatchLocked).
	committedAt := time.Now().UnixMilli()
	payloads := make([][]byte, len(records))
	payloadBytes := 0
	for i, record := range records {
		payloads[i] = storage.EncodeKeyedRecord(record.Key, committedAt, record.Payload)
		payloadBytes += len(record.Payload)
	}

	req := &commitRequest{ctx: ctx, incarnation: incarnation, payloads: payloads, committedAt: committedAt}
	if err := e.commitCombined(topicName, partition, req); err != nil {
		e.recordProduceError(err)
		return nil, err
	}
	offsets := make([]int64, len(records))
	for i := range offsets {
		offsets[i] = req.first + int64(i)
	}
	e.recordProduceCommitted(topicName, partition, len(records), payloadBytes)
	return offsets, nil
}

// commitPayload is the synchronous Produce path's commit: one record
// through the same combined cycle as the WAL-first path, so it passes
// the same gate under the produce lock and gets its commit time from the
// same per-partition clock.
func (e *Engine) commitPayload(ctx context.Context, topicName string, partition int, key string, payload []byte) (int64, error) {
	committedAt := time.Now().UnixMilli()
	req := &commitRequest{
		ctx:         ctx,
		payloads:    [][]byte{storage.EncodeKeyedRecord(key, committedAt, payload)},
		committedAt: committedAt,
	}
	if err := e.commitCombined(topicName, partition, req); err != nil {
		return 0, err
	}
	return req.first, nil
}

// singleBatchTarget validates that every record in the batch is
// well-formed and targets the same (topic, partition), returning that
// target.
func singleBatchTarget(records []ingress.ProduceRecord) (string, int, error) {
	topicName := records[0].Topic
	partition := records[0].TargetPartition
	for _, record := range records {
		if record.Topic == "" {
			return "", 0, fmt.Errorf("%w: topic required", ErrInvalid)
		}
		if record.Topic != topicName || record.TargetPartition != partition {
			return "", 0, fmt.Errorf("%w: accepted produce batch must target one topic partition", ErrInvalid)
		}
		if len(record.Payload) == 0 {
			return "", 0, fmt.Errorf("%w: payload required", ErrInvalid)
		}
	}
	return topicName, partition, nil
}

// batchIncarnation returns the topic incarnation the batch was accepted
// for, after checking it against liveID, the incarnation this node holds
// under the name. A record with no TopicID was accepted before
// incarnations were stamped and is committed by name, as it always was;
// a batch of only such records returns "". Every stamped record must
// carry liveID.
func batchIncarnation(topicName string, records []ingress.ProduceRecord, liveID string) (string, error) {
	var id string
	for i := range records {
		rid := records[i].TopicID
		if rid == "" {
			continue
		}
		if rid != liveID {
			return "", incarnationMismatch(topicName, rid, liveID)
		}
		id = rid
	}
	return id, nil
}

func incarnationMismatch(topicName, recordID, liveID string) error {
	return fmt.Errorf("%w: %s is incarnation %q here, the records were accepted for %q",
		ErrTopicIncarnationMismatch, topicName, liveID, recordID)
}

// Group commit. Every node's dispatcher sends each partition its own
// batch per pass, and the fan-out runner adds its own, so batches for one
// partition often arrive together. The produce lock spans append and
// durable commit (a failed commit discards everything above the
// high-watermark, including another caller's records, so the two can
// never be split), which used to make each of those batches pay its own
// write, fdatasync and read-back, back to back.
//
// Flat combining keeps that lock discipline and shares the cycle
// instead. A caller queues its batch on the partition's combiner. With no
// cycle running it becomes the leader: it takes the produce lock, drains
// every batch queued by then (its own included), appends them as one run
// and commits them with one CommitDurable. Everyone in the cycle shares
// its outcome: offsets on success, the same error on failure, which the
// ingress dispatcher and the fan-out runner already retry by appending
// again. Batches that arrive during a cycle wait for the next one, which
// the leader hands to the oldest of them on its way out, so no caller
// waits behind more than the cycle in progress and one more.

// commitRequest is one caller's batch in a combined commit.
type commitRequest struct {
	ctx context.Context
	// incarnation is the topic ID the records were accepted for, checked
	// again under the produce lock; "" skips the check.
	incarnation string
	// payloads are the encoded envelopes. The log takes ownership of them
	// once appended.
	payloads    [][]byte
	committedAt int64

	// Written by the cycle that commits the request, before its caller is
	// woken.
	first int64
	err   error

	// wake is made only for a caller that finds a cycle running. The
	// leader sends on it once the request is done, or once it has handed
	// the caller the next cycle (lead).
	wake chan struct{}
	lead bool
}

// produceCombiner queues the commits of one partition.
type produceCombiner struct {
	mu    sync.Mutex
	queue []*commitRequest
	// spare is the previous cycle's drained queue, emptied, which the
	// next drain installs as the queue so steady commits reuse two
	// backing arrays instead of allocating one per cycle.
	spare   []*commitRequest
	leading bool

	// lastCommittedAt is the newest commit time stamped into the
	// partition by this process. Read and written only under the
	// partition's produce lock.
	lastCommittedAt int64
}

// partitionKey names one topic partition in the engine's maps.
type partitionKey struct {
	topic     string
	partition int
}

// combinerFor returns the partition's combiner, creating it on first
// use. ForgetTopic drops a retired topic's combiners.
func (e *Engine) combinerFor(topicName string, partition int) *produceCombiner {
	key := partitionKey{topic: topicName, partition: partition}
	e.combineMu.RLock()
	c := e.combiners[key]
	e.combineMu.RUnlock()
	if c != nil {
		return c
	}
	e.combineMu.Lock()
	defer e.combineMu.Unlock()
	if c = e.combiners[key]; c == nil {
		if e.combiners == nil {
			e.combiners = make(map[partitionKey]*produceCombiner)
		}
		c = &produceCombiner{}
		e.combiners[key] = c
	}
	return c
}

// forgetCombiners drops a retired topic's combiners. A cycle still
// running on one finishes on it; a commit that arrives afterwards starts
// a fresh combiner, and the produce lock keeps the two apart. The fresh
// one's commit-time floor starts over, so a batch stamped before the
// forget can land a millisecond or so below one committed just before
// it; only a same-named topic created around the delete can see that.
func (e *Engine) forgetCombiners(topicName string) {
	e.combineMu.Lock()
	for key := range e.combiners {
		if key.topic == topicName {
			delete(e.combiners, key)
		}
	}
	e.combineMu.Unlock()
}

// commitCombined commits req's records in a combined cycle and returns
// the cycle's outcome for them. On success req.first is the offset of
// the first record; the rest follow contiguously.
func (e *Engine) commitCombined(topicName string, partition int, req *commitRequest) error {
	c := e.combinerFor(topicName, partition)
	c.mu.Lock()
	c.queue = append(c.queue, req)
	if c.leading {
		req.wake = make(chan struct{}, 1)
		c.mu.Unlock()
		<-req.wake
		if !req.lead {
			return req.err
		}
	} else {
		c.leading = true
		c.mu.Unlock()
	}

	e.runCommitCycle(topicName, partition, c, req)

	// Hand the next cycle to the oldest caller that queued during this
	// one, or stand down.
	c.mu.Lock()
	if len(c.queue) == 0 {
		c.leading = false
		c.mu.Unlock()
		return req.err
	}
	next := c.queue[0]
	next.lead = true
	c.mu.Unlock()
	next.wake <- struct{}{}
	return req.err
}

// staleLogAttempts bounds how many times one cycle resolves the
// partition's log (see runCommitCycle). Each extra attempt follows a
// change to the topic's record that landed during the cycle, so a second
// one is already rare.
const staleLogAttempts = 3

// runCommitCycle runs one combined cycle under the partition's produce
// lock, then wakes every caller it served except self, the leader.
//
// The cycle writes every batch it drained into the one log it resolved,
// and batches keep queueing until the drain. So a delete plus recreate
// of the topic that the owner applies after the resolution (during a
// lazy open, or while the leader waited for the lock) could send a
// batch of the new incarnation into the old incarnation's log, to be
// acked and then quarantined with it. commitBatchLocked asks whether the
// log is still current after the drain; when it is not, the cycle lets
// go of the lock and resolves the log again, with the same batches, so
// they are checked against the log they will be written to.
func (e *Engine) runCommitCycle(topicName string, partition int, c *produceCombiner, self *commitRequest) {
	var batch []*commitRequest
	var err error
	for attempt := 1; ; attempt++ {
		stale := false
		err = e.logs.WithProduceLockIncarnation(topicName, partition, func(log *storage.Log, incarnation string) error {
			if batch == nil {
				// Drained under the produce lock, so every batch that queued
				// while the leader waited for it rides this cycle.
				batch = c.drain()
			}
			stale = !e.commitBatchLocked(topicName, partition, c, log, incarnation, batch, attempt == staleLogAttempts)
			return nil
		})
		if err != nil || !stale {
			break
		}
	}
	if err != nil {
		// The partition's log could not be resolved, so nothing in the
		// cycle was committed: every queued batch shares that failure.
		if batch == nil {
			batch = c.drain()
		}
		for _, r := range batch {
			r.err = err
		}
	}
	for _, r := range batch {
		if r != self {
			r.wake <- struct{}{}
		}
	}
	// Nobody reads batch any more: the woken callers only read their own
	// request.
	clear(batch)
	c.mu.Lock()
	c.spare = batch[:0]
	c.mu.Unlock()
}

// drain takes everything queued, leaving the spare array as the queue.
func (c *produceCombiner) drain() []*commitRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	batch := c.queue
	c.queue, c.spare = c.spare, nil
	return batch
}

// commitBatchLocked appends and durably commits every request in batch
// that may still be committed, as one run, into log, which was opened
// under the topic incarnation logIncarnation, and records each
// request's outcome. The caller holds the partition's produce lock.
//
// It returns false, having touched no request, when log is no longer
// current (see runCommitCycle) and last is not set; the caller then
// resolves the log again and calls it with the same batch. With last
// set, a stale log refuses the whole batch with ErrNotPartitionOwner,
// which every caller retries.
func (e *Engine) commitBatchLocked(topicName string, partition int, c *produceCombiner, log *storage.Log, logIncarnation string, batch []*commitRequest, last bool) bool {
	// The owner and freeze gate again, now under the produce lock. A
	// handoff arms its freeze and only then takes this lock to read the
	// final high-watermark, so a commit that passed the outer gate before
	// the freeze but reaches the lock after that read is turned away
	// here, with nothing appended, instead of landing after the fence and
	// stranding acknowledged records on the old owner.
	if !e.isLocalOwner(topicName, partition) || e.isProducePaused(topicName, partition) {
		for _, r := range batch {
			r.err = ErrNotPartitionOwner
		}
		return true
	}
	// The log was resolved before the drain, so the topic's record may
	// have changed since: a batch that queued after a delete plus
	// recreate would be checked below against the new incarnation and
	// written into the old one's log. A current log was re-checked
	// against the record as it is now, after every drained batch queued.
	if !e.logs.Current(topicName, partition, log) {
		if !last {
			return false
		}
		for _, r := range batch {
			r.err = ErrNotPartitionOwner
		}
		return true
	}

	live := batch
	if refused := e.refuseUncommittable(topicName, logIncarnation, batch); refused > 0 {
		if refused == len(batch) {
			return true
		}
		live = make([]*commitRequest, 0, len(batch)-refused)
		for _, r := range batch {
			if r.err == nil {
				live = append(live, r)
			}
		}
	}
	if len(live) > 1 {
		// No order holds between different callers' batches, so append
		// them oldest commit time first; that leaves the clamp below
		// nothing to rewrite unless an earlier cycle ran ahead.
		slices.SortStableFunc(live, func(a, b *commitRequest) int { return cmp.Compare(a.committedAt, b.committedAt) })
	}

	// Commit times never go backwards along the partition: a batch
	// stamped before the lock can land after a later-stamped one (it was
	// still encoding, or lost the race for the lock), and the wall clock
	// can step back. The fan-out delay gate stops at the first record not
	// yet due, so an inversion would hold due records back.
	stamp := c.lastCommittedAt
	for _, r := range live {
		if r.committedAt < stamp {
			restampKeyedRecords(r.payloads, stamp)
			r.committedAt = stamp
		}
		stamp = r.committedAt
	}
	c.lastCommittedAt = stamp

	payloads := live[0].payloads
	if len(live) > 1 {
		total := 0
		for _, r := range live {
			total += len(r.payloads)
		}
		payloads = make([][]byte, 0, total)
		for _, r := range live {
			payloads = append(payloads, r.payloads...)
		}
	}
	// One append for the whole cycle: an append is all-or-nothing, so a
	// failure cannot leave one caller's records above the high-watermark
	// for the next commit to expose. The envelopes were built for this
	// commit only and are never read again (commitDurable needs just
	// their count), so the log may take ownership instead of copying.
	first, _, err := log.AppendBatchOwned(payloads)
	if err != nil {
		err = produceStageError{stage: produceStageAppend, err: err}
	} else {
		err = e.commitDurable(log, first, len(payloads))
	}
	if err != nil {
		// A failed commit discarded the whole run, so every caller in it
		// retries by appending again.
		for _, r := range live {
			r.err = err
		}
		return true
	}
	next := first
	for _, r := range live {
		r.first = next
		next += int64(len(r.payloads))
	}
	return true
}

// refuseUncommittable records an error on every request in batch that
// may no longer be committed and returns how many it refused. A caller
// that gave up (its RPC timed out, say) is not appended: it retries
// anyway, and appending it now would only commit a duplicate. A batch
// accepted for another incarnation than the one this node now holds, or
// than the one the log it would be written to belongs to
// (logIncarnation), is turned away with ErrTopicIncarnationMismatch.
func (e *Engine) refuseUncommittable(topicName, logIncarnation string, batch []*commitRequest) int {
	refused := 0
	liveID, resolved := "", false
	var lookupErr error
	for _, r := range batch {
		if err := r.ctx.Err(); err != nil {
			r.err = err
			refused++
			continue
		}
		if r.incarnation == "" {
			continue
		}
		if !resolved {
			// Cached and versioned: the metastore is read only when the
			// topic's record changed.
			t, err := e.getTopic(context.Background(), topicName)
			liveID, lookupErr, resolved = t.ID, err, true
		}
		switch {
		case lookupErr != nil:
			r.err = lookupErr
			refused++
		case r.incarnation != liveID:
			r.err = incarnationMismatch(topicName, r.incarnation, liveID)
			refused++
		case r.incarnation != logIncarnation:
			// The live record moved on after the log was checked (the
			// check and this lookup are not one read): the batch belongs
			// to a log this cycle does not hold. Retriable, and the next
			// cycle resolves the log again.
			r.err = incarnationMismatch(topicName, r.incarnation, logIncarnation)
			refused++
		}
	}
	return refused
}

// restampKeyedRecords rewrites the commit time of keyed envelopes built
// by storage.EncodeKeyedRecord in this file, in place: the 8 bytes after
// the version byte, the uvarint key length and the key.
func restampKeyedRecords(payloads [][]byte, committedAt int64) {
	for _, env := range payloads {
		keyLen, n := binary.Uvarint(env[1:])
		at := 1 + n + int(keyLen)
		binary.BigEndian.PutUint64(env[at:at+8], uint64(committedAt))
	}
}

// commitDurable is the no-follower durability boundary. Narad has no
// replicas, so before a record is made visible (and before the ingress
// WAL is allowed to compact past it) the owner's partition log must be
// the proven-durable, uncorrupted copy. storage.CommitDurable, on the
// partition's single flusher goroutine:
//
//  1. writes and fdatasyncs the partition log,
//  2. reads each frame back so its on-disk CRC is validated over the
//     stored bytes (guards against a torn or corrupt write; no decode,
//     because decoding per record was the cause of the commit-throughput
//     collapse),
//  3. advances the high-watermark to make the records visible. Before
//     an open log's first advance it empties the hwm file once (the file
//     holds a boundary only while the log is closed), so a crash after
//     the ingress WAL checkpoints past this batch recovers the boundary
//     from the fsynced tail and can never leave the records
//     durable-but-hidden.
//
// A failed CommitDurable leaves nothing of the batch in the log: the
// storage layer discards every record above the high-watermark and
// rewinds its next offset, so the ingress dispatcher's retry (which
// appends the same WAL records again) lands exactly one copy at the
// offsets the failed batch had. This is what makes "retry by
// re-appending" safe; the caller must never try to re-commit the old
// offsets instead.
//
// firstOffset is the offset of the first record; the count records are
// contiguous. The caller must hold the partition produce lock.
func (e *Engine) commitDurable(log *storage.Log, firstOffset int64, count int) error {
	if count <= 0 {
		return nil
	}
	lastOffset := firstOffset + int64(count) - 1
	if err := log.CommitDurable(firstOffset, lastOffset); err != nil {
		if verr, ok := errors.AsType[storage.VerifyError](err); ok {
			return produceStageError{stage: produceStageVerify, err: verr}
		}
		return produceStageError{stage: produceStageCommit, err: err}
	}
	return nil
}

// recordProduceCommitted bumps the produced counters for a committed
// batch: one label resolution per batch, not per record. Every record in
// a commit batch targets the same (topic, partition), see
// singleBatchTarget.
func (e *Engine) recordProduceCommitted(topicName string, partition, count, payloadBytes int) {
	if e.metrics == nil || count <= 0 {
		return
	}
	pc := e.metrics.PartitionCounters(topicName, partition)
	pc.MessagesProduced.Add(float64(count))
	pc.BytesProduced.Add(float64(payloadBytes))
}

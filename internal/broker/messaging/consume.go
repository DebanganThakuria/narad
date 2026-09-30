package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// Consume returns the next available message for a topic, supporting
// queue-mode pull, partition-pinned pull, replay-by-offset, and HTTP
// long-polling. See ConsumeOpts for the full semantics.
func (e *Engine) Consume(ctx context.Context, topicName string, opts ConsumeOpts) (topic.Message, bool, error) {
	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return topic.Message{}, false, err
	}

	if opts.Offset != nil && opts.Partition == nil {
		return topic.Message{}, false, ErrPartitionRequired
	}
	if opts.Offset != nil {
		return e.consumeReplay(topicName, *opts.Partition, *opts.Offset, t.Partitions)
	}

	scan, err := e.localProbePartitions(topicName, t.Partitions, opts.Partition)
	if err != nil {
		return topic.Message{}, false, err
	}

	visibilityTimeout := time.Duration(t.VisibilityTimeoutMs) * time.Millisecond
	scanStart := e.consumeScanStart(topicName, scan, opts)
	start := time.Now()

	msg, found, err := e.tryQueueRead(ctx, topicName, scan, scanStart, visibilityTimeout)
	if err != nil {
		if e.metrics != nil {
			e.metrics.IncError("messaging", "consume")
		}
		return msg, false, err
	}
	if found {
		e.recordConsumed(topicName, msg.Partition, len(msg.Payload))
		e.recordConsumeWait(topicName, "hit", time.Since(start))
		return msg, true, nil
	}
	if opts.Wait <= 0 {
		e.recordConsumeEmpty(topicName, "no_wait", time.Since(start))
		return topic.Message{}, false, nil
	}
	// Park on the topic's waiter queue rather than scanning again: the
	// dispatcher hands this consumer a record when one becomes
	// reservable. Same path the split ConsumeProbe/ConsumeWait pair
	// uses, so a forwarded consume behaves exactly like a local one.
	msg, found, _, err = e.ConsumeWait(ctx, &ConsumeWaiter{
		topic:             topicName,
		scan:              scan,
		pinned:            opts.Partition,
		scanStart:         scanStart,
		visibilityTimeout: visibilityTimeout,
		start:             start,
	}, opts.Wait, nil)
	return msg, found, err
}

// ConsumeWaiter is the state a ConsumeProbe leaves behind so a later
// ConsumeWait can park without re-scanning. There is no wake-up channel
// snapshot to get wrong any more: the dispatcher notices new data
// through the log's wake notifier and hands a record to whichever
// waiter is at the head of the topic's queue.
type ConsumeWaiter struct {
	topic string
	// scan is the partition list the probe scanned. The pump does not
	// reuse it (see readForWaiter); it only seeds the dispatcher's
	// estimate of what this node owns.
	scan []int
	// pinned is the one partition a partition-pinned consume may take
	// from; nil for a queue-style consume over every owned partition.
	// The dispatcher keeps pinned waiters on their own per-partition
	// FIFO, because one of them at the head of the shared queue would
	// stall every waiter behind it whenever its partition was empty.
	pinned            *int
	scanStart         int
	visibilityTimeout time.Duration
	start             time.Time
}

// readForWaiter is the pump's reservation attempt for one parked
// consumer. The partitions to scan are resolved afresh rather than
// taken from the probe that parked it: a waiter can sit for its whole
// wait, and in that time this node may have gained a partition of the
// topic, which the probe's list would never look at, or lost one, which
// a lazy log open on the probe's list would recreate as an empty
// directory on a node that no longer owns it. ErrNotPartitionOwner and
// ErrTopicNotFound mean nothing here can serve the waiter any more.
func (e *Engine) readForWaiter(w *ConsumeWaiter) (topic.Message, bool, error) {
	ctx := context.Background()
	t, err := e.getTopic(ctx, w.topic)
	if err != nil {
		return topic.Message{}, false, err
	}
	scan, err := e.localProbePartitions(w.topic, t.Partitions, w.pinned)
	if err != nil {
		return topic.Message{}, false, err
	}
	visibility := time.Duration(t.VisibilityTimeoutMs) * time.Millisecond
	return e.tryQueueRead(ctx, w.topic, scan, w.scanStart, visibility)
}

// ConsumeProbe is the non-blocking half of a queue-style Consume: it
// scans the locally owned partitions once and returns the first
// reservable message. When nothing is available it also returns a
// waiter for ConsumeWait. Replay and pinned-partition options are not
// supported here (use Consume).
func (e *Engine) ConsumeProbe(ctx context.Context, topicName string, opts ConsumeOpts) (topic.Message, bool, *ConsumeWaiter, error) {
	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return topic.Message{}, false, nil, err
	}
	if opts.Offset != nil || opts.Partition != nil {
		return topic.Message{}, false, nil, fmt.Errorf("%w: probe supports queue-style consumes only", ErrInvalid)
	}
	scan, err := e.localProbePartitions(topicName, t.Partitions, nil)
	if err != nil {
		return topic.Message{}, false, nil, err
	}
	scanStart := e.consumeScanStart(topicName, scan, opts)
	visibilityTimeout := time.Duration(t.VisibilityTimeoutMs) * time.Millisecond
	start := time.Now()
	msg, found, err := e.tryQueueRead(ctx, topicName, scan, scanStart, visibilityTimeout)
	if err != nil {
		if e.metrics != nil {
			e.metrics.IncError("messaging", "consume")
		}
		return msg, false, nil, err
	}
	if found {
		e.recordConsumed(topicName, msg.Partition, len(msg.Payload))
		e.recordConsumeWait(topicName, "hit", time.Since(start))
		return msg, true, nil, nil
	}
	// Built only on a miss: a hit never parks, so it has no use for one.
	return topic.Message{}, false, &ConsumeWaiter{
		topic:             topicName,
		scan:              scan,
		scanStart:         scanStart,
		visibilityTimeout: visibilityTimeout,
		start:             start,
	}, nil
}

// ConsumeWait is the blocking half. It does not scan and it does not
// race: the caller is enqueued on the topic's waiter FIFO and parks on
// one channel until the dispatcher's pump hands it a record, the wait
// expires, or ctx ends.
//
// The pump reserves only once it has taken a waiter off that queue, so
// a consumer that gives up can still find a record was handed to it in
// the meantime. That record is reserved, so it is released here rather
// than left invisible until its visibility timeout.
func (e *Engine) ConsumeWait(ctx context.Context, w *ConsumeWaiter, wait time.Duration, external <-chan struct{}) (topic.Message, bool, bool, error) {
	if w == nil {
		return topic.Message{}, false, false, fmt.Errorf("%w: nil consume waiter", ErrInvalid)
	}
	if wait <= 0 {
		e.recordConsumeEmpty(w.topic, "no_wait", time.Since(w.start))
		return topic.Message{}, false, false, nil
	}

	pw := &waiter{cw: w, ch: make(chan waiterDelivery, 1)}
	e.dispatch.enqueue(w.topic, pw)

	timer := time.NewTimer(wait)
	defer timer.Stop()

	var outcome string
	select {
	case dl, ok := <-pw.ch:
		if !ok {
			// The dispatcher shut down under us.
			e.recordConsumeEmpty(w.topic, "cancelled", time.Since(w.start))
			return topic.Message{}, false, false, nil
		}
		e.recordConsumeWait(w.topic, "hit", time.Since(w.start))
		return dl.msg, true, false, nil
	case <-external:
		// The cross-node half has something. Retire this waiter first so
		// the pump cannot hand it a local record we are no longer going
		// to read, then let the caller go and claim.
		if dl, handed := e.dispatch.dequeue(w.topic, pw); handed {
			// A local record arrived in the same instant. Serving it is
			// better than claiming remotely, and it saves a round trip.
			e.recordConsumeWait(w.topic, "hit", time.Since(w.start))
			return dl.msg, true, false, nil
		}
		return topic.Message{}, false, true, nil
	case <-timer.C:
		outcome = "timeout"
	case <-ctx.Done():
		outcome = "cancelled"
	}

	if dl, handed := e.dispatch.dequeue(w.topic, pw); handed {
		if outcome == "timeout" {
			// It arrived in the instant the budget ran out. The caller is
			// still there and asked for a message, so serving it beats
			// throwing it away.
			e.recordConsumeWait(w.topic, "hit", time.Since(w.start))
			return dl.msg, true, false, nil
		}
		// The client is gone, so nobody can receive this. It is reserved,
		// so give it back now instead of leaving it invisible for a full
		// visibility timeout.
		e.releaseUndelivered(w.topic, dl.msg)
	}
	e.recordConsumeEmpty(w.topic, outcome, time.Since(w.start))
	return topic.Message{}, false, false, nil
}

// RegisterRemoteDemand records a peer's standing interest in a topic:
// its token. The peer joins the same queue as local consumers, but it
// is only ever *told* that records may be available and claims them
// itself with an ordinary consume, so nothing is reserved on its
// behalf and there is no give-back if it never returns.
//
// A token is one turn. The cluster layer keeps exactly one live token
// per (peer, topic) and replaces it on re-registration, so a peer with
// several consumers parked on the topic registers again after each
// turn it spends; see cluster.Router.RouteConsumeWait.
func (e *Engine) RegisterRemoteDemand(ctx context.Context, topicName string, rd RemoteDemand) error {
	if rd == nil {
		return fmt.Errorf("%w: nil remote demand", ErrInvalid)
	}
	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return err
	}
	scan, err := e.localProbePartitions(topicName, t.Partitions, nil)
	if err != nil {
		return err
	}
	if len(scan) == 0 {
		// This node owns nothing of the topic, so it can never serve the
		// interest. Refusing beats holding a token that cannot be spent.
		return ErrNotPartitionOwner
	}
	e.dispatch.registerRemote(topicName, scan, rd)
	return nil
}

// ReleaseTopicWaiters wakes every consumer parked on topicName with an
// empty answer and drops the topic's dispatch state. The topic manager
// calls it when a topic is deleted, so a long poll on a deleted topic
// returns at once rather than at its wait's end.
func (e *Engine) ReleaseTopicWaiters(topicName string) {
	e.dispatch.releaseTopic(topicName)
}

// NoteRemoteClaim tells the dispatcher that a peer's claim for the topic
// has arrived, so the notification it answers is resolved now rather
// than at its deadline. The cluster layer calls it on every local-only
// consume it serves for a peer.
func (e *Engine) NoteRemoteClaim(topicName string) {
	e.dispatch.claimArrived(topicName)
}

// DropRemoteDemand removes a peer's interest ahead of its expiry, for a
// connection that died or a peer that said it no longer wants the
// topic.
func (e *Engine) DropRemoteDemand(topicName string, rd RemoteDemand) {
	if rd != nil {
		e.dispatch.dropRemote(topicName, rd)
	}
}

// releaseUndelivered gives back a record the dispatcher reserved for a
// consumer that vanished before it could be handed over. Best effort: a
// handle that is already stale means something else resolved it, which
// is the outcome we wanted anyway.
func (e *Engine) releaseUndelivered(topicName string, msg topic.Message) {
	h, err := consumer.DecodeHandle(msg.ReceiptHandle)
	if err != nil {
		return
	}
	if err := e.offsets.ReleaseHandle(topicName, h.Partition, h.Offset, h.Nonce); err != nil &&
		!errors.Is(err, consumer.ErrHandleStale) {
		e.logger.Warn("release record reserved for a consumer that left",
			"topic", topicName, "partition", h.Partition, "offset", h.Offset, "err", err)
	}
}

// consumeScanStart picks where a queue scan begins: the requested
// partition when it is in the scan, else the rotating per-topic cursor.
func (e *Engine) consumeScanStart(topicName string, scan []int, opts ConsumeOpts) int {
	if opts.ScanStart != nil {
		for i, p := range scan {
			if p == *opts.ScanStart {
				return i
			}
		}
	}
	return e.nextConsumeScanStart(topicName, len(scan))
}

// consumeReplay serves an offset-pinned Consume: an ownership check
// followed by a direct read at (partition, offset). Replay never
// reserves — the returned message carries no receipt handle.
func (e *Engine) consumeReplay(topicName string, partitionIdx int, offset int64, totalPartitions int) (topic.Message, bool, error) {
	if partitionIdx < 0 || partitionIdx >= totalPartitions {
		return topic.Message{}, false, fmt.Errorf("%w: partition out of range", ErrInvalid)
	}

	if !e.isLocalOwner(topicName, partitionIdx) {
		return topic.Message{}, false, ErrNotPartitionOwner
	}

	// Not counted as consumed: a replay reserves nothing and settles
	// nothing, and `narad sub --peek` is documented as invisible to the
	// production consumers whose drain rate the counter reports.
	return e.replayRead(topicName, partitionIdx, offset, totalPartitions)
}

// recordConsumed bumps the per-partition delivered counters.
func (e *Engine) recordConsumed(topicName string, partition, payloadBytes int) {
	if e.metrics == nil {
		return
	}
	// Pre-bound children: one cache lookup instead of two label-hash
	// resolutions and an Itoa per delivered message.
	pc := e.metrics.PartitionCounters(topicName, partition)
	pc.MessagesConsumed.Inc()
	pc.BytesConsumed.Add(float64(payloadBytes))
}

// recordConsumeWait observes the long-poll histogram for a hit
// outcome (a message was returned).
func (e *Engine) recordConsumeWait(topicName, outcome string, dur time.Duration) {
	if e.metrics == nil {
		return
	}
	e.metrics.ConsumeWaitSeconds.WithLabelValues(topicName, outcome).Observe(dur.Seconds())
}

// recordConsumeEmpty observes the histogram for a no-message outcome
// (timeout, cancellation, or wait<=0 with empty queue) and increments
// the empty-consume counter.
func (e *Engine) recordConsumeEmpty(topicName, outcome string, dur time.Duration) {
	if e.metrics == nil {
		return
	}
	e.metrics.ConsumeWaitSeconds.WithLabelValues(topicName, outcome).Observe(dur.Seconds())
	e.metrics.ConsumeEmptyTotal.WithLabelValues(topicName).Inc()
}

// replayRead serves a Consume request that pinned an exact (partition,
// offset). Returns (msg, false, nil) when the offset is past the log
// tail — same "no message yet" signal as queue mode.
func (e *Engine) replayRead(topicName string, partitionIdx int, offset int64, totalPartitions int) (topic.Message, bool, error) {
	if partitionIdx < 0 || partitionIdx >= totalPartitions {
		return topic.Message{}, false, fmt.Errorf("%w: partition out of range", ErrInvalid)
	}
	if offset < 0 {
		return topic.Message{}, false, fmt.Errorf("%w: offset must be >= 0", ErrInvalid)
	}
	log, err := e.logs.Get(topicName, partitionIdx)
	if err != nil {
		return topic.Message{}, false, err
	}
	if offset >= log.HighWatermark() {
		return topic.Message{}, false, nil
	}
	if offset < log.OldestOffset() {
		// Retention already reaped this offset: it existed and is gone —
		// a fact about the request, not a server fault.
		return topic.Message{}, false, fmt.Errorf("%w: offset %d aged out of retention (oldest retained: %d)", errs.ErrHandleStale, offset, log.OldestOffset())
	}
	key, committedAt, payload, err := log.ReadKeyedShared(offset)
	if err != nil {
		if storage.IsCorrupt(err) || errors.Is(err, storage.ErrOffsetNotFound) {
			// Same fate for a hole the reaper (or corruption skip) left:
			// not readable, never will be, and not our 500 to own.
			return topic.Message{}, false, fmt.Errorf("%w: offset %d is not readable", errs.ErrHandleStale, offset)
		}
		return topic.Message{}, false, err
	}
	return topic.Message{
		Topic:     topicName,
		Partition: partitionIdx,
		Offset:    offset,
		Key:       key,
		Payload:   payload,
		Timestamp: committedAt / 1000,
	}, true, nil
}

// tryQueueRead scans the given partitions in order and returns the
// first message whose offset can be reserved (i.e. not currently
// in-flight with another consumer and within the partition's
// MaxInFlight cap). It does not block — callers handle long-polling.
//
// Reservation marks the offset invisible for visibilityTimeout. The
// returned message carries a receipt handle the consumer must echo on
// Ack — the broker rejects acks for offsets the consumer did not
// reserve, and acks whose visibility window has elapsed.
func (e *Engine) tryQueueRead(ctx context.Context, topicName string, partitions []int, scanStart int, visibilityTimeout time.Duration) (topic.Message, bool, error) {
	if len(partitions) == 0 {
		return topic.Message{}, false, nil
	}
	// The first partition is resolved on its own, so a consume that hits
	// there opens and stamps nothing else; the rest are resolved only
	// once it has nothing (tryQueueReadRest). The reservation loop is
	// written out here and again there rather than shared through a
	// function: the extra call cost a single-partition consume and a hit
	// on the first partition 1.5 to 2.5%. The log is resolved before the
	// pause check, so a paused partition's log is still opened and kept
	// warm; a paused partition is handing off, and the new owner serves
	// it in a moment.
	pos := scanStart % len(partitions)
	idx := partitions[pos]
	log, err := e.logs.Get(topicName, idx)
	if err != nil {
		return topic.Message{}, false, err
	}
	if !e.isConsumePaused(topicName, idx) {
		for {
			res, err := e.offsets.ReserveNext(ctx, topicName, idx, visibilityTimeout, log.HighWatermark())
			if err != nil {
				return topic.Message{}, false, err
			}
			if !res.Reserved {
				break // partition empty, fully reserved, or in-flight cap hit: try the next one
			}
			key, committedAt, payload, err := log.ReadKeyedShared(res.Offset)
			if err != nil {
				if err := e.resolveUnreadable(topicName, idx, log, res, err); err != nil {
					return topic.Message{}, false, err
				}
				continue
			}
			return topic.Message{
				Topic:     topicName,
				Partition: idx,
				Offset:    res.Offset,
				Key:       key,
				Payload:   payload,
				Timestamp: committedAt / 1000,
				ReceiptHandle: consumer.EncodeHandle(consumer.Handle{
					Partition: idx,
					Offset:    res.Offset,
					Nonce:     res.Nonce,
				}),
			}, true, nil
		}
	}
	if len(partitions) == 1 {
		return topic.Message{}, false, nil
	}
	return e.tryQueueReadRest(ctx, topicName, partitions, pos, visibilityTimeout)
}

// tryQueueReadRest is tryQueueRead past a first partition that had
// nothing: the rest of the scan, in order from the partition after
// partitions[first]. Their logs are resolved in one Logs.GetMany, which
// finds the open ones under one read lock, one topic-version read and
// one clock read, where a Get each paid for those per partition. It is
// a function of its own so that the buffer GetMany fills is set up on a
// miss only, never on a hit on the first partition.
//
// A topic with more local partitions than the buffer holds, or a
// GetMany that fails, resolves each log as the scan reaches it, as
// before: a partition that cannot be opened then fails the scan only
// once the scan gets to it, so it does not hide a record on a partition
// before it.
func (e *Engine) tryQueueReadRest(ctx context.Context, topicName string, partitions []int, first int, visibilityTimeout time.Duration) (topic.Message, bool, error) {
	var buf [16]*storage.Log
	var logs []*storage.Log
	if len(partitions) <= len(buf) {
		logs, _ = e.logs.GetMany(topicName, partitions, buf[:0])
	}
	for i := 1; i < len(partitions); i++ {
		pos := (first + i) % len(partitions)
		idx := partitions[pos]
		var log *storage.Log
		if logs != nil {
			log = logs[pos]
		} else {
			var err error
			if log, err = e.logs.Get(topicName, idx); err != nil {
				return topic.Message{}, false, err
			}
		}
		if e.isConsumePaused(topicName, idx) {
			continue // handing off: the new owner serves it in a moment
		}
		for {
			res, err := e.offsets.ReserveNext(ctx, topicName, idx, visibilityTimeout, log.HighWatermark())
			if err != nil {
				return topic.Message{}, false, err
			}
			if !res.Reserved {
				break // partition empty, fully reserved, or in-flight cap hit: try the next one
			}
			key, committedAt, payload, err := log.ReadKeyedShared(res.Offset)
			if err != nil {
				if err := e.resolveUnreadable(topicName, idx, log, res, err); err != nil {
					return topic.Message{}, false, err
				}
				continue
			}
			return topic.Message{
				Topic:     topicName,
				Partition: idx,
				Offset:    res.Offset,
				Key:       key,
				Payload:   payload,
				Timestamp: committedAt / 1000,
				ReceiptHandle: consumer.EncodeHandle(consumer.Handle{
					Partition: idx,
					Offset:    res.Offset,
					Nonce:     res.Nonce,
				}),
			}, true, nil
		}
	}
	return topic.Message{}, false, nil
}

// resolveUnreadable handles a record the scan reserved (res) but could
// not read (readErr). nil means it was skipped as recorded loss, or
// someone else resolved it, and the scan reserves again on the same
// partition; an error ends the scan with it.
func (e *Engine) resolveUnreadable(topicName string, idx int, log *storage.Log, res consumer.ReserveResult, readErr error) error {
	// A frontier that fell behind retention (the segment holding
	// the reserved offset was reaped) would otherwise be walked
	// one poison offset per round trip, each with a log line and
	// a frontier persist. Jump it to the oldest retained offset
	// in one step; the loss is already visible on the consumer
	// lag/dropped gauges.
	if errors.Is(readErr, storage.ErrOffsetNotFound) {
		if oldest := log.OldestOffset(); res.Offset < oldest {
			skipped, serr := e.offsets.SkipMissingBelow(topicName, idx, res.Offset, res.Nonce, oldest)
			if errors.Is(serr, consumer.ErrHandleStale) {
				// A concurrent consume skipped the same gap first, and
				// dropped this reservation with everything else below
				// oldest. The gap is resolved: reserve again, from at
				// or above oldest. Returning the error answered a
				// plain consume, which never held a handle, with 410.
				return nil
			}
			if serr != nil {
				return serr
			}
			e.logger.Warn("consumer frontier fell behind retention; skipped to oldest retained offset",
				"topic", topicName, "partition", idx, "from", res.Offset, "to", oldest, "skipped", skipped)
			return nil
		}
	}
	// A permanently-unreadable (corrupt) frame, or a gap left by a
	// corrupt frame recovery skipped, would otherwise head-of-line-block
	// this partition forever (the offset can never be acked). Skip past
	// it (recorded loss, never silent) and retry the SAME partition:
	// the record after the skipped frame may be immediately deliverable,
	// and moving on could stall a long-poll for the full Wait even
	// though data is available here.
	if storage.IsCorrupt(readErr) || errors.Is(readErr, storage.ErrOffsetNotFound) {
		serr := e.offsets.SkipCorrupt(topicName, idx, res.Offset, res.Nonce)
		if errors.Is(serr, consumer.ErrHandleStale) {
			// The reservation lapsed or was dropped while the read
			// failed, so the offset is no longer this consume's to
			// resolve. Reserve again rather than fail a consume that
			// never held a handle.
			return nil
		}
		if serr != nil {
			return serr
		}
		if e.metrics != nil {
			e.metrics.IncCorruptSkipped(topicName, idx)
		}
		e.logger.Warn("skipped permanently-unreadable record",
			"topic", topicName, "partition", idx, "offset", res.Offset, "err", readErr)
		return nil
	}
	// Transient error (I/O, log closed): give the reservation
	// back right away. Leaving it in place would hide the message
	// for the full visibility timeout after a 500 the consumer
	// will retry immediately.
	if rerr := e.offsets.ReleaseHandle(topicName, idx, res.Offset, res.Nonce); rerr != nil && !errors.Is(rerr, consumer.ErrHandleStale) {
		e.logger.Warn("release reservation after read error", "topic", topicName, "partition", idx, "offset", res.Offset, "err", rerr)
	}
	return readErr
}

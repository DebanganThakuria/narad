package messaging

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	return e.consumeQueue(ctx, topicName, scan, scanStart, visibilityTimeout, opts.Wait, nil)
}

// ConsumeWaiter is the state a ConsumeProbe leaves behind so a later
// ConsumeWait can park on exactly the wake-up channels that were
// snapshotted BEFORE the probe. That ordering is what makes the
// handler's ladder (probe locally, ask remote owners, then wait) free
// of lost wake-ups without probing the local partitions a second time.
type ConsumeWaiter struct {
	topic             string
	scan              []int
	scanStart         int
	visibilityTimeout time.Duration
	chans             []<-chan struct{}
	start             time.Time
}

// ConsumeProbe is the non-blocking half of a queue-style Consume: it
// snapshots the wake-up channels, scans the locally owned partitions
// once, and returns the first reservable message. When nothing is
// available it also returns a waiter for ConsumeWait. Replay and
// pinned-partition options are not supported here (use Consume).
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
	w := &ConsumeWaiter{
		topic:             topicName,
		scan:              scan,
		scanStart:         e.consumeScanStart(topicName, scan, opts),
		visibilityTimeout: time.Duration(t.VisibilityTimeoutMs) * time.Millisecond,
		start:             time.Now(),
	}
	logs, err := e.partitionLogs(topicName, scan)
	if err != nil {
		return topic.Message{}, false, nil, err
	}
	w.chans = notifyChannelsFor(logs)
	msg, found, err := e.tryQueueReadLogs(ctx, topicName, scan, logs, w.scanStart, w.visibilityTimeout)
	if err != nil {
		if e.metrics != nil {
			e.metrics.IncError("messaging", "consume")
		}
		return msg, false, nil, err
	}
	if found {
		e.recordConsumed(topicName, msg.Partition, len(msg.Payload))
		e.recordConsumeWait(topicName, "hit", time.Since(w.start))
		return msg, true, nil, nil
	}
	return topic.Message{}, false, w, nil
}

// ConsumeWait is the blocking half: it parks on the waiter's channel
// snapshot for up to wait, then continues as a normal long-poll
// (re-probe, re-snapshot, park) until a message arrives or the wait is
// spent. Activity that happened after the probe (a commit, an expiry, a
// nack) closed a snapshotted channel and returns immediately.
func (e *Engine) ConsumeWait(ctx context.Context, w *ConsumeWaiter, wait time.Duration) (topic.Message, bool, error) {
	if w == nil {
		return topic.Message{}, false, fmt.Errorf("%w: nil consume waiter", ErrInvalid)
	}
	if wait <= 0 {
		e.recordConsumeEmpty(w.topic, "no_wait", time.Since(w.start))
		return topic.Message{}, false, nil
	}
	return e.consumeQueue(ctx, w.topic, w.scan, w.scanStart, w.visibilityTimeout, wait, w)
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

	msg, found, err := e.replayRead(topicName, partitionIdx, offset, totalPartitions)
	if found {
		e.recordConsumed(topicName, msg.Partition, len(msg.Payload))
	}
	return msg, found, err
}

// consumeQueue serves a queue-mode Consume over the given partitions:
// probe for a reservable message, and if none is available long-poll
// up to wait for partition activity before probing again.
//
// When pending is non-nil the first iteration skips the probe and parks
// on pending's channel snapshot (a ConsumeProbe already probed after
// taking it); the deadline is measured from now either way.
func (e *Engine) consumeQueue(ctx context.Context, topicName string, scan []int, scanStart int, visibilityTimeout, wait time.Duration, pending *ConsumeWaiter) (topic.Message, bool, error) {
	start := time.Now()
	if pending != nil {
		start = pending.start
	}
	deadline := time.Now().Add(wait)
	for {
		if pending != nil {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				e.recordConsumeEmpty(topicName, "timeout", time.Since(start))
				return topic.Message{}, false, nil
			}
			chans := pending.chans
			pending = nil
			if err := e.waitForActivity(ctx, chans, remaining); err != nil {
				return e.consumeWaitError(topicName, start, err)
			}
			continue
		}
		// Fetch the notify channels BEFORE probing for data. The
		// channels are close-and-replace broadcasts, so a snapshot
		// taken after an empty probe could miss a wake-up that fired
		// in between; fetching first guarantees any post-probe
		// activity closes a channel we are (about to be) waiting on.
		// Re-fetched every iteration because each broadcast installs a
		// fresh channel.
		//
		// The partition logs are resolved once per iteration and shared by
		// the notify snapshot and the probe (each Logs.Get is a map lookup
		// under a process-wide lock plus a key allocation); re-resolved
		// every iteration so a reopened log after CloseTopic is picked up.
		logs, err := e.partitionLogs(topicName, scan)
		if err != nil {
			return topic.Message{}, false, err
		}
		var notifyChans []<-chan struct{}
		if wait > 0 {
			notifyChans = notifyChannelsFor(logs)
		}

		msg, found, err := e.tryQueueReadLogs(ctx, topicName, scan, logs, scanStart, visibilityTimeout)
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

		if wait <= 0 {
			e.recordConsumeEmpty(topicName, "no_wait", time.Since(start))
			return topic.Message{}, false, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			e.recordConsumeEmpty(topicName, "timeout", time.Since(start))
			return topic.Message{}, false, nil
		}
		if err := e.waitForActivity(ctx, notifyChans, remaining); err != nil {
			return e.consumeWaitError(topicName, start, err)
		}
	}
}

// consumeWaitError maps a waitForActivity failure to the consume result:
// a timeout or cancellation is an empty consume, anything else an error.
func (e *Engine) consumeWaitError(topicName string, start time.Time, err error) (topic.Message, bool, error) {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		outcome := "timeout"
		if errors.Is(err, context.Canceled) {
			outcome = "cancelled"
		}
		e.recordConsumeEmpty(topicName, outcome, time.Since(start))
		return topic.Message{}, false, nil
	}
	if e.metrics != nil {
		e.metrics.IncError("messaging", "consume_wait")
	}
	return topic.Message{}, false, err
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
	logs, err := e.partitionLogs(topicName, partitions)
	if err != nil {
		return topic.Message{}, false, err
	}
	return e.tryQueueReadLogs(ctx, topicName, partitions, logs, scanStart, visibilityTimeout)
}

// partitionLogs resolves the log of every partition in the scan, in scan
// order. One Logs.Get per partition per consume iteration, shared by the
// notify snapshot and the probe.
func (e *Engine) partitionLogs(topicName string, partitions []int) ([]*storage.Log, error) {
	logs := make([]*storage.Log, len(partitions))
	for i, idx := range partitions {
		log, err := e.logs.Get(topicName, idx)
		if err != nil {
			return nil, err
		}
		logs[i] = log
	}
	return logs, nil
}

// tryQueueReadLogs is tryQueueRead over pre-resolved logs (logs[i] is
// the log of partitions[i]).
func (e *Engine) tryQueueReadLogs(ctx context.Context, topicName string, partitions []int, logs []*storage.Log, scanStart int, visibilityTimeout time.Duration) (topic.Message, bool, error) {
	for i := range partitions {
		pos := (scanStart + i) % len(partitions)
		idx := partitions[pos]
		log := logs[pos]
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
				// A frontier that fell behind retention (the segment holding
				// the reserved offset was reaped) would otherwise be walked
				// one poison offset per round trip, each with a log line and
				// a frontier persist. Jump it to the oldest retained offset
				// in one step; the loss is already visible on the consumer
				// lag/dropped gauges.
				if errors.Is(err, storage.ErrOffsetNotFound) {
					if oldest := log.OldestOffset(); res.Offset < oldest {
						skipped, serr := e.offsets.SkipMissingBelow(topicName, idx, res.Offset, res.Nonce, oldest)
						if serr != nil {
							return topic.Message{}, false, serr
						}
						e.logger.Warn("consumer frontier fell behind retention; skipped to oldest retained offset",
							"topic", topicName, "partition", idx, "from", res.Offset, "to", oldest, "skipped", skipped)
						continue
					}
				}
				// A permanently-unreadable (corrupt) frame, or a gap left by a
				// corrupt frame recovery skipped, would otherwise head-of-line-block
				// this partition forever (the offset can never be acked). Skip past
				// it (recorded loss, never silent) and retry the SAME partition:
				// the record after the skipped frame may be immediately deliverable,
				// and moving on could stall a long-poll for the full Wait even
				// though data is available here.
				if storage.IsCorrupt(err) || errors.Is(err, storage.ErrOffsetNotFound) {
					if serr := e.offsets.SkipCorrupt(topicName, idx, res.Offset, res.Nonce); serr != nil {
						return topic.Message{}, false, serr
					}
					if e.metrics != nil {
						e.metrics.IncCorruptSkipped(topicName, idx)
					}
					e.logger.Warn("skipped permanently-unreadable record",
						"topic", topicName, "partition", idx, "offset", res.Offset, "err", err)
					continue
				}
				// Transient error (I/O, log closed): give the reservation
				// back right away. Leaving it in place would hide the message
				// for the full visibility timeout after a 500 the consumer
				// will retry immediately.
				if rerr := e.offsets.ReleaseHandle(topicName, idx, res.Offset, res.Nonce); rerr != nil && !errors.Is(rerr, consumer.ErrHandleStale) {
					e.logger.Warn("release reservation after read error", "topic", topicName, "partition", idx, "offset", res.Offset, "err", rerr)
				}
				return topic.Message{}, false, err
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

// notifyChannels snapshots the partitions' current broadcast notify
// channels. The snapshot must be taken before probing for data (see
// consumeQueue) and re-taken before every wait, because each broadcast
// closes the channel and installs a fresh one.
func (e *Engine) notifyChannels(topicName string, partitions []int) ([]<-chan struct{}, error) {
	logs, err := e.partitionLogs(topicName, partitions)
	if err != nil {
		return nil, err
	}
	return notifyChannelsFor(logs), nil
}

// notifyChannelsFor is notifyChannels over pre-resolved logs.
func notifyChannelsFor(logs []*storage.Log) []<-chan struct{} {
	chans := make([]<-chan struct{}, len(logs))
	for i, log := range logs {
		chans[i] = log.NotifyC()
	}
	return chans
}

// waitForActivity blocks until any of the given broadcast channels is
// closed, the timeout elapses, or ctx is cancelled. The channels are
// closed (never sent on) to wake ALL waiters at once — a single commit
// can make records available to many blocked long-pollers.
func (e *Engine) waitForActivity(ctx context.Context, notifyChans []<-chan struct{}, timeout time.Duration) error {
	cases := make([]reflect.SelectCase, 0, len(notifyChans)+2)
	for _, ch := range notifyChans {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		})
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	cases = append(cases,
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
		reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(timer.C)},
	)

	chosen, _, _ := reflect.Select(cases)
	switch chosen {
	case len(cases) - 2: // ctx
		return ctx.Err()
	case len(cases) - 1: // timer
		return context.DeadlineExceeded
	default:
		return nil
	}
}

package messaging

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// Consume returns the next available message for a topic, supporting
// queue-mode pull, partition-pinned pull, replay-by-offset, and HTTP
// long-polling. See ConsumeOpts for the full semantics.
func (e *Engine) Consume(ctx context.Context, topicName string, opts ConsumeOpts) (topic.Message, bool, error) {
	totalStart := time.Now()
	totalOutcome := "empty"
	defer func() {
		e.observe("consume", "total", totalOutcome, time.Since(totalStart))
	}()

	stageStart := time.Now()
	t, err := e.getTopic(ctx, topicName)
	e.observe("consume", "get_topic", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return topic.Message{}, false, err
	}

	if opts.Offset != nil && opts.Partition == nil {
		totalOutcome = "error"
		return topic.Message{}, false, ErrPartitionRequired
	}

	if opts.Offset != nil {
		if *opts.Partition < 0 || *opts.Partition >= t.Partitions {
			totalOutcome = "error"
			return topic.Message{}, false, fmt.Errorf("%w: partition out of range", ErrInvalid)
		}
		stageStart = time.Now()
		if !e.isLocalOwner(topicName, *opts.Partition) {
			e.observe("consume", "owner_check", "remote", time.Since(stageStart))
			totalOutcome = "error"
			return topic.Message{}, false, ErrNotPartitionOwner
		}
		e.observe("consume", "owner_check", "local", time.Since(stageStart))
		stageStart = time.Now()
		msg, found, err := e.replayRead(topicName, *opts.Partition, *opts.Offset, t.Partitions)
		e.observe("consume", "replay_read", consumeStageOutcome(found, err), time.Since(stageStart))
		if found {
			e.recordConsumed(topicName, msg.Partition, len(msg.Payload))
			totalOutcome = "hit"
		}
		if err != nil {
			totalOutcome = "error"
		}
		return msg, found, err
	}

	stageStart = time.Now()
	scan, err := e.localProbePartitions(topicName, t.Partitions, opts.Partition)
	e.observe("consume", "local_partitions", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return topic.Message{}, false, err
	}

	visibilityTimeout := time.Duration(t.VisibilityTimeoutMs) * time.Millisecond

	start := time.Now()
	deadline := start.Add(opts.Wait)
	for {
		stageStart = time.Now()
		msg, found, err := e.tryQueueRead(ctx, topicName, scan, visibilityTimeout)
		e.observe("consume", "try_queue_read", consumeStageOutcome(found, err), time.Since(stageStart))
		if err != nil {
			if e.metrics != nil {
				e.metrics.IncError("messaging", "consume")
			}
			totalOutcome = "error"
			return msg, false, err
		}
		if found {
			e.recordConsumed(topicName, msg.Partition, len(msg.Payload))
			e.recordConsumeWait(topicName, "hit", time.Since(start))
			totalOutcome = "hit"
			return msg, true, nil
		}

		if opts.Wait <= 0 {
			e.recordConsumeEmpty(topicName, "no_wait", time.Since(start))
			totalOutcome = "empty"
			return topic.Message{}, false, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			e.recordConsumeEmpty(topicName, "timeout", time.Since(start))
			totalOutcome = "timeout"
			return topic.Message{}, false, nil
		}
		stageStart = time.Now()
		if err := e.waitForActivity(ctx, topicName, scan, remaining); err != nil {
			e.observe("consume", "wait_for_activity", waitOutcome(err), time.Since(stageStart))
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				outcome := "timeout"
				if errors.Is(err, context.Canceled) {
					outcome = "cancelled"
				}
				e.recordConsumeEmpty(topicName, outcome, time.Since(start))
				totalOutcome = outcome
				return topic.Message{}, false, nil
			}
			if e.metrics != nil {
				e.metrics.IncError("messaging", "consume_wait")
			}
			totalOutcome = "error"
			return topic.Message{}, false, err
		}
		e.observe("consume", "wait_for_activity", "wakeup", time.Since(stageStart))
	}
}

// recordConsumed bumps the per-partition delivered counters.
func (e *Engine) recordConsumed(topicName string, partition, payloadBytes int) {
	if e.metrics == nil {
		return
	}
	partLabel := strconv.Itoa(partition)
	e.metrics.MessagesConsumedTotal.WithLabelValues(topicName, partLabel).Inc()
	e.metrics.BytesConsumedTotal.WithLabelValues(topicName, partLabel).Add(float64(payloadBytes))
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
	log, err := e.logs.Get(topicName, partitionIdx)
	if err != nil {
		return topic.Message{}, false, err
	}
	if offset >= log.HighWatermark() {
		return topic.Message{}, false, nil
	}
	payload, err := log.Read(offset)
	if err != nil {
		return topic.Message{}, false, err
	}
	return topic.Message{
		Topic:     topicName,
		Partition: partitionIdx,
		Offset:    offset,
		Payload:   payload,
		Timestamp: time.Now().Unix(),
	}, true, nil
}

// tryQueueRead scans the given partitions in order and returns the
// first message whose offset can be reserved (i.e. not currently
// in-flight with another consumer and within the partition's
// MaxInFlight cap). It does not block — callers handle long-polling.
//
// Reservation marks the offset invisible for visibilityTimeout. The
// returned message carries an HMAC-signed receipt handle the consumer
// must echo on Ack — the broker rejects acks for offsets the consumer
// did not reserve, and acks whose visibility window has elapsed.
func (e *Engine) tryQueueRead(ctx context.Context, topicName string, partitions []int, visibilityTimeout time.Duration) (topic.Message, bool, error) {
	for _, idx := range partitions {
		stageStart := time.Now()
		log, err := e.logs.Get(topicName, idx)
		e.observe("consume", "log_open", observeOutcome(err), time.Since(stageStart))
		if err != nil {
			return topic.Message{}, false, err
		}
		stageStart = time.Now()
		res, err := e.offsets.ReserveNext(ctx, topicName, idx, visibilityTimeout, log.HighWatermark())
		e.observe("consume", "reserve_next", reserveOutcome(res, err), time.Since(stageStart))
		if err != nil {
			return topic.Message{}, false, err
		}
		if !res.Reserved {
			if e.metrics != nil {
				e.metrics.IncReserveSkipped(topicName, res.SkipReason)
			}
			continue // partition empty, fully reserved, or in-flight cap hit
		}
		stageStart = time.Now()
		payload, err := log.Read(res.Offset)
		e.observe("consume", "storage_read", observeOutcome(err), time.Since(stageStart))
		if err != nil {
			// A permanently-unreadable (corrupt) frame, or a gap left by a
			// corrupt frame recovery skipped, would otherwise head-of-line-block
			// this partition forever (the offset can never be acked). Skip past
			// it — recorded loss, never silent — and resume on the next poll.
			// Transient errors (I/O, log closed) are returned for retry.
			if storage.IsCorrupt(err) || errors.Is(err, storage.ErrOffsetNotFound) {
				if serr := e.offsets.SkipCorrupt(topicName, idx, res.Offset, res.Nonce); serr != nil {
					return topic.Message{}, false, serr
				}
				e.metrics.IncCorruptSkipped(topicName, idx)
				e.logger.Warn("skipped permanently-unreadable record",
					"topic", topicName, "partition", idx, "offset", res.Offset, "err", err)
				continue
			}
			return topic.Message{}, false, err
		}
		return topic.Message{
			Topic:     topicName,
			Partition: idx,
			Offset:    res.Offset,
			Payload:   payload,
			Timestamp: time.Now().Unix(),
			ReceiptHandle: consumer.EncodeHandle(consumer.Handle{
				Topic:     topicName,
				Partition: idx,
				Offset:    res.Offset,
				Nonce:     res.Nonce,
			}),
		}, true, nil
	}
	return topic.Message{}, false, nil
}

// waitForActivity blocks until any of the partitions' notify channels
// fires, the timeout elapses, or ctx is cancelled.
func (e *Engine) waitForActivity(ctx context.Context, topicName string, partitions []int, timeout time.Duration) error {
	cases := make([]reflect.SelectCase, 0, len(partitions)+2)
	for _, idx := range partitions {
		log, err := e.logs.Get(topicName, idx)
		if err != nil {
			return err
		}
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(log.NotifyC()),
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

// allPartitions returns [0, n).
func allPartitions(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

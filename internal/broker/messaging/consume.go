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
		msg, found, err := e.replayRead(topicName, *opts.Partition, *opts.Offset, t.Partitions)
		if found {
			e.recordConsumed(topicName, msg.Partition, len(msg.Payload))
		}
		return msg, found, err
	}

	scan := allPartitions(t.Partitions)
	if opts.Partition != nil {
		if *opts.Partition < 0 || *opts.Partition >= t.Partitions {
			return topic.Message{}, false, fmt.Errorf("%w: partition out of range", ErrInvalid)
		}
		scan = []int{*opts.Partition}
	}

	visibilityTimeout := time.Duration(t.VisibilityTimeoutMs) * time.Millisecond

	start := time.Now()
	deadline := start.Add(opts.Wait)
	for {
		msg, found, err := e.tryQueueRead(ctx, topicName, scan, visibilityTimeout)
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

		remaining := time.Until(deadline)
		if remaining <= 0 {
			e.recordConsumeEmpty(topicName, "timeout", time.Since(start))
			return topic.Message{}, false, nil
		}
		if err := e.waitForActivity(ctx, topicName, scan, remaining); err != nil {
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
	if offset >= log.NextOffset() {
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
		log, err := e.logs.Get(topicName, idx)
		if err != nil {
			return topic.Message{}, false, err
		}
		res, err := e.offsets.ReserveNext(ctx, topicName, idx, visibilityTimeout, log.NextOffset())
		if err != nil {
			return topic.Message{}, false, err
		}
		if !res.Reserved {
			e.metrics.IncReserveSkipped(topicName, res.SkipReason)
			continue // partition empty, fully reserved, or in-flight cap hit
		}
		payload, err := log.Read(res.Offset)
		if err != nil {
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

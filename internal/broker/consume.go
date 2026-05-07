package broker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/debanganthakuria/narad/internal/topic"
)

// Consume returns the next available message for a topic, supporting
// queue-mode pull, partition-pinned pull, replay-by-offset, and HTTP
// long-polling. See ConsumeOpts for the full semantics.
func (b *impl) Consume(ctx context.Context, topicName string, opts ConsumeOpts) (topic.Message, bool, error) {
	t, err := b.GetTopic(ctx, topicName)
	if err != nil {
		return topic.Message{}, false, err
	}

	if opts.Offset != nil && opts.Partition == nil {
		return topic.Message{}, false, ErrPartitionRequired
	}

	if opts.Offset != nil {
		return b.replayRead(topicName, *opts.Partition, *opts.Offset, t.Partitions)
	}

	scan := allPartitions(t.Partitions)
	if opts.Partition != nil {
		if *opts.Partition < 0 || *opts.Partition >= t.Partitions {
			return topic.Message{}, false, fmt.Errorf("%w: partition out of range", ErrInvalidArgument)
		}
		scan = []int{*opts.Partition}
	}

	deadline := time.Now().Add(opts.Wait)
	for {
		msg, found, err := b.tryQueueRead(ctx, topicName, scan)
		if err != nil || found {
			return msg, found, err
		}

		if opts.Wait <= 0 {
			return topic.Message{}, false, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return topic.Message{}, false, nil
		}
		if err := b.waitForActivity(ctx, topicName, scan, remaining); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return topic.Message{}, false, nil
			}
			return topic.Message{}, false, err
		}
	}
}

// replayRead serves a Consume request that pinned an exact (partition,
// offset). Returns (msg, false, nil) when the offset is past the log
// tail — same "no message yet" signal as queue mode.
func (b *impl) replayRead(topicName string, partitionIdx int, offset int64, totalPartitions int) (topic.Message, bool, error) {
	if partitionIdx < 0 || partitionIdx >= totalPartitions {
		return topic.Message{}, false, fmt.Errorf("%w: partition out of range", ErrInvalidArgument)
	}
	log, err := b.partitionLog(topicName, partitionIdx)
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
		Timestamp: time.Now().UTC(),
	}, true, nil
}

// tryQueueRead scans the given partitions in order and returns the first
// message available beyond each partition's committed offset. It does not
// block — callers handle long-polling.
func (b *impl) tryQueueRead(ctx context.Context, topicName string, partitions []int) (topic.Message, bool, error) {
	for _, idx := range partitions {
		log, err := b.partitionLog(topicName, idx)
		if err != nil {
			return topic.Message{}, false, err
		}
		next, err := b.deps.Offsets.Next(ctx, topicName, idx)
		if err != nil {
			return topic.Message{}, false, err
		}
		if next >= log.NextOffset() {
			continue // partition empty / fully consumed
		}
		payload, err := log.Read(next)
		if err != nil {
			return topic.Message{}, false, err
		}
		return topic.Message{
			Topic:     topicName,
			Partition: idx,
			Offset:    next,
			Payload:   payload,
			Timestamp: time.Now().UTC(),
		}, true, nil
	}
	return topic.Message{}, false, nil
}

// waitForActivity blocks until any of the partitions' notify channels
// fires, the timeout elapses, or ctx is cancelled.
func (b *impl) waitForActivity(ctx context.Context, topicName string, partitions []int, timeout time.Duration) error {
	cases := make([]reflect.SelectCase, 0, len(partitions)+2)
	for _, idx := range partitions {
		log, err := b.partitionLog(topicName, idx)
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

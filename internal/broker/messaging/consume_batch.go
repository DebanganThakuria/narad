package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// ConsumeBatch is the non-blocking batch form of a queue-style or
// partition-pinned consume: one scan of the locally owned partitions
// (just opts.Partition when it is set) that reserves up to max records
// and appends them to dst. Each record is reserved exactly as a single
// consume reserves it, with its own receipt handle and its own
// visibility window, and counts against its partition's in-flight cap.
//
// It never waits. With nothing reservable it returns no messages and a
// waiter to park on with ConsumeWait, as ConsumeProbe does; the caller
// can top up the one record a wait delivers with another ConsumeBatch.
//
// opts.MaxBytes, when set, ends the batch early once the records taken
// carry that many key and payload bytes.
//
// The scan starts where a single consume's would (opts.ScanStart, else
// the rotating cursor) and stays on a partition until it runs dry or
// reaches its in-flight cap, then moves on, so a batch takes a
// partition's backlog in offset order rather than one record from each.
//
// A failure after at least one record was reserved ends the batch with
// what it has and no error: those records are reserved, and failing the
// call would hide them from every consumer until their leases lapsed.
// The next consume meets the failure again and reports it.
func (e *Engine) ConsumeBatch(ctx context.Context, topicName string, opts ConsumeOpts, max int, dst []topic.Message) ([]topic.Message, *ConsumeWaiter, error) {
	if max <= 0 {
		return dst, nil, fmt.Errorf("%w: batch size must be positive", ErrInvalid)
	}
	if opts.Offset != nil {
		return dst, nil, fmt.Errorf("%w: a replay reads one record; batch consume is queue-style or partition-pinned", ErrInvalid)
	}
	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return dst, nil, err
	}
	scan, err := e.localProbePartitions(topicName, t.Partitions, opts.Partition)
	if err != nil {
		return dst, nil, err
	}
	visibilityTimeout := time.Duration(t.VisibilityTimeoutMs) * time.Millisecond
	scanStart := e.consumeScanStart(topicName, scan, opts)
	start := time.Now()

	pos, got, size := scanStart, 0, 0
	for got < max && (opts.MaxBytes <= 0 || size < opts.MaxBytes) {
		msg, found, err := e.tryQueueRead(ctx, topicName, scan, pos, visibilityTimeout)
		if err != nil {
			if got > 0 {
				break
			}
			if e.metrics != nil {
				e.metrics.IncError("messaging", "consume")
			}
			return dst, nil, err
		}
		if !found {
			break
		}
		dst = append(dst, msg)
		got++
		size += len(msg.Key) + len(msg.Payload)
		e.recordConsumed(topicName, msg.Partition, len(msg.Payload))
		// Resume on the partition this record came from: the next one
		// is most likely right behind it.
		pos = scanIndex(scan, msg.Partition, pos)
	}
	if got > 0 {
		e.recordConsumeWait(topicName, "hit", time.Since(start))
		return dst, nil, nil
	}
	return dst, &ConsumeWaiter{
		topic:             topicName,
		scan:              scan,
		pinned:            opts.Partition,
		scanStart:         scanStart,
		visibilityTimeout: visibilityTimeout,
		start:             start,
	}, nil
}

// scanIndex returns partition p's index in scan, trying hint (the index
// the previous record came from) first. A partition that is somehow not
// in the scan leaves the position at hint.
func scanIndex(scan []int, p, hint int) int {
	if hint >= 0 && hint < len(scan) && scan[hint] == p {
		return hint
	}
	for i, q := range scan {
		if q == p {
			return i
		}
	}
	return hint
}

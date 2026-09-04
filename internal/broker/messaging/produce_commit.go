package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// CommitAcceptedProduce appends an ingress WAL record to this node's
// partition log and advances the partition high-watermark. It is the
// owner-side visibility step for the WAL-first produce design.
func (e *Engine) CommitAcceptedProduce(ctx context.Context, record ingress.ProduceRecord) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if e.logs == nil {
		return 0, unavailableError("partition logs")
	}
	if record.Topic == "" {
		return 0, fmt.Errorf("%w: topic required", ErrInvalid)
	}
	if len(record.Payload) == 0 {
		return 0, fmt.Errorf("%w: payload required", ErrInvalid)
	}

	t, err := e.getTopic(ctx, record.Topic)
	if err != nil {
		return 0, err
	}
	if record.TargetPartition < 0 || record.TargetPartition >= t.Partitions {
		return 0, fmt.Errorf("%w: partition out of range", ErrInvalid)
	}
	if !e.isLocalOwner(record.Topic, record.TargetPartition) || e.isProducePaused(record.Topic, record.TargetPartition) {
		// A partition frozen for a rebalance handoff rejects commits so
		// nothing lands after the destination captured the final tail;
		// the ingress dispatcher retries and delivers to the new owner.
		return 0, ErrNotPartitionOwner
	}

	offset, err := e.logs.WithProduceLockResult(record.Topic, record.TargetPartition, func(log *storage.Log) (int64, error) {
		return e.appendAndCommit(log, storage.EncodeKeyedRecord(record.Key, time.Now().UnixMilli(), record.Payload))
	})
	if err != nil {
		e.recordProduceError(err)
		return 0, err
	}

	e.recordProduceCommitted(record.Topic, record.TargetPartition, 1, len(record.Payload))
	return offset, nil
}

// CommitAcceptedProduceBatch commits a batch of ingress WAL records to
// one locally owned topic partition under a single produce lock, with
// one append+fsync+verify cycle and a single high-watermark advance.
// All records must target the same (topic, partition). Returns the
// assigned offsets in record order.
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
	if !e.isLocalOwner(topicName, partition) || e.isProducePaused(topicName, partition) {
		// Frozen for handoff — see the single-commit path above.
		return nil, ErrNotPartitionOwner
	}

	// Records are stored wrapped in the keyed envelope so the produce
	// key and commit time survive the commit (fan-out re-keys parent
	// records with the key, delay children anchor due times to the
	// commit time, and consumers get Message.Key/Timestamp from them).
	committedAt := time.Now().UnixMilli()
	payloads := make([][]byte, len(records))
	payloadBytes := 0
	for i, record := range records {
		payloads[i] = storage.EncodeKeyedRecord(record.Key, committedAt, record.Payload)
		payloadBytes += len(record.Payload)
	}

	var offsets []int64
	err = e.logs.WithProduceLock(topicName, partition, func(log *storage.Log) error {
		// The envelopes were built above for this call only and are never
		// read again (commitDurable needs just their count), so the log may
		// take ownership instead of copying every record a second time.
		first, last, err := log.AppendBatchOwned(payloads)
		if err != nil {
			return produceStageError{stage: produceStageAppend, err: err}
		}
		if last < first {
			return nil
		}
		if err := e.commitDurable(log, first, len(payloads)); err != nil {
			return err
		}
		offsets = make([]int64, len(records))
		for i := range records {
			offsets[i] = first + int64(i)
		}
		return nil
	})
	if err != nil {
		e.recordProduceError(err)
		return nil, err
	}

	e.recordProduceCommitted(topicName, partition, len(records), payloadBytes)
	return offsets, nil
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

// appendAndCommit is the single durability chokepoint shared by the
// synchronous Produce path and the WAL-first commit path. It appends one
// payload to the partition log, then runs commitDurable.
//
// The caller must hold the partition produce lock.
func (e *Engine) appendAndCommit(log *storage.Log, payload []byte) (int64, error) {
	offset, err := log.Append(payload)
	if err != nil {
		return 0, produceStageError{stage: produceStageAppend, err: err}
	}
	if err := e.commitDurable(log, offset, 1); err != nil {
		return 0, err
	}
	return offset, nil
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
//  3. advances the high-watermark to make the records visible,
//  4. persists the advanced high-watermark before returning, so a crash
//     after the ingress WAL checkpoints past this batch can never leave
//     the records durable-but-hidden.
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

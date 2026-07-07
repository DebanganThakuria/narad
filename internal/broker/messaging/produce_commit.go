package messaging

import (
	"context"
	"fmt"
	"strconv"
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
	if !e.isLocalOwner(record.Topic, record.TargetPartition) {
		return 0, ErrNotPartitionOwner
	}

	offset, err := e.logs.WithProduceLockResult(record.Topic, record.TargetPartition, func(log *storage.Log) (int64, error) {
		return e.appendAndCommit(log, storage.EncodeKeyedRecord(record.Key, time.Now().UnixMilli(), record.Payload))
	})
	if err != nil {
		return 0, err
	}

	e.recordAcceptedProduceCommitted(record)
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
	if !e.isLocalOwner(topicName, partition) {
		return nil, ErrNotPartitionOwner
	}

	// Records are stored wrapped in the keyed envelope so the produce
	// key and commit time survive the commit (fan-out re-keys parent
	// records with the key, delay children anchor due times to the
	// commit time, and consumers get Message.Key/Timestamp from them).
	committedAt := time.Now().UnixMilli()
	payloads := make([][]byte, len(records))
	for i, record := range records {
		payloads[i] = storage.EncodeKeyedRecord(record.Key, committedAt, record.Payload)
	}

	var offsets []int64
	err = e.logs.WithProduceLock(topicName, partition, func(log *storage.Log) error {
		first, last, err := log.AppendBatch(payloads)
		if err != nil {
			return produceStageError{stage: produceStageAppend, err: err}
		}
		if last < first {
			return nil
		}
		if err := e.commitDurable(log, first, payloads); err != nil {
			return err
		}
		offsets = make([]int64, len(records))
		for i := range records {
			offsets[i] = first + int64(i)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		e.recordAcceptedProduceCommitted(record)
	}
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
	if err := e.commitDurable(log, offset, [][]byte{payload}); err != nil {
		return 0, err
	}
	return offset, nil
}

// commitDurable is the no-follower durability boundary. Narad has no
// replicas, so before a record is made visible (and before the ingress
// WAL is allowed to compact past it) the owner's partition log must be
// the proven-durable, uncorrupted copy. It:
//
//  1. synchronously fsyncs the partition log,
//  2. reads each record back so its on-disk frame CRC is validated and
//     the bytes round-trip (guards against a torn or corrupt write),
//  3. only then advances the high-watermark to make the records visible.
//
// firstOffset is the offset of payloads[0]; the records are contiguous.
// The caller must hold the partition produce lock.
func (e *Engine) commitDurable(log *storage.Log, firstOffset int64, payloads [][]byte) error {
	if len(payloads) == 0 {
		return nil
	}
	if err := log.Sync(); err != nil {
		return produceStageError{stage: produceStageCommit, err: err}
	}
	lastOffset := firstOffset + int64(len(payloads)) - 1
	// Verify the durable copy by re-reading each frame and checking its CRC
	// over the on-disk bytes — no decode. The CRC was computed over the
	// stored (possibly compressed) payload at write time, so this proves the
	// bytes survived intact before we advance the high-watermark and let the
	// WAL compact past them. Decoding per record here would be O(N) full-frame
	// zstd decodes per commit (the cause of the commit-throughput collapse).
	if err := log.VerifyDurable(firstOffset, lastOffset); err != nil {
		return produceStageError{stage: produceStageVerify, err: fmt.Errorf("verify [%d,%d]: %w", firstOffset, lastOffset, err)}
	}
	if err := log.AdvanceHighWatermark(lastOffset + 1); err != nil {
		return produceStageError{stage: produceStageCommit, err: err}
	}
	return nil
}

// recordAcceptedProduceCommitted bumps the produced counters for a
// committed WAL record.
func (e *Engine) recordAcceptedProduceCommitted(record ingress.ProduceRecord) {
	if e.metrics == nil {
		return
	}
	partLabel := strconv.Itoa(record.TargetPartition)
	e.metrics.MessagesProducedTotal.WithLabelValues(record.Topic, partLabel).Inc()
	e.metrics.BytesProducedTotal.WithLabelValues(record.Topic, partLabel).Add(float64(len(record.Payload)))
}

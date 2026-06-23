package messaging

import (
	"bytes"
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
	totalStart := time.Now()
	totalOutcome := "ok"
	defer func() {
		e.observe("produce_commit", "total", totalOutcome, time.Since(totalStart))
	}()

	if err := ctx.Err(); err != nil {
		totalOutcome = "error"
		return 0, err
	}
	if e.logs == nil {
		totalOutcome = "error"
		return 0, errorsUnavailable("partition logs")
	}
	if record.Topic == "" {
		totalOutcome = "error"
		return 0, fmt.Errorf("%w: topic required", ErrInvalid)
	}
	if len(record.Payload) == 0 {
		totalOutcome = "error"
		return 0, fmt.Errorf("%w: payload required", ErrInvalid)
	}

	stageStart := time.Now()
	t, err := e.getTopic(ctx, record.Topic)
	e.observe("produce_commit", "get_topic", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return 0, err
	}
	if record.TargetPartition < 0 || record.TargetPartition >= t.Partitions {
		totalOutcome = "error"
		return 0, fmt.Errorf("%w: partition out of range", ErrInvalid)
	}
	if !e.isLocalOwner(record.Topic, record.TargetPartition) {
		totalOutcome = "error"
		return 0, ErrNotPartitionOwner
	}

	stageStart = time.Now()
	offset, err := e.logs.WithProduceLockResult(record.Topic, record.TargetPartition, func(log *storage.Log) (int64, error) {
		return e.appendAndCommit(log, record.Payload)
	})
	e.observe("produce_commit", "append_visible", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return 0, err
	}

	if e.metrics != nil {
		e.recordAcceptedProduceCommitted(record)
	}
	return offset, nil
}

func (e *Engine) CommitAcceptedProduceBatch(ctx context.Context, records []ingress.ProduceRecord) ([]int64, error) {
	totalStart := time.Now()
	totalOutcome := "ok"
	defer func() {
		e.observe("produce_commit_batch", "total", totalOutcome, time.Since(totalStart))
	}()

	if len(records) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		totalOutcome = "error"
		return nil, err
	}
	if e.logs == nil {
		totalOutcome = "error"
		return nil, errorsUnavailable("partition logs")
	}

	topicName := records[0].Topic
	partition := records[0].TargetPartition
	for _, record := range records {
		if record.Topic == "" {
			totalOutcome = "error"
			return nil, fmt.Errorf("%w: topic required", ErrInvalid)
		}
		if record.Topic != topicName || record.TargetPartition != partition {
			totalOutcome = "error"
			return nil, fmt.Errorf("%w: accepted produce batch must target one topic partition", ErrInvalid)
		}
		if len(record.Payload) == 0 {
			totalOutcome = "error"
			return nil, fmt.Errorf("%w: payload required", ErrInvalid)
		}
	}

	stageStart := time.Now()
	t, err := e.getTopic(ctx, topicName)
	e.observe("produce_commit_batch", "get_topic", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return nil, err
	}
	if partition < 0 || partition >= t.Partitions {
		totalOutcome = "error"
		return nil, fmt.Errorf("%w: partition out of range", ErrInvalid)
	}
	if !e.isLocalOwner(topicName, partition) {
		totalOutcome = "error"
		return nil, ErrNotPartitionOwner
	}

	payloads := make([][]byte, len(records))
	for i, record := range records {
		payloads[i] = record.Payload
	}

	stageStart = time.Now()
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
	e.observe("produce_commit_batch", "append_visible", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return nil, err
	}

	for _, record := range records {
		e.recordAcceptedProduceCommitted(record)
	}
	return offsets, nil
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
	for i, payload := range payloads {
		offset := firstOffset + int64(i)
		got, err := log.Read(offset)
		if err != nil {
			return produceStageError{stage: produceStageVerify, err: fmt.Errorf("read back offset %d: %w", offset, err)}
		}
		if !bytes.Equal(got, payload) {
			return produceStageError{stage: produceStageVerify, err: fmt.Errorf("durability verify mismatch at offset %d", offset)}
		}
	}
	lastOffset := firstOffset + int64(len(payloads)) - 1
	if err := log.AdvanceHighWatermark(lastOffset + 1); err != nil {
		return produceStageError{stage: produceStageCommit, err: err}
	}
	return nil
}

func (e *Engine) recordAcceptedProduceCommitted(record ingress.ProduceRecord) {
	if e.metrics == nil {
		return
	}
	partLabel := strconv.Itoa(record.TargetPartition)
	e.metrics.MessagesProducedTotal.WithLabelValues(record.Topic, partLabel).Inc()
	e.metrics.BytesProducedTotal.WithLabelValues(record.Topic, partLabel).Add(float64(len(record.Payload)))
	if record.CreatedAtUnixMs > 0 {
		createdAt := time.UnixMilli(record.CreatedAtUnixMs)
		e.observe("produce_commit", "accepted_to_visible", "ok", time.Since(createdAt))
	}
}

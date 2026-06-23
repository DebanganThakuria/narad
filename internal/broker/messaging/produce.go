package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

type produceStage string

const (
	produceStageAppend produceStage = "append"
	produceStageVerify produceStage = "durability verify"
	produceStageCommit produceStage = "commit boundary durability"
)

type produceStageError struct {
	stage produceStage
	err   error
}

func (e produceStageError) Error() string {
	return fmt.Sprintf("messaging: %s: %v", e.stage, e.err)
}

func (e produceStageError) Unwrap() error {
	return e.err
}

func (e produceStageError) metricReason() string {
	switch e.stage {
	case produceStageCommit:
		return "commit_boundary"
	case produceStageVerify:
		return "durability_verify"
	default:
		return string(e.stage)
	}
}

// Produce validates the payload, picks a partition, appends the record
// to the owning partition log, and advances the high-watermark so the
// record becomes visible to consumers.
//
// Narad has no follower replication: the partition owner's durable log
// is the sole copy of the record. Durability is provided by the
// partition flusher's fsync of the segment file.
func (e *Engine) Produce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (int64, int, error) {
	totalStart := time.Now()
	totalOutcome := "ok"
	defer func() {
		e.observe("produce", "total", totalOutcome, time.Since(totalStart))
	}()

	stageStart := time.Now()
	t, err := e.getTopic(ctx, topicName)
	e.observe("produce", "get_topic", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return 0, 0, err
	}

	stageStart = time.Now()
	if err = e.validateProducePayload(ctx, topicName, payload); err != nil {
		e.observe("produce", "validate_payload", "error", time.Since(stageStart))
		if e.metrics != nil {
			e.metrics.ProduceRejectionsTotal.WithLabelValues(topicName, "schema").Inc()
		}
		totalOutcome = "error"
		return 0, 0, err
	}
	e.observe("produce", "validate_payload", "ok", time.Since(stageStart))

	stageStart = time.Now()
	partIdx, err := e.resolveProducePartition(topicName, key, t.Partitions, partition)
	e.observe("produce", "resolve_partition", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		if e.metrics != nil {
			e.metrics.IncError("messaging", "partition_pick")
		}
		totalOutcome = "error"
		return 0, 0, err
	}

	stageStart = time.Now()
	offset, err := e.logs.WithProduceLockResult(topicName, partIdx, func(log *storage.Log) (int64, error) {
		return e.appendAndCommit(log, payload)
	})
	e.observe("produce", "append_commit", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		e.recordProduceError(err)
		totalOutcome = "error"
		return 0, 0, err
	}

	if e.metrics != nil {
		partLabel := strconv.Itoa(partIdx)
		e.metrics.MessagesProducedTotal.WithLabelValues(topicName, partLabel).Inc()
		e.metrics.BytesProducedTotal.WithLabelValues(topicName, partLabel).Add(float64(len(payload)))
	}

	return offset, partIdx, nil
}

func (e *Engine) recordProduceError(err error) {
	if e.metrics == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		e.metrics.IncError("messaging", "commit_boundary")
		return
	}
	if stageErr, ok := errors.AsType[produceStageError](err); ok {
		e.metrics.IncError("messaging", stageErr.metricReason())
		return
	}
	e.metrics.IncError("messaging", "partition_open")
}

func (e *Engine) resolveProducePartition(topicName, key string, partitions int, pinned []int) (int, error) {
	if len(pinned) > 1 {
		return 0, fmt.Errorf("%w: at most one partition may be specified", ErrInvalid)
	}
	if len(pinned) == 1 {
		partIdx := pinned[0]
		if partIdx < 0 || partIdx >= partitions {
			return 0, fmt.Errorf("%w: partition out of range", ErrInvalid)
		}
		if !e.isWritableLocalProducePartition(topicName, partIdx) {
			if e.isLocalOwner(topicName, partIdx) {
				return 0, fmt.Errorf("messaging: no alive partition owner for topic %s", topicName)
			}
			return 0, ErrNotPartitionOwner
		}
		return partIdx, nil
	}

	partIdx, err := e.pickProducePartition(topicName, key, partitions)
	if err != nil {
		return 0, err
	}
	if !e.isLocalOwner(topicName, partIdx) {
		return 0, ErrNotPartitionOwner
	}
	return partIdx, nil
}

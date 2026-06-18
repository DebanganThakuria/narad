package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

type produceStage string

const (
	produceStageAppend    produceStage = "append"
	produceStageReplicate produceStage = "replicate"
	produceStageCommit    produceStage = "commit boundary durability"
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
	if e.stage == produceStageCommit {
		return "commit_boundary"
	}
	return string(e.stage)
}

// Produce validates the payload, picks a partition, appends to the
// partition log, then asks the replicator to fan the record out.
func (e *Engine) Produce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (int64, int, error) {
	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return 0, 0, err
	}

	if err = e.validateProducePayload(ctx, topicName, payload); err != nil {
		if e.metrics != nil {
			e.metrics.ProduceRejectionsTotal.WithLabelValues(topicName, "schema").Inc()
		}
		return 0, 0, err
	}

	partIdx, err := e.resolveProducePartition(topicName, key, t.Partitions, partition)
	if err != nil {
		if e.metrics != nil {
			e.metrics.IncError("messaging", "partition_pick")
		}
		return 0, 0, err
	}
	offset, _, err := e.logs.WithProduceLockValue(topicName, partIdx, func(log *storage.Log) (int64, int, error) {
		offset, err := log.Append(payload)
		if err != nil {
			return 0, 0, produceStageError{stage: produceStageAppend, err: err}
		}

		if err := e.replicator.Replicate(ctx, topicName, partIdx, offset, payload); err != nil {
			return 0, 0, produceStageError{stage: produceStageReplicate, err: err}
		}
		if err := log.AdvanceHighWatermark(offset + 1); err != nil {
			return 0, 0, produceStageError{stage: produceStageCommit, err: err}
		}

		return offset, partIdx, nil
	})
	if err != nil {
		e.recordProduceError(err)
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
		e.metrics.IncError("messaging", "replicate")
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

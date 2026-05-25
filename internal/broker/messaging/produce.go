package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// Produce validates the payload, picks a partition, appends to the
// partition log, then asks the replicator to fan the record out.
func (e *Engine) Produce(ctx context.Context, topicName, key string, payload []byte) (int64, int, error) {
	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return 0, 0, err
	}

	if err = e.schemas.Validate(ctx, topicName, payload); err != nil && !errors.Is(err, errs.ErrSchemaNotFound) {
		if e.metrics != nil {
			e.metrics.ProduceRejectionsTotal.WithLabelValues(topicName, "schema").Inc()
		}
		return 0, 0, err
	}

	partIdx, err := e.pickProducePartition(topicName, key, t.Partitions)
	if err != nil {
		if e.metrics != nil {
			e.metrics.IncError("messaging", "partition_pick")
		}
		return 0, 0, err
	}
	offset, _, err := e.logs.WithProduceLockValue(topicName, partIdx, func(log *storage.Log) (int64, int, error) {
		offset, err := log.Append(payload)
		if err != nil {
			return 0, 0, fmt.Errorf("messaging: append: %w", err)
		}

		if err := e.replicator.Replicate(ctx, topicName, partIdx, offset, payload); err != nil {
			return 0, 0, fmt.Errorf("messaging: replicate: %w", err)
		}
		if err := log.AdvanceHighWatermark(offset + 1); err != nil {
			return 0, 0, fmt.Errorf("messaging: commit boundary durability: %w", err)
		}

		return offset, partIdx, nil
	})
	if err != nil {
		if e.metrics != nil {
			switch {
			case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
				e.metrics.IncError("messaging", "replicate")
			case strings.Contains(err.Error(), "messaging: append:"):
				e.metrics.IncError("messaging", "append")
			case strings.Contains(err.Error(), "messaging: replicate:"):
				e.metrics.IncError("messaging", "replicate")
			case strings.Contains(err.Error(), "messaging: commit boundary durability:"):
				e.metrics.IncError("messaging", "commit_boundary")
			default:
				e.metrics.IncError("messaging", "partition_open")
			}
		}
		return 0, 0, err
	}

	if e.metrics != nil {
		partLabel := strconv.Itoa(partIdx)
		e.metrics.MessagesProducedTotal.WithLabelValues(topicName, partLabel).Inc()
		e.metrics.BytesProducedTotal.WithLabelValues(topicName, partLabel).Add(float64(len(payload)))
	}

	return offset, partIdx, nil
}

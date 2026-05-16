package messaging

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/debanganthakuria/narad/internal/errs"
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
	log, err := e.logs.Get(topicName, partIdx)
	if err != nil {
		if e.metrics != nil {
			e.metrics.IncError("messaging", "partition_open")
		}
		return 0, 0, err
	}

	offset, err := log.Append(payload)
	if err != nil {
		if e.metrics != nil {
			e.metrics.IncError("messaging", "append")
		}
		return 0, 0, fmt.Errorf("messaging: append: %w", err)
	}

	if err = e.replicator.Replicate(ctx, topicName, partIdx, offset, payload); err != nil {
		if e.metrics != nil {
			e.metrics.IncError("messaging", "replicate")
		}
		return 0, 0, fmt.Errorf("messaging: replicate: %w", err)
	}

	if e.metrics != nil {
		partLabel := strconv.Itoa(partIdx)
		e.metrics.MessagesProducedTotal.WithLabelValues(topicName, partLabel).Inc()
		e.metrics.BytesProducedTotal.WithLabelValues(topicName, partLabel).Add(float64(len(payload)))
	}

	return offset, partIdx, nil
}

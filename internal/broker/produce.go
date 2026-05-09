package broker

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/debanganthakuria/narad/internal/schema"
)

// Produce validates the payload, picks a partition, appends to the
// partition log, then asks the replicator to fan the record out.
func (b *impl) Produce(ctx context.Context, topicName, key string, payload []byte) (int64, int, error) {
	t, err := b.GetTopic(ctx, topicName)
	if err != nil {
		return 0, 0, err
	}

	if err = b.deps.Schemas.Validate(ctx, topicName, payload); err != nil && !errors.Is(err, schema.ErrSchemaNotFound) {
		if m := b.deps.Metrics; m != nil {
			m.ProduceRejectionsTotal.WithLabelValues(topicName, "schema").Inc()
		}
		return 0, 0, err
	}

	partIdx := b.deps.Partitions.Pick(topicName, key, t.Partitions)
	log, err := b.partitionLog(topicName, partIdx)
	if err != nil {
		if m := b.deps.Metrics; m != nil {
			m.IncError("broker", "partition_open")
		}
		return 0, 0, err
	}

	offset, err := log.Append(payload)
	if err != nil {
		if m := b.deps.Metrics; m != nil {
			m.IncError("broker", "append")
		}
		return 0, 0, fmt.Errorf("broker: append: %w", err)
	}

	if err = b.deps.Replicator.Replicate(ctx, topicName, partIdx, offset, payload); err != nil {
		if m := b.deps.Metrics; m != nil {
			m.IncError("broker", "replicate")
		}
		return 0, 0, fmt.Errorf("broker: replicate: %w", err)
	}

	if m := b.deps.Metrics; m != nil {
		partLabel := strconv.Itoa(partIdx)
		m.MessagesProducedTotal.WithLabelValues(topicName, partLabel).Inc()
		m.BytesProducedTotal.WithLabelValues(topicName, partLabel).Add(float64(len(payload)))
	}

	return offset, partIdx, nil
}
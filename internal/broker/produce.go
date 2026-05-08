package broker

import (
	"context"
	"fmt"
)

// Produce validates the payload, picks a partition, appends to the
// partition log, then asks the replicator to fan the record out.
func (b *impl) Produce(ctx context.Context, topicName, key string, payload []byte) (int64, int, error) {
	t, err := b.GetTopic(ctx, topicName)
	if err != nil {
		return 0, 0, err
	}

	if err := b.deps.Schemas.Validate(ctx, topicName, payload); err != nil {
		return 0, 0, err
	}

	partIdx := b.deps.Partitions.Pick(topicName, key, t.Partitions)
	log, err := b.partitionLog(topicName, partIdx)
	if err != nil {
		return 0, 0, err
	}

	offset, err := log.Append(payload)
	if err != nil {
		return 0, 0, fmt.Errorf("broker: append: %w", err)
	}

	if err := b.deps.Replicator.Replicate(ctx, topicName, partIdx, offset, payload); err != nil {
		return 0, 0, fmt.Errorf("broker: replicate: %w", err)
	}

	return offset, partIdx, nil
}

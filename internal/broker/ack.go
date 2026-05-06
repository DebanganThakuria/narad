package broker

import (
	"context"
	"fmt"
)

// Ack advances the consumer offset for (topic, partition) up to and
// including offset. The next Consume on that partition will deliver
// offset+1.
func (b *impl) Ack(ctx context.Context, topicName string, partitionIdx int, offset int64) error {
	t, err := b.GetTopic(ctx, topicName)
	if err != nil {
		return err
	}
	if partitionIdx < 0 || partitionIdx >= t.Partitions {
		return fmt.Errorf("%w: partition out of range", ErrInvalidArgument)
	}
	if offset < 0 {
		return fmt.Errorf("%w: offset must be >= 0", ErrInvalidArgument)
	}
	return b.deps.Offsets.Commit(ctx, topicName, partitionIdx, offset)
}

package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/consumer"
)

// Ack accepts a decoded receipt handle previously returned by Consume.
// It verifies the nonce against the active in-flight reservation and
// commits the offset under the request path topic.
func (e *Engine) Ack(ctx context.Context, topicName string, h consumer.Handle) error {
	if topicName == "" {
		return fmt.Errorf("%w: topic required", ErrInvalid)
	}

	if err := consumer.ValidateHandle(h); err != nil {
		return err
	}

	// Confirm the topic still exists — clean 404 beats an opaque 410.
	if _, err := e.getTopic(ctx, topicName); err != nil {
		return err
	}

	if err := e.offsets.CommitHandle(topicName, h.Partition, h.Offset, h.Nonce); err != nil {
		if errors.Is(err, consumer.ErrAckedAheadFull) && e.metrics != nil {
			e.metrics.IncAckRejected("cap")
		}
		return err
	}
	return nil
}

package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/consumer"
)

// Ack accepts a receipt handle previously returned by Consume.
// It decodes the handle, checks the encoded topic matches the request,
// verifies the nonce against the active in-flight reservation, and
// commits the offset.
func (e *Engine) Ack(ctx context.Context, topicName string, receiptHandle string) error {
	if topicName == "" {
		return fmt.Errorf("%w: topic required", ErrInvalid)
	}
	if receiptHandle == "" {
		return consumer.ErrHandleMalformed
	}

	h, err := consumer.DecodeHandle(receiptHandle)
	if err != nil {
		return err
	}
	if h.Topic != topicName {
		return consumer.ErrHandleTopicMismatch
	}

	// Confirm the topic still exists — clean 404 beats an opaque 410.
	if _, err := e.getTopic(ctx, topicName); err != nil {
		return err
	}

	if err := e.offsets.CommitHandle(topicName, h.Partition, h.Offset, h.Nonce); err != nil {
		if errors.Is(err, consumer.ErrAckedAheadFull) {
			e.metrics.IncAckRejected("cap")
		}
		return err
	}
	return nil
}

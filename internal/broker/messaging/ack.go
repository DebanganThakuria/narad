package messaging

import (
	"context"
	"errors"
	"fmt"
	"time"

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

// ExtendAck renews the visibility window of a reserved message to a
// full fresh window (now + the topic's visibility timeout), so a slow
// consumer can keep its lease alive instead of racing redelivery. The
// receipt handle stays valid — the consumer acks with the same handle
// when done. An expired or superseded handle fails with ErrHandleStale,
// exactly like a late ack.
func (e *Engine) ExtendAck(ctx context.Context, topicName string, h consumer.Handle) error {
	if topicName == "" {
		return fmt.Errorf("%w: topic required", ErrInvalid)
	}

	if err := consumer.ValidateHandle(h); err != nil {
		return err
	}

	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return err
	}

	visibility := time.Duration(t.VisibilityTimeoutMs) * time.Millisecond
	if _, err := e.offsets.ExtendHandle(topicName, h.Partition, h.Offset, h.Nonce, visibility); err != nil {
		return err
	}
	if e.metrics != nil {
		e.metrics.IncAckExtended(topicName)
	}
	return nil
}

// Nack releases a reserved message immediately (visibility zero): the
// offset becomes redeliverable right away instead of waiting out the
// visibility timeout. Nothing is committed. Same handle validation as
// Ack — a lapsed handle fails with ErrHandleStale.
func (e *Engine) Nack(ctx context.Context, topicName string, h consumer.Handle) error {
	if topicName == "" {
		return fmt.Errorf("%w: topic required", ErrInvalid)
	}

	if err := consumer.ValidateHandle(h); err != nil {
		return err
	}

	if _, err := e.getTopic(ctx, topicName); err != nil {
		return err
	}

	if err := e.offsets.ReleaseHandle(topicName, h.Partition, h.Offset, h.Nonce); err != nil {
		return err
	}
	if e.metrics != nil {
		e.metrics.IncNack(topicName)
	}
	return nil
}

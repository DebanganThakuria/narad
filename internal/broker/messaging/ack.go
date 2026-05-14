package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
)

var (
	ErrHandleMalformed     = errors.New("consumer: receipt handle is malformed")
)

// Ack accepts an opaque receipt handle previously returned by Consume.
// It decodes the handle, verifies its HMAC against the broker's
// process-local signing key, confirms the encoded topic matches the
// path-supplied topic, and confirms the handle's nonce still matches
// an active reservation. Only then does the underlying Commit run.
//
// Errors are typed (consumer.ErrHandle*, ErrTopicNotFound) so the
// HTTP layer can map them to specific status codes.
func (e *Engine) Ack(ctx context.Context, topicName string, receiptHandle string) error {
	if topicName == "" {
		return fmt.Errorf("%w: topic required", ErrInvalid)
	}
	if receiptHandle == "" {
		return ErrHandleMalformed
	}

	// Confirm the topic still exists before doing handle work — gives
	// us a clean ErrTopicNotFound (404) instead of an opaque 410 if
	// the topic was deleted.
	if _, err := e.getTopic(ctx, topicName); err != nil {
		return err
	}

	// TODO Get reservation and match handle and commit
	if err := e.offsets.Commit(ctx, t.Name, int(.Partition), h.Offset); err != nil {
		// ackedAhead-cap is the one Commit error worth bubbling as a
		// distinct sentinel so the HTTP layer can return 503 instead
		// of a generic 5xx.
		if errors.Is(err, consumer.ErrAckedAheadFull) {
			e.metrics.IncAckRejected("cap")
			return err
		}
		return fmt.Errorf("messaging: commit ack: %w", err)
	}
	return nil
}

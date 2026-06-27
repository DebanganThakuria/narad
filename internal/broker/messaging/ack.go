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
	totalStart := time.Now()
	totalOutcome := "ok"
	defer func() {
		e.observe("ack", "total", totalOutcome, time.Since(totalStart))
	}()
	if topicName == "" {
		totalOutcome = "error"
		return fmt.Errorf("%w: topic required", ErrInvalid)
	}

	stageStart := time.Now()
	err := consumer.ValidateHandle(h)
	e.observe("ack", "decode_handle", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return err
	}

	// Confirm the topic still exists — clean 404 beats an opaque 410.
	stageStart = time.Now()
	if _, err := e.getTopic(ctx, topicName); err != nil {
		e.observe("ack", "get_topic", "error", time.Since(stageStart))
		totalOutcome = "error"
		return err
	}
	e.observe("ack", "get_topic", "ok", time.Since(stageStart))

	stageStart = time.Now()
	if err := e.offsets.CommitHandle(topicName, h.Partition, h.Offset, h.Nonce); err != nil {
		e.observe("ack", "commit_handle", "error", time.Since(stageStart))
		if errors.Is(err, consumer.ErrAckedAheadFull) && e.metrics != nil {
			e.metrics.IncAckRejected("cap")
		}
		totalOutcome = "error"
		return err
	}
	e.observe("ack", "commit_handle", "ok", time.Since(stageStart))
	return nil
}

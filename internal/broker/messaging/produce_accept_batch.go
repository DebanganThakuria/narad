package messaging

import (
	"context"
	"fmt"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/errs"
)

// ProduceMessage is one message of AcceptProduceBatch: the key, the
// payload and the optional partition AcceptProduce takes for one
// message.
type ProduceMessage struct {
	Key          string
	Payload      []byte
	Partition    int
	HasPartition bool
}

// AcceptProduceBatch is AcceptProduce for several messages to one topic,
// all or nothing. Every message is validated exactly as AcceptProduce
// validates one (schema, then partition) before any is accepted, and the
// first that fails fails the batch with its error, prefixed with the
// message's index. A valid batch goes into the ingress WAL in order and
// in one call (see ingress.Manager.AcceptProduceBatch), and the call
// returns once every message is durable, with a receipt per message.
func (e *Engine) AcceptProduceBatch(ctx context.Context, topicName string, msgs []ProduceMessage) ([]ingress.AcceptedProduce, error) {
	if e.ingress == nil {
		return nil, unavailableError("ingress manager")
	}
	if len(msgs) == 0 {
		return nil, fmt.Errorf("%w: messages required", ErrInvalid)
	}

	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return nil, err
	}
	// A delayed child only receives records through fan-out, as for a
	// single produce.
	if t.IsChild() && t.FanoutDelayMs > 0 {
		if e.metrics != nil {
			e.metrics.ProduceRejectionsTotal.WithLabelValues(topicName, "delayed_child").Inc()
		}
		return nil, errs.ErrDelayedChildProduce
	}

	records := make([]ingress.BatchRecord, len(msgs))
	for i := range msgs {
		msg := &msgs[i]
		if err := e.validateProducePayload(ctx, topicName, msg.Payload); err != nil {
			if e.metrics != nil {
				e.metrics.ProduceRejectionsTotal.WithLabelValues(topicName, "schema").Inc()
			}
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
		var one [1]int
		pinned := one[:0]
		if msg.HasPartition {
			pinned = append(pinned, msg.Partition)
		}
		partIdx, err := e.resolveAcceptedProducePartition(topicName, msg.Key, t.Partitions, pinned)
		if err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
		records[i] = ingress.BatchRecord{Key: msg.Key, TargetPartition: partIdx, Payload: msg.Payload}
	}
	// Stamped with the incarnation the payloads were validated against,
	// as AcceptProduce stamps one.
	return e.ingress.AcceptProduceBatch(ctx, topicName, t.ID, records)
}

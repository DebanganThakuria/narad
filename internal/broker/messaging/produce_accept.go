package messaging

import (
	"context"
	"fmt"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
)

// AcceptProduce validates a produce request and durably accepts it into
// this node's ingress WAL. It deliberately does not require the target
// partition to be locally owned; a background dispatcher is responsible
// for moving accepted records to their final partition owner.
func (e *Engine) AcceptProduce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (ingress.AcceptedProduce, error) {
	if e.ingress == nil {
		return ingress.AcceptedProduce{}, errUnavailable("ingress manager")
	}

	t, err := e.getTopic(ctx, topicName)
	if err != nil {
		return ingress.AcceptedProduce{}, err
	}

	if err = e.validateProducePayload(ctx, topicName, payload); err != nil {
		if e.metrics != nil {
			e.metrics.ProduceRejectionsTotal.WithLabelValues(topicName, "schema").Inc()
		}
		return ingress.AcceptedProduce{}, err
	}

	partIdx, err := e.resolveAcceptedProducePartition(topicName, key, t.Partitions, partition)
	if err != nil {
		return ingress.AcceptedProduce{}, err
	}

	accepted, err := e.ingress.AcceptProduce(ctx, topicName, key, partIdx, payload)
	if err != nil {
		return ingress.AcceptedProduce{}, err
	}
	return accepted, nil
}

func (e *Engine) resolveAcceptedProducePartition(topicName, key string, partitions int, pinned []int) (int, error) {
	if len(pinned) > 1 {
		return 0, fmt.Errorf("%w: at most one partition may be specified", ErrInvalid)
	}
	if partitions <= 0 {
		return 0, fmt.Errorf("%w: topic has no partitions", ErrInvalid)
	}
	if len(pinned) == 1 {
		partIdx := pinned[0]
		if partIdx < 0 || partIdx >= partitions {
			return 0, fmt.Errorf("%w: partition out of range", ErrInvalid)
		}
		return partIdx, nil
	}
	if e.partitions == nil {
		return 0, errUnavailable("partition manager")
	}
	partIdx := e.partitions.Pick(topicName, key, partitions)
	if partIdx < 0 || partIdx >= partitions {
		return 0, fmt.Errorf("%w: partition manager returned out-of-range partition", ErrInvalid)
	}
	return partIdx, nil
}

// errUnavailable reports that a required dependency was not wired in,
// as an ErrInvalid so callers map it to a client error.
func errUnavailable(component string) error {
	return fmt.Errorf("%w: %s unavailable", ErrInvalid, component)
}

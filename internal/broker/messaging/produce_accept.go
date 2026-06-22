package messaging

import (
	"context"
	"fmt"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
)

// AcceptProduce validates a produce request and durably accepts it into
// this node's ingress WAL. It deliberately does not require the target
// partition to be locally owned; a background dispatcher is responsible
// for moving accepted records to their final partition owner.
func (e *Engine) AcceptProduce(ctx context.Context, topicName, key string, payload []byte, partition ...int) (ingress.AcceptedProduce, error) {
	totalStart := time.Now()
	totalOutcome := "ok"
	defer func() {
		e.observe("produce_accept", "total", totalOutcome, time.Since(totalStart))
	}()

	if e.ingress == nil {
		totalOutcome = "error"
		return ingress.AcceptedProduce{}, errorsUnavailable("ingress manager")
	}

	stageStart := time.Now()
	t, err := e.getTopic(ctx, topicName)
	e.observe("produce_accept", "get_topic", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return ingress.AcceptedProduce{}, err
	}

	stageStart = time.Now()
	if err = e.validateProducePayload(ctx, topicName, payload); err != nil {
		e.observe("produce_accept", "validate_payload", "error", time.Since(stageStart))
		if e.metrics != nil {
			e.metrics.ProduceRejectionsTotal.WithLabelValues(topicName, "schema").Inc()
		}
		totalOutcome = "error"
		return ingress.AcceptedProduce{}, err
	}
	e.observe("produce_accept", "validate_payload", "ok", time.Since(stageStart))

	stageStart = time.Now()
	partIdx, err := e.resolveAcceptedProducePartition(topicName, key, t.Partitions, partition)
	e.observe("produce_accept", "resolve_partition", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
		return ingress.AcceptedProduce{}, err
	}

	stageStart = time.Now()
	accepted, err := e.ingress.AcceptProduce(ctx, topicName, key, partIdx, payload)
	e.observe("produce_accept", "wal_append", observeOutcome(err), time.Since(stageStart))
	if err != nil {
		totalOutcome = "error"
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
		return 0, errorsUnavailable("partition manager")
	}
	partIdx := e.partitions.Pick(topicName, key, partitions)
	if partIdx < 0 || partIdx >= partitions {
		return 0, fmt.Errorf("%w: partition manager returned out-of-range partition", ErrInvalid)
	}
	return partIdx, nil
}

func errorsUnavailable(component string) error {
	return fmt.Errorf("%w: %s unavailable", ErrInvalid, component)
}

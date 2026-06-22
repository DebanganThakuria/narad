package messaging

import (
	"context"
	"errors"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
)

func (e *Engine) observe(operation, stage, outcome string, duration time.Duration) {
	if e.metrics == nil {
		return
	}
	e.metrics.ObserveHotPathStage("broker", operation, stage, outcome, duration)
}

func observeOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func consumeStageOutcome(found bool, err error) string {
	if err != nil {
		return "error"
	}
	if found {
		return "hit"
	}
	return "empty"
}

func reserveOutcome(res consumer.ReserveResult, err error) string {
	if err != nil {
		return "error"
	}
	if res.Reserved {
		return "reserved"
	}
	if res.SkipReason != "" {
		return res.SkipReason
	}
	return "empty"
}

func waitOutcome(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case err != nil:
		return "error"
	default:
		return "wakeup"
	}
}

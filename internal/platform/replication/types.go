package replication

import "time"

type stageObserver interface {
	ObserveHotPathStage(component, operation, stage, outcome string, duration time.Duration)
}

func observeOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

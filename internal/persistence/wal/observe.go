package wal

import "time"

func (l *Log) observe(stage, outcome string, duration time.Duration) {
	if l == nil || l.opts.Observer == nil {
		return
	}
	component := l.opts.ObserverComponent
	if component == "" {
		component = "wal"
	}
	operation := l.opts.ObserverOperation
	if operation == "" {
		operation = "append"
	}
	l.opts.Observer.ObserveHotPathStage(component, operation, stage, outcome, duration)
}

func observeOutcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

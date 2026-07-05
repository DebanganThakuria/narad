package runtime

import (
	"context"
	"errors"
	"sync/atomic"
)

var errNotReady = errors.New("not ready")

// Lifecycle owns broker-level startup/shutdown hooks. Embedded into
// the broker facade so Ready and Close satisfy the Broker interface.
type Lifecycle struct {
	logs    *Logs
	closers []func() error
	ready   atomic.Bool
}

// NewLifecycle wires a Lifecycle. The optional closers run before the
// partition logs are closed, in the order given.
func NewLifecycle(logs *Logs, closers ...func() error) *Lifecycle {
	return &Lifecycle{logs: logs, closers: closers}
}

// MarkReady makes Ready report success.
func (l *Lifecycle) MarkReady() {
	l.ready.Store(true)
}

// MarkNotReady makes Ready fail until MarkReady is called again.
func (l *Lifecycle) MarkNotReady() {
	l.ready.Store(false)
}

// Ready reports whether the broker has finished startup. The error it
// returns is recognized by IsNotReady.
func (l *Lifecycle) Ready(_ context.Context) error {
	if !l.ready.Load() {
		return errNotReady
	}
	return nil
}

// Close marks the broker not ready, runs the registered closers, and
// releases all open partition logs (each Log.Close does a final flush
// before releasing its file handles). Returns the first error.
func (l *Lifecycle) Close() error {
	l.MarkNotReady()
	var firstErr error
	for _, closeFn := range l.closers {
		if err := closeFn(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := l.logs.CloseAll(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// IsNotReady reports whether err is the not-ready sentinel returned by
// Ready (possibly wrapped).
func IsNotReady(err error) bool {
	return errors.Is(err, errNotReady)
}

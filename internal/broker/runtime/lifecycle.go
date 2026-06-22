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

// NewLifecycle wires a Lifecycle.
func NewLifecycle(logs *Logs, closers ...func() error) *Lifecycle {
	return &Lifecycle{logs: logs, closers: closers}
}

func (l *Lifecycle) MarkReady() {
	l.ready.Store(true)
}

func (l *Lifecycle) MarkNotReady() {
	l.ready.Store(false)
}

func (l *Lifecycle) Ready(_ context.Context) error {
	if !l.ready.Load() {
		return errNotReady
	}
	return nil
}

// Close releases all open partition logs. Each Log.Close() does a
// final flush before releasing its file handles.
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

var _ interface {
	MarkReady()
	MarkNotReady()
} = (*Lifecycle)(nil)

func IsNotReady(err error) bool {
	return errors.Is(err, errNotReady)
}

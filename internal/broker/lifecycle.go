package broker

import "context"

// Ready reports whether the broker is in a state where it can serve
// traffic. It currently always returns nil; once we wire in real
// readiness signals (replication caught up, metastore reachable, etc.)
// they fan in here.
func (b *impl) Ready(_ context.Context) error { return nil }

// Close releases all open partition logs. Subsequent Produce/Consume
// calls return errors from the closed file handles.
func (b *impl) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var firstErr error
	for k, l := range b.logs {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(b.logs, k)
		delete(b.locks, k)
	}
	return firstErr
}

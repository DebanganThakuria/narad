package broker

import "context"

func (b *impl) Ready(_ context.Context) error { return nil }

// Close releases all open partition logs. Each Log.Close() does a
// final flush before releasing its file handles.
func (b *impl) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var firstErr error
	for k, l := range b.logs {
		if err := l.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(b.logs, k)
	}
	return firstErr
}

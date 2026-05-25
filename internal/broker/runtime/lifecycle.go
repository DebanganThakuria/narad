package runtime

import "context"

// Lifecycle owns broker-level startup/shutdown hooks. Embedded into
// the broker facade so Ready and Close satisfy the Broker interface.
type Lifecycle struct {
	logs *Logs
}

// NewLifecycle wires a Lifecycle.
func NewLifecycle(logs *Logs) *Lifecycle {
	return &Lifecycle{logs: logs}
}

// Ready returns nil — the broker is single-node and is ready as soon
// as construction completes. Future replication will check follower
// liveness here.
// TODO complete implementation. Check if all the nodes in the cluster are ready
func (l *Lifecycle) Ready(_ context.Context) error { return nil }

// Close releases all open partition logs. Each Log.Close() does a
// final flush before releasing its file handles.
func (l *Lifecycle) Close() error {
	return l.logs.CloseAll()
}

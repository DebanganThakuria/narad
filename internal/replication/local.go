package replication

import "context"

// Local is a no-op replicator for single-node deployments. Once the
// leader has fsynced the append, the write is durable; nothing else to
// do.
type Local struct{}

// NewLocal returns the single-node replicator.
func NewLocal() Local { return Local{} }

// Replicate is a no-op for Local.
func (Local) Replicate(_ context.Context, _ string, _ int, _ int64, _ []byte) error {
	return nil
}

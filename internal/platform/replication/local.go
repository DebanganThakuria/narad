package replication

import "context"

// Local is a no-op replicator for single-node deployments. Storage durability
// is handled by the partition flusher according to its sync policy.
type Local struct{}

// NewLocal returns the single-node replicator.
func NewLocal() Local { return Local{} }

// Replicate is a no-op for Local.
func (Local) Replicate(_ context.Context, _ string, _ int, _ int64, _ []byte) error {
	return nil
}

func (Local) ReplicateBatch(_ context.Context, _ string, _ int, _ []Record) error {
	return nil
}

func (Local) CatchUp(_ context.Context, _ string, _ int, _ LeaderLog, _ CatchUpOptions) error {
	return nil
}

// Package replication is the contract for fanning a write out to
// follower replicas after the leader has committed it.
//
// The wiring pass ships only the Local stub (single-node, no
// replication). Real leader/follower replication will land here behind
// the same interface, so the broker doesn't need to be re-plumbed.
package replication

import "context"

// Replicator is invoked by the broker after a successful leader append.
// It must block until quorum (or local-only) is satisfied, or return an
// error to roll the produce attempt back.
type Replicator interface {
	Replicate(ctx context.Context, topic string, partition int, offset int64, payload []byte) error
}

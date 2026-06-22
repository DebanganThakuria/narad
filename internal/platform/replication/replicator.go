// Package replication is the contract for fanning a write out to
// follower replicas after the leader has committed it.
//
// The wiring pass ships only the Local stub (single-node, no
// replication). Real leader/follower replication will land here behind
// the same interface, so the broker doesn't need to be re-plumbed.
package replication

import "context"

type Record struct {
	Offset  int64
	Payload []byte
}

// Replicator is invoked by the broker after a successful leader append.
// It must block until quorum (or local-only) is satisfied, or return an
// error to roll the produce attempt back.
type Replicator interface {
	Replicate(ctx context.Context, topic string, partition int, offset int64, payload []byte) error
}

type BatchReplicator interface {
	Replicator
	ReplicateBatch(ctx context.Context, topic string, partition int, records []Record) error
}

// LeaderLog is the minimal owner-side log surface needed to repair a
// follower before allowing more writes to the partition.
type LeaderLog interface {
	Read(offset int64) ([]byte, error)
	HighWatermark() int64
	NextOffset() int64
	AdvanceHighWatermark(newHWM int64) error
}

// CatchUpOptions carries optional hints learned from a failed replicate
// request. Without a hint, implementations may probe the follower.
type CatchUpOptions struct {
	FollowerNextOffset *int64
}

// CatchUpReplicator can make a follower contiguous with the owner log.
// Produce uses this as a partition availability gate: if the local
// high-watermark is behind the local tail, new appends wait until catch-up
// succeeds.
type CatchUpReplicator interface {
	Replicator
	CatchUp(ctx context.Context, topic string, partition int, log LeaderLog, opts CatchUpOptions) error
}

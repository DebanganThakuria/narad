package cluster

// Where a fan-out cursor starts when it has no resume point.
//
// The client contract is "from the attach point forward, no backfill".
// Anchoring at the parent's tail as of the cursor's FIRST READ is not
// that: the cursor spawns a reconcile interval plus a leader round trip
// after the attach committed, and everything committed in between was
// silently skipped. Worse, a partition created by a partition-count
// increase received producer traffic within milliseconds of being
// assigned, and its cursor's tail anchor skipped all of it, although a
// brand-new partition has no pre-attach history to skip at all.
//
// So the attach records the parent's committed tail per partition
// (topic.Topic.AttachOffsets, observed by AttachOffsets below while
// the attach is being proposed), and a cursor with no file starts
// exactly there; a partition beyond the recorded slice is newer than
// the attach and starts at 0. Records without offsets (written before
// the field existed) keep the tail-anchor behaviour.

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// attachOffsetsConcurrency bounds how many parent partition owners are
// asked at once while resolving an attach point.
const attachOffsetsConcurrency = 16

// attachAnchor is the offset a cursor for (child, parentPartition)
// starts from when it has no cursor file for the child's live epoch:
// the recorded attach point, 0 for a partition created after the
// attach, or topic.FanoutTailOffset (anchor at the live tail, the
// older behaviour) when the record carries no offsets.
func attachAnchor(child topic.Topic, parentPartition int) int64 {
	if child.AttachOffsets == nil {
		return topic.FanoutTailOffset
	}
	if parentPartition < len(child.AttachOffsets) {
		return max(child.AttachOffsets[parentPartition], 0)
	}
	return 0
}

// AttachOffsets reports the committed high watermark of every partition
// of parent, asking each partition's owner (locally for partitions this
// node owns, one stats RPC otherwise), concurrently. It is registered
// on the metastore as the attach-offset resolver, so it runs on the
// node proposing an attach, just before the attach is applied. Any
// owner that cannot answer fails the whole resolution: a guessed offset
// would either skip records or backfill history, and the attach is
// cheap to retry.
func (r *FanoutRunner) AttachOffsets(ctx context.Context, parent string) ([]int64, error) {
	t, err := r.store.GetTopic(ctx, parent)
	if err != nil {
		return nil, err
	}
	offsets := make([]int64, t.Partitions)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(attachOffsetsConcurrency)
	for p := range t.Partitions {
		g.Go(func() error {
			hwm, err := r.partitionHighWatermark(gctx, parent, p)
			if err != nil {
				return fmt.Errorf("partition %d: %w", p, err)
			}
			offsets[p] = hwm
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return offsets, nil
}

// partitionHighWatermark reads one parent partition's committed tail
// from wherever it lives: a tail-position slab read on this node, or
// the owner's partition stats.
func (r *FanoutRunner) partitionHighWatermark(ctx context.Context, parent string, p int) (int64, error) {
	local, addr, err := r.resolveOwner(parent, p)
	if err != nil {
		return 0, err
	}
	if local {
		slab, err := r.broker.ReadFanoutSlab(ctx, parent, p,
			topic.FanoutReadOpts{FromOffset: topic.FanoutTailOffset, MaxRecords: 1, MaxBytes: 1})
		if err != nil {
			return 0, err
		}
		return slab.NextOffset, nil
	}
	if r.peer == nil {
		return 0, fmt.Errorf("no peer client to reach owner %s", addr)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, defaultPeerRPCTimeout)
	defer cancel()
	stats, err := r.peer.TopicPartitionStats(rpcCtx, addr, parent, p)
	if err != nil {
		return 0, err
	}
	return stats.HighWatermark, nil
}

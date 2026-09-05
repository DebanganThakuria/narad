package metastore

// The attach point of a fan-out link. A child receives "everything
// from the attach forward, no backfill", and the only way to make that
// exact is to write down where the parent's committed tail stood while
// the attach was being committed. The metastore cannot observe partition
// logs itself (they live on the partition owners), so the fan-out
// runner registers a resolver that asks each owner; the Store calls it
// once per attach and stores the answer in the link record.

import (
	"context"
	"fmt"

	"github.com/debanganthakuria/narad/internal/errs"
)

// AttachOffsetResolver reports the committed high watermark of every
// partition of parent, index = partition, exactly len(partitions)
// long. It runs on the proposing node before the attach is applied
// through Raft. An error fails the attach (the client retries); it must
// not guess, because an offset that is too high silently skips records
// and one that is too low replays pre-attach history into the child.
type AttachOffsetResolver func(ctx context.Context, parent string) ([]int64, error)

// SetAttachOffsetResolver registers the resolver consulted by
// AttachChild. Without one (older deployments, embedded use, tests that
// never construct a fan-out runner) attach records carry no offsets and
// fan-out cursors keep the tail-anchor behaviour.
func (s *Store) SetAttachOffsetResolver(r AttachOffsetResolver) {
	s.attachOffsetsMu.Lock()
	defer s.attachOffsetsMu.Unlock()
	s.attachOffsets = r
}

// resolveAttachOffsets runs the registered resolver, or returns nil
// offsets when none is registered. A resolver failure is surfaced as
// errs.ErrUnavailable: the parent partition owners could not all be
// asked right now, which is a retryable condition, not an invalid
// request.
func (s *Store) resolveAttachOffsets(ctx context.Context, parent string) ([]int64, error) {
	s.attachOffsetsMu.RLock()
	resolve := s.attachOffsets
	s.attachOffsetsMu.RUnlock()
	if resolve == nil {
		return nil, nil
	}
	offsets, err := resolve(ctx, parent)
	if err != nil {
		return nil, fmt.Errorf("%w: fan-out attach point of %q could not be resolved: %v", errs.ErrUnavailable, parent, err)
	}
	for p, off := range offsets {
		if off < 0 {
			return nil, fmt.Errorf("%w: fan-out attach point of %q partition %d is negative (%d)", errs.ErrUnavailable, parent, p, off)
		}
	}
	return offsets, nil
}

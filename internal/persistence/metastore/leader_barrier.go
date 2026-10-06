package metastore

import (
	"context"
	"fmt"

	"github.com/hashicorp/raft"

	"github.com/debanganthakuria/narad/internal/errs"
)

// LeaderBarrier makes sure this leader's FSM has applied every entry
// committed before its current term, so a read-modify-write built from
// the local replica reflects what earlier leaders committed. Election
// proves a new leader's log is complete, not that its FSM has replayed
// it: an alter served right after a leader change could otherwise read
// a topic as it was several writes ago and propose that stale record
// back (shrinking the partition count, undoing the previous write), and
// the controller could place a partition an earlier leader had already
// placed.
//
// The barrier runs once per leadership term: within a term every entry
// this node proposes is applied in order, so only the entries inherited
// from before the election can be missing. Concurrent first callers of a
// term share one barrier. A node that is not the leader returns nil at
// once: any write it proposes fails with "not the leader" anyway.
//
// A failed barrier is errs.ErrUnavailable (503, retry); nothing has been
// read or written, and the next call tries again.
func (s *Store) LeaderBarrier(ctx context.Context) error {
	if s.r == nil || s.r.State() != raft.Leader {
		return nil
	}
	term := s.r.CurrentTerm()
	if s.barrierTerm.Load() == term {
		return nil
	}
	s.barrierMu.Lock()
	defer s.barrierMu.Unlock()
	if s.barrierTerm.Load() == term {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.leaderBarriers.Add(1)
	if err := s.Barrier(); err != nil {
		return fmt.Errorf("%w: the new leader is still applying the log; retry: %v", errs.ErrUnavailable, err)
	}
	// Recorded only after the barrier succeeded, for the term read
	// before it: if leadership changed in between, the stored term is
	// already stale and the next call barriers again.
	s.barrierTerm.Store(term)
	return nil
}

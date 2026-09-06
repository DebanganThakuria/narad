package metastore

import (
	"context"
	"strconv"
	"time"
)

// appliedPollInterval is how often WaitApplied re-checks. Control-plane
// writes are rare and the wait is normally a single replication round
// trip, so a short fixed poll beats wiring an FSM notification through
// the store.
const appliedPollInterval = 2 * time.Millisecond

// AppliedIndex reports the highest Raft log index whose effects are in
// this node's metadata database: the last entry the FSM finished
// applying, or the index of a restored snapshot, whichever is higher.
// It is deliberately NOT Raft's applied_index stat, which advances when
// a batch is handed to the FSM goroutine, before its bbolt transaction
// has run. A store without Raft (tests, tools) reports 0.
func (s *Store) AppliedIndex() uint64 {
	if s == nil || s.fsm == nil {
		return 0
	}
	applied := s.fsm.applied.Load()
	if s.r != nil {
		// A snapshot restore replaces the database wholesale with state
		// covering every entry up to the snapshot index, none of which
		// went through Apply on this node.
		if snap, err := strconv.ParseUint(s.r.Stats()["last_snapshot_index"], 10, 64); err == nil && snap > applied {
			return snap
		}
	}
	return applied
}

// WaitApplied blocks until this node's metadata database reflects every
// log entry up to and including index, or ctx ends. It is how a
// follower gives a client read-your-writes on a write it forwarded to
// the leader: the leader reports the index it had applied when the
// write completed, and the follower does not answer until its own
// replica has caught up to that point. A store without Raft returns
// immediately.
func (s *Store) WaitApplied(ctx context.Context, index uint64) error {
	if s == nil || s.r == nil {
		return nil
	}
	for {
		if s.AppliedIndex() >= index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(appliedPollInterval):
		}
	}
}

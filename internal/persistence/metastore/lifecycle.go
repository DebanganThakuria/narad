package metastore

import (
	"strconv"
	"time"

	"github.com/hashicorp/raft"
)

// appliedCaughtUpContactWindow bounds how stale a follower's last leader
// contact may be for AppliedCaughtUp to trust its indexes. A node restored
// from a Raft snapshot satisfies applied >= commit against purely LOCAL
// indexes seconds after boot while its FSM is hours stale; requiring fresh
// leader contact means the commit index being compared was learned from the
// current leader, not carried over from before the restart.
const appliedCaughtUpContactWindow = 5 * time.Second

// Close hands leadership to another voter if this node leads, shuts
// Raft down, and closes the FSM database.
func (s *Store) Close() error {
	if s.r.State() == raft.Leader {
		s.r.LeadershipTransfer() //nolint:errcheck
	}
	if err := s.r.Shutdown().Error(); err != nil {
		return err
	}
	s.fsm.mu.Lock()
	defer s.fsm.mu.Unlock()
	return s.fsm.db.Close()
}

// IsLeader reports whether this node is the current Raft leader.
func (s *Store) IsLeader() bool {
	return s.r.State() == raft.Leader
}

// LeaderCh returns Raft's leadership notification channel; it receives
// true when this node gains leadership and false when it loses it.
func (s *Store) LeaderCh() <-chan bool {
	return s.r.LeaderCh()
}

// LeaderAddr returns the current leader's Raft address, or "" if there
// is no known leader.
func (s *Store) LeaderAddr() string {
	serverAddress, _ := s.r.LeaderWithID()
	return string(serverAddress)
}

// LeaderID returns the current leader's node ID, or "" if there is no
// known leader.
func (s *Store) LeaderID() string {
	_, serverID := s.r.LeaderWithID()
	return string(serverID)
}

// barrierTimeout bounds how long Barrier waits for the FSM to apply all
// preceding log entries. Callers treat a timeout as "not confirmed" and
// retry later, so this only needs to cover a healthy apply backlog.
const barrierTimeout = 5 * time.Second

// Barrier blocks until every log entry preceding it has been applied to
// this node's FSM, or fails. Leader-only (followers get ErrNotLeader). A
// FRESHLY ELECTED leader is the critical caller: election guarantees its
// LOG is complete, not that its FSM has applied it — a leader restored
// from an old snapshot legally serves reads from a stale FSM until the
// replay finishes. Any "I am the leader, my local state is authoritative"
// decision must barrier first, then re-read.
func (s *Store) Barrier() error {
	return s.r.Barrier(barrierTimeout).Error()
}

// AppliedCaughtUp reports whether the cluster has a leader, this node has
// heard from it recently (or IS it), AND the FSM has applied every entry it
// knows to be committed. It gates destructive reconciliation (orphan sweeps,
// fan-out cursor management): a node still catching up has a stale view of
// which topics exist and which attach epochs are live, so it must not act on
// local absence or mismatch until this is true. The freshness requirement is
// load-bearing — without it a snapshot-restored replica reads as "caught up"
// against its own pre-restart indexes.
func (s *Store) AppliedCaughtUp() bool {
	if s.r == nil || s.r.Leader() == "" {
		return false
	}
	if s.r.State() != raft.Leader {
		if lc := s.r.LastContact(); lc.IsZero() || time.Since(lc) > appliedCaughtUpContactWindow {
			return false
		}
	}
	stats := s.r.Stats()
	applied, err := strconv.ParseUint(stats["applied_index"], 10, 64)
	if err != nil {
		return false
	}
	commit, err := strconv.ParseUint(stats["commit_index"], 10, 64)
	if err != nil {
		return false
	}
	return applied >= commit
}

var _ Metastore = (*Store)(nil)

package metastore

import (
	"strconv"

	"github.com/hashicorp/raft"
)

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

// AppliedCaughtUp reports whether the cluster has a leader AND this node's
// FSM has applied every entry it knows to be committed. It gates
// destructive startup reconciliation (the orphan sweep): a node still
// catching up has a stale view of which topics exist, so it must not act
// on "topic dir present but absent from metastore" until this is true.
func (s *Store) AppliedCaughtUp() bool {
	if s.r == nil || s.r.Leader() == "" {
		return false
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

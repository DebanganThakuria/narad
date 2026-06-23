package metastore

import (
	"strconv"

	"github.com/hashicorp/raft"
)

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

func (s *Store) IsLeader() bool {
	return s.r.State() == raft.Leader
}

func (s *Store) LeaderCh() <-chan bool {
	return s.r.LeaderCh()
}

func (s *Store) LeaderAddr() string {
	serverAddress, _ := s.r.LeaderWithID()
	return string(serverAddress)
}

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

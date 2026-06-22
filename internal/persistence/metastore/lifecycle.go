package metastore

import "github.com/hashicorp/raft"

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

var _ Metastore = (*Store)(nil)

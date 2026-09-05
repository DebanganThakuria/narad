package metastore

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
)

var errPreVoteUnsupported = errors.New("metastore: transport does not support pre-vote")

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
// heard from it recently (or IS it), AND the FSM has applied every entry
// the LEADER has committed. It gates destructive reconciliation (orphan
// sweeps, fan-out cursor management) and the ownership latch: a node
// still catching up has a stale view of which topics exist, which attach
// epochs are live, and which partitions it owns, so it must not act on
// local absence or mismatch until this is true.
//
// Two local numbers are NOT enough. The freshness requirement is
// load-bearing: without it a snapshot-restored replica reads as "caught
// up" against its own pre-restart indexes. And the local commit_index
// is not the leader's: heartbeats refresh LastContact without carrying a
// commit index, and a follower caps commit_index at its own last log
// index, so a restarted follower satisfies applied >= commit_index
// before it has received a single entry, or after any partial batch.
// A follower therefore compares applied_index against the highest
// commit index a leader has actually reported to it (leader_commit.go),
// and reads as caught up only once that is known and applied.
//
// A leader compares against its own commit_index, which is
// authoritative once its no-op entry for the term has committed; until
// then commit_index is 0 in a fresh process and the leader is NOT
// caught up, however complete its log.
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
	if s.r.State() == raft.Leader {
		return commit > 0 && applied >= commit
	}
	if lc := s.r.LastContact(); lc.IsZero() || time.Since(lc) > appliedCaughtUpContactWindow {
		return false
	}
	leaderCommit := s.leaderCommitIndex()
	if leaderCommit == 0 {
		// Only heartbeats so far: nothing is known about what the leader
		// has committed, so nothing can be said about being caught up.
		return false
	}
	return applied >= leaderCommit && applied >= commit
}

// leaderCommitIndex returns the highest commit index a leader has
// reported to this node since the process started, or 0 if none.
func (s *Store) leaderCommitIndex() uint64 {
	if s.leaderCommit == nil {
		return 0
	}
	return s.leaderCommit.LeaderCommitIndex()
}

// ErrNotReady is the sentinel wrapped by every ClusterReady failure so
// callers can distinguish "not routable right now" from a broken store.
var ErrNotReady = errors.New("metastore: node is not ready")

// ClusterReady is the LIVE readiness check: nil only while this node has
// a leader in view, has heard from it within the contact window (or is
// it), and its ownership latch is set (setting it now if the replica is
// provably caught up). It is not a latch: a node that loses its leader,
// stops hearing from it, or is removed from the voter set flips back to
// not ready and stays there until it is in contact again, so the load
// balancer stops routing to a node serving a frozen replica (stale
// users, stale grants, stale topics) instead of routing to it for as
// long as the process lives. The error names the failing condition for
// the /readyz body.
func (s *Store) ClusterReady() error {
	if s.r == nil {
		return nil
	}
	if s.r.Leader() == "" {
		return fmt.Errorf("%w: no raft leader known", ErrNotReady)
	}
	if s.r.State() != raft.Leader {
		lc := s.r.LastContact()
		if lc.IsZero() {
			return fmt.Errorf("%w: no contact with the raft leader yet", ErrNotReady)
		}
		if since := time.Since(lc); since > appliedCaughtUpContactWindow {
			return fmt.Errorf("%w: last raft leader contact %s ago", ErrNotReady, since.Truncate(time.Millisecond))
		}
	}
	if !s.ownershipViewReady() {
		return fmt.Errorf("%w: replica has not caught up with the leader since start", ErrNotReady)
	}
	return nil
}

var _ Metastore = (*Store)(nil)

// AddVoter admits (or re-addresses) a node in the Raft voter set.
// Leader-only — followers fail with raft.ErrNotLeader — and idempotent:
// re-adding an existing voter with the same address is a no-op config
// entry. This is the scale-out admission path (OpJoinCluster).
func (s *Store) AddVoter(id, clusterAddr string) error {
	return s.r.AddVoter(raft.ServerID(id), raft.ServerAddress(clusterAddr), 0, barrierTimeout).Error()
}

// RemoveServer removes a node from the Raft configuration. Leader-only
// (followers fail with raft.ErrNotLeader). This is the decommission path:
// the controller calls it once a draining node owns no partitions, so the
// removed node's data is already safely relocated. Idempotent — removing a
// node already absent is a no-op config entry.
func (s *Store) RemoveServer(id string) error {
	return s.r.RemoveServer(raft.ServerID(id), 0, barrierTimeout).Error()
}

// Voters returns the IDs of the current Raft voters. Used by the
// decommission guard to refuse a removal that would drop the cluster below
// a safe voter count.
func (s *Store) Voters() ([]string, error) {
	future := s.r.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var voters []string
	for _, srv := range future.Configuration().Servers {
		if srv.Suffrage == raft.Voter {
			voters = append(voters, string(srv.ID))
		}
	}
	return voters, nil
}

// TransferLeadership hands Raft leadership to another voter. The
// decommission guard calls it when the node being removed is the current
// leader — you cannot cleanly remove the leader from its own config.
func (s *Store) TransferLeadership() error {
	return s.r.LeadershipTransfer().Error()
}

// HasRaftConfiguration reports whether this node's Raft configuration
// contains any servers. A join-only node that has not yet been admitted
// has an empty configuration and cannot make progress until the leader
// adds it.
func (s *Store) HasRaftConfiguration() (bool, error) {
	future := s.r.GetConfiguration()
	if err := future.Error(); err != nil {
		return false, err
	}
	return len(future.Configuration().Servers) > 0, nil
}

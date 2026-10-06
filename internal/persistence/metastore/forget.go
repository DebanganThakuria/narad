package metastore

// Forgetting a Raft server that has no member record.
//
// Decommission removes a node from Raft once it has drained, and it
// starts from the node's member record. A server in the Raft
// configuration with no record has nothing to start from: a joiner that
// was admitted (a 3.0.x leader admitted joiners straight into the voter
// set) and never registered, because it crashed, could not reach the
// cluster, or was replaced under another ID. As a voter it counts
// against quorum for good, and as any server it holds back new Raft
// entry types (EveryMemberKnows), since its release is unknown.
//
// ForgetServer removes such a server, and only such a server: it owns
// no partitions (nothing could be assigned to a node that never
// registered, and an assignment naming it is refused), so removing it
// moves and deletes no data. A server with a member record, alive, dead
// or draining, goes through decommission.
//
// Removing a voter can still cost the cluster its leader. Raft commits
// a configuration change under the new configuration from the moment
// the leader appends it, so the removal itself needs a quorum of the
// voters that are left. Take voters A (the leader), B and D, where D is
// a reachable server with no record and B is down: A and D commit today,
// but the configuration {A, B} needs B, the leader loses its lease, no
// vote from D counts any more, and no configuration change can commit
// to undo it. So a voter is forgotten only when the leader and the
// remaining voters it reaches make a quorum of the voters left.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/hashicorp/raft"
	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/errs"
)

var (
	// ErrMemberRecordExists refuses to forget a Raft server that has a
	// member record: decommission it instead.
	ErrMemberRecordExists = errors.New("the server has a member record; decommission it instead")
	// ErrServerNamedByAssignment refuses to forget a Raft server that a
	// partition assignment names as its owner or move target.
	ErrServerNamedByAssignment = errors.New("a partition assignment names the server")
	// ErrQuorumAtRisk refuses to forget a voter when the leader cannot
	// show that the voters left after the removal still make a quorum.
	ErrQuorumAtRisk = errors.New("forgetting the voter could leave the cluster without a quorum")
)

// ForgetServer removes the Raft server id, voter or non-voter, from the
// Raft configuration, when it has no member record and no partition
// assignment names it. voter reports whether it was a voter.
//
// Leader only: a follower gets an errs.ErrUnavailable wrapping
// raft.ErrNotLeader. It barriers first, so the checks read every entry
// committed before it. It refuses this node (errs.ErrInvalidArgument),
// an id not in the latest configuration (ErrNotFound), a server with a
// member record (ErrMemberRecordExists), one an assignment names
// (ErrServerNamedByAssignment), and a voter whose removal could leave
// the cluster without a quorum (ErrQuorumAtRisk, see
// forgetVoterRefusal). It is serialised with join admission, so a
// concurrent join cannot change the server's suffrage between the
// checks and the removal.
func (s *Store) ForgetServer(ctx context.Context, id string) (voter bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.admitMu.Lock()
	defer s.admitMu.Unlock()
	if s.r.State() != raft.Leader {
		return false, classifyRaftError(raft.ErrNotLeader)
	}
	if id == string(s.id) {
		return false, fmt.Errorf("%w: raft server %q is this node, the leader", errs.ErrInvalidArgument, id)
	}
	if err := s.Barrier(); err != nil {
		return false, classifyRaftError(err)
	}
	future := s.r.GetConfiguration()
	if err := future.Error(); err != nil {
		return false, classifyRaftError(err)
	}
	var found *raft.Server
	for _, srv := range future.Configuration().Servers {
		if srv.ID == raft.ServerID(id) {
			found = &srv
			break
		}
	}
	if found == nil {
		return false, fmt.Errorf("%w: raft server %q is not in the raft configuration", ErrNotFound, id)
	}
	switch _, err := s.GetMember(id); {
	case err == nil:
		return false, fmt.Errorf("%w: raft server %q", ErrMemberRecordExists, id)
	case !errors.Is(err, ErrNotFound):
		return false, err
	}
	named, err := s.assignmentNaming(id)
	if err != nil {
		return false, err
	}
	if named != "" {
		return false, fmt.Errorf("%w: raft server %q (%s)", ErrServerNamedByAssignment, id, named)
	}
	voter = found.Suffrage == raft.Voter
	if voter {
		if reason := s.forgetVoterRefusal(future.Configuration(), found.ID); reason != "" {
			return false, fmt.Errorf("%w: raft server %q: %s", ErrQuorumAtRisk, id, reason)
		}
	}
	if err := s.RemoveServer(id); err != nil {
		return false, classifyRaftError(err)
	}
	s.log.Warn("forgot a raft server with no member record",
		"component", "audit", "id", id, "address", string(found.Address), "voter", voter)
	return voter, nil
}

// forgetVoterRefusal returns why the voter id may not be removed from
// cfg now, or "" when it may: the voters left must keep a quorum the
// leader can reach. The leader counts itself and each voter left whose
// heartbeats it is not failing (raftHealth.failingSince), and needs a
// strict majority of the voters left. A leader that has led for less
// than promotionSettle has not had time to see a failed heartbeat to a
// peer that drops packets, so it refuses until it has.
func (s *Store) forgetVoterRefusal(cfg raft.Configuration, id raft.ServerID) string {
	if led := s.health.leaderFor(); led < promotionSettle {
		return fmt.Sprintf("the leader has led for less than %s, too short to have seen a failed heartbeat to an unreachable voter; ask again", promotionSettle)
	}
	left, reached := 0, 0
	var failing []string
	for _, srv := range cfg.Servers {
		if srv.Suffrage != raft.Voter || srv.ID == id {
			continue
		}
		left++
		if srv.ID == s.id {
			reached++
			continue
		}
		if _, bad := s.health.failingSince(srv.ID); bad {
			failing = append(failing, string(srv.ID))
			continue
		}
		reached++
	}
	if 2*reached > left {
		return ""
	}
	return fmt.Sprintf("the %d voters left would need %d to commit, and the leader reaches only %d (its raft heartbeats to %s are failing); bring them back first",
		left, left/2+1, reached, strings.Join(failing, ", "))
}

// assignmentNaming returns the first partition (as "topic/partition")
// whose assignment names id as owner or move target, or "".
func (s *Store) assignmentNaming(id string) (string, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var named string
	err := s.fsm.view(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketAssignments).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			var a Assignment
			if err := json.Unmarshal(v, &a); err != nil {
				return err
			}
			if a.OwnerID == id || a.TargetID == id {
				named = fmt.Sprintf("%s/%d", a.Topic, a.Partition)
				return nil
			}
		}
		return nil
	})
	return named, err
}

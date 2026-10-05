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

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
)

// ForgetServer removes the Raft server id, voter or non-voter, from the
// Raft configuration, when it has no member record and no partition
// assignment names it. voter reports whether it was a voter.
//
// Leader only: a follower gets an errs.ErrUnavailable wrapping
// raft.ErrNotLeader. It barriers first, so the checks read every entry
// committed before it. It refuses this node (errs.ErrInvalidArgument),
// an id not in the latest configuration (ErrNotFound), a server with a
// member record (ErrMemberRecordExists) and one an assignment names
// (ErrServerNamedByAssignment). It is serialised with join admission,
// so a concurrent join cannot change the server's suffrage between the
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
	if err := s.RemoveServer(id); err != nil {
		return false, classifyRaftError(err)
	}
	s.log.Warn("forgot a raft server with no member record",
		"component", "audit", "id", id, "address", string(found.Address), "voter", voter)
	return voter, nil
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

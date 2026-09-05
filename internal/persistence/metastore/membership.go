package metastore

import (
	"context"
	"errors"

	bolt "go.etcd.io/bbolt"
)

// ErrMemberRemoved is returned when a registration names a member that
// was removed by decommission and not readmitted since.
var ErrMemberRemoved = errors.New("metastore: member was removed from the cluster")

// RemoveMember deletes the member record and tombstones the ID through
// Raft. The controller applies it right after RemoveServer succeeds (or
// when it finds a draining member already out of the voter set), so a
// decommissioned node stops being listed and its heartbeats are refused
// from then on. at is Unix seconds, recorded on the tombstone.
func (s *Store) RemoveMember(ctx context.Context, podID string, at int64) error {
	return s.apply(ctx, opRemoveMember, memberRemovalPayload{ID: podID, At: at})
}

// ReadmitMember clears a removal tombstone through Raft so a node of
// that ID can register again. The join handler applies it when it
// admits a fresh (stateless) node under a tombstoned ID.
func (s *Store) ReadmitMember(ctx context.Context, podID string) error {
	return s.apply(ctx, opReadmitMember, memberRemovalPayload{ID: podID})
}

// MemberRemoved reports whether podID carries a removal tombstone in
// the local replica.
func (s *Store) MemberRemoved(podID string) (bool, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var removed bool
	err := s.fsm.view(func(tx *bolt.Tx) error {
		removed = memberTombstoned(tx, podID)
		return nil
	})
	return removed, err
}

package metastore

import (
	"context"
	"encoding/json"
	"errors"

	bolt "go.etcd.io/bbolt"
)

// RegisterMember creates or replaces the member record for m.ID through
// Raft.
func (s *Store) RegisterMember(ctx context.Context, m Member) error {
	return s.apply(ctx, opMemberJoin, m)
}

// Heartbeat records a liveness timestamp (Unix seconds) for the member
// through Raft. It returns ErrNotFound if the member is not registered.
func (s *Store) Heartbeat(ctx context.Context, podID string, at int64) error {
	return s.apply(ctx, opMemberHeartbeat, heartbeatPayload{ID: podID, At: at})
}

// MarkMemberDead sets the member's status to MemberDead through Raft,
// deciding from the heartbeat the local replica holds for it (see
// MarkMemberDeadObserved). It returns ErrNotFound if the member is not
// registered.
func (s *Store) MarkMemberDead(ctx context.Context, podID string) error {
	m, err := s.GetMember(podID)
	if err != nil {
		return err
	}
	return s.MarkMemberDeadObserved(ctx, podID, m.LastHeartbeat)
}

// MarkMemberDeadObserved marks the member dead through Raft, carrying
// observed, the LastHeartbeat (Unix seconds) the decision was made from.
// Once every member applies MarkMemberDeadIf the mark is refused with
// ErrMemberHeartbeatNewer when a newer heartbeat committed ahead of it:
// the member is alive. Until then it is the unconditional mark.
func (s *Store) MarkMemberDeadObserved(ctx context.Context, podID string, observed int64) error {
	err := s.MarkMemberDeadIf(ctx, podID, observed)
	if errors.Is(err, ErrEntryTypeNotYetUsable) {
		return s.apply(ctx, opMemberDead, podID)
	}
	return err
}

// MarkMemberDeadIf marks the member dead through Raft unless its
// heartbeat on record is newer than observed (ErrMemberHeartbeatNewer).
// While some member does not apply it, it proposes nothing and returns
// ErrEntryTypeNotYetUsable.
func (s *Store) MarkMemberDeadIf(ctx context.Context, podID string, observed int64) error {
	return s.applyIfUsable(ctx, opMarkMemberDeadIf, markMemberDeadIfPayload{ID: podID, Observed: observed})
}

// SetMemberDraining marks (or unmarks) a member as draining through Raft.
// A draining member keeps serving but the rebalance planner stops sending
// it partitions and sheds everything it owns onto the other live nodes —
// the placement half of decommission. It returns ErrNotFound if the member
// is not registered.
func (s *Store) SetMemberDraining(ctx context.Context, podID string, draining bool) error {
	return s.apply(ctx, opSetMemberDraining, memberDrainingPayload{ID: podID, Draining: draining})
}

// GetMember reads the member from the local replica. It returns
// ErrNotFound if the member is not registered.
func (s *Store) GetMember(podID string) (Member, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var m Member
	err := s.fsm.view(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketMembers).Get([]byte(podID))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &m)
	})
	return m, err
}

// ListMembers reads all registered members, alive and dead, from the
// local replica.
func (s *Store) ListMembers() ([]Member, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []Member
	err := s.fsm.view(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMembers).ForEach(func(_, v []byte) error {
			var m Member
			if err := json.Unmarshal(v, &m); err != nil {
				return err
			}
			out = append(out, m)
			return nil
		})
	})
	return out, err
}

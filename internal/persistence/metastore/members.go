package metastore

import (
	"context"
	"encoding/json"

	bolt "go.etcd.io/bbolt"
)

func (s *Store) RegisterMember(ctx context.Context, m Member) error {
	return s.apply(ctx, opMemberJoin, m)
}

func (s *Store) Heartbeat(ctx context.Context, podID string, at int64) error {
	return s.apply(ctx, opMemberHeartbeat, heartbeatPayload{ID: podID, At: at})
}

func (s *Store) MarkMemberDead(ctx context.Context, podID string) error {
	return s.apply(ctx, opMemberDead, podID)
}

func (s *Store) GetMember(podID string) (Member, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var m Member
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketMembers).Get([]byte(podID))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &m)
	})
	return m, err
}

func (s *Store) ListMembers() ([]Member, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []Member
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
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

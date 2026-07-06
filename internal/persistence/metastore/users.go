package metastore

import (
	"context"
	"encoding/json"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

// CreateUser creates u through Raft. It returns ErrAlreadyExists if a
// user with the same username exists. The root flag is ignored here —
// only SeedRootUser may create a root account.
func (s *Store) CreateUser(ctx context.Context, u user.User) error {
	return s.apply(ctx, opCreateUser, u)
}

// SeedRootUser creates the root admin. It is the only path that may
// persist a root account, and it is idempotent: if the user already
// exists it returns ErrAlreadyExists without modifying it.
func (s *Store) SeedRootUser(ctx context.Context, u user.User) error {
	return s.apply(ctx, opSeedRootUser, u)
}

// UpdateUser replaces the stored record for u.Username through Raft. It
// returns ErrNotFound if the user does not exist.
func (s *Store) UpdateUser(ctx context.Context, u user.User) error {
	return s.apply(ctx, opUpdateUser, u)
}

// DeleteUser removes the user through Raft. It returns ErrNotFound if
// the user does not exist.
func (s *Store) DeleteUser(ctx context.Context, username string) error {
	return s.apply(ctx, opDeleteUser, username)
}

// GetUser reads the user from the local replica. It returns ErrNotFound
// if the user does not exist.
func (s *Store) GetUser(_ context.Context, username string) (user.User, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var u user.User
	err := s.fsm.view(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketUsers).Get([]byte(username))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &u)
	})
	return u, err
}

// ListUsers reads every user from the local replica in username order.
func (s *Store) ListUsers(_ context.Context) ([]user.User, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []user.User
	err := s.fsm.view(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketUsers).ForEach(func(_, v []byte) error {
			var u user.User
			if err := json.Unmarshal(v, &u); err != nil {
				return err
			}
			out = append(out, u)
			return nil
		})
	})
	return out, err
}

// HasUsers reports whether at least one user exists on the local
// replica. Seeding uses it to decide whether the root admin is needed.
func (s *Store) HasUsers(_ context.Context) (bool, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	found := false
	err := s.fsm.view(func(tx *bolt.Tx) error {
		k, _ := tx.Bucket(bucketUsers).Cursor().First()
		found = k != nil
		return nil
	})
	return found, err
}

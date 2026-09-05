package metastore

import (
	"context"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
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
//
// Prefer SetUserPassword / SetUserGrants: a whole-record replace built
// from a read of the local replica carries every field that replica
// happened to hold, so an update proposed from a lagging follower can
// undo a change the leader already committed (a password change
// resurrecting revoked grants). UpdateUser remains for callers that
// really do mean "replace everything" and for the legacy forward path.
func (s *Store) UpdateUser(ctx context.Context, u user.User) error {
	return s.apply(ctx, opUpdateUser, u)
}

// SetUserPassword replaces only the password hash of an existing user
// through Raft, applied against the record as the FSM sees it. It
// returns ErrNotFound if the user does not exist.
func (s *Store) SetUserPassword(ctx context.Context, username string, hash []byte, updatedAtMs int64) error {
	return s.apply(ctx, opSetUserPassword, userPasswordPayload{Username: username, PasswordHash: hash, UpdatedAtMs: updatedAtMs})
}

// SetUserGrants replaces only the grants of an existing user through
// Raft. It returns ErrNotFound if the user does not exist and
// ErrRootProtected for the root account, whose grants are immutable.
func (s *Store) SetUserGrants(ctx context.Context, username string, grants []user.Grant, updatedAtMs int64) error {
	return s.apply(ctx, opSetUserGrants, userGrantsPayload{Username: username, Grants: grants, UpdatedAtMs: updatedAtMs})
}

// UserUpdateField names the one field a UserUpdate replaces.
type UserUpdateField string

// The field-scoped user updates.
const (
	UserUpdatePassword UserUpdateField = "password"
	UserUpdateGrants   UserUpdateField = "grants"
)

// UserUpdate is the wire shape of a user update forwarded from an HTTP
// ingress node to the cluster leader. Field selects which field of User
// to apply; the rest of User is ignored for that field's op. An empty
// Field is the legacy whole-record replace, kept so a follower running
// an older build can still forward to a newer leader. User is embedded
// so the JSON is a superset of the old body (a plain user.User).
type UserUpdate struct {
	Field UserUpdateField `json:"field,omitempty"`
	user.User
}

// ApplyUserUpdate dispatches a UserUpdate to the matching Raft op.
func (s *Store) ApplyUserUpdate(ctx context.Context, upd UserUpdate) error {
	switch upd.Field {
	case UserUpdatePassword:
		return s.SetUserPassword(ctx, upd.Username, upd.PasswordHash, upd.UpdatedAtMs)
	case UserUpdateGrants:
		return s.SetUserGrants(ctx, upd.Username, upd.Grants, upd.UpdatedAtMs)
	case "":
		return s.UpdateUser(ctx, upd.User)
	default:
		return fmt.Errorf("%w: unknown user update field %q", errs.ErrInvalidArgument, upd.Field)
	}
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

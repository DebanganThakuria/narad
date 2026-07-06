package metastore

// User apply handlers. Same determinism contract as fsm_apply.go: these
// run on Raft's FSM goroutine on every node and must produce identical
// bbolt state and identical business errors for identical inputs. The
// users domain version bumps only after a successful update so auth
// caches never re-read a state that did not change.

import (
	"encoding/json"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/user"
)

// Root-account invariants are enforced HERE, at the Raft chokepoint every
// write path funnels through — the HTTP handlers, leader-forwarded RPC,
// and any future caller — rather than only in the HTTP layer. A caller
// who can reach the cluster RPC port (secured by the cluster secret) thus
// still cannot mint a root admin, flip a user's root flag, tamper with
// root's grants, or delete root. The seed path uses opSeedRootUser, the
// only op that may persist Root=true.

func (f *fsmState) applyCreateUser(data []byte) error {
	var u user.User
	if err := json.Unmarshal(data, &u); err != nil {
		return err
	}
	// Root is never conferred through the public create path; only
	// applySeedRootUser may set it.
	u.Root = false
	return f.putNewUser(u)
}

// applySeedRootUser creates the root admin. It is idempotent: if any user
// with this name already exists it returns ErrAlreadyExists, so a restart
// or a second node never clobbers an operator-changed root password.
func (f *fsmState) applySeedRootUser(data []byte) error {
	var u user.User
	if err := json.Unmarshal(data, &u); err != nil {
		return err
	}
	u.Root = true
	return f.putNewUser(u)
}

func (f *fsmState) putNewUser(u user.User) error {
	encoded, err := json.Marshal(u)
	if err != nil {
		return err
	}
	err = f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketUsers)
		if b.Get([]byte(u.Username)) != nil {
			return ErrAlreadyExists
		}
		return b.Put([]byte(u.Username), encoded)
	})
	if err == nil {
		f.versions.bumpUsers()
	}
	return err
}

func (f *fsmState) applyUpdateUser(data []byte) error {
	var u user.User
	if err := json.Unmarshal(data, &u); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketUsers)
		raw := b.Get([]byte(u.Username))
		if raw == nil {
			return ErrNotFound
		}
		var current user.User
		if err := json.Unmarshal(raw, &current); err != nil {
			return err
		}
		// The root flag is immutable via update: a non-root user can
		// never be escalated to root, and root can never be demoted.
		u.Root = current.Root
		// Root's grants are immutable — only its password may change.
		if current.Root {
			u.Grants = current.Grants
		}
		encoded, err := json.Marshal(u)
		if err != nil {
			return err
		}
		return b.Put([]byte(u.Username), encoded)
	})
	if err == nil {
		f.versions.bumpUsers()
	}
	return err
}

func (f *fsmState) applyDeleteUser(data []byte) error {
	var username string
	if err := json.Unmarshal(data, &username); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketUsers)
		raw := b.Get([]byte(username))
		if raw == nil {
			return ErrNotFound
		}
		var current user.User
		if err := json.Unmarshal(raw, &current); err != nil {
			return err
		}
		if current.Root {
			return ErrRootProtected
		}
		return b.Delete([]byte(username))
	})
	if err == nil {
		f.versions.bumpUsers()
	}
	return err
}

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

func (f *fsmState) applyCreateUser(data []byte) error {
	var u user.User
	if err := json.Unmarshal(data, &u); err != nil {
		return err
	}
	err := f.update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketUsers)
		if b.Get([]byte(u.Username)) != nil {
			return ErrAlreadyExists
		}
		return b.Put([]byte(u.Username), data)
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
		if b.Get([]byte(u.Username)) == nil {
			return ErrNotFound
		}
		return b.Put([]byte(u.Username), data)
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
		if b.Get([]byte(username)) == nil {
			return ErrNotFound
		}
		return b.Delete([]byte(username))
	})
	if err == nil {
		f.versions.bumpUsers()
	}
	return err
}

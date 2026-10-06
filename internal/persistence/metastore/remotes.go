package metastore

import (
	"context"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	"github.com/debanganthakuria/narad/internal/errs"
)

// The remote entry types are newer than v3.1.0: each write below
// proposes nothing and returns ErrEntryTypeNotYetUsable, naming the
// member that holds it back, until every member applies it. There is
// no older entry to fall back to, so the caller refuses the request.

// PutRemote creates a remote through Raft.
func (s *Store) PutRemote(ctx context.Context, op PutRemoteOp) error {
	return s.applyIfUsable(ctx, opPutRemote, op)
}

// UpdateRemote applies a field-scoped remote change through Raft.
func (s *Store) UpdateRemote(ctx context.Context, op UpdateRemoteOp) error {
	return s.applyIfUsable(ctx, opUpdateRemote, op)
}

// DeleteRemote deletes a remote through Raft.
func (s *Store) DeleteRemote(ctx context.Context, op DeleteRemoteOp) error {
	return s.applyIfUsable(ctx, opDeleteRemote, op)
}

// GetRemote reads one remote from the local replica. It returns
// ErrNotFound (wrapping errs.ErrRemoteNotFound) when there is none.
func (s *Store) GetRemote(name string) (domremote.Record, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var r domremote.Record
	err := s.fsm.view(func(tx *bolt.Tx) error {
		raw, err := getRemoteRaw(tx, name)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, &r)
	})
	return r, err
}

// ListRemotes reads every remote from the local replica in name order,
// skipping the reserved key (the salt and seal counts).
func (s *Store) ListRemotes() ([]domremote.Record, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []domremote.Record
	err := s.fsm.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketRemotes)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			if reservedRemoteKey(string(k)) {
				return nil
			}
			var r domremote.Record
			if err := json.Unmarshal(v, &r); err != nil {
				return err
			}
			out = append(out, r)
			return nil
		})
	})
	return out, err
}

// RemoteKeys reads the reserved keys entry: the per-cluster salt and,
// per key version, the seal count and first seal time. The zero Keys
// means no remote was ever created.
func (s *Store) RemoteKeys() (domremote.Keys, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var k domremote.Keys
	err := s.fsm.view(func(tx *bolt.Tx) error {
		var err error
		k, err = readRemoteKeys(tx)
		return err
	})
	return k, err
}

// getRemoteRaw returns the stored bytes of one remote.
func getRemoteRaw(tx *bolt.Tx, name string) ([]byte, error) {
	b := tx.Bucket(bucketRemotes)
	if name == "" || reservedRemoteKey(name) || b == nil {
		return nil, notFoundRemote()
	}
	raw := b.Get([]byte(name))
	if raw == nil {
		return nil, notFoundRemote()
	}
	return raw, nil
}

// readRemoteKeys decodes the reserved keys entry.
func readRemoteKeys(tx *bolt.Tx) (domremote.Keys, error) {
	var k domremote.Keys
	b := tx.Bucket(bucketRemotes)
	if b == nil {
		return k, nil
	}
	raw := b.Get([]byte(domremote.KeysKey))
	if raw == nil {
		return k, nil
	}
	return k, json.Unmarshal(raw, &k)
}

// notFoundRemote is a missing remote: ErrNotFound for callers that
// classify generically, errs.ErrRemoteNotFound for the remote mapper.
func notFoundRemote() error {
	return remoteNotFoundError{}
}

type remoteNotFoundError struct{}

func (remoteNotFoundError) Error() string { return errs.ErrRemoteNotFound.Error() }

func (remoteNotFoundError) Is(target error) bool {
	return target == errs.ErrRemoteNotFound || target == ErrNotFound
}

// RemotesHeldBackError is RemotesUsable's refusal: the member (or Raft
// server without a member record) that does not apply the remote entry
// types yet, and why. It matches ErrEntryTypeNotYetUsable.
type RemotesHeldBackError struct {
	// Member is the holder's ID; empty when the members could not be read.
	Member string
	Reason string
}

func (e *RemotesHeldBackError) Error() string {
	return fmt.Sprintf("%v: raft entry types %d to %d (remotes and remote children): %s",
		ErrEntryTypeNotYetUsable, opAttachRemoteChild, opDeleteRemote, e.Reason)
}

// Unwrap exposes ErrEntryTypeNotYetUsable.
func (e *RemotesHeldBackError) Unwrap() error { return ErrEntryTypeNotYetUsable }

// RemotesUsable reports whether every member applies the remote entry
// types (opAttachRemoteChild to opDeleteRemote): nil, or a
// *RemotesHeldBackError naming the first member that holds them back.
// A member's report is the newest type it applies, so the newest remote
// type answers for all of them. It reads the member records (a member
// that is down counts with its last report), so it needs no member to
// answer. Callers ask it before work a refused proposal would waste,
// such as sealing a password.
func (s *Store) RemotesUsable() error {
	reports, err := s.entryTypeReports()
	if err != nil {
		return &RemotesHeldBackError{Reason: "the cluster's members could not be read: " + err.Error()}
	}
	for _, r := range reports {
		if r.entryTypes < uint32(opDeleteRemote) {
			return &RemotesHeldBackError{Member: r.id, Reason: r.holdsBack(uint32(opDeleteRemote))}
		}
	}
	return nil
}

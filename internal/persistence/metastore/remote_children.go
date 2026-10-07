package metastore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// AttachRemoteChild creates a remote child's stub under op.Parent and
// links it, through Raft, in one transaction (see
// applyAttachRemoteChild for the invariants). A zero StubID, Epoch or
// CreatedAt is minted here, on the proposer, so the apply stays
// deterministic. The caller resolves op.Offsets in the link's From mode.
// Nothing is proposed, and ErrEntryTypeNotYetUsable is returned, until
// every member applies the type.
func (s *Store) AttachRemoteChild(ctx context.Context, op AttachRemoteChildOp) error {
	if op.StubID == "" {
		id, err := newRandomHex8("stub id")
		if err != nil {
			return err
		}
		op.StubID = id
	}
	if op.Epoch == "" {
		epoch, err := newAttachEpoch()
		if err != nil {
			return err
		}
		op.Epoch = epoch
	}
	if op.CreatedAt == 0 {
		op.CreatedAt = time.Now().Unix()
	}
	return s.applyIfUsable(ctx, opAttachRemoteChild, op)
}

// SetRemoteChildState applies a field-scoped change to a remote child's
// link through Raft; it applies only while the stub's attach epoch is
// still op.Epoch. Like AttachRemoteChild it proposes nothing and
// returns ErrEntryTypeNotYetUsable until every member applies the type.
func (s *Store) SetRemoteChildState(ctx context.Context, op RemoteChildStateOp) error {
	return s.applyIfUsable(ctx, opSetRemoteChildState, op)
}

// RemoteChildrenOf returns the remote children that name remoteName, as
// "parent/child" pairs, from the local replica.
func (s *Store) RemoteChildrenOf(remoteName string) ([]string, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []string
	err := s.fsm.view(func(tx *bolt.Tx) error {
		var err error
		out, err = remoteChildrenNaming(tx, remoteName)
		return err
	})
	return out, err
}

// RemoteChildren returns every remote child's stub record, from the
// local replica, in name order.
func (s *Store) RemoteChildren() ([]topic.Topic, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []topic.Topic
	err := s.fsm.view(func(tx *bolt.Tx) error {
		return forEachStub(tx, func(stub topic.Topic) bool {
			stub.Role = stub.EffectiveRole()
			out = append(out, stub)
			return true
		})
	})
	return out, err
}

func newRandomHex8(what string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("metastore: %s: %w", what, err)
	}
	return hex.EncodeToString(b[:]), nil
}

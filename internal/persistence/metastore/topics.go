package metastore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// CreateTopic creates t through Raft. It returns ErrAlreadyExists if a
// topic with the same name exists.
func (s *Store) CreateTopic(ctx context.Context, t topic.Topic) error {
	return s.apply(ctx, opCreateTopic, t)
}

// UpdateTopic replaces the stored config for t.Name through Raft. It
// returns ErrNotFound if the topic does not exist.
func (s *Store) UpdateTopic(ctx context.Context, t topic.Topic) error {
	return s.apply(ctx, opUpdateTopic, t)
}

// DeleteTopic removes the topic through Raft, along with its schemas
// and partition assignments. It returns ErrNotFound if the topic does
// not exist.
func (s *Store) DeleteTopic(ctx context.Context, name string) error {
	return s.apply(ctx, opDeleteTopic, name)
}

// AttachChild links child under parent for fan-out through Raft. The
// FSM enforces every fan-out invariant atomically: both topics must
// exist, roles are exclusive and depth 1, a child has exactly one
// parent, the parent's child count is capped, and the child's schema
// must be absent (it adopts the parent's) or identical to the parent's.
// Each attach is stamped with a fresh epoch so fan-out cursor state
// from an earlier attachment can never be resumed.
func (s *Store) AttachChild(ctx context.Context, parent, child string) error {
	epoch, err := newAttachEpoch()
	if err != nil {
		return err
	}
	return s.apply(ctx, opAttachChild, childLinkPayload{Parent: parent, Child: child, Epoch: epoch})
}

// newAttachEpoch returns a random identifier for one attach. Generated
// by the proposer (not the FSM) so the Raft apply stays deterministic.
func newAttachEpoch() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("metastore: attach epoch: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// DetachChild unlinks child from parent through Raft. The child keeps
// its data and schema and becomes standalone. It returns ErrNotFound
// if the child is not attached to that parent.
func (s *Store) DetachChild(ctx context.Context, parent, child string) error {
	return s.apply(ctx, opDetachChild, childLinkPayload{Parent: parent, Child: child})
}

// GetTopic reads the topic from the local replica. It returns
// ErrNotFound if the topic does not exist.
func (s *Store) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var t topic.Topic
	err := s.fsm.view(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketTopics).Get([]byte(name))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &t)
	})
	if err != nil {
		return topic.Topic{}, err
	}
	t.Role = t.EffectiveRole()
	return t, nil
}

// ListTopics reads topics from the local replica in name order. With a
// positive opts.Limit it returns at most that many topics plus a page
// token for the next call; an empty token means the listing is complete.
func (s *Store) ListTopics(_ context.Context, opts ListOptions) ([]topic.Topic, string, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []topic.Topic
	var nextToken string
	err := s.fsm.view(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketTopics).Cursor()
		var k, v []byte
		if opts.PageToken != "" {
			k, v = c.Seek([]byte(opts.PageToken))
			// Only step past the token if it still exists; if it was
			// deleted, Seek already landed on the next topic.
			if k != nil && string(k) == opts.PageToken {
				k, v = c.Next()
			}
		} else {
			k, v = c.First()
		}
		for ; k != nil; k, v = c.Next() {
			if opts.Limit > 0 && len(out) >= opts.Limit {
				nextToken = out[len(out)-1].Name
				break
			}
			var t topic.Topic
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			t.Role = t.EffectiveRole()
			out = append(out, t)
		}
		return nil
	})
	return out, nextToken, err
}

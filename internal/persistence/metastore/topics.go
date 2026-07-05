package metastore

import (
	"context"
	"encoding/json"

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
	return t, err
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
			out = append(out, t)
		}
		return nil
	})
	return out, nextToken, err
}

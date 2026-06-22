package metastore

import (
	"context"
	"encoding/json"

	bolt "go.etcd.io/bbolt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func (s *Store) CreateTopic(ctx context.Context, t topic.Topic) error {
	return s.apply(ctx, opCreateTopic, t)
}

func (s *Store) UpdateTopic(ctx context.Context, t topic.Topic) error {
	return s.apply(ctx, opUpdateTopic, t)
}

func (s *Store) DeleteTopic(ctx context.Context, name string) error {
	return s.apply(ctx, opDeleteTopic, name)
}

func (s *Store) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var t topic.Topic
	err := s.fsm.view("get_topic", func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketTopics).Get([]byte(name))
		if v == nil {
			return ErrNotFound
		}
		return json.Unmarshal(v, &t)
	})
	return t, err
}

func (s *Store) ListTopics(_ context.Context, opts ListOptions) ([]topic.Topic, string, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []topic.Topic
	var nextToken string
	err := s.fsm.view("list_topics", func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketTopics).Cursor()
		var k, v []byte
		if opts.PageToken != "" {
			k, v = c.Seek([]byte(opts.PageToken))
			if k != nil {
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

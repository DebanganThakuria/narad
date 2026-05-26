package metastore

import (
	"context"

	bolt "go.etcd.io/bbolt"
)

func (s *Store) PutSchema(ctx context.Context, topicName string, version int, schema []byte) error {
	return s.apply(ctx, opPutSchema, schemaPayload{Topic: topicName, Version: version, Schema: schema})
}

func (s *Store) GetSchema(_ context.Context, topicName string, version int) ([]byte, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []byte
	err := s.fsm.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketSchemas).Get(schemaKey(topicName, version))
		if v == nil {
			return ErrNotFound
		}
		out = make([]byte, len(v))
		copy(out, v)
		return nil
	})
	return out, err
}

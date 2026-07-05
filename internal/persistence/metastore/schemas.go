package metastore

import (
	"context"

	bolt "go.etcd.io/bbolt"
)

// PutSchema stores (or overwrites) a schema version for a topic through
// Raft.
func (s *Store) PutSchema(ctx context.Context, topicName string, version int, schema []byte) error {
	return s.apply(ctx, opPutSchema, schemaPayload{Topic: topicName, Version: version, Schema: schema})
}

// GetSchema reads a schema version from the local replica. It returns
// ErrNotFound if that version does not exist. The result is a copy:
// bbolt values are only valid inside their transaction.
func (s *Store) GetSchema(_ context.Context, topicName string, version int) ([]byte, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var out []byte
	err := s.fsm.view(func(tx *bolt.Tx) error {
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

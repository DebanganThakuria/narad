package metastore

import (
	"bytes"
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

// LatestSchema returns the topic's latest persisted schema version and
// a copy of its bytes, or version 0 and nil when the topic has none.
// It reads what schema.PersistedHistory would end with (the last of
// the contiguous versions from 1) in one transaction, copying only
// that version, so the produce path's hydrate costs one read however
// long the history is.
func (s *Store) LatestSchema(_ context.Context, topicName string) (int, []byte, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var (
		latest int
		out    []byte
	)
	err := s.fsm.view(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketSchemas)
		var raw []byte
		for version := 1; ; version++ {
			v := b.Get(schemaKey(topicName, version))
			if v == nil {
				break
			}
			latest, raw = version, v
		}
		if latest > 0 {
			out = bytes.Clone(raw)
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	return latest, out, nil
}

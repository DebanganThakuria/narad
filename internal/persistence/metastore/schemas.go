package metastore

import (
	"bytes"
	"context"
	"strconv"

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
		// schemaKey's "<topic>:<version>", built once and rewritten in
		// place, and looked up through one cursor: a key and a lookup
		// allocated per version were most of a long history's cost, and
		// a topic describe reads the schema through here on every GET.
		key := make([]byte, 0, len(topicName)+1+20)
		key = append(key, topicName...)
		key = append(key, ':')
		prefix := len(key)
		c := tx.Bucket(bucketSchemas).Cursor()
		var raw []byte
		for version := 1; ; version++ {
			key = strconv.AppendInt(key[:prefix], int64(version), 10)
			k, v := c.Seek(key)
			if v == nil || !bytes.Equal(k, key) {
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

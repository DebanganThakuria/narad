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

// SchemaBytes reports, from the local replica, how many bytes the
// topic's stored schema versions hold together. Topic names cannot
// contain ':', so the "<topic>:" prefix scan is exact. The topic
// manager reads it to refuse a schema write that would take a history
// past its byte budget before proposing it.
func (s *Store) SchemaBytes(_ context.Context, topicName string) (int64, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var total int64
	err := s.fsm.view(func(tx *bolt.Tx) error {
		prefix := []byte(topicName + ":")
		c := tx.Bucket(bucketSchemas).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			total += int64(len(v))
		}
		return nil
	})
	return total, err
}

// ClusterSchemaBytes reports, from the local replica, how many bytes
// every stored schema version of every topic holds together, fan-out
// children's copies included. It walks keys only: a value's length is
// read from its leaf element, so large values' overflow pages are not
// touched.
func (s *Store) ClusterSchemaBytes(_ context.Context) (int64, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	var total int64
	err := s.fsm.view(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketSchemas).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			total += int64(len(v))
		}
		return nil
	})
	return total, err
}

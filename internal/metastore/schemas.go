package metastore

import "context"

// PutSchema stores the raw schema bytes under (topic, version). The
// payload is copied so the caller is free to mutate the slice afterward.
func (s *JSONFileStore) PutSchema(_ context.Context, topic string, version int, schema []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	versions, ok := s.state.Schemas[topic]
	if !ok {
		versions = map[int][]byte{}
		s.state.Schemas[topic] = versions
	}
	versions[version] = append([]byte(nil), schema...) // detach from caller
	return s.flush()
}

// GetSchema returns the raw schema bytes for (topic, version). Returns
// ErrNotFound if no such schema exists.
func (s *JSONFileStore) GetSchema(_ context.Context, topic string, version int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	versions, ok := s.state.Schemas[topic]
	if !ok {
		return nil, ErrNotFound
	}
	schema, ok := versions[version]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), schema...), nil
}

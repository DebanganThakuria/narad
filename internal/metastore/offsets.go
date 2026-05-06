package metastore

import "context"

// GetConsumerOffset returns the last committed offset for a partition.
// Returns ErrNotFound if no commit has been recorded yet — callers
// should treat that as "start from offset 0".
func (s *JSONFileStore) GetConsumerOffset(_ context.Context, topic string, partition int) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	parts, ok := s.state.Offsets[topic]
	if !ok {
		return 0, ErrNotFound
	}
	off, ok := parts[partition]
	if !ok {
		return 0, ErrNotFound
	}
	return off, nil
}

// SetConsumerOffset records the committed offset for a partition. The
// caller is responsible for monotonicity; this method overwrites
// blindly.
func (s *JSONFileStore) SetConsumerOffset(_ context.Context, topic string, partition int, offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	parts, ok := s.state.Offsets[topic]
	if !ok {
		parts = map[int]int64{}
		s.state.Offsets[topic] = parts
	}
	parts[partition] = offset
	return s.flush()
}

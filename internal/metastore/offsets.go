package metastore

import "context"

func (s *SQLiteStore) GetConsumerOffset(_ context.Context, topicName string, partition int) (int64, error) {
	if v, ok := s.offsets.get(topicName, partition); ok {
		return v, nil
	}
	return 0, ErrNotFound
}

func (s *SQLiteStore) SetConsumerOffset(_ context.Context, topicName string, partition int, offset int64) error {
	s.offsets.set(topicName, partition, offset)
	return nil
}
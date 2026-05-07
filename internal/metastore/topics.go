package metastore

import (
	"context"
	"errors"

	"github.com/debanganthakuria/narad/internal/topic"
)

func (s *JSONFileStore) CreateTopic(_ context.Context, t topic.Topic) error {
	if t.Name == "" {
		return errors.New("metastore: topic name required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.state.Topics[t.Name]; exists {
		return ErrAlreadyExists
	}
	s.state.Topics[t.Name] = t
	return s.flush()
}

// UpdateTopic replaces an existing topic record. Returns ErrNotFound
// if no topic with that name is registered. Callers (the broker)
// validate business rules; the metastore enforces only existence and
// persistence.
func (s *JSONFileStore) UpdateTopic(_ context.Context, t topic.Topic) error {
	if t.Name == "" {
		return errors.New("metastore: topic name required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.state.Topics[t.Name]; !exists {
		return ErrNotFound
	}
	s.state.Topics[t.Name] = t
	return s.flush()
}

// DeleteTopic removes the topic record, all consumer offsets, and all
// schema versions. Callers must stop in-flight traffic and remove
// on-disk segment data BEFORE calling this; we only delete metadata.
func (s *JSONFileStore) DeleteTopic(_ context.Context, name string) error {
	if name == "" {
		return errors.New("metastore: topic name required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.state.Topics[name]; !exists {
		return ErrNotFound
	}
	delete(s.state.Topics, name)
	delete(s.state.Offsets, name)
	delete(s.state.Schemas, name)
	return s.flush()
}

func (s *JSONFileStore) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.state.Topics[name]
	if !ok {
		return topic.Topic{}, ErrNotFound
	}
	return t, nil
}

func (s *JSONFileStore) ListTopics(_ context.Context) ([]topic.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]topic.Topic, 0, len(s.state.Topics))
	for _, t := range s.state.Topics {
		out = append(out, t)
	}
	return out, nil
}

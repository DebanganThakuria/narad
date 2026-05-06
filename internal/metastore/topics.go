package metastore

import (
	"context"
	"errors"

	"github.com/debanganthakuria/narad/internal/topic"
)

// CreateTopic registers a new topic. Returns ErrAlreadyExists if a topic
// with the same name is already present.
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

// GetTopic returns the topic record by name. Returns ErrNotFound if no
// such topic exists.
func (s *JSONFileStore) GetTopic(_ context.Context, name string) (topic.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	t, ok := s.state.Topics[name]
	if !ok {
		return topic.Topic{}, ErrNotFound
	}
	return t, nil
}

// ListTopics returns every registered topic. Order is unspecified.
func (s *JSONFileStore) ListTopics(_ context.Context) ([]topic.Topic, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]topic.Topic, 0, len(s.state.Topics))
	for _, t := range s.state.Topics {
		out = append(out, t)
	}
	return out, nil
}

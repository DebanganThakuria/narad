package topics

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

type PurgeError struct {
	Topic string
	Err   error
}

func (e PurgeError) Error() string {
	return fmt.Sprintf("topics: purge %q after metadata delete: %v", e.Topic, e.Err)
}

func (e PurgeError) Unwrap() error {
	return e.Err
}

// DeleteTopic removes a topic and all of its data: closes cached
// partition logs (each does a final flush), drops in-flight
// reservations, removes the on-disk directory, and wipes the
// metastore record + offsets + schemas. Irreversible.
func (m *Manager) DeleteTopic(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: name required", ErrInvalid)
	}
	unlock := m.lockTopicName(name)
	defer unlock()

	if _, err := m.GetTopic(ctx, name); err != nil {
		return err
	}

	if err := m.metastore.DeleteTopic(ctx, name); err != nil {
		return err
	}
	if err := m.purgeTopicLocked(ctx, name); err != nil {
		return PurgeError{Topic: name, Err: err}
	}
	m.logger.Info("topic deleted", "topic", name)
	return nil
}

// PurgeTopic drops all local state for a topic (cached logs, in-flight
// reservations, in-memory schemas, on-disk files). Also invoked
// directly via the cluster purge broadcast on non-coordinating nodes.
func (m *Manager) PurgeTopic(ctx context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: name required", ErrInvalid)
	}
	unlock := m.lockTopicName(name)
	defer unlock()
	return m.purgeTopicLocked(ctx, name)
}

// purgeTopicLocked is PurgeTopic's body; callers must hold the topic's
// name lock.
func (m *Manager) purgeTopicLocked(ctx context.Context, name string) error {
	dir, err := m.topicDir(name)
	if err != nil {
		return err
	}
	firstErr := m.logs.CloseTopic(name)
	m.offsets.DropTopic(name)
	if err := m.schemas.DropTopic(ctx, name); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("topics: drop topic schemas: %w", err)
	}
	if err := os.RemoveAll(dir); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("topics: remove topic dir: %w", err)
	}
	return firstErr
}

// topicDir resolves the on-disk directory for a topic and verifies —
// defense in depth behind validateTopicName — that it is strictly a
// direct child of dataDir/topics. A crafted name like ".." would
// otherwise resolve to the data dir itself and RemoveAll would wipe it;
// "." would resolve to the topics root and purge every topic.
func (m *Manager) topicDir(name string) (string, error) {
	root := filepath.Join(m.dataDir, "topics")
	dir := filepath.Join(root, name)
	if dir == root || filepath.Dir(dir) != root {
		return "", fmt.Errorf("%w: topic name %q escapes the topics directory", ErrInvalid, name)
	}
	return dir, nil
}

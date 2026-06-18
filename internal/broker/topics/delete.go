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
	if _, err := m.GetTopic(ctx, name); err != nil {
		return err
	}

	if err := m.metastore.DeleteTopic(ctx, name); err != nil {
		return err
	}
	if err := m.PurgeTopic(ctx, name); err != nil {
		return PurgeError{Topic: name, Err: err}
	}
	m.logger.Info("topic deleted", "topic", name)
	return nil
}

func (m *Manager) PurgeTopic(_ context.Context, name string) error {
	if name == "" {
		return fmt.Errorf("%w: name required", ErrInvalid)
	}
	firstErr := m.logs.CloseTopic(name)
	m.offsets.DropTopic(name)
	dir := filepath.Join(m.dataDir, "topics", name)
	if err := os.RemoveAll(dir); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("topics: remove topic dir: %w", err)
	}
	return firstErr
}

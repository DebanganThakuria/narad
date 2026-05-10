package topics

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

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

	firstErr := m.logs.CloseTopic(name)

	if err := m.metastore.DeleteTopic(ctx, name); err != nil {
		if !errors.Is(err, metastore.ErrNotFound) && firstErr == nil {
			firstErr = err
		}
	}
	m.offsets.DropTopic(name)

	dir := filepath.Join(m.dataDir, "topics", name)
	if err := os.RemoveAll(dir); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("topics: remove topic dir: %w", err)
	}

	if firstErr == nil {
		m.logger.Info("topic deleted", "topic", name)
	}
	return firstErr
}

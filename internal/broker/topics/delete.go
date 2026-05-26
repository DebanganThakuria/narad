package topics

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/debanganthakuria/narad/internal/errs"
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

	firstErr := error(nil)
	if err := m.metastore.DeleteTopic(ctx, name); err != nil {
		if !errors.Is(err, errs.ErrNotFound) {
			firstErr = err
		}
	}
	if err := m.PurgeTopic(ctx, name); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr == nil {
		m.logger.Info("topic deleted", "topic", name)
	}
	return firstErr
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

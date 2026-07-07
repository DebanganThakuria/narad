package topics

// Fan-out attach/detach. The invariants live in the metastore FSM,
// where both topic records are mutated in one transaction; this layer
// adds name validation and friendly not-found errors.

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/errs"
)

// AttachChild links child under parent so every message produced to
// parent is fanned out to child. The child receives only messages
// produced from the attach point forward (no backfill). A child with
// no schema adopts the parent's; a child whose schema differs from the
// parent's is rejected (errs.ErrFanoutSchemaMismatch). A positive
// delayMs makes the child a delay child; the delay is immutable while
// attached (detach and re-attach to change it).
func (m *Manager) AttachChild(ctx context.Context, parent, child string, delayMs int64) error {
	if err := validateTopicName(parent); err != nil {
		return err
	}
	if err := validateTopicName(child); err != nil {
		return err
	}
	if parent == child {
		return fmt.Errorf("%w: a topic cannot be attached to itself", ErrInvalid)
	}
	if delayMs < 0 {
		return fmt.Errorf("%w: delay_ms must be >= 0", ErrInvalid)
	}
	if err := m.checkTopicExists(ctx, parent); err != nil {
		return err
	}
	if err := m.checkTopicExists(ctx, child); err != nil {
		return err
	}
	if err := m.metastore.AttachChild(ctx, parent, child, delayMs); err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return fmt.Errorf("%w: %v", ErrNotFound, err)
		}
		return err
	}
	m.logger.Info("fan-out child attached", "parent", parent, "child", child, "delay_ms", delayMs)
	return nil
}

// DetachChild unlinks child from parent. The child keeps everything it
// already received (data and schema) and becomes standalone again.
func (m *Manager) DetachChild(ctx context.Context, parent, child string) error {
	if err := validateTopicName(parent); err != nil {
		return err
	}
	if err := validateTopicName(child); err != nil {
		return err
	}
	if err := m.metastore.DetachChild(ctx, parent, child); err != nil {
		if errors.Is(err, errs.ErrNotFound) {
			return fmt.Errorf("%w: %v", ErrNotFound, err)
		}
		return err
	}
	m.logger.Info("fan-out child detached", "parent", parent, "child", child)
	return nil
}

// checkTopicExists produces a not-found error that names the topic,
// which the raced-through FSM check cannot.
func (m *Manager) checkTopicExists(ctx context.Context, name string) error {
	if _, err := m.GetTopic(ctx, name); err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, errs.ErrNotFound) {
			return fmt.Errorf("%w: %q", ErrNotFound, name)
		}
		return err
	}
	return nil
}

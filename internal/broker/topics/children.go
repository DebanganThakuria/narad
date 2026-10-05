package topics

// Fan-out attach/detach. The invariants live in the metastore FSM,
// where both topic records are mutated in one transaction; this layer
// adds name validation, friendly not-found errors, and the leader's
// ownership re-check (audit H1) under both names' locks.

import (
	"context"
	"errors"
	"fmt"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

// AttachChild links child under parent so every message produced to
// parent is fanned out to child. The child receives only messages
// produced from the attach point forward (no backfill). A child with
// no schema adopts the parent's; a child whose schema differs from the
// parent's is rejected (errs.ErrFanoutSchemaMismatch). A positive
// delayMs makes the child a delay child; the delay is immutable while
// attached (detach and re-attach to change it).
//
// The request identity must manage BOTH topics as they stand under
// their name locks: an attach rewrites the child's schema history and
// pumps every parent record into it, so neither owner alone may link
// the other's topic.
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
	if err := validateFanoutChildName(child); err != nil {
		return err
	}
	if delayMs < 0 {
		return fmt.Errorf("%w: delay_ms must be >= 0", ErrInvalid)
	}
	if delayMs > topic.MaxFanoutDelayMs {
		return fmt.Errorf("%w: delay_ms (%d) exceeds the maximum of %d (1 year)",
			ErrInvalid, delayMs, topic.MaxFanoutDelayMs)
	}
	unlock := m.lockTopicNames(parent, child)
	defer unlock()
	if err := m.leaderBarrier(ctx); err != nil {
		return err
	}
	p, err := m.getExistingTopic(ctx, parent)
	if err != nil {
		return err
	}
	c, err := m.getExistingTopic(ctx, child)
	if err != nil {
		return err
	}
	if err := authorizeManage(ctx, p); err != nil {
		return err
	}
	if err := authorizeManage(ctx, c); err != nil {
		return err
	}
	// A child with no schema adopts a copy of the parent's history.
	childBytes, err := m.topicSchemaBytes(ctx, child)
	if err != nil {
		return err
	}
	if childBytes == 0 {
		if err := m.checkAdoptSchemaBudget(ctx, parent, child); err != nil {
			return err
		}
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
// Either side's owner (or an admin) may detach, checked under both
// names' locks against the topics as they stand.
func (m *Manager) DetachChild(ctx context.Context, parent, child string) error {
	if err := validateTopicName(parent); err != nil {
		return err
	}
	if err := validateTopicName(child); err != nil {
		return err
	}
	unlock := m.lockTopicNames(parent, child)
	defer unlock()
	if err := m.leaderBarrier(ctx); err != nil {
		return err
	}
	var sides []topic.Topic
	for _, name := range []string{parent, child} {
		t, err := m.getExistingTopic(ctx, name)
		switch {
		case err == nil:
			sides = append(sides, t)
		case !errors.Is(err, ErrNotFound):
			return err
		}
	}
	if len(sides) == 0 {
		return fmt.Errorf("%w: neither %q nor %q exists", ErrNotFound, parent, child)
	}
	if err := authorizeManageAny(ctx, sides...); err != nil {
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

// getExistingTopic reads the topic, with a not-found error that names
// it, which the raced-through FSM check cannot.
func (m *Manager) getExistingTopic(ctx context.Context, name string) (topic.Topic, error) {
	t, err := m.GetTopic(ctx, name)
	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, fmt.Errorf("%w: %q", ErrNotFound, name)
		}
		return topic.Topic{}, err
	}
	return t, nil
}

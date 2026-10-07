package topics

// Fan-out attach/detach. The invariants live in the metastore FSM,
// where both topic records are mutated in one transaction; this layer
// adds name validation, friendly not-found errors, and the leader's
// ownership re-check under both names' locks.

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
	for attempt := 1; ; attempt++ {
		p, c, err := m.attachSides(ctx, parent, child)
		if err != nil {
			return err
		}
		err = m.attachChildRecord(ctx, parent, child, delayMs, p.ID, c.ID)
		if m.retryTopicChanged(err, attempt, "attach", parent+"/"+child) {
			continue
		}
		if errors.Is(err, errs.ErrNotFound) {
			return fmt.Errorf("%w: %v", ErrNotFound, err)
		}
		if err != nil {
			return err
		}
		break
	}
	m.logger.Info("fan-out child attached", "parent", parent, "child", child, "delay_ms", delayMs)
	return nil
}

// attachSides reads both topics of an attach under their name locks,
// checks that the request identity manages both, and that a child with
// no schema of its own can adopt a copy of the parent's history within
// the schema byte budgets.
func (m *Manager) attachSides(ctx context.Context, parent, child string) (p, c topic.Topic, err error) {
	if p, err = m.getExistingTopic(ctx, parent); err != nil {
		return p, c, err
	}
	if c, err = m.getExistingTopic(ctx, child); err != nil {
		return p, c, err
	}
	if err := authorizeManage(ctx, p); err != nil {
		return p, c, err
	}
	if err := authorizeManage(ctx, c); err != nil {
		return p, c, err
	}
	childBytes, err := m.topicSchemaBytes(ctx, child)
	if err != nil {
		return p, c, err
	}
	if childBytes == 0 {
		if err := m.checkAdoptSchemaBudget(ctx, parent, child); err != nil {
			return p, c, err
		}
	}
	return p, c, nil
}

// DetachChild unlinks child from parent. The child keeps everything it
// already received (data and schema) and becomes standalone again.
// Either side's owner (or an admin) may detach, checked under both
// names' locks against the topics as they stand. A remote child's stub,
// as it stands under the locks, is refused
// (errs.ErrRemoteAwareDeleteRequired): only DetachRemoteChild, after
// the unshipped check, detaches one.
func (m *Manager) DetachChild(ctx context.Context, parent, child string) error {
	return m.detachChild(ctx, parent, child, func(_, c topic.Topic) error {
		if c.IsRemoteChild() {
			return refuseRemoteLinked(child, map[string]string{child: c.ID})
		}
		return nil
	})
}

// detachChild is DetachChild with a check of both sides as they stand
// under the locks, after the leader barrier; a side that is missing is
// the zero topic.
func (m *Manager) detachChild(ctx context.Context, parent, child string, check func(p, c topic.Topic) error) error {
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
	for attempt := 1; ; attempt++ {
		sides, err := m.detachSides(ctx, parent, child)
		if err != nil {
			return err
		}
		if err := check(sides[0], sides[1]); err != nil {
			return err
		}
		err = m.detachChildRecord(ctx, parent, child, sides[0].ID, sides[1].ID)
		if m.retryTopicChanged(err, attempt, "detach", parent+"/"+child) {
			continue
		}
		if errors.Is(err, errs.ErrNotFound) {
			return fmt.Errorf("%w: %v", ErrNotFound, err)
		}
		if err != nil {
			return err
		}
		break
	}
	m.logger.Info("fan-out child detached", "parent", parent, "child", child)
	return nil
}

// detachSides reads both topics of a detach under their name locks and
// checks that the request identity manages at least one of them. It
// returns the topics read, parent first; a side that is missing is the
// zero topic, with no incarnation (the detach is then refused as not
// found).
func (m *Manager) detachSides(ctx context.Context, parent, child string) (read [2]topic.Topic, err error) {
	var sides []topic.Topic
	for i, name := range []string{parent, child} {
		t, err := m.getExistingTopic(ctx, name)
		switch {
		case err == nil:
			sides = append(sides, t)
			read[i] = t
		case !errors.Is(err, ErrNotFound):
			return read, err
		}
	}
	if len(sides) == 0 {
		return read, fmt.Errorf("%w: neither %q nor %q exists", ErrNotFound, parent, child)
	}
	return read, authorizeManageAny(ctx, sides...)
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

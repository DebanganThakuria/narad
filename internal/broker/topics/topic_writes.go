package topics

// Topic writes that carry the incarnation they were checked against.
// The Manager reads a topic under its name lock, after the leader
// barrier, and checks the request against that record; once every
// member applies the compare-and-set entry types, the write carries the
// incarnation it read and the state machine refuses it
// (errs.ErrTopicChanged) when the stored topic is another one. Until
// then, and with a metastore that has no such writes (tests, embedded
// use), the Manager proposes the entries every member applies.
//
// A refusal is applied on the leader's own replica before the write
// returns, so the Manager reads the topic again, checks the request
// again and retries once; a second refusal is answered as 409.

import (
	"context"
	"errors"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// casStore is the metastore capability behind the compare-and-set topic
// writes (implemented by *metastore.Store). Each method returns
// metastore.ErrEntryTypeNotYetUsable, having proposed nothing, while
// some member of the cluster does not apply its entry type.
type casStore interface {
	CreateTopicWith(ctx context.Context, t topic.Topic, spec metastore.CreateTopicSpec) error
	UpdateTopicIf(ctx context.Context, t topic.Topic, expectID string) error
	DeleteTopicIf(ctx context.Context, name, expectID string) error
	PutSchemaIf(ctx context.Context, topicName string, version int, schema []byte, expectID string) error
	AttachChildIf(ctx context.Context, parent, child string, delayMs int64, parentID, childID string) error
	DetachChildIf(ctx context.Context, parent, child, parentID, childID string) error
}

// topicChangedAttempts bounds how often a write refused because the
// topic changed since it was read is read, checked and proposed again.
const topicChangedAttempts = 2

// retryTopicChanged reports whether err is a refusal because the topic
// changed since it was read and attempt may be followed by another; it
// logs the retry.
func (m *Manager) retryTopicChanged(err error, attempt int, op, name string) bool {
	if !errors.Is(err, errs.ErrTopicChanged) || attempt >= topicChangedAttempts {
		return false
	}
	m.logger.Warn("topic changed since it was read; reading it again", "op", op, "topic", name, "err", err)
	return true
}

// notUsable reports whether err says the compare-and-set write was not
// proposed because some member does not apply its entry type yet.
func notUsable(err error) bool {
	return errors.Is(err, metastore.ErrEntryTypeNotYetUsable)
}

// createTopicWith creates t (and its first schema version and fan-out
// link) as one Raft entry. single is false when that is not possible
// yet; nothing was proposed then, and the caller creates the topic step
// by step.
func (m *Manager) createTopicWith(ctx context.Context, t topic.Topic, spec metastore.CreateTopicSpec) (single bool, err error) {
	s, ok := m.metastore.(casStore)
	if !ok {
		return false, nil
	}
	err = s.CreateTopicWith(ctx, t, spec)
	if notUsable(err) {
		return false, nil
	}
	return true, err
}

// updateTopicRecord writes t over the topic read as incarnation readID.
func (m *Manager) updateTopicRecord(ctx context.Context, t topic.Topic, readID string) error {
	if s, ok := m.metastore.(casStore); ok {
		if err := s.UpdateTopicIf(ctx, t, readID); !notUsable(err) {
			return err
		}
	}
	return m.metastore.UpdateTopic(ctx, t)
}

// deleteTopicRecord deletes the topic read as incarnation readID.
func (m *Manager) deleteTopicRecord(ctx context.Context, name, readID string) error {
	if s, ok := m.metastore.(casStore); ok {
		if err := s.DeleteTopicIf(ctx, name, readID); !notUsable(err) {
			return err
		}
	}
	return m.metastore.DeleteTopic(ctx, name)
}

// putSchemaRecord appends a schema version to the topic read as
// incarnation readID.
func (m *Manager) putSchemaRecord(ctx context.Context, name string, version int, schema []byte, readID string) error {
	if s, ok := m.metastore.(casStore); ok {
		if err := s.PutSchemaIf(ctx, name, version, schema, readID); !notUsable(err) {
			return err
		}
	}
	return m.metastore.PutSchema(ctx, name, version, schema)
}

// attachChildRecord links child under parent, read as incarnations
// parentID and childID.
func (m *Manager) attachChildRecord(ctx context.Context, parent, child string, delayMs int64, parentID, childID string) error {
	if s, ok := m.metastore.(casStore); ok {
		if err := s.AttachChildIf(ctx, parent, child, delayMs, parentID, childID); !notUsable(err) {
			return err
		}
	}
	return m.metastore.AttachChild(ctx, parent, child, delayMs)
}

// detachChildRecord unlinks child from parent, read as incarnations
// parentID and childID.
func (m *Manager) detachChildRecord(ctx context.Context, parent, child, parentID, childID string) error {
	if s, ok := m.metastore.(casStore); ok {
		if err := s.DetachChildIf(ctx, parent, child, parentID, childID); !notUsable(err) {
			return err
		}
	}
	return m.metastore.DetachChild(ctx, parent, child)
}

// alterTopic reads the topic under the caller's name lock, checks the
// request identity against it, builds the new record from it with build
// and writes it over the incarnation it read. On a refusal because the
// topic changed since the read it reads, checks and builds again, once.
// It returns the record read and the record written.
func (m *Manager) alterTopic(ctx context.Context, name string, build func(current topic.Topic) (topic.Topic, error)) (current, updated topic.Topic, err error) {
	for attempt := 1; ; attempt++ {
		current, err = m.GetTopic(ctx, name)
		if err != nil {
			return topic.Topic{}, topic.Topic{}, err
		}
		if err := authorizeManage(ctx, current); err != nil {
			return topic.Topic{}, topic.Topic{}, err
		}
		updated, err = build(current)
		if err != nil {
			return topic.Topic{}, topic.Topic{}, err
		}
		err = m.updateTopicRecord(ctx, updated, current.ID)
		if m.retryTopicChanged(err, attempt, "alter", name) {
			continue
		}
		if errors.Is(err, errs.ErrNotFound) {
			return topic.Topic{}, topic.Topic{}, ErrNotFound
		}
		if err != nil {
			return topic.Topic{}, topic.Topic{}, err
		}
		return current, updated, nil
	}
}

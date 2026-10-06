package metastore

// Topic writes that carry what they were checked against. Each proposes
// an entry type newer than 3.0.x and so first asks whether every member
// applies it; while one does not, it proposes nothing and returns
// ErrEntryTypeNotYetUsable, and the caller uses CreateTopic, UpdateTopic,
// DeleteTopic, PutSchema, AttachChild or DetachChild instead. A refusal
// because the topic changed matches errs.ErrTopicChanged: it is applied
// on this node's replica before the call returns, so a caller that reads
// the topic again sees what changed.

import (
	"context"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// CreateTopicSpec is what a single-entry create carries besides the
// topic record.
type CreateTopicSpec struct {
	// Schema is the first schema version, validated and compacted by
	// the caller, or nil for none.
	Schema []byte
	// Parent, when set, creates the topic as a fan-out child of Parent.
	Parent string
	// ParentID is the parent incarnation the create was checked
	// against; the create is refused when the parent is now another.
	ParentID string
	// DelayMs makes the child a delay child.
	DelayMs int64
}

// CreateTopicWith creates t, its first schema version and its fan-out
// parent link as one Raft entry: all of it commits, or none of it. The
// attach point of a link (its epoch and the parent's committed tails) is
// resolved as AttachChild resolves it. A retry after a create whose
// answer was lost (a leader change as it committed) answers
// ErrAlreadyExists, which is then the truth. A name that differs from an
// existing topic's only in letter case is refused with an error matching
// errs.ErrTopicAlreadyExists, and a schema or an adopted parent history
// that would pass the schema byte budgets with errs.ErrSchemaHistoryFull.
func (s *Store) CreateTopicWith(ctx context.Context, t topic.Topic, spec CreateTopicSpec) error {
	if err := s.usable(opCreateTopicWith); err != nil {
		return err
	}
	p := createTopicWithPayload{Topic: t, Schema: spec.Schema}
	if spec.Parent != "" {
		epoch, err := newAttachEpoch()
		if err != nil {
			return err
		}
		offsets, err := s.resolveAttachOffsets(ctx, spec.Parent)
		if err != nil {
			return err
		}
		p.Link = &createLinkPayload{Parent: spec.Parent, ParentID: spec.ParentID, Epoch: epoch, DelayMs: spec.DelayMs, Offsets: offsets}
	}
	return s.apply(ctx, opCreateTopicWith, p)
}

// UpdateTopicIf replaces the config of t.Name with t if the stored
// topic is still incarnation expectID. The partition count only grows,
// and the incarnation, owner, creation time, visibility timeout and
// fan-out link stay as stored.
func (s *Store) UpdateTopicIf(ctx context.Context, t topic.Topic, expectID string) error {
	return s.applyIfUsable(ctx, opUpdateTopicIf, updateTopicIfPayload{Topic: t, ExpectID: expectID})
}

// DeleteTopicIf deletes the topic, as DeleteTopic does, if it is still
// incarnation expectID.
func (s *Store) DeleteTopicIf(ctx context.Context, name, expectID string) error {
	return s.applyIfUsable(ctx, opDeleteTopicIf, deleteTopicIfPayload{Name: name, ExpectID: expectID})
}

// PutSchemaIf appends schema version version to the topic, and to each
// fan-out child's copy, if the topic is still incarnation expectID and
// the versions fit the schema byte budgets.
func (s *Store) PutSchemaIf(ctx context.Context, topicName string, version int, schema []byte, expectID string) error {
	return s.applyIfUsable(ctx, opPutSchemaIf, putSchemaIfPayload{Topic: topicName, Version: version, Schema: schema, ExpectID: expectID})
}

// AttachChildIf links child under parent, as AttachChild does, if they
// are still incarnations parentID and childID. Their schema histories
// compare by JSON value, and a history the child adopts counts against
// the schema byte budgets.
func (s *Store) AttachChildIf(ctx context.Context, parent, child string, delayMs int64, parentID, childID string) error {
	if err := s.usable(opAttachChildIf); err != nil {
		return err
	}
	epoch, err := newAttachEpoch()
	if err != nil {
		return err
	}
	offsets, err := s.resolveAttachOffsets(ctx, parent)
	if err != nil {
		return err
	}
	return s.apply(ctx, opAttachChildIf, attachChildIfPayload{
		Link:     childLinkPayload{Parent: parent, Child: child, Epoch: epoch, DelayMs: delayMs, Offsets: offsets},
		ParentID: parentID,
		ChildID:  childID,
	})
}

// DetachChildIf unlinks child from parent, as DetachChild does, if they
// are still incarnations parentID and childID.
func (s *Store) DetachChildIf(ctx context.Context, parent, child, parentID, childID string) error {
	return s.applyIfUsable(ctx, opDetachChildIf, detachChildIfPayload{Parent: parent, Child: child, ParentID: parentID, ChildID: childID})
}

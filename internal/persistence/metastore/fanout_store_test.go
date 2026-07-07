package metastore_test

// Black-box coverage for AttachChild/DetachChild through a real Raft
// apply. Invariant-by-invariant coverage lives in the white-box FSM
// tests (fsm_fanout_test.go); this verifies the op plumbing, error
// surfacing across Apply, and version bumps end to end.

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func TestAttachDetachChildThroughRaft(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	for _, name := range []string{"parent", "child"} {
		if err := s.CreateTopic(ctx, topic.Topic{Name: name, Partitions: 3, RetentionMs: 3_600_000}); err != nil {
			t.Fatalf("CreateTopic(%s): %v", name, err)
		}
	}

	parentVerBefore := s.TopicVersion("parent")
	childVerBefore := s.TopicVersion("child")

	if err := s.AttachChild(ctx, "parent", "child", 0); err != nil {
		t.Fatalf("AttachChild: %v", err)
	}

	p, err := s.GetTopic(ctx, "parent")
	if err != nil {
		t.Fatalf("GetTopic(parent): %v", err)
	}
	if p.Role != topic.RoleParent || !slices.Equal(p.Children, []string{"child"}) {
		t.Fatalf("parent = %+v, want role=parent children=[child]", p)
	}
	c, err := s.GetTopic(ctx, "child")
	if err != nil {
		t.Fatalf("GetTopic(child): %v", err)
	}
	if c.Role != topic.RoleChild || c.Parent != "parent" {
		t.Fatalf("child = %+v, want role=child parent=parent", c)
	}
	if v := s.TopicVersion("parent"); v <= parentVerBefore {
		t.Fatalf("parent version = %d, want > %d", v, parentVerBefore)
	}
	if v := s.TopicVersion("child"); v <= childVerBefore {
		t.Fatalf("child version = %d, want > %d", v, childVerBefore)
	}

	// Business errors must surface through the Raft apply.
	if err := s.AttachChild(ctx, "parent", "child", 0); !errors.Is(err, metastore.ErrAlreadyExists) {
		t.Fatalf("duplicate AttachChild error = %v, want %v", err, metastore.ErrAlreadyExists)
	}
	if err := s.AttachChild(ctx, "parent", "ghost", 0); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("AttachChild(ghost) error = %v, want %v", err, metastore.ErrNotFound)
	}
	if err := s.AttachChild(ctx, "child", "parent", 0); !errors.Is(err, errs.ErrFanoutRoleConflict) {
		t.Fatalf("reversed AttachChild error = %v, want %v", err, errs.ErrFanoutRoleConflict)
	}

	if err := s.DetachChild(ctx, "parent", "child"); err != nil {
		t.Fatalf("DetachChild: %v", err)
	}
	p, err = s.GetTopic(ctx, "parent")
	if err != nil {
		t.Fatalf("GetTopic(parent) after detach: %v", err)
	}
	c, err = s.GetTopic(ctx, "child")
	if err != nil {
		t.Fatalf("GetTopic(child) after detach: %v", err)
	}
	if p.Role != topic.RoleStandalone || len(p.Children) != 0 || c.Role != topic.RoleStandalone || c.Parent != "" {
		t.Fatalf("after detach parent=%+v child=%+v, want both standalone", p, c)
	}

	if err := s.DetachChild(ctx, "parent", "child"); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("second DetachChild error = %v, want %v", err, metastore.ErrNotFound)
	}
}

// Reads must report an explicit standalone role for topics created
// without one (creates never carry a role).
func TestGetTopicNormalizesRole(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if err := s.CreateTopic(ctx, topic.Topic{Name: "plain", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	got, err := s.GetTopic(ctx, "plain")
	if err != nil {
		t.Fatalf("GetTopic: %v", err)
	}
	if got.Role != topic.RoleStandalone {
		t.Fatalf("GetTopic role = %q, want %q", got.Role, topic.RoleStandalone)
	}
	list, _, err := s.ListTopics(ctx, metastore.ListOptions{})
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if len(list) != 1 || list[0].Role != topic.RoleStandalone {
		t.Fatalf("ListTopics roles = %+v, want standalone", list)
	}
}

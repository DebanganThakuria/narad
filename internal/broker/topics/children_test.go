package topics

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

func TestAttachChild_ValidatesAndLinks(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 3}
	ms.topics["child"] = topic.Topic{Name: "child", Partitions: 3}
	manager := newTestManager(t, ms, nil)
	ctx := context.Background()

	if err := manager.AttachChild(ctx, "parent", "child", 0); err != nil {
		t.Fatalf("AttachChild() error = %v", err)
	}
	if got := ms.topics["parent"]; !got.IsParent() || len(got.Children) != 1 {
		t.Fatalf("parent after attach = %+v", got)
	}

	if err := manager.DetachChild(ctx, "parent", "child"); err != nil {
		t.Fatalf("DetachChild() error = %v", err)
	}
	if got := ms.topics["child"]; got.IsChild() {
		t.Fatalf("child after detach = %+v, want standalone", got)
	}
	if got := ms.topics["parent"]; got.IsParent() || len(got.Children) != 0 {
		t.Fatalf("parent after detach = %+v, want standalone with no children", got)
	}
}

func TestAttachChild_RejectsBadInput(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 3}
	ms.topics["child"] = topic.Topic{Name: "child", Partitions: 3}
	manager := newTestManager(t, ms, nil)
	ctx := context.Background()

	cases := []struct {
		name          string
		parent, child string
		wantErr       error
		wantSubstring string
	}{
		{"empty parent", "", "child", ErrInvalid, "name required"},
		{"bad child name", "parent", "bad/name", ErrInvalid, "topic name"},
		{"self attach", "parent", "parent", ErrInvalid, "attached to itself"},
		{"missing parent", "ghost", "child", ErrNotFound, `"ghost"`},
		{"missing child", "parent", "ghost", ErrNotFound, `"ghost"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := manager.AttachChild(ctx, tc.parent, tc.child, 0)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("AttachChild(%q, %q) error = %v, want %v", tc.parent, tc.child, err, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantSubstring) {
				t.Fatalf("AttachChild error %q does not name the problem %q", err, tc.wantSubstring)
			}
		})
	}
}

func TestDetachChild_MapsNotFound(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["parent"] = topic.Topic{Name: "parent", Partitions: 3}
	ms.topics["child"] = topic.Topic{Name: "child", Partitions: 3}
	manager := newTestManager(t, ms, nil)

	err := manager.DetachChild(context.Background(), "parent", "child")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("DetachChild() of unattached child error = %v, want %v", err, ErrNotFound)
	}
}

func TestUpdateTopicSchema_RejectsAttachedChild(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["child"] = topic.Topic{Name: "child", Partitions: 3, Role: topic.RoleChild, Parent: "parent"}
	manager := newTestManager(t, ms, nil)

	_, err := manager.UpdateTopicSchema(context.Background(), "child", []byte(`{"type":"object"}`))
	if !errors.Is(err, errs.ErrFanoutSchemaManaged) {
		t.Fatalf("UpdateTopicSchema() on attached child error = %v, want %v", err, errs.ErrFanoutSchemaManaged)
	}
}

func TestRetentionFloor_CreateAndUpdateReject(t *testing.T) {
	ms := newFakeMetastore()
	manager := newTestManager(t, ms, nil)
	ctx := context.Background()

	_, err := manager.CreateTopic(ctx, CreateOpts{Name: testTopicName, RetentionMs: 60_000})
	if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "below the minimum") {
		t.Fatalf("CreateTopic(sub-hour retention) error = %v, want floor rejection", err)
	}

	created, err := manager.CreateTopic(ctx, CreateOpts{Name: testTopicName, RetentionMs: topic.MinRetentionMs})
	if err != nil {
		t.Fatalf("CreateTopic(floor retention) error = %v", err)
	}
	if created.RetentionMs != topic.MinRetentionMs {
		t.Fatalf("CreateTopic retention = %d, want %d", created.RetentionMs, topic.MinRetentionMs)
	}

	_, err = manager.UpdateTopicRetention(ctx, testTopicName, topic.MinRetentionMs-1)
	if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "below the minimum") {
		t.Fatalf("UpdateTopicRetention(sub-hour) error = %v, want floor rejection", err)
	}
	if _, err := manager.UpdateTopicRetention(ctx, testTopicName, 2*topic.MinRetentionMs); err != nil {
		t.Fatalf("UpdateTopicRetention(2h) error = %v", err)
	}
}

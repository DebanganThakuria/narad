package topics

// Create-as-child (CreateOpts.Parent) is create → attach → assign in one
// operation, so the child's parent link exists before its partitions are
// placed — the precondition for anti-affine (replica) placement. These
// tests pin the validation surface, the partition-count inheritance, the
// attach linkage, and the rollback when the attach cannot complete.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
)

func TestCreateAsChildLinksAndInheritsPartitions(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 6}
	manager := newTestManager(t, ms, nil)
	ctx := context.Background()

	got, err := manager.CreateTopic(ctx, CreateOpts{Name: "orders-replica", Parent: "orders"})
	if err != nil {
		t.Fatalf("CreateTopic(parent=orders) error = %v", err)
	}
	if got.Partitions != 6 {
		t.Fatalf("child partitions = %d, want the parent's 6", got.Partitions)
	}
	if !got.IsChild() || got.Parent != "orders" {
		t.Fatalf("created topic = %+v, want a child of orders", got)
	}
	if parent := ms.topics["orders"]; !parent.IsParent() || len(parent.Children) != 1 || parent.Children[0] != "orders-replica" {
		t.Fatalf("parent after create-as-child = %+v", parent)
	}
}

func TestCreateAsChildExplicitPartitionsWin(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 6}
	manager := newTestManager(t, ms, nil)

	got, err := manager.CreateTopic(context.Background(), CreateOpts{Name: "wide", Parent: "orders", Partitions: 12})
	if err != nil {
		t.Fatalf("CreateTopic error = %v", err)
	}
	if got.Partitions != 12 {
		t.Fatalf("partitions = %d, want the explicit 12", got.Partitions)
	}
}

func TestCreateAsChildDelayChild(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	manager := newTestManager(t, ms, nil)

	got, err := manager.CreateTopic(context.Background(), CreateOpts{Name: "orders-later", Parent: "orders", FanoutDelayMs: 60_000})
	if err != nil {
		t.Fatalf("CreateTopic error = %v", err)
	}
	if got.FanoutDelayMs != 60_000 {
		t.Fatalf("fanout_delay_ms = %d, want 60000", got.FanoutDelayMs)
	}
}

func TestCreateAsChildValidation(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	manager := newTestManager(t, ms, nil)
	ctx := context.Background()

	cases := []struct {
		name          string
		opts          CreateOpts
		wantErr       error
		wantSubstring string
	}{
		{"delay without parent", CreateOpts{Name: "x", FanoutDelayMs: 1000}, ErrInvalid, "requires parent"},
		{"negative delay", CreateOpts{Name: "x", Parent: "orders", FanoutDelayMs: -1}, ErrInvalid, ">= 0"},
		{"delay beyond max", CreateOpts{Name: "x", Parent: "orders", FanoutDelayMs: topic.MaxFanoutDelayMs + 1}, ErrInvalid, "exceeds the maximum"},
		{"own parent", CreateOpts{Name: "x", Parent: "x"}, ErrInvalid, "own parent"},
		{"bad parent name", CreateOpts{Name: "x", Parent: "bad/name"}, ErrInvalid, "topic name"},
		{"missing parent", CreateOpts{Name: "x", Parent: "ghost"}, ErrNotFound, `"ghost"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := manager.CreateTopic(ctx, tc.opts)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("CreateTopic(%+v) error = %v, want %v", tc.opts, err, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantSubstring) {
				t.Fatalf("error %q does not name the problem %q", err, tc.wantSubstring)
			}
			if _, exists := ms.topics["x"]; exists {
				t.Fatal("failed create left the topic behind")
			}
		})
	}
}

// If the attach fails after the create (e.g. an FSM-level invariant the
// pre-checks can't see, like the parent being a child itself), the
// created topic must be rolled back, not left half-linked.
func TestCreateAsChildRollsBackOnAttachFailure(t *testing.T) {
	ms := newFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 3}
	ms.attachChildErr = errs.ErrFanoutRoleConflict
	manager := newTestManager(t, ms, nil)

	_, err := manager.CreateTopic(context.Background(), CreateOpts{Name: "orders-replica", Parent: "orders"})
	if !errors.Is(err, errs.ErrFanoutRoleConflict) {
		t.Fatalf("CreateTopic error = %v, want the attach failure surfaced", err)
	}
	if _, exists := ms.topics["orders-replica"]; exists {
		t.Fatalf("failed create-as-child left the topic behind: %+v", ms.topics["orders-replica"])
	}
}

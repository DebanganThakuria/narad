package metastore_test

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// The incarnation ID is fixed at create. An update proposed from a
// record that lacks it (an older binary, or a read-modify-write of a
// stale copy) must not clear or replace it, or every directory stamped
// with it would read as a deleted incarnation's.
func TestUpdateTopicPreservesIncarnationID(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "0123456789abcdef", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, proposed := range []string{"", "fedcba9876543210"} {
		if err := s.UpdateTopic(ctx, topic.Topic{Name: "orders", ID: proposed, Partitions: 3, RetentionMs: 7_200_000}); err != nil {
			t.Fatalf("UpdateTopic(id=%q): %v", proposed, err)
		}
		got, err := s.GetTopic(ctx, "orders")
		if err != nil {
			t.Fatalf("GetTopic: %v", err)
		}
		if got.ID != "0123456789abcdef" {
			t.Fatalf("after UpdateTopic(id=%q): ID = %q, want the create-time ID preserved", proposed, got.ID)
		}
		if got.RetentionMs != 7_200_000 {
			t.Fatalf("update was not applied: retention = %d", got.RetentionMs)
		}
	}

	// A record created without an ID stays without one: an incarnation
	// never gains an ID mid-life.
	if err := s.CreateTopic(ctx, topic.Topic{Name: "legacy", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic(legacy): %v", err)
	}
	if err := s.UpdateTopic(ctx, topic.Topic{Name: "legacy", ID: "1111111111111111", Partitions: 3}); err != nil {
		t.Fatalf("UpdateTopic(legacy): %v", err)
	}
	got, err := s.GetTopic(ctx, "legacy")
	if err != nil {
		t.Fatalf("GetTopic(legacy): %v", err)
	}
	if got.ID != "" {
		t.Fatalf("legacy record gained an ID through an update: %q", got.ID)
	}
	_ = metastore.ErrNotFound
}

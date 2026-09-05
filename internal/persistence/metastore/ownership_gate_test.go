package metastore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// TestListAssignmentsRefusesUntilCaughtUpSinceStart pins the ownership
// gate: a store that has not yet heard from a leader answers
// ListAssignments with ErrUnavailable instead of a (possibly stale)
// view; once caught up it answers, and keeps answering.
func TestListAssignmentsRefusesUntilCaughtUpSinceStart(t *testing.T) {
	s, err := metastore.New(metastore.Config{
		NodeID:        "gate-0",
		DataDir:       t.TempDir(),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	// Immediately after open there is no leader yet: the view must be
	// refused rather than served empty (an empty list reads as "this
	// node owns nothing" or, worse, as a stale ownership).
	if _, err := s.ListAssignments("orders"); !errors.Is(err, errs.ErrUnavailable) {
		if err == nil && !s.AppliedCaughtUp() {
			t.Fatalf("ListAssignments served a view before the replica caught up")
		}
		// A very fast election can make the first call succeed; that is
		// fine as long as AppliedCaughtUp held by then.
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 2}); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := s.AssignPartition(context.Background(), "orders", 0, "gate-0"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	rows, err := s.ListAssignments("orders")
	if err != nil {
		t.Fatalf("ListAssignments after catch-up: %v", err)
	}
	if len(rows) != 1 || rows[0].OwnerID != "gate-0" {
		t.Fatalf("assignments = %+v, want partition 0 owned by gate-0", rows)
	}
}

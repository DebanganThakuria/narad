package metastore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// A topic created before any member has registered (the first seconds of
// a fresh cluster) cannot be placed. AssignNewPartitions used to return
// nil there, so the create path's "created without immediate partition
// assignment" warning never fired while every partition stayed unowned.
func TestWP10BAssignNewPartitionsReportsNoAliveMembers(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	if err := s.CreateTopic(ctx, topic.Topic{Name: "cold", Partitions: 4, RetentionMs: 3_600_000}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	err := s.AssignNewPartitions(ctx, "cold", 0, 4)
	if !errors.Is(err, metastore.ErrNoAliveMembers) {
		t.Fatalf("AssignNewPartitions with no members = %v, want ErrNoAliveMembers", err)
	}
	if got := assignmentsByPartition(t, s, "cold"); len(got) != 0 {
		t.Fatalf("assignments = %v, want none", got)
	}

	// Dead members are no better than none.
	registerAliveMembers(t, s, "narad-0")
	if err := s.MarkMemberDead(ctx, "narad-0"); err != nil {
		t.Fatalf("MarkMemberDead: %v", err)
	}
	if err := s.AssignNewPartitions(ctx, "cold", 0, 4); !errors.Is(err, metastore.ErrNoAliveMembers) {
		t.Fatalf("AssignNewPartitions with only dead members = %v, want ErrNoAliveMembers", err)
	}

	// An empty range has nothing to place, so it is not an error.
	if err := s.AssignNewPartitions(ctx, "cold", 4, 4); err != nil {
		t.Fatalf("AssignNewPartitions over an empty range = %v, want nil", err)
	}

	// Once a member is alive the same call places every partition.
	registerAliveMembers(t, s, "narad-1")
	if err := s.AssignNewPartitions(ctx, "cold", 0, 4); err != nil {
		t.Fatalf("AssignNewPartitions with a live member: %v", err)
	}
	got := assignmentsByPartition(t, s, "cold")
	if len(got) != 4 {
		t.Fatalf("assignments = %v, want all 4 partitions placed", got)
	}
	for p, owner := range got {
		if owner != "narad-1" {
			t.Fatalf("partition %d owner = %q, want narad-1 (the only live member)", p, owner)
		}
	}
}

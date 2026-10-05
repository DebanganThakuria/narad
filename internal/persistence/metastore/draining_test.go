package metastore_test

// Decommission's placement half: a member can be marked draining, the flag
// survives a heartbeat-style re-registration (a node restarting mid-drain
// must stay draining), and clearing it works.

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func TestSetMemberDrainingSticksAcrossReRegister(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	m := metastore.Member{ID: "narad-2", Addr: "10.0.0.2:7943", ClusterAddr: "10.0.0.2:7942", Status: metastore.MemberAlive, LastHeartbeat: 1}
	if err := s.RegisterMember(ctx, m); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}

	if err := s.SetMemberDraining(ctx, "narad-2", true); err != nil {
		t.Fatalf("SetMemberDraining: %v", err)
	}
	if got, _ := s.GetMember("narad-2"); !got.Draining {
		t.Fatal("member not draining after SetMemberDraining(true)")
	}

	// A re-registration carries the join defaults (Draining=false); the
	// in-progress decommission must survive it.
	m.LastHeartbeat = 2
	if err := s.RegisterMember(ctx, m); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if got, _ := s.GetMember("narad-2"); !got.Draining {
		t.Fatal("re-register cleared the draining flag")
	}

	if err := s.SetMemberDraining(ctx, "narad-2", false); err != nil {
		t.Fatalf("clear draining: %v", err)
	}
	if got, _ := s.GetMember("narad-2"); got.Draining {
		t.Fatal("member still draining after clear")
	}
}

func TestSetMemberDrainingUnknownMember(t *testing.T) {
	s := newTestStore(t)
	if err := s.SetMemberDraining(context.Background(), "ghost", true); err == nil {
		t.Fatal("draining an unregistered member must error")
	}
}

// New partitions (a create, a partition increase) must not land on a
// member being decommissioned: decommission would only have to move
// them off again, and a drain that keeps receiving new partitions may
// never finish (verify-topics-7: master placed 2 of 6 on the draining
// member).
func TestNewPartitionsAvoidDrainingMember(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"narad-0", "narad-1", "narad-2"} {
		if err := s.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ":7943", Status: metastore.MemberAlive, LastHeartbeat: 1}); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
	}
	if err := s.SetMemberDraining(ctx, "narad-1", true); err != nil {
		t.Fatalf("SetMemberDraining: %v", err)
	}
	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 6}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if err := s.AssignNewPartitions(ctx, "orders", 0, 6); err != nil {
		t.Fatalf("AssignNewPartitions(0, 6): %v", err)
	}
	if err := s.AssignNewPartitions(ctx, "orders", 6, 12); err != nil {
		t.Fatalf("AssignNewPartitions(6, 12): %v", err)
	}
	assignments, err := s.ListAssignments("orders")
	if err != nil {
		t.Fatalf("ListAssignments: %v", err)
	}
	if len(assignments) != 12 {
		t.Fatalf("assigned %d partitions, want 12", len(assignments))
	}
	for _, a := range assignments {
		if a.OwnerID == "narad-1" {
			t.Fatalf("partition %d placed on the draining member narad-1 (all: %+v)", a.Partition, assignments)
		}
	}
}

// When every live member is draining, a new partition still gets an
// owner: an unowned partition takes no produces at all, which is worse
// than one more partition to move.
func TestPlacementMembersFallsBackWhenAllDraining(t *testing.T) {
	members := []metastore.Member{
		{ID: "narad-0", Status: metastore.MemberAlive, Draining: true},
		{ID: "narad-1", Status: metastore.MemberDead},
		{ID: "narad-2", Status: metastore.MemberAlive},
	}
	if got := metastore.PlacementMembers(members); len(got) != 1 || got[0].ID != "narad-2" {
		t.Fatalf("PlacementMembers = %+v, want only narad-2", got)
	}
	members[2].Draining = true
	got := metastore.PlacementMembers(members)
	if len(got) != 2 || got[0].ID != "narad-0" || got[1].ID != "narad-2" {
		t.Fatalf("PlacementMembers with every live member draining = %+v, want the two draining live members", got)
	}
	if got := metastore.PlacementMembers(members[1:2]); len(got) != 0 {
		t.Fatalf("PlacementMembers with no live member = %+v, want none", got)
	}
}

package metastore_test

// Decommission's placement half: a member can be marked draining, the flag
// survives a heartbeat-style re-registration (a node restarting mid-drain
// must stay draining), and clearing it works.

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
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
// never finish (before, 2 of 6 landed on the draining member).
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

// A new partition never goes to a draining member, not even when every
// live member is draining: that would override the operator's drain,
// and while every live member drains the decommission has nowhere to
// move it either. PlacementMembers then returns none, and
// PlacementRefusal says why.
func TestPlacementMembersNeverFallsBackToDrainingMembers(t *testing.T) {
	members := []metastore.Member{
		{ID: "narad-0", Status: metastore.MemberAlive, Draining: true},
		{ID: "narad-1", Status: metastore.MemberDead},
		{ID: "narad-2", Status: metastore.MemberAlive},
	}
	if got := metastore.PlacementMembers(members); len(got) != 1 || got[0].ID != "narad-2" {
		t.Fatalf("PlacementMembers = %+v, want only narad-2", got)
	}
	if err := metastore.PlacementRefusal(members); err != nil {
		t.Fatalf("PlacementRefusal with narad-2 placeable = %v, want nil", err)
	}

	members[2].Draining = true
	if got := metastore.PlacementMembers(members); len(got) != 0 {
		t.Fatalf("PlacementMembers with every live member draining = %+v, want none", got)
	}
	err := metastore.PlacementRefusal(members)
	if !errors.Is(err, metastore.ErrAllMembersDraining) || !errors.Is(err, errs.ErrUnavailable) {
		t.Fatalf("PlacementRefusal with every live member draining = %v, want ErrAllMembersDraining (503)", err)
	}
	if !strings.Contains(err.Error(), "abort a decommission or add a node") {
		t.Errorf("refusal %q does not say what to do", err)
	}

	if got := metastore.PlacementMembers(members[1:2]); len(got) != 0 {
		t.Fatalf("PlacementMembers with no live member = %+v, want none", got)
	}
	if err := metastore.PlacementRefusal(members[1:2]); !errors.Is(err, metastore.ErrNoAliveMembers) {
		t.Fatalf("PlacementRefusal with no live member = %v, want ErrNoAliveMembers", err)
	}
}

// With every live member draining, placing a create's or partition
// increase's partitions assigns nothing and returns
// ErrAllMembersDraining, CheckPlacement refuses up front, and the
// leader says so at error level, once for the episode. Once a member
// that is not draining is alive, the partitions go to it.
func TestAssignNewPartitionsRefusesWhenEveryLiveMemberDrains(t *testing.T) {
	var out lockedBuffer
	s := newTestStoreLogging(t, slog.New(slog.NewTextHandler(&out, nil)))
	ctx := context.Background()
	// End any episode an earlier test left open.
	metastore.PlacementMembers([]metastore.Member{{ID: "x", Status: metastore.MemberAlive}})

	for _, id := range []string{"narad-0", "narad-1"} {
		if err := s.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ":7943", Status: metastore.MemberAlive, LastHeartbeat: 1}); err != nil {
			t.Fatalf("RegisterMember(%s): %v", id, err)
		}
		if err := s.SetMemberDraining(ctx, id, true); err != nil {
			t.Fatalf("SetMemberDraining(%s): %v", id, err)
		}
	}
	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	if err := s.CheckPlacement(); !errors.Is(err, metastore.ErrAllMembersDraining) {
		t.Fatalf("CheckPlacement with every live member draining = %v, want ErrAllMembersDraining", err)
	}
	for range 2 {
		if err := s.AssignNewPartitions(ctx, "orders", 0, 3); !errors.Is(err, metastore.ErrAllMembersDraining) {
			t.Fatalf("AssignNewPartitions with every live member draining = %v, want ErrAllMembersDraining", err)
		}
	}
	if got, err := s.ListAssignments("orders"); err != nil || len(got) != 0 {
		t.Fatalf("assignments with every live member draining = %+v (err %v), want none", got, err)
	}
	logged := out.String()
	if n := strings.Count(logged, "every live member is being decommissioned"); n != 1 || !strings.Contains(logged, "level=ERROR") {
		t.Fatalf("logged %d error lines for one episode, want 1:\n%s", n, logged)
	}

	if err := s.SetMemberDraining(ctx, "narad-1", false); err != nil {
		t.Fatalf("clear draining: %v", err)
	}
	if err := s.CheckPlacement(); err != nil {
		t.Fatalf("CheckPlacement with narad-1 placeable = %v, want nil", err)
	}
	if err := s.AssignNewPartitions(ctx, "orders", 0, 3); err != nil {
		t.Fatalf("AssignNewPartitions with narad-1 placeable: %v", err)
	}
	got, err := s.ListAssignments("orders")
	if err != nil || len(got) != 3 {
		t.Fatalf("assignments = %+v (err %v), want 3", got, err)
	}
	for _, a := range got {
		if a.OwnerID != "narad-1" {
			t.Fatalf("partition %d placed on %q, want the only member not draining, narad-1", a.Partition, a.OwnerID)
		}
	}
}

// lockedBuffer is a bytes.Buffer safe for a logger and a test to share.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

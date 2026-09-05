package metastore_test

// A member removed by decommission must stay removed: its record goes,
// and the heartbeat re-registration the still-running pod keeps sending
// is refused rather than resurrecting it. Only an explicit readmission
// (a fresh node under the same ID) lets it register again.

import (
	"context"
	"errors"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

func TestRemoveMemberForgetsAndRefusesHeartbeatResurrection(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)

	m := metastore.Member{ID: "narad-5", Addr: "10.0.0.5:7942", Status: metastore.MemberAlive, LastHeartbeat: 1}
	if err := s.RegisterMember(ctx, m); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	if err := s.SetMemberDraining(ctx, "narad-5", true); err != nil {
		t.Fatalf("SetMemberDraining: %v", err)
	}

	if err := s.RemoveMember(ctx, "narad-5", 42); err != nil {
		t.Fatalf("RemoveMember: %v", err)
	}
	if _, err := s.GetMember("narad-5"); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("GetMember after removal err = %v, want ErrNotFound", err)
	}
	members, err := s.ListMembers()
	if err != nil {
		t.Fatalf("ListMembers: %v", err)
	}
	for _, got := range members {
		if got.ID == "narad-5" {
			t.Fatalf("removed member still listed: %+v", got)
		}
	}
	if removed, err := s.MemberRemoved("narad-5"); err != nil || !removed {
		t.Fatalf("MemberRemoved = %v, %v; want true", removed, err)
	}

	// The departed pod's heartbeat (a RegisterMember upsert) is refused.
	m.LastHeartbeat = 2
	if err := s.RegisterMember(ctx, m); !errors.Is(err, metastore.ErrMemberRemoved) {
		t.Fatalf("RegisterMember after removal err = %v, want ErrMemberRemoved", err)
	}
	if err := s.Heartbeat(ctx, "narad-5", 3); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("Heartbeat after removal err = %v, want ErrNotFound", err)
	}
	if _, err := s.GetMember("narad-5"); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("member resurrected by heartbeat: err = %v", err)
	}

	// Removal is idempotent (a retry after a leadership change).
	if err := s.RemoveMember(ctx, "narad-5", 43); err != nil {
		t.Fatalf("repeat RemoveMember: %v", err)
	}

	// Readmission clears the tombstone; a fresh node may register.
	if err := s.ReadmitMember(ctx, "narad-5"); err != nil {
		t.Fatalf("ReadmitMember: %v", err)
	}
	if removed, _ := s.MemberRemoved("narad-5"); removed {
		t.Fatal("tombstone survived ReadmitMember")
	}
	if err := s.RegisterMember(ctx, m); err != nil {
		t.Fatalf("RegisterMember after readmission: %v", err)
	}
	got, err := s.GetMember("narad-5")
	if err != nil || got.Draining {
		t.Fatalf("readmitted member = %+v, %v; want a clean (non-draining) record", got, err)
	}
}

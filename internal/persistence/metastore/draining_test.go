package metastore_test

// Decommission's placement half: a member can be marked draining, the flag
// survives a heartbeat-style re-registration (a node restarting mid-drain
// must stay draining), and clearing it works.

import (
	"context"
	"testing"

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

package metastore

import (
	"context"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// TestWP17LatestDomainVersionTracksDomainsNotHeartbeats: the fan-out and
// move reconcilers skip passes while LatestDomainVersion holds still, so
// it must advance on every topic and assignment change they derive work
// from, and must not advance on member heartbeats (MetadataVersion does,
// every few seconds, which is why the reconcilers cannot gate on it).
func TestWP17LatestDomainVersionTracksDomainsNotHeartbeats(t *testing.T) {
	s := wp4bNewStore(t)
	ctx := context.Background()
	must := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	must("RegisterMember", s.RegisterMember(ctx, Member{ID: "m1", Addr: "127.0.0.1:1", Status: MemberAlive}))

	quiet := s.LatestDomainVersion()
	meta := s.MetadataVersion()
	for i := range 3 {
		must("Heartbeat", s.Heartbeat(ctx, "m1", int64(100+i)))
	}
	must("re-register", s.RegisterMember(ctx, Member{ID: "m1", Addr: "127.0.0.1:1", Status: MemberAlive, LastHeartbeat: 200}))
	must("SetMemberDraining", s.SetMemberDraining(ctx, "m1", true))
	if got := s.LatestDomainVersion(); got != quiet {
		t.Fatalf("LatestDomainVersion moved %d -> %d on heartbeats and a drain flag", quiet, got)
	}
	if s.MetadataVersion() == meta {
		t.Fatal("MetadataVersion did not move on heartbeats; the premise of this test is stale")
	}

	for _, step := range []struct {
		name string
		do   func() error
	}{
		{"CreateTopic parent", func() error { return s.CreateTopic(ctx, topic.Topic{Name: "p", Partitions: 1, RetentionMs: 7_200_000}) }},
		{"CreateTopic child", func() error { return s.CreateTopic(ctx, topic.Topic{Name: "c", Partitions: 1, RetentionMs: 7_200_000}) }},
		{"AssignPartition", func() error { return s.AssignPartition(ctx, "p", 0, "m1") }},
		{"AttachChild", func() error { return s.AttachChild(ctx, "p", "c", 0) }},
		{"SetAssignmentTarget", func() error { return s.SetAssignmentTarget(ctx, "p", 0, "m2") }},
		{"AbortMove", func() error { return s.AbortMove(ctx, "p", 0, "m2") }},
		{"UpdateTopic", func() error { return s.UpdateTopic(ctx, topic.Topic{Name: "p", Partitions: 2, RetentionMs: 7_200_000}) }},
		{"DetachChild", func() error { return s.DetachChild(ctx, "p", "c") }},
		{"DeleteTopic", func() error { return s.DeleteTopic(ctx, "c") }},
	} {
		before := s.LatestDomainVersion()
		must(step.name, step.do())
		if got := s.LatestDomainVersion(); got <= before {
			t.Fatalf("LatestDomainVersion did not advance on %s: %d -> %d", step.name, before, got)
		}
	}
}

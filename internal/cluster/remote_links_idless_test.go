package cluster

import (
	"context"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// A parent created before topic IDs existed (v2.1 and earlier, never
// backfilled) takes a remote child like any other: the checks ran
// against its ID-less record, and the attach is conditional on that.
func TestRemoteAttachToAParentWithoutAnIncarnationID(t *testing.T) {
	s := linksRig(t)
	ctx := context.Background()
	if err := s.store.CreateTopic(ctx, topic.Topic{
		Name: "legacy", Partitions: 1, RetentionMs: topic.MinRemoteSourceRetentionMs, CreatedAt: 1_600_000_000,
		VisibilityTimeoutMs: 30_000, MaxInFlightPerPartition: 64, MaxAckedAheadPerPartition: 64,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.store.AssignPartition(ctx, "legacy", 0, "node-self"); err != nil {
		t.Fatal(err)
	}
	if p, err := s.store.GetTopic(ctx, "legacy"); err != nil || p.ID != "" {
		t.Fatalf("legacy parent: %+v %v, want a record without an ID", p, err)
	}
	res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "legacy", "child": "legacy-to-b", "remote": "b"})
	if res.Status != http.StatusCreated {
		t.Fatalf("attach to an ID-less parent: %d %s, want 201", res.Status, res.Body)
	}
	if got := s.checks.lastRequest(); got.Source != "legacy" || got.SourceID != "" {
		t.Fatalf("check request = %+v", got)
	}
	if stub, err := s.store.GetTopic(ctx, "legacy-to-b"); err != nil || !stub.IsRemoteChild() || stub.Parent != "legacy" {
		t.Fatalf("stub = %+v %v", stub, err)
	}
}

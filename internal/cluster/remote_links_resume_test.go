package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Resume runs the attach checks again, all of them: the target-is-source
// check needs the parent's ID and the schema check its current schema,
// so a target whose schema changed during a pause is refused at resume
// rather than stalling the link on its first record.
func TestRemoteChildResumeRunsTheAttachChecksWithTheParentSchema(t *testing.T) {
	s := linksRig(t)
	ctx := context.Background()
	v1 := json.RawMessage(`{"type":"object"}`)
	if err := s.store.PutSchema(ctx, "orders", 1, v1); err != nil {
		t.Fatal(err)
	}
	if res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"}); res.Status != http.StatusCreated {
		t.Fatalf("attach: %d %s", res.Status, res.Body)
	}
	attachReq := s.checks.lastRequest()
	if res := s.write(t, nodewire.RemoteSubPause, map[string]any{"parent": "orders", "child": "orders-to-b", "reason": "target maintenance"}); res.Status != http.StatusOK {
		t.Fatalf("pause: %d %s", res.Status, res.Body)
	}
	// The parent's schema evolves during the pause.
	v2 := json.RawMessage(`{"type":"object","required":["id"]}`)
	if err := s.store.PutSchema(ctx, "orders", 2, v2); err != nil {
		t.Fatal(err)
	}
	if res := s.write(t, nodewire.RemoteSubResume, map[string]any{"parent": "orders", "child": "orders-to-b"}); res.Status != http.StatusOK {
		t.Fatalf("resume: %d %s", res.Status, res.Body)
	}
	got := s.checks.lastRequest()
	if got.SourceID != "src-orders-id" || got.SourceID != attachReq.SourceID {
		t.Fatalf("resume check source_id = %q, want the parent's ID %q as attach sends", got.SourceID, attachReq.SourceID)
	}
	if string(got.SourceSchema) != string(v2) {
		t.Fatalf("resume check source schema = %s, want the parent's current schema %s", got.SourceSchema, v2)
	}
	if got.Remote != "b" || got.Topic != "orders" || got.Source != "orders" {
		t.Fatalf("resume check = %+v", got)
	}
}

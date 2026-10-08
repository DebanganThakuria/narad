package cluster

import (
	"net/http"
	"testing"

	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Without a host allowlist an attach's dry run runs the checks on the
// leader alone, not from every member's network position, and neither
// it nor a failed attach returns what the target answered (ch. 5.8):
// only results and classes, and for a failure the members that failed.
func TestAttachIsBlindWithoutAnAllowlist(t *testing.T) {
	s := linksRig(t)
	s.checks.mu.Lock()
	s.checks.blind = true
	s.checks.mu.Unlock()

	res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b", "dry_run": true})
	if res.Status != http.StatusOK {
		t.Fatalf("dry run: %d %s", res.Status, res.Body)
	}
	s.checks.mu.Lock()
	everywhere, here := s.checks.calls, s.checks.hereCalls
	s.checks.mu.Unlock()
	if everywhere != 0 || here != 1 {
		t.Fatalf("dry run without an allowlist ran the checks everywhere %d times and here %d times, want here once", everywhere, here)
	}
	for _, raw := range bodyOf(t, res)["checks"].([]any) {
		if id, ok := raw.(map[string]any)["target_id"]; ok {
			t.Fatalf("dry run answer carries the target ID %v", id)
		}
		if v, ok := raw.(map[string]any)["target_serves_ids"]; ok {
			t.Fatalf("dry run answer carries target_serves_ids %v", v)
		}
	}

	s.checks.mu.Lock()
	s.checks.fail = "auth_failed"
	s.checks.mu.Unlock()
	res = s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"})
	out := bodyOf(t, res)
	if res.Status < 400 || out["class"] != "auth_failed" || out["checks"] != nil {
		t.Fatalf("failed attach without an allowlist: %d %s, want the class and no per-member reports", res.Status, res.Body)
	}
	if members, _ := out["members"].([]any); len(members) != 1 || members[0] != "node-self" {
		t.Fatalf("failed attach: members = %v, want the failing member", out["members"])
	}
}

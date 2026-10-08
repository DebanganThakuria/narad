package cluster

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	domremote "github.com/debanganthakuria/narad/internal/domain/remote"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// Without a host allowlist an attach's dry run runs the checks on the
// leader alone, not from every member's network position, and neither
// it nor a failed attach returns what the target answered (ch. 5.8):
// only results and classes, and for a failure the members that failed.
//
// The target here is an older release: its children listing reports no
// parent IDs and its batch produce takes 100 messages. With an allowlist
// the answer warns of both; blind, it carries only the warning drawn
// from this cluster's own parent, so it does not tell an older target
// from a current one.
func TestAttachIsBlindWithoutAnAllowlist(t *testing.T) {
	s := linksRig(t)
	older := olderBatchTarget(t)
	s.lookup.Set(entryFor(t, older, "b", rigRandomString(18), 1, domremote.Limits{}, nil))
	s.checks.mu.Lock()
	s.checks.older = true
	s.checks.mu.Unlock()

	retentionOnly := func(t *testing.T, what string, warnings any) {
		t.Helper()
		list, _ := warnings.([]any)
		if len(list) != 1 || !strings.HasPrefix(list[0].(string), "parent retention") {
			t.Fatalf("%s: warnings = %v, want only the parent retention warning", what, warnings)
		}
	}

	res := s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b", "dry_run": true})
	if res.Status != http.StatusOK {
		t.Fatalf("dry run with an allowlist: %d %s", res.Status, res.Body)
	}
	seen := bodyOf(t, res)["warnings"].([]any)
	if !slices.ContainsFunc(seen, func(w any) bool { return strings.Contains(w.(string), "does not report remote children") }) ||
		!slices.ContainsFunc(seen, func(w any) bool { return w == olderTargetBodyWarning }) {
		t.Fatalf("dry run with an allowlist: warnings = %v, want the older-target warnings", seen)
	}

	s.checks.mu.Lock()
	s.checks.blind = true
	s.checks.calls, s.checks.hereCalls = 0, 0
	s.checks.mu.Unlock()

	res = s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b", "dry_run": true})
	if res.Status != http.StatusOK {
		t.Fatalf("dry run: %d %s", res.Status, res.Body)
	}
	retentionOnly(t, "blind dry run", bodyOf(t, res)["warnings"])
	if caps, ok := bodyOf(t, res)["capabilities"]; ok {
		t.Fatalf("blind dry run answer carries the target's capabilities %v", caps)
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

	s.checks.mu.Lock()
	s.checks.fail = ""
	s.checks.mu.Unlock()
	res = s.write(t, nodewire.RemoteSubAttach, map[string]any{"parent": "orders", "child": "orders-to-b", "remote": "b"})
	if res.Status != http.StatusCreated {
		t.Fatalf("attach without an allowlist: %d %s", res.Status, res.Body)
	}
	retentionOnly(t, "blind attach", bodyOf(t, res)["warnings"])
}

// olderBatchTarget stands in for a v3.1.0 target's batch produce: it
// refuses more than 100 messages, as the capability probe expects.
func olderBatchTarget(t *testing.T) *rigTarget {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/produce/batch") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"too many messages (max 100)"}`))
	}))
	t.Cleanup(srv.Close)
	return &rigTarget{server: srv, caPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))}
}

package e2e

// End-to-end fan-out coverage through the public HTTP API: attach
// mid-flow with no backfill, re-keyed delivery with per-key routing,
// children listing with lag, detach semantics, re-attach without
// replay, the attach-time schema gate, parent-managed child schemas,
// owner-or-admin authorization, and the no-RBAC-gate fan-out rule.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// childrenListing mirrors the GET /v1/topics/{parent}/children shape.
type childrenListing struct {
	Parent   string `json:"parent"`
	Children []struct {
		Name        string `json:"name"`
		LagMessages int64  `json:"lag_messages"`
		LagComplete bool   `json:"lag_complete"`
	} `json:"children"`
}

// attachChild attaches child under parent and returns the response.
func (e *env) attachChild(parent, child string) *http.Response {
	e.t.Helper()
	return e.post("/v1/topics/"+parent+"/children", map[string]any{"child": child})
}

// totalCommitted sums the committed high-watermarks across a topic's
// partitions, via the public describe endpoint.
func totalCommitted(t *testing.T, e *env, name string) int64 {
	t.Helper()
	resp := e.get("/v1/topics/" + name)
	expectOK(t, resp)
	details := readJSON[topic.Details](t, resp)
	var total int64
	for _, p := range details.Partitions {
		total += p.HighWatermark
	}
	return total
}

// awaitFanout polls until the child holds at least want committed
// records.
func awaitFanout(t *testing.T, e *env, child string, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if totalCommitted(t, e, child) >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d fanned-out records in %q; have %d", want, child, totalCommitted(t, e, child))
}

// childPayloads replays every committed child record and returns the
// payloads grouped by key, in offset order per partition.
func childPayloads(t *testing.T, e *env, child string) map[string][]string {
	t.Helper()
	resp := e.get("/v1/topics/" + child)
	expectOK(t, resp)
	details := readJSON[topic.Details](t, resp)

	byKey := map[string][]string{}
	for _, p := range details.Partitions {
		for off := int64(0); off < p.HighWatermark; off++ {
			partition, offset := p.Index, off
			msg, found := mustConsume(t, e, child, consumeQuery{Partition: &partition, Offset: &offset})
			if !found {
				t.Fatalf("replay %s/%d@%d: no message below the high-watermark", child, partition, offset)
			}
			byKey[msg.Key] = append(byKey[msg.Key], string(msg.Payload))
		}
	}
	return byKey
}

func TestFanoutEndToEnd(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("fan-parent", 3, 0)
	e.createTopic("fan-child", 3, 0)

	// Pre-attach traffic must never reach the child (no backfill).
	mustProduce(t, e, "fan-parent", "k1", map[string]any{"pre": 1})
	mustProduce(t, e, "fan-parent", "k2", map[string]any{"pre": 2})

	resp := e.attachChild("fan-parent", "fan-child")
	expectOK(t, resp)
	attached := readJSON[topic.Topic](t, resp)
	if attached.Role != topic.RoleParent || len(attached.Children) != 1 || attached.Children[0] != "fan-child" {
		t.Fatalf("attach response = %+v, want parent with [fan-child]", attached)
	}

	// Describe reports the link from both sides.
	resp = e.get("/v1/topics/fan-child")
	expectOK(t, resp)
	childDetails := readJSON[topic.Details](t, resp)
	if childDetails.Role != topic.RoleChild || childDetails.Parent != "fan-parent" {
		t.Fatalf("child describe = %+v, want role=child parent=fan-parent", childDetails.Topic)
	}

	// Post-attach traffic fans out, re-keyed per record.
	const n = 6
	for i := range n {
		mustProduce(t, e, "fan-parent", fmt.Sprintf("key-%d", i%3), map[string]any{"seq": i})
	}
	awaitFanout(t, e, "fan-child", n)

	byKey := childPayloads(t, e, "fan-child")
	got := 0
	for key, payloads := range byKey {
		if key == "" {
			t.Fatalf("fanned-out record lost its key: %v", payloads)
		}
		got += len(payloads)
	}
	if got != n {
		t.Fatalf("child received %d records (%v), want exactly %d (no backfill of pre-attach traffic)", got, byKey, n)
	}
	// Per-key order: each key's sequence numbers must be ascending.
	for key, payloads := range byKey {
		last := -1
		for _, payload := range payloads {
			var v struct {
				Seq int `json:"seq"`
			}
			if err := json.Unmarshal([]byte(payload), &v); err != nil {
				t.Fatalf("decode child payload %q: %v", payload, err)
			}
			if v.Seq <= last {
				t.Fatalf("key %q out of order: %v", key, payloads)
			}
			last = v.Seq
		}
	}

	// The listing reports the child and its lag drains to zero.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp = e.get("/v1/topics/fan-parent/children")
		expectOK(t, resp)
		listing := readJSON[childrenListing](t, resp)
		if len(listing.Children) != 1 || listing.Children[0].Name != "fan-child" {
			t.Fatalf("children listing = %+v, want [fan-child]", listing)
		}
		if listing.Children[0].LagComplete && listing.Children[0].LagMessages == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lag never drained: %+v", listing.Children[0])
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Detach: the child keeps its records, stops receiving, and both
	// sides revert to standalone.
	resp = e.del("/v1/topics/fan-parent/children/fan-child")
	expectStatus(t, resp, http.StatusNoContent)
	resp = e.get("/v1/topics/fan-child")
	expectOK(t, resp)
	detachedChild := readJSON[topic.Details](t, resp)
	if detachedChild.Role != topic.RoleStandalone || detachedChild.Parent != "" {
		t.Fatalf("child after detach = %+v, want standalone", detachedChild.Topic)
	}

	mustProduce(t, e, "fan-parent", "k-detached", map[string]any{"detached": true})
	time.Sleep(200 * time.Millisecond)
	if got := totalCommitted(t, e, "fan-child"); got != n {
		t.Fatalf("child received records after detach: %d, want %d", got, n)
	}

	// Re-attach starts fresh at the tail: the detached-window record
	// never replays, new traffic flows.
	resp = e.attachChild("fan-parent", "fan-child")
	expectOK(t, resp)
	mustProduce(t, e, "fan-parent", "k-new", map[string]any{"post": 1})
	awaitFanout(t, e, "fan-child", n+1)
	time.Sleep(200 * time.Millisecond)
	if got := totalCommitted(t, e, "fan-child"); got != n+1 {
		t.Fatalf("child after re-attach = %d records, want %d (no replay of the detached window)", got, n+1)
	}
}

func TestFanoutAttachRejectsInvalidLinks(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("link-parent", 3, 0)
	e.createTopic("link-child", 3, 0)
	e.createTopic("link-other", 3, 0)
	e.createTopic("link-spare", 3, 0)

	expectOK(t, e.attachChild("link-parent", "link-child"))

	cases := []struct {
		name          string
		parent, child string
		wantStatus    int
	}{
		{"missing parent", "ghost", "link-spare", http.StatusNotFound},
		{"missing child", "link-parent", "ghost", http.StatusNotFound},
		{"self attach", "link-spare", "link-spare", http.StatusBadRequest},
		{"child of a child (depth 2)", "link-child", "link-spare", http.StatusConflict},
		{"parent as child", "link-spare", "link-parent", http.StatusConflict},
		{"second parent", "link-other", "link-child", http.StatusConflict},
		{"duplicate attach", "link-parent", "link-child", http.StatusConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := e.attachChild(tc.parent, tc.child)
			expectStatus(t, resp, tc.wantStatus)
		})
	}

	// Detaching something that was never attached is a 404.
	resp := e.del("/v1/topics/link-parent/children/link-spare")
	expectStatus(t, resp, http.StatusNotFound)
}

func TestFanoutSchemaGate(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("sg-parent", 3, 0)
	e.createTopic("sg-child", 3, 0)
	e.createTopic("sg-schemad", 3, 0)
	e.createTopic("sg-different", 3, 0)

	setSchema := func(name, schema string, wantStatus int) *http.Response {
		t.Helper()
		resp := e.patch("/v1/topics/"+name, map[string]any{"schema": json.RawMessage(schema)})
		expectStatus(t, resp, wantStatus)
		return resp
	}
	setSchema("sg-parent", schemaV1, http.StatusOK)
	setSchema("sg-different", schemaV2Additive, http.StatusOK)

	// A schema'd child cannot attach to a schema-less parent.
	expectStatus(t, e.attachChild("sg-child", "sg-different"), http.StatusConflict)
	// A child whose schema differs from the parent's is rejected.
	expectStatus(t, e.attachChild("sg-parent", "sg-different"), http.StatusConflict)

	// A schema-less child adopts the parent's schema at attach...
	expectOK(t, e.attachChild("sg-parent", "sg-child"))

	// ...so direct produces to the child are now validated against it.
	resp := rawReq(t, http.MethodPost, e.url("/v1/topics/sg-child/produce"), []byte(`{"name":"missing-required-id"}`))
	expectStatus(t, resp, http.StatusBadRequest)
	mustProduce(t, e, "sg-child", "", map[string]any{"id": 1})

	// An attached child's schema is parent-managed.
	setSchema("sg-child", schemaV2Additive, http.StatusConflict)
	// The parent's schema can still evolve (and propagates to children).
	setSchema("sg-parent", schemaV2Additive, http.StatusOK)

	// After detach the child keeps its schema and manages it again.
	expectStatus(t, e.del("/v1/topics/sg-parent/children/sg-child"), http.StatusNoContent)
	resp = rawReq(t, http.MethodPost, e.url("/v1/topics/sg-child/produce"), []byte(`{"name":"still-invalid"}`))
	expectStatus(t, resp, http.StatusBadRequest)
}

func TestFanoutSecurity(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t, withSecurity())
	au, ap := e.adminUser, e.adminPass

	// alice owns both topics; bob has no grants on either; carol can
	// only produce to the parent.
	for _, u := range []map[string]any{
		{"username": "alice", "password": "alicepw", "grants": []map[string]any{
			{"action": "create", "patterns": []string{"sec-*"}},
			{"action": "produce", "patterns": []string{"sec-*"}},
			{"action": "consume", "patterns": []string{"sec-*"}},
		}},
		{"username": "bob", "password": "bobpw"},
		{"username": "carol", "password": "carolpw", "grants": []map[string]any{
			{"action": "produce", "patterns": []string{"sec-parent"}},
		}},
	} {
		resp := e.authReq(t, http.MethodPost, "/v1/users", u, au, ap)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create user %v: status = %d", u["username"], resp.StatusCode)
		}
		resp.Body.Close()
	}
	for _, name := range []string{"sec-parent", "sec-child"} {
		resp := e.authReq(t, http.MethodPost, "/v1/topics", map[string]any{"name": name, "partitions": 3}, "alice", "alicepw")
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("create %s: status = %d", name, resp.StatusCode)
		}
		resp.Body.Close()
		if !e.awaitPartitionAssignments(name, 3) {
			t.Fatalf("timed out waiting for %s assignments", name)
		}
	}

	// Attach is owner-or-admin on the parent.
	resp := e.authReq(t, http.MethodPost, "/v1/topics/sec-parent/children", map[string]any{"child": "sec-child"}, "bob", "bobpw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("bob attach: status = %d, want 403", resp.StatusCode)
	}
	resp.Body.Close()
	resp = e.authReq(t, http.MethodPost, "/v1/topics/sec-parent/children", map[string]any{"child": "sec-child"}, "alice", "alicepw")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("alice attach: status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// No fan-out RBAC gate: carol can only produce to the parent, yet
	// her message reaches the child.
	resp = e.authReq(t, http.MethodPost, "/v1/topics/sec-parent/produce?key=k", map[string]any{"v": 1}, "carol", "carolpw")
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("carol produce: status = %d, want 202", resp.StatusCode)
	}
	resp.Body.Close()

	deadline := time.Now().Add(10 * time.Second)
	for {
		msg := e.authReq(t, http.MethodGet, "/v1/topics/sec-child/consume?wait=500ms", nil, "alice", "alicepw")
		if msg.StatusCode == http.StatusOK {
			msg.Body.Close()
			break
		}
		msg.Body.Close()
		if time.Now().After(deadline) {
			t.Fatal("carol's produce never fanned out to sec-child")
		}
	}

	// Detach is owner-or-admin too.
	resp = e.authReq(t, http.MethodDelete, "/v1/topics/sec-parent/children/sec-child", nil, "carol", "carolpw")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("carol detach: status = %d, want 403", resp.StatusCode)
	}
	resp.Body.Close()
	resp = e.authReq(t, http.MethodDelete, "/v1/topics/sec-parent/children/sec-child", nil, au, ap)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("admin detach: status = %d, want 204", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestFanoutDeleteParentDetachesChildren(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("del-parent", 3, 0)
	e.createTopic("del-child", 3, 0)
	expectOK(t, e.attachChild("del-parent", "del-child"))

	mustProduce(t, e, "del-parent", "k", map[string]any{"v": 1})
	awaitFanout(t, e, "del-child", 1)

	resp := e.del("/v1/topics/del-parent")
	expectStatus(t, resp, http.StatusNoContent)

	// The child survives as a standalone topic with its data intact.
	resp = e.get("/v1/topics/del-child")
	expectOK(t, resp)
	child := readJSON[topic.Details](t, resp)
	if child.Role != topic.RoleStandalone || child.Parent != "" {
		t.Fatalf("child after parent delete = %+v, want standalone", child.Topic)
	}
	if got := totalCommitted(t, e, "del-child"); got != 1 {
		t.Fatalf("child records after parent delete = %d, want 1", got)
	}
}

func TestRetentionFloorEnforcedByAPI(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)

	resp := jsonReq(t, http.MethodPost, e.url("/v1/topics"),
		map[string]any{"name": "floor-reject", "retention_ms": int64(1_800_000)})
	expectBadRequest(t, resp)

	e.createTopic("floor-ok", 3, 3_600_000)
	resp = e.patch("/v1/topics/floor-ok", map[string]any{"retention_ms": int64(1_800_000)})
	expectBadRequest(t, resp)
}

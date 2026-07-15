package e2e

// Create-as-child: POST /v1/topics with `parent` creates, attaches, and
// assigns in one call — the flow that earns a fan-out child anti-affine
// (replica) partition placement, because the parent link exists before
// the partitions are placed. Single-node here, so placement itself is
// covered by the metastore and controller suites; this proves the API
// surface and that a created-as-child behaves as a full fan-out child
// end to end.

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestCreateAsChildEndToEnd(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("rep-parent", 3, 0)

	// One call: child exists, is attached, and inherited the parent's
	// partition count.
	resp := e.post("/v1/topics", map[string]any{"name": "rep-child", "parent": "rep-parent"})
	expectStatus(t, resp, http.StatusCreated)
	child := readJSON[topic.Topic](t, resp)
	if child.Role != topic.RoleChild || child.Parent != "rep-parent" {
		t.Fatalf("created topic = %+v, want an attached child of rep-parent", child)
	}
	if child.Partitions != 3 {
		t.Fatalf("child partitions = %d, want the parent's 3", child.Partitions)
	}
	if !e.awaitPartitionAssignments("rep-child", 3) {
		t.Fatal("child partitions were never assigned")
	}
	awaitCursorsAnchored(t, e, "rep-parent", "rep-child")

	// Records fan out to it like any attached child.
	const n = 6
	for i := range n {
		mustProduce(t, e, "rep-parent", fmt.Sprintf("key-%d", i%3), map[string]any{"seq": i})
	}
	awaitFanout(t, e, "rep-child", n)

	got := 0
	for key, payloads := range childPayloads(t, e, "rep-child") {
		if key == "" {
			t.Fatalf("fanned-out record lost its key: %v", payloads)
		}
		got += len(payloads)
	}
	if got != n {
		t.Fatalf("child received %d records, want %d", got, n)
	}
}

func TestCreateAsChildDelayChildRejectsDirectProduce(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("delay-parent", 3, 0)

	resp := e.post("/v1/topics", map[string]any{
		"name": "delay-replica", "parent": "delay-parent", "fanout_delay_ms": 60_000,
	})
	expectStatus(t, resp, http.StatusCreated)
	child := readJSON[topic.Topic](t, resp)
	if child.FanoutDelayMs != 60_000 {
		t.Fatalf("fanout_delay_ms = %d, want 60000", child.FanoutDelayMs)
	}

	// A delay child only receives records through fan-out; a direct
	// produce must be rejected exactly as it is for an attached one.
	resp = e.post("/v1/topics/delay-replica/produce?key=k", map[string]any{"x": 1})
	expectStatus(t, resp, http.StatusConflict)
}

func TestCreateAsChildValidationSurface(t *testing.T) {
	t.Parallel()
	e := newTestEnv(t)
	e.createTopic("val-parent", 3, 0)

	cases := []struct {
		name string
		body map[string]any
		want int
	}{
		{"missing parent topic", map[string]any{"name": "x1", "parent": "ghost"}, http.StatusNotFound},
		{"delay without parent", map[string]any{"name": "x2", "fanout_delay_ms": 1000}, http.StatusBadRequest},
		{"own parent", map[string]any{"name": "x3", "parent": "x3"}, http.StatusBadRequest},
		{"negative delay", map[string]any{"name": "x4", "parent": "val-parent", "fanout_delay_ms": -1}, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := e.post("/v1/topics", tc.body)
			expectStatus(t, resp, tc.want)
			// The failed create must not leave the topic behind.
			resp = e.get("/v1/topics/" + tc.body["name"].(string))
			expectStatus(t, resp, http.StatusNotFound)
		})
	}
}

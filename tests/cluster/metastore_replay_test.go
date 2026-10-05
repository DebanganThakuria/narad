//go:build cluster

package cluster

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// TestFollowerRestartDoesNotReEvaluateAppliedEntries is the metastore
// replay divergence through the real binary. Before any Raft
// snapshot exists (the first 8192 entries), a restarted node used to
// replay its whole log onto its already populated fsm.db. An attach the
// cluster refused (the child had a schema, the parent none) was then
// re-evaluated against the later state, where both have the same schema,
// and succeeded on the restarted node only: that node alone treated the
// topic as a child, for good. The FSM now records the applied index with
// every entry and skips what its database already holds.
func TestFollowerRestartDoesNotReEvaluateAppliedEntries(t *testing.T) {
	c := newCluster(t, clusterOptions{})
	c.startAll()
	c.waitAllReady(90 * time.Second)
	c.waitAdmin(30 * time.Second)
	leader := c.waitLeader(20 * time.Second)
	victim := c.anyRunning(leader)

	schema := json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"}}}`)
	c.apiWant(leader, http.MethodPost, "/v1/topics", map[string]any{"name": "par", "partitions": 3}, 30*time.Second, http.StatusCreated)
	c.apiWant(leader, http.MethodPost, "/v1/topics", map[string]any{"name": "kid", "partitions": 3, "schema": schema}, 30*time.Second, http.StatusCreated)
	status, body, err := c.api(leader, http.MethodPost, "/v1/topics/par/children", map[string]any{"child": "kid"})
	if err != nil {
		t.Fatal(err)
	}
	if status < 400 {
		t.Fatalf("attach of a child with a schema to a parent without one was accepted (%d %s); the scenario needs a refusal", status, truncate(body, 300))
	}
	t.Logf("attach refused as expected: %d %s", status, truncate(body, 200))
	c.apiWant(leader, http.MethodPatch, "/v1/topics/par", map[string]any{"schema": schema}, 30*time.Second, http.StatusOK)

	children := func(i int) []string {
		t.Helper()
		_, out := c.apiWant(i, http.MethodGet, "/v1/topics/par/children", nil, 30*time.Second, http.StatusOK)
		var resp struct {
			Children []struct {
				Name string `json:"name"`
			} `json:"children"`
		}
		if err := json.Unmarshal(out, &resp); err != nil {
			t.Fatalf("children of par on %s: %v (%s)", c.nodes[i].id, err, truncate(out, 200))
		}
		var names []string
		for _, ch := range resp.Children {
			names = append(names, ch.Name)
		}
		return names
	}
	// Let the victim apply the schema change before it goes down.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var history struct {
			Version int `json:"version"`
		}
		status, out, err := c.api(victim, http.MethodGet, "/v1/topics/par/schema", nil)
		if err == nil && status == http.StatusOK && json.Unmarshal(out, &history) == nil && history.Version >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never saw par's schema", c.nodes[victim].id)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got := children(victim); len(got) != 0 {
		t.Fatalf("before the restart %s already lists children of par: %v", c.nodes[victim].id, got)
	}

	c.kill(victim)
	c.start(victim)
	took := c.waitReady(victim, 90*time.Second)
	t.Logf("%s ready %s after restart", c.nodes[victim].id, took.Round(time.Millisecond))

	if got := children(victim); len(got) != 0 {
		t.Fatalf("after a restart without a snapshot %s lists %v as children of par; the leader lists %v (replica diverged)",
			c.nodes[victim].id, got, children(leader))
	}
	if got := children(leader); len(got) != 0 {
		t.Fatalf("leader lists children of par: %v", got)
	}
}

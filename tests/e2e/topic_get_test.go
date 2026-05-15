package e2e

import (
	"net/http"
	"testing"

	"github.com/debanganthakuria/narad/internal/topic"
)

func TestGetTopic_ReturnsDetailsAndStats(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "details", Partitions: 3})

	resp := getJSON(t, env.Server.URL+"/v1/topics/details")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readBody(resp))
	}
	var d topic.Details
	decodeJSON(t, resp, &d)

	if d.Name != "details" {
		t.Errorf("name: got %q want %q", d.Name, "details")
	}
	if got := len(d.Partitions); got != 3 {
		t.Errorf("partition stats len: got %d want 3", got)
	}
	for i, ps := range d.Partitions {
		if ps.Index != i {
			t.Errorf("partition[%d].Index: got %d want %d", i, ps.Index, i)
		}
		if ps.Segments < 1 {
			// One pre-allocated active segment per partition.
			t.Errorf("partition[%d].Segments: got %d want >= 1", i, ps.Segments)
		}
	}
}

func TestGetTopic_ReportsNextOffsetAfterProduce(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTopic(t, env, createTopicReq{Name: "stats", Partitions: 2})

	// Produce with the same key 3 times — HashRoundRobin keyed mode is
	// deterministic, so all 3 land on the same partition.
	for range 3 {
		mustProduce(t, env, "stats", "fixed", map[string]int{"v": 1})
	}

	resp := getJSON(t, env.Server.URL+"/v1/topics/stats")
	var d topic.Details
	decodeJSON(t, resp, &d)

	var sumNext int64
	var nonEmpty int
	for _, ps := range d.Partitions {
		sumNext += ps.NextOffset
		if ps.NextOffset > 0 {
			nonEmpty++
		}
	}
	if sumNext != 3 {
		t.Errorf("sum of NextOffset: got %d want 3", sumNext)
	}
	// Same key → exactly one partition should have all the records.
	if nonEmpty != 1 {
		t.Errorf("non-empty partitions: got %d want 1 (key-pinning)", nonEmpty)
	}
}

func TestGetTopic_NotFound(t *testing.T) {
	env := newTestEnv(t)
	resp := getJSON(t, env.Server.URL+"/v1/topics/missing")
	expectStatus(t, resp, http.StatusNotFound)
}

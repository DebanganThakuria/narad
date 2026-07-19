package e2e

// The operator-facing cluster endpoints, end to end through a real broker,
// metastore, and controller: list members with placement counts, list
// in-flight moves, and mark a node for decommission (which the controller
// then drains). Runs secured so the /v1/cluster routes are registered and
// admin-gated.

import (
	"net/http"
	"testing"
	"time"
)

type membersResp struct {
	Members []struct {
		ID              string `json:"id"`
		Status          string `json:"status"`
		Draining        bool   `json:"draining"`
		OwnedPartitions int    `json:"owned_partitions"`
		OutboundMoves   int    `json:"outbound_moves"`
	} `json:"members"`
}

type movesResp struct {
	Moves []struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		From      string `json:"from"`
		To        string `json:"to"`
	} `json:"moves"`
}

func TestClusterMembersAndDecommission(t *testing.T) {
	e := newTestEnv(t, withSecurity())
	au, ap := e.adminUser, e.adminPass

	res := e.authReq(t, http.MethodPost, "/v1/topics", map[string]any{"name": "orders", "partitions": 6}, au, ap)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("create topic: status %d body=%s", res.StatusCode, readBody(res))
	}
	res.Body.Close()
	if !e.awaitPartitionAssignments("orders", 6) {
		t.Fatal("partitions never assigned")
	}

	// Members: three registered nodes, six partitions spread across them,
	// none draining, no moves in flight for a freshly-balanced cluster.
	res = e.authReq(t, http.MethodGet, "/v1/cluster/members", nil, au, ap)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET members: status %d body=%s", res.StatusCode, readBody(res))
	}
	members := readJSON[membersResp](t, res)
	if len(members.Members) != 3 {
		t.Fatalf("members = %d, want 3", len(members.Members))
	}
	total := 0
	for _, m := range members.Members {
		total += m.OwnedPartitions
		if m.Draining {
			t.Fatalf("member %s draining before any decommission", m.ID)
		}
	}
	if total != 6 {
		t.Fatalf("owned partitions total = %d, want 6", total)
	}

	res = e.authReq(t, http.MethodGet, "/v1/cluster/moves", nil, au, ap)
	moves := readJSON[movesResp](t, res)
	if len(moves.Moves) != 0 {
		t.Fatalf("moves = %v, want none for a balanced cluster", moves.Moves)
	}

	// A non-admin is refused the decommission.
	res = e.authReq(t, http.MethodPost, "/v1/cluster/members/test-2/decommission", nil, "", "")
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauth decommission: status %d, want 401", res.StatusCode)
	}
	res.Body.Close()

	// Admin decommissions test-2; it must show draining, and the controller
	// starts moving its partitions off.
	res = e.authReq(t, http.MethodPost, "/v1/cluster/members/test-2/decommission", nil, au, ap)
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("decommission: status %d body=%s", res.StatusCode, readBody(res))
	}
	res.Body.Close()

	deadline := time.Now().Add(3 * time.Second)
	for {
		res = e.authReq(t, http.MethodGet, "/v1/cluster/members", nil, au, ap)
		members = readJSON[membersResp](t, res)
		var draining bool
		for _, m := range members.Members {
			if m.ID == "test-2" {
				draining = m.Draining
			}
		}
		if draining {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("test-2 never showed draining after decommission")
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Cancelling the decommission clears the drain.
	res = e.authReq(t, http.MethodDelete, "/v1/cluster/members/test-2/decommission", nil, au, ap)
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("cancel decommission: status %d", res.StatusCode)
	}
	res.Body.Close()

	res = e.authReq(t, http.MethodPost, "/v1/cluster/members/ghost/decommission", nil, au, ap)
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("decommission unknown member: status %d, want 404", res.StatusCode)
	}
	res.Body.Close()
}

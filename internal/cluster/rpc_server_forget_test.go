package cluster

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

// The leader's forget handler answers each refusal with its own status:
// an unknown server 404, this node 400, a server with a member record
// 409 (decommission it instead), a malformed request 400; and removes a
// server with no record.
func TestRPCServerForgetServerMapsRefusals(t *testing.T) {
	store := newTestStore(t)
	server := NewRPCServer(nil, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, id := range []string{"ghost", "registered"} {
		if adm, err := store.AdmitJoiner(id, freeTCPAddr(t)); err != nil || adm.Status != metastore.JoinStaged {
			t.Fatalf("AdmitJoiner(%s) = %+v, %v; want staged", id, adm, err)
		}
	}
	if err := store.RegisterMember(context.Background(), metastore.Member{ID: "registered", Addr: "10.0.0.9:7942", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	forget := func(id string) nodewire.Response {
		t.Helper()
		payload, err := nodewire.EncodeForgetServerRequest(nodewire.ForgetServerRequest{ID: id})
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		return server.handleForgetServer(payload)
	}

	for _, tc := range []struct {
		id     string
		status int
		says   string
	}{
		{"nobody", http.StatusNotFound, "not in the raft configuration"},
		{"node-self", http.StatusBadRequest, "this node"},
		{"registered", http.StatusConflict, "decommission it instead"},
	} {
		res := forget(tc.id)
		if res.Status != tc.status || !strings.Contains(string(res.Body), tc.says) {
			t.Fatalf("forget %s = %d %s, want %d saying %q", tc.id, res.Status, res.Body, tc.status, tc.says)
		}
	}
	if res := server.handleForgetServer([]byte{byte(nodewire.OpForgetServer)}); res.Status != http.StatusBadRequest {
		t.Fatalf("malformed forget = %d %s, want 400", res.Status, res.Body)
	}

	res := forget("ghost")
	var got forgetResult
	if res.Status != http.StatusOK || json.Unmarshal(res.Body, &got) != nil || got != (forgetResult{ID: "ghost"}) {
		t.Fatalf("forget ghost = %d %s, want 200 {id: ghost, voter: false}", res.Status, res.Body)
	}
	if in, err := store.RaftServer("ghost"); err != nil || in {
		t.Fatalf("ghost still in the raft configuration (%v, %v)", in, err)
	}
	if handle, ok := server.controlHandler(nodewire.OpForgetServer); !ok || handle == nil {
		t.Fatal("the forget op is not served")
	}
}

// A voter whose removal could leave the cluster without a quorum (here
// one of the two other voters is down, so forgetting the reachable one
// would leave the leader and a down voter) is refused with 409 and the
// reason, and stays in the Raft configuration.
func TestRPCServerForgetServerRefusesAVoterTheQuorumNeeds(t *testing.T) {
	stores := newTestStoreCluster(t, "node-a", "node-b", "node-c")
	leaderID, leader := waitForClusterLeader(t, stores)
	var others []string
	for id := range stores {
		if id != leaderID {
			others = append(others, id)
		}
	}
	down, stray := others[0], others[1]
	if err := stores[down].Close(); err != nil {
		t.Fatalf("Close(%s): %v", down, err)
	}
	server := NewRPCServer(nil, leader, slog.New(slog.NewTextHandler(io.Discard, nil)))
	payload, err := nodewire.EncodeForgetServerRequest(nodewire.ForgetServerRequest{ID: stray})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	res := server.handleForgetServer(payload)
	if res.Status != http.StatusConflict || !strings.Contains(string(res.Body), "without a quorum") {
		t.Fatalf("forget %s with %s down = %d %s, want 409 saying it could leave the cluster without a quorum", stray, down, res.Status, res.Body)
	}
	if in, err := leader.RaftServer(stray); err != nil || !in {
		t.Fatalf("a refused forget removed %s (%v, %v)", stray, in, err)
	}
}

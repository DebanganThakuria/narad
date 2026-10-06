package cluster

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// A user write forwarded while the leader is unreachable (election,
// rolling restart) must answer 503 like topic writes do, not 502: clients
// that treat Bad Gateway as terminal would stop retrying during failover.
func TestUserForwardsAnswer503WhenLeaderUnreachable(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	unreachable := errors.New("dial: connection refused")
	router.peer = fakePeerClient{
		createUserFn: func(context.Context, string, []byte) (nodewire.Response, error) {
			return nodewire.Response{}, unreachable
		},
		updateUserFn: func(context.Context, string, string, []byte) (nodewire.Response, error) {
			return nodewire.Response{}, unreachable
		},
		deleteUserFn: func(context.Context, string, string) (nodewire.Response, error) {
			return nodewire.Response{}, unreachable
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/users", nil)
	forwards := []struct {
		name string
		call func(w http.ResponseWriter) bool
	}{
		{"create", func(w http.ResponseWriter) bool { return router.RouteCreateUser(ctx, w, req, []byte(`{}`)) }},
		{"update", func(w http.ResponseWriter) bool { return router.RouteUpdateUser(ctx, w, req, "alice", []byte(`{}`)) }},
		{"delete", func(w http.ResponseWriter) bool { return router.RouteDeleteUser(ctx, w, req, "alice") }},
	}
	for _, f := range forwards {
		t.Run(f.name, func(t *testing.T) {
			res := httptest.NewRecorder()
			if !f.call(res) {
				t.Fatal("forward returned false, want true (this node is not the leader)")
			}
			if res.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want 503 (retryable), body %q", res.Code, res.Body.String())
			}
		})
	}
}

// A successful forward relays the leader's response untouched.
func TestUserForwardRelaysLeaderResponse(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{updateUserFn: func(context.Context, string, string, []byte) (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusNotFound, ContentType: nodewire.ContentTypeJSON, Body: []byte(`{"error":"user not found"}`)}, nil
	}}
	res := httptest.NewRecorder()
	if !router.RouteUpdateUser(ctx, res, httptest.NewRequest(http.MethodPut, "/v1/users/ghost/password", nil), "ghost", []byte(`{}`)) {
		t.Fatal("forward returned false, want true")
	}
	if res.Code != http.StatusNotFound || res.Body.String() != `{"error":"user not found"}` {
		t.Fatalf("relayed status = %d body = %q", res.Code, res.Body.String())
	}
}

// A forward whose leader reply never came back may still have been
// applied on the leader, so the router marks the writer undecided before
// its 503: the audit line then says unknown, not failed. A relayed answer
// carries the leader's decision and is not marked.
func TestFailedLeaderForwardMarksTheWriterUndecided(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	req := httptest.NewRequest(http.MethodPost, "/v1/users", nil)
	forwards := []struct {
		name string
		call func(w http.ResponseWriter) bool
	}{
		{"create user", func(w http.ResponseWriter) bool { return router.RouteCreateUser(ctx, w, req, []byte(`{}`)) }},
		{"update user", func(w http.ResponseWriter) bool { return router.RouteUpdateUser(ctx, w, req, "alice", []byte(`{}`)) }},
		{"delete user", func(w http.ResponseWriter) bool { return router.RouteDeleteUser(ctx, w, req, "alice") }},
		{"decommission", func(w http.ResponseWriter) bool { return router.RouteDecommissionMember(ctx, w, req, "node-x", false) }},
	}

	// fakePeerClient answers every one of these with a transport error.
	router.peer = fakePeerClient{}
	for _, f := range forwards {
		aw := handlers.NewAuditWriter(httptest.NewRecorder())
		if !f.call(aw) {
			t.Fatalf("%s: forward returned false, want true (this node is not the leader)", f.name)
		}
		if got := aw.Outcome(); got != handlers.AuditUnknown {
			t.Fatalf("%s: outcome after a lost leader reply = %q, want %q", f.name, got, handlers.AuditUnknown)
		}
	}

	conflict := func() (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusConflict, ContentType: nodewire.ContentTypeJSON, Body: []byte(`{"error":"exists"}`)}, nil
	}
	router.peer = fakePeerClient{
		createUserFn: func(context.Context, string, []byte) (nodewire.Response, error) { return conflict() },
		updateUserFn: func(context.Context, string, string, []byte) (nodewire.Response, error) { return conflict() },
		deleteUserFn: func(context.Context, string, string) (nodewire.Response, error) { return conflict() },
	}
	for _, f := range forwards[:3] {
		aw := handlers.NewAuditWriter(httptest.NewRecorder())
		f.call(aw)
		if got := aw.Outcome(); got != handlers.AuditRejected {
			t.Fatalf("%s: outcome after the leader's 409 = %q, want %q", f.name, got, handlers.AuditRejected)
		}
	}
}

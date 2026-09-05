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

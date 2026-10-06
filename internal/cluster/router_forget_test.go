package cluster

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// A follower forwards a forget to the leader and relays its answer. A
// leader on a release before forget answers "unsupported rpc operation",
// which the follower turns into a 501 that says to upgrade the leader;
// a leader it cannot reach is a 503 audited as unknown.
func TestRouteForgetServerReportsAnOlderLeader(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	answer := func(status int, body string) func(context.Context, string, string) (nodewire.Response, error) {
		return func(context.Context, string, string) (nodewire.Response, error) {
			return nodewire.Response{Status: status, ContentType: nodewire.ContentTypeJSON, Body: []byte(body)}, nil
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/members/ghost/forget", nil)

	for _, tc := range []struct {
		name    string
		peer    func(context.Context, string, string) (nodewire.Response, error)
		status  int
		says    string
		outcome string
	}{
		{"older leader", answer(http.StatusBadRequest, fmt.Sprintf(`{"error":"unsupported rpc operation %d"}`, nodewire.OpForgetServer)), http.StatusNotImplemented, "upgrade it first", handlers.AuditFailed},
		{"forgotten", answer(http.StatusOK, `{"id":"ghost","voter":true}`), http.StatusOK, `"voter":true`, handlers.AuditOK},
		{"refused", answer(http.StatusConflict, `{"error":"the server has a member record; decommission it instead"}`), http.StatusConflict, "decommission it instead", handlers.AuditRejected},
		{"leader unreachable", func(context.Context, string, string) (nodewire.Response, error) {
			return nodewire.Response{}, errors.New("dial: connection refused")
		}, http.StatusServiceUnavailable, "connection refused", handlers.AuditUnknown},
	} {
		router.peer = fakePeerClient{forgetServerFn: tc.peer}
		rec := httptest.NewRecorder()
		aw := handlers.NewAuditWriter(rec)
		if !router.RouteForgetServer(ctx, aw, req, "ghost") {
			t.Fatalf("%s: forward returned false, want true (this node is not the leader)", tc.name)
		}
		if rec.Code != tc.status || !strings.Contains(rec.Body.String(), tc.says) {
			t.Fatalf("%s: answered %d %s, want %d saying %q", tc.name, rec.Code, rec.Body, tc.status, tc.says)
		}
		if got := aw.Outcome(); got != tc.outcome {
			t.Fatalf("%s: outcome %q, want %q", tc.name, got, tc.outcome)
		}
	}
}

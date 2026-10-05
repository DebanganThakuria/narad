package cluster_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httpcluster "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/cluster"
)

// stubBroker satisfies handlers.New; the cluster handlers only touch the
// metastore, so any accidental broker call panics on the nil interface.
type stubBroker struct{ broker.Broker }

func newStore(t *testing.T) *metastore.Store {
	t.Helper()
	s, err := metastore.New(metastore.Config{
		NodeID: "cluster-0", DataDir: t.TempDir(),
		BindAddr: "127.0.0.1:0", AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := s.CreateTopic(context.Background(), topic.Topic{Name: "secret-topic", Partitions: 1}); err == nil {
			return s
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for leader")
	return nil
}

func seededSet(t *testing.T) *handlers.Set {
	t.Helper()
	s := newStore(t)
	ctx := context.Background()
	if err := s.RegisterMember(ctx, metastore.Member{ID: "cluster-0", Addr: "10.0.0.7:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	if err := s.AssignPartition(ctx, "secret-topic", 0, "cluster-0"); err != nil {
		t.Fatalf("AssignPartition: %v", err)
	}
	if err := s.SetAssignmentTarget(ctx, "secret-topic", 0, "cluster-1"); err != nil {
		t.Fatalf("SetAssignmentTarget: %v", err)
	}
	return handlers.New(handlers.Deps{
		Broker: stubBroker{}, Metastore: s,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

func asUser(r *http.Request, u user.User) *http.Request {
	return r.WithContext(security.WithIdentity(r.Context(), u))
}

// A principal holding a single produce grant must not be able to read
// member addresses, liveness, or the topic names of in-flight moves;
// admins (and dev mode with no identity) still can.
func TestClusterReadEndpointsAreAdminOnly(t *testing.T) {
	set := seededSet(t)
	lowPriv := user.User{Username: "lowpriv", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"only-this-topic"}}}}
	admin := user.User{Username: "root", Root: true}

	endpoints := []struct {
		name    string
		path    string
		handler http.HandlerFunc
		leak    string
	}{
		{"members", "/v1/cluster/members", httpcluster.Members(set), "10.0.0.7:7942"},
		{"moves", "/v1/cluster/moves", httpcluster.Moves(set), "secret-topic"},
	}
	for _, ep := range endpoints {
		t.Run(ep.name+"/non-admin", func(t *testing.T) {
			res := httptest.NewRecorder()
			ep.handler.ServeHTTP(res, asUser(httptest.NewRequest(http.MethodGet, ep.path, nil), lowPriv))
			if res.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403 (body %s)", res.Code, res.Body)
			}
			if strings.Contains(res.Body.String(), ep.leak) {
				t.Fatalf("403 body leaked %q: %s", ep.leak, res.Body)
			}
		})
		t.Run(ep.name+"/admin", func(t *testing.T) {
			res := httptest.NewRecorder()
			ep.handler.ServeHTTP(res, asUser(httptest.NewRequest(http.MethodGet, ep.path, nil), admin))
			if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), ep.leak) {
				t.Fatalf("status = %d body = %s, want 200 containing %q", res.Code, res.Body, ep.leak)
			}
		})
		t.Run(ep.name+"/security-disabled", func(t *testing.T) {
			res := httptest.NewRecorder()
			ep.handler.ServeHTTP(res, httptest.NewRequest(http.MethodGet, ep.path, nil))
			if res.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 with no identity (dev mode)", res.Code)
			}
		})
	}
}

func TestDecommissionIsAdminOnly(t *testing.T) {
	set := seededSet(t)
	lowPriv := user.User{Username: "lowpriv", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"*"}}}}

	req := asUser(httptest.NewRequest(http.MethodPost, "/v1/cluster/members/cluster-0/decommission", nil), lowPriv)
	req.SetPathValue("id", "cluster-0")
	res := httptest.NewRecorder()
	httpcluster.Decommission(set).ServeHTTP(res, req)
	if res.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", res.Code)
	}
}

// decommissionRouter stands in for the cluster router on a follower: a
// decommission is forwarded and answered with status.
type decommissionRouter struct {
	handlers.Router
	status int
}

func (f decommissionRouter) RouteDecommissionMember(_ context.Context, w http.ResponseWriter, _ *http.Request, _ string, _ bool) bool {
	w.WriteHeader(f.status)
	return true
}

// auditLines returns the component=audit lines of a JSON log.
func auditLines(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if len(raw) == 0 {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatalf("log line %q: %v", raw, err)
		}
		if m["component"] == "audit" {
			out = append(out, m)
		}
	}
	return out
}

// A decommission (or its cancel) a follower forwards to the leader is
// audited on the node the client called, with the leader's answer.
func TestForwardedDecommissionIsAudited(t *testing.T) {
	admin := user.User{Username: "root", Root: true}
	for _, tc := range []struct {
		method, event string
		status        int
		outcome       string
	}{
		{http.MethodPost, "cluster.decommission", http.StatusNoContent, handlers.AuditOK},
		{http.MethodDelete, "cluster.decommission.cancel", http.StatusNoContent, handlers.AuditOK},
		{http.MethodPost, "cluster.decommission", http.StatusNotFound, handlers.AuditRejected},
	} {
		var logs bytes.Buffer
		set := handlers.New(handlers.Deps{
			Broker: stubBroker{}, Metastore: newStore(t),
			Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
			Router: decommissionRouter{status: tc.status},
		})
		req := asUser(httptest.NewRequest(tc.method, "/v1/cluster/members/cluster-1/decommission", nil), admin)
		req.SetPathValue("id", "cluster-1")
		res := httptest.NewRecorder()
		httpcluster.Decommission(set).ServeHTTP(res, req)
		if res.Code != tc.status {
			t.Fatalf("%s: status = %d, want the leader's %d", tc.event, res.Code, tc.status)
		}
		lines := auditLines(t, &logs)
		if len(lines) != 1 || lines[0]["event"] != tc.event || lines[0]["actor"] != "root" || lines[0]["target"] != "cluster-1" ||
			lines[0]["outcome"] != tc.outcome || lines[0]["status"] != float64(tc.status) {
			t.Fatalf("%s answered %d: audit lines %v, want one with actor root, target cluster-1, outcome %s", tc.event, tc.status, lines, tc.outcome)
		}
	}
}

// freeAddr returns a local address nothing listens on.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// stageGhost leaves id in s's Raft configuration as a non-voter with no
// member record, as a joiner that never registered is left behind.
func stageGhost(t *testing.T, s *metastore.Store, id string) {
	t.Helper()
	if adm, err := s.AdmitJoiner(id, freeAddr(t)); err != nil || adm.Status != metastore.JoinStaged {
		t.Fatalf("AdmitJoiner(%s) = %+v, %v; want staged", id, adm, err)
	}
}

func forgetRequest(id string, u *user.User) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/v1/cluster/members/"+id+"/forget", nil)
	req.SetPathValue("id", id)
	if u != nil {
		req = asUser(req, *u)
	}
	return req
}

// Forget is admin only; an admin's forget is audited with its outcome,
// a refused one as rejected.
func TestForgetIsAdminOnlyAndAudited(t *testing.T) {
	s := newStore(t)
	stageGhost(t, s, "ghost")
	stageGhost(t, s, "registered")
	if err := s.RegisterMember(context.Background(), metastore.Member{ID: "registered", Addr: "10.0.0.7:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	var logs bytes.Buffer
	set := handlers.New(handlers.Deps{
		Broker: stubBroker{}, Metastore: s,
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	lowPriv := user.User{Username: "lowpriv", Grants: []user.Grant{{Action: user.ActionProduce, Patterns: []string{"*"}}}}
	admin := user.User{Username: "root", Root: true}

	res := httptest.NewRecorder()
	httpcluster.Forget(set).ServeHTTP(res, forgetRequest("ghost", &lowPriv))
	if res.Code != http.StatusForbidden {
		t.Fatalf("non-admin forget: status = %d, want 403", res.Code)
	}
	if in, err := s.RaftServer("ghost"); err != nil || !in {
		t.Fatalf("a refused forget removed the server (%v, %v)", in, err)
	}

	res = httptest.NewRecorder()
	httpcluster.Forget(set).ServeHTTP(res, forgetRequest("registered", &admin))
	if res.Code != http.StatusConflict || !strings.Contains(res.Body.String(), "decommission it instead") {
		t.Fatalf("forget of a server with a member record: %d %s, want 409 saying to decommission", res.Code, res.Body)
	}

	res = httptest.NewRecorder()
	httpcluster.Forget(set).ServeHTTP(res, forgetRequest("ghost", &admin))
	if res.Code != http.StatusOK || strings.TrimSpace(res.Body.String()) != `{"id":"ghost","voter":false}` {
		t.Fatalf("admin forget: %d %s, want 200 {id: ghost, voter: false}", res.Code, res.Body)
	}
	if in, err := s.RaftServer("ghost"); err != nil || in {
		t.Fatalf("ghost still in the raft configuration (%v, %v)", in, err)
	}

	res = httptest.NewRecorder()
	httpcluster.Forget(set).ServeHTTP(res, forgetRequest("ghost", &admin))
	if res.Code != http.StatusNotFound {
		t.Fatalf("forget of a server already gone: %d %s, want 404", res.Code, res.Body)
	}

	var got []string
	for _, line := range auditLines(t, &logs) {
		if line["event"] != "cluster.forget" {
			continue
		}
		if line["actor"] != "root" {
			t.Fatalf("forget audit line %v, want actor root", line)
		}
		got = append(got, fmt.Sprintf("%s %s %v", line["target"], line["outcome"], line["status"]))
	}
	want := []string{"registered rejected 409", "ghost ok 200", "ghost rejected 404"}
	if !slices.Equal(got, want) {
		t.Fatalf("forget audit lines = %q, want %q", got, want)
	}
}

// forgetForwarder stands in for the cluster router on a follower: a
// forget is forwarded and answered by "the leader".
type forgetForwarder struct {
	handlers.Router
	calls *int
}

func (f forgetForwarder) RouteForgetServer(_ context.Context, w http.ResponseWriter, _ *http.Request, id string) bool {
	*f.calls++
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"id":%q,"voter":true}`, id)
	return true
}

// On a follower the forget goes to the leader through the router, and is
// audited on the node the client called.
func TestForgetForwardsToTheLeader(t *testing.T) {
	s := newStore(t)
	stageGhost(t, s, "ghost")
	var logs bytes.Buffer
	calls := 0
	set := handlers.New(handlers.Deps{
		Broker: stubBroker{}, Metastore: s,
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		Router: forgetForwarder{calls: &calls},
	})
	res := httptest.NewRecorder()
	httpcluster.Forget(set).ServeHTTP(res, forgetRequest("ghost", &user.User{Username: "root", Root: true}))
	if res.Code != http.StatusOK || calls != 1 || !strings.Contains(res.Body.String(), `"voter":true`) {
		t.Fatalf("forward: %d %s after %d router calls, want the leader's 200 after one", res.Code, res.Body, calls)
	}
	if in, err := s.RaftServer("ghost"); err != nil || !in {
		t.Fatalf("the follower forgot the server itself (%v, %v); only the leader may", in, err)
	}
	lines := auditLines(t, &logs)
	if len(lines) != 1 || lines[0]["event"] != "cluster.forget" || lines[0]["outcome"] != handlers.AuditOK || lines[0]["target"] != "ghost" {
		t.Fatalf("audit lines %v, want one cluster.forget of ghost, outcome ok", lines)
	}
}

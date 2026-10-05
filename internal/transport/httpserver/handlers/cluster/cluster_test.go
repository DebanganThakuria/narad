package cluster_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/cluster/controller"
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

func adminRequest(method, target string, pathValues ...string) *http.Request {
	req := asUser(httptest.NewRequest(method, target, nil), user.User{Username: "root", Root: true})
	for i := 0; i+1 < len(pathValues); i += 2 {
		req.SetPathValue(pathValues[i], pathValues[i+1])
	}
	return req
}

func isDraining(t *testing.T, s *metastore.Store, id string) bool {
	t.Helper()
	m, err := s.GetMember(id)
	if err != nil {
		t.Fatalf("GetMember(%s): %v", id, err)
	}
	return m.Draining
}

func decodeBody[T any](t *testing.T, res *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(res.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %s: %v", res.Body, err)
	}
	return v
}

// A decommission that could never complete safely is refused up front
// with 409 and every reason, and nothing is written: here the only voter
// (too few voters would be left, and nobody could take its partition),
// and a dead member that owns a partition.
func TestDecommissionPreflightRefusesWithReasons(t *testing.T) {
	set := seededSet(t)
	ms := set.Deps.Metastore
	ctx := context.Background()
	if err := ms.RegisterMember(ctx, metastore.Member{ID: "cluster-2", Addr: "10.0.0.9:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateTopic(ctx, topic.Topic{Name: "dead-owned", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if err := ms.AssignPartition(ctx, "dead-owned", 0, "cluster-2"); err != nil {
		t.Fatal(err)
	}
	if err := ms.MarkMemberDead(ctx, "cluster-2"); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		id    string
		codes []string
	}{
		{"cluster-0", []string{controller.BlockedBelowMinVoters, controller.BlockedNoReceivers}},
		{"cluster-2", []string{controller.BlockedOwnerDead}},
	} {
		res := httptest.NewRecorder()
		httpcluster.Decommission(set).ServeHTTP(res, adminRequest(http.MethodPost, "/v1/cluster/members/"+tc.id+"/decommission", "id", tc.id))
		if res.Code != http.StatusConflict {
			t.Fatalf("decommission %s: status %d (%s), want 409", tc.id, res.Code, res.Body)
		}
		body := decodeBody[controller.DecommissionRefusal](t, res)
		var codes []string
		for _, r := range body.Reasons {
			codes = append(codes, r.Code)
			if r.Message == "" {
				t.Fatalf("reason %s has no message", r.Code)
			}
		}
		if !slices.Equal(codes, tc.codes) || body.Error == "" {
			t.Fatalf("decommission %s refused with %v (error %q), want %v", tc.id, codes, body.Error, tc.codes)
		}
		if isDraining(t, ms, tc.id) {
			t.Fatalf("a refused decommission still marked %s draining", tc.id)
		}
	}
}

// ?dry_run=true says what a decommission would do and writes nothing,
// and a cancel cannot be dry-run.
func TestDecommissionDryRunWritesNothing(t *testing.T) {
	set := seededSet(t)
	ms := set.Deps.Metastore
	if err := ms.RegisterMember(context.Background(), metastore.Member{ID: "cluster-1", Addr: "10.0.0.8:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatal(err)
	}

	res := httptest.NewRecorder()
	httpcluster.Decommission(set).ServeHTTP(res, adminRequest(http.MethodPost, "/v1/cluster/members/cluster-1/decommission?dry_run=true", "id", "cluster-1"))
	if res.Code != http.StatusOK {
		t.Fatalf("dry run: status %d (%s), want 200", res.Code, res.Body)
	}
	got := decodeBody[httpcluster.DryRunView](t, res)
	if got.Member != "cluster-1" || !got.WouldDecommission || got.Voter || got.InboundMoves != 1 || got.Reasons == nil || len(got.Reasons) != 0 {
		t.Fatalf("dry run = %+v, want cluster-1 decommissionable, not a voter, one inbound move, no reasons", got)
	}
	if isDraining(t, ms, "cluster-1") {
		t.Fatal("a dry run marked cluster-1 draining")
	}

	res = httptest.NewRecorder()
	httpcluster.Decommission(set).ServeHTTP(res, adminRequest(http.MethodPost, "/v1/cluster/members/cluster-0/decommission?dry_run=1", "id", "cluster-0"))
	if got := decodeBody[httpcluster.DryRunView](t, res); res.Code != http.StatusOK || got.WouldDecommission || !got.Voter || got.OwnedPartitions != 1 || len(got.Reasons) != 1 || got.Reasons[0].Code != controller.BlockedBelowMinVoters {
		t.Fatalf("dry run of the only voter: status %d, %+v; want would_decommission false, below_min_voters", res.Code, got)
	}

	res = httptest.NewRecorder()
	httpcluster.Decommission(set).ServeHTTP(res, adminRequest(http.MethodDelete, "/v1/cluster/members/cluster-1/decommission?dry_run=true", "id", "cluster-1"))
	if res.Code != http.StatusBadRequest {
		t.Fatalf("dry-run cancel: status %d, want 400", res.Code)
	}
	res = httptest.NewRecorder()
	httpcluster.Decommission(set).ServeHTTP(res, adminRequest(http.MethodPost, "/v1/cluster/members/cluster-1/decommission?dry_run=maybe", "id", "cluster-1"))
	if res.Code != http.StatusBadRequest || isDraining(t, ms, "cluster-1") {
		t.Fatalf("dry_run=maybe: status %d, want 400 and nothing written", res.Code)
	}
}

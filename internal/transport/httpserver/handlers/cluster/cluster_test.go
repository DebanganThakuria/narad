package cluster_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

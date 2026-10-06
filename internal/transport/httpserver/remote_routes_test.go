package httpserver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
)

// Every route of the remote-replication contract is registered, with a
// metastore wired in: none answers the mux's own 404 or 405.
func TestRemoteRoutesAreRegistered(t *testing.T) {
	ms, err := metastore.New(metastore.Config{NodeID: "n0", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for ms.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}) != nil {
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	set := handlers.New(handlers.Deps{Broker: &fakeBroker{}, Logger: newTestLogger(), Metastore: ms})
	h := NewRouterWithOptions(set, newTestLogger(), nil, nil, nil, DefaultRouterOptions())
	routes := []struct{ method, path string }{
		{http.MethodPost, "/v1/remotes"},
		{http.MethodGet, "/v1/remotes"},
		{http.MethodGet, "/v1/remotes/b"},
		{http.MethodPatch, "/v1/remotes/b"},
		{http.MethodDelete, "/v1/remotes/b"},
		{http.MethodPost, "/v1/remotes/b/test"},
		{http.MethodPost, "/v1/cluster/reencrypt-remotes"},
		{http.MethodPost, "/v1/topics/orders/children/orders-to-b/pause"},
		{http.MethodPost, "/v1/topics/orders/children/orders-to-b/resume"},
		{http.MethodPost, "/v1/topics/orders/children/orders-to-b/skip"},
	}
	for _, rt := range routes {
		r := httptest.NewRequest(rt.method, rt.path, strings.NewReader(`{}`))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		body, _ := io.ReadAll(w.Body)
		if w.Code == http.StatusMethodNotAllowed || (w.Code == http.StatusNotFound && string(body) == "404 page not found\n") {
			t.Fatalf("%s %s is not registered: %d %q", rt.method, rt.path, w.Code, body)
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s %s: Cache-Control = %q, want no-store", rt.method, rt.path, w.Header().Get("Cache-Control"))
		}
	}
}

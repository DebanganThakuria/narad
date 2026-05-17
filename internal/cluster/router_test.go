package cluster

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
)

func newTestStore(t *testing.T) *metastore.Store {
	t.Helper()
	store, err := metastore.New(metastore.Config{
		NodeID:   "node-self",
		DataDir:  t.TempDir(),
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("metastore.New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := store.CreateTopic(context.Background(), topic.Topic{Name: "__probe__", Partitions: 1}); err == nil {
			_ = store.DeleteTopic(context.Background(), "__probe__")
			return store
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("timed out waiting for leader")
	return nil
}

func seedTopicRouteState(t *testing.T, store *metastore.Store) {
	t.Helper()
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote", "node-follower"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
}

func TestNewRouter(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	if router == nil {
		t.Fatal("NewRouter() returned nil")
	}
}

func TestOwnerAddrReturnsRemoteMemberAddress(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	got := router.ownerAddr("orders", 1)
	if got != "remote.example:7942" {
		t.Fatalf("ownerAddr() = %q, want %q", got, "remote.example:7942")
	}
}

func TestOwnerAddrReturnsEmptyForLocalOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self", "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	if got := router.ownerAddr("orders", 0); got != "" {
		t.Fatalf("ownerAddr() = %q, want empty", got)
	}
}

func TestOwnerAddrReturnsEmptyForDeadMember(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-remote", "node-follower"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	if got := router.ownerAddr("orders", 2); got != "" {
		t.Fatalf("ownerAddr() = %q, want empty", got)
	}
}

func TestRouteProduceReturnsFalseWhenTopicMissing(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":{"id":1}}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"message":{"id":1}}`))
	if forwarded {
		t.Fatal("RouteProduce() = true, want false")
	}
}

func TestRouteConsumeReturnsFalseWhenTopicMissing(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)

	forwarded := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if forwarded {
		t.Fatal("RouteConsume() = true, want false")
	}
}

func TestRouteConsumeReturnsFalseWhenTopicHasNoPartitions(t *testing.T) {
	store := newTestStore(t)
	if err := store.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 0}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)

	forwarded := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if forwarded {
		t.Fatal("RouteConsume() = true, want false")
	}
}

func TestRouteAckReturnsFalseWhenOwnerMissing(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", bytes.NewBufferString(`{"receipt_handle":"h1"}`))

	forwarded := router.RouteAck(context.Background(), res, req, "orders", 0, []byte(`{"receipt_handle":"h1"}`))
	if forwarded {
		t.Fatal("RouteAck() = true, want false")
	}
}

func TestRouteProduceForwardsToRemoteOwner(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if string(body) != `{"message":{"id":1}}` {
			t.Fatalf("body = %q, want %q", body, `{"message":{"id":1}}`)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote", "node-follower"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 1})
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":{"id":1}}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"message":{"id":1}}`))
	if !forwarded {
		t.Fatal("RouteProduce() = false, want true")
	}
	if res.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusAccepted)
	}
}

func TestRouteConsumeForwardsPinnedPartitionToRemoteOwner(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("partition"); got != "1" {
			t.Fatalf("partition query = %q, want %q", got, "1")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote", "node-follower"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)
	forwarded := router.RouteConsume(context.Background(), res, req, "orders", new(1))
	if !forwarded {
		t.Fatal("RouteConsume() = false, want true")
	}
	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusNoContent)
	}
}

func TestRouteAckForwardsBodyToRemoteOwner(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if string(body) != `{"receipt_handle":"h1"}` {
			t.Fatalf("body = %q, want %q", body, `{"receipt_handle":"h1"}`)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote", "node-follower"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", bytes.NewBufferString(`{"receipt_handle":"h1"}`))

	forwarded := router.RouteAck(context.Background(), res, req, "orders", 0, []byte(`{"receipt_handle":"h1"}`))
	if !forwarded {
		t.Fatal("RouteAck() = false, want true")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusOK)
	}
}

type fixedPartitionManager struct {
	picked int
}

func (f fixedPartitionManager) Pick(string, string, int) int { return f.picked }

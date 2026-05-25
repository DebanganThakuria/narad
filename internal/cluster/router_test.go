package cluster

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	if router == nil {
		t.Fatal("NewRouter() returned nil")
	}
}

func TestOwnerAddrReturnsRemoteMemberAddress(t *testing.T) {
	store := newTestStore(t)
	seedTopicRouteState(t, store)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)

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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)

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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)

	if got := router.ownerAddr("orders", 2); got != "" {
		t.Fatalf("ownerAddr() = %q, want empty", got)
	}
}

func TestRouteProduceReturnsFalseWhenTopicMissing(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":{"id":1}}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"message":{"id":1}}`))
	if forwarded {
		t.Fatal("RouteProduce() = true, want false")
	}
}

func TestRouteConsumeReturnsFalseWhenTopicMissing(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)

	forwarded := router.RouteConsume(context.Background(), res, req, "orders", nil, 0)
	if forwarded {
		t.Fatal("RouteConsume() = true, want false")
	}
}

func TestRouteConsumeReturnsFalseWhenTopicHasNoPartitions(t *testing.T) {
	store := newTestStore(t)
	if err := store.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 0}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume", nil)

	forwarded := router.RouteConsume(context.Background(), res, req, "orders", nil, 0)
	if forwarded {
		t.Fatal("RouteConsume() = true, want false")
	}
}

func TestRouteAckReturnsFalseWhenOwnerMissing(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
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
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 1}, nil)
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)
	pinned := 1
	forwarded := router.RouteConsume(context.Background(), res, req, "orders", &pinned, time.Second)
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
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

func TestRouteConsumeWalksRankedPartitionsOnce(t *testing.T) {
	var partitions []int
	remote1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := strconv.Atoi(r.URL.Query().Get("partition"))
		if err != nil {
			t.Fatalf("partition query parse error = %v", err)
		}
		if got := r.URL.Query().Get("wait"); got != "0s" {
			t.Fatalf("wait query = %q, want %q", got, "0s")
		}
		partitions = append(partitions, p)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer remote1.Close()

	remote2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := strconv.Atoi(r.URL.Query().Get("partition"))
		if err != nil {
			t.Fatalf("partition query parse error = %v", err)
		}
		if got := r.URL.Query().Get("wait"); got != "0s" {
			t.Fatalf("wait query = %q, want %q", got, "0s")
		}
		partitions = append(partitions, p)
		w.WriteHeader(http.StatusOK)
	}))
	defer remote2.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote-1", Addr: remote1.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote-2", Addr: remote2.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-1", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-remote-2", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), snapshotProviderFunc(func(context.Context) ([]metrics.TopicSnapshot, error) {
		return []metrics.TopicSnapshot{{
			Topic: "orders",
			Partitions: []metrics.PartitionSnapshot{
				{Partition: 0, LogEndOffset: 0, CommittedOffset: 0},
				{Partition: 1, LogEndOffset: 10, CommittedOffset: 1},
				{Partition: 2, LogEndOffset: 9, CommittedOffset: 1},
			},
		}}, nil
	}))
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)

	forwarded := router.RouteConsume(context.Background(), res, req, "orders", nil, time.Second)
	if !forwarded {
		t.Fatal("RouteConsume() = false, want true")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusOK)
	}
	if len(partitions) != 2 {
		t.Fatalf("probed partitions = %v, want [1 2]", partitions)
	}
	if partitions[0] != 1 || partitions[1] != 2 {
		t.Fatalf("probed partitions = %v, want [1 2]", partitions)
	}
}

type snapshotProviderFunc func(context.Context) ([]metrics.TopicSnapshot, error)

func (f snapshotProviderFunc) Snapshot(ctx context.Context) ([]metrics.TopicSnapshot, error) {
	return f(ctx)
}

type fixedPartitionManager struct {
	picked int
}

func (f fixedPartitionManager) Pick(string, string, int) int { return f.picked }

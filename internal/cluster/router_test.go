package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/observability/metrics"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

type fakePeerClient struct {
	produceFn             func(context.Context, string, nodewire.ProduceRequest) (nodewire.Response, error)
	commitProduceFn       func(context.Context, string, nodewire.CommitProduceRequest) (nodewire.Response, error)
	commitProduceBatchFn  func(context.Context, string, nodewire.CommitProduceBatchRequest) (nodewire.Response, error)
	consumeFn             func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error)
	ackFn                 func(context.Context, string, nodewire.AckRequest) (nodewire.Response, error)
	createTopicFn         func(context.Context, string, []byte) (nodewire.Response, error)
	alterTopicFn          func(context.Context, string, string, []byte) (nodewire.Response, error)
	deleteTopicFn         func(context.Context, string, string) (nodewire.Response, error)
	purgeTopicFn          func(context.Context, string, string) (nodewire.Response, error)
	topicPartitionStatsFn func(context.Context, string, string, int) (topic.PartitionStats, error)
	registerMemberFn      func(context.Context, string, nodewire.MemberRequest) (nodewire.Response, error)
}

func (f fakePeerClient) Produce(ctx context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
	if f.produceFn != nil {
		return f.produceFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) CommitProduce(ctx context.Context, addr string, req nodewire.CommitProduceRequest) (nodewire.Response, error) {
	if f.commitProduceFn != nil {
		return f.commitProduceFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) CommitProduceBatch(ctx context.Context, addr string, req nodewire.CommitProduceBatchRequest) (nodewire.Response, error) {
	if f.commitProduceBatchFn != nil {
		return f.commitProduceBatchFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) Consume(ctx context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
	if f.consumeFn != nil {
		return f.consumeFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) Ack(ctx context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
	if f.ackFn != nil {
		return f.ackFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) CreateTopic(ctx context.Context, addr string, body []byte) (nodewire.Response, error) {
	if f.createTopicFn != nil {
		return f.createTopicFn(ctx, addr, body)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) AlterTopic(ctx context.Context, addr, topicName string, body []byte) (nodewire.Response, error) {
	if f.alterTopicFn != nil {
		return f.alterTopicFn(ctx, addr, topicName, body)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) DeleteTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
	if f.deleteTopicFn != nil {
		return f.deleteTopicFn(ctx, addr, topicName)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) PurgeTopic(ctx context.Context, addr, topicName string) (nodewire.Response, error) {
	if f.purgeTopicFn != nil {
		return f.purgeTopicFn(ctx, addr, topicName)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

func (f fakePeerClient) TopicPartitionStats(ctx context.Context, addr, topicName string, partition int) (topic.PartitionStats, error) {
	if f.topicPartitionStatsFn != nil {
		return f.topicPartitionStatsFn(ctx, addr, topicName, partition)
	}
	return topic.PartitionStats{}, context.DeadlineExceeded
}

func (f fakePeerClient) RegisterMember(ctx context.Context, addr string, req nodewire.MemberRequest) (nodewire.Response, error) {
	if f.registerMemberFn != nil {
		return f.registerMemberFn(ctx, addr, req)
	}
	return nodewire.Response{}, context.DeadlineExceeded
}

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

	forwarded, localPartition := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if forwarded {
		t.Fatal("RouteConsume() = true, want false")
	}
	if localPartition != nil {
		t.Fatalf("RouteConsume() local partition = %d, want nil", *localPartition)
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

	forwarded, localPartition := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if forwarded {
		t.Fatal("RouteConsume() = true, want false")
	}
	if localPartition != nil {
		t.Fatalf("RouteConsume() local partition = %d, want nil", *localPartition)
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
		if got := r.URL.Query().Get("partition"); got != "1" {
			t.Fatalf("partition query = %q, want %q", got, "1")
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
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-follower", Addr: "follower.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(follower) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote", "node-follower"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 1}, nil)
	router.peer = fakePeerClient{produceFn: func(_ context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if req.Partition != 1 {
			t.Fatalf("partition = %d, want 1", req.Partition)
		}
		if string(req.Payload) != `{"message":{"id":1}}` {
			t.Fatalf("body = %q, want %q", req.Payload, `{"message":{"id":1}}`)
		}
		return nodewire.Response{Status: http.StatusAccepted}, nil
	}}
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

func TestRouteProduceFallsBackToNextReachablePartition(t *testing.T) {
	failedAddr := unreachableAddr(t)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("partition"); got != "2" {
			t.Fatalf("partition query = %q, want %q", got, "2")
		}
		w.Header().Set("X-Route", "partition-2")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote-failed", Addr: failedAddr, Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-failed", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 1}, nil)
	router.peer = fakePeerClient{produceFn: func(_ context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
		if addr == failedAddr {
			return nodewire.Response{}, context.DeadlineExceeded
		}
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if req.Partition != 2 {
			t.Fatalf("partition = %d, want 2", req.Partition)
		}
		return nodewire.Response{Status: http.StatusOK, Body: []byte(`{"ok":true}`)}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":{"id":1}}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"message":{"id":1}}`))
	if !forwarded {
		t.Fatal("RouteProduce() = false, want true")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusOK)
	}
	if got := res.Body.String(); got != `{"ok":true}` {
		t.Fatalf("body = %q, want %q", got, `{"ok":true}`)
	}
}

func TestRouteProduceSkipsUnwritableLocalOwner(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("partition"); got != "1" {
			t.Fatalf("partition query = %q, want %q", got, "1")
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(self) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: "dead.example:7942", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember(dead) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-follower", Addr: "follower.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(follower) error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember(remote) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self", "node-dead"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote", "node-follower"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 0}, nil)
	router.peer = fakePeerClient{produceFn: func(_ context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if req.Partition != 1 {
			t.Fatalf("partition = %d, want 1", req.Partition)
		}
		return nodewire.Response{Status: http.StatusAccepted}, nil
	}}
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

func TestRouteProduceReturnsFalseWhenNoOwnersReachable(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote-0", Addr: unreachableAddr(t), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote-1", Addr: unreachableAddr(t), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote-0", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-1", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 0}, nil)
	router.peer = fakePeerClient{produceFn: func(context.Context, string, nodewire.ProduceRequest) (nodewire.Response, error) {
		return nodewire.Response{}, context.DeadlineExceeded
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":{"id":1}}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"message":{"id":1}}`))
	if forwarded {
		t.Fatal("RouteProduce() = true, want false")
	}
}

func TestRouteProduceSkipsDeadMemberThenRetriesTransportFailure(t *testing.T) {
	failedAddr := unreachableAddr(t)
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("partition"); got != "2" {
			t.Fatalf("partition query = %q, want %q", got, "2")
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: "dead.example:7942", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote-failed", Addr: failedAddr, Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-dead", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-failed", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 0}, nil)
	router.peer = fakePeerClient{produceFn: func(_ context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
		if addr == failedAddr {
			return nodewire.Response{}, context.DeadlineExceeded
		}
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if req.Partition != 2 {
			t.Fatalf("partition = %d, want 2", req.Partition)
		}
		return nodewire.Response{Status: http.StatusAccepted}, nil
	}}
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

func TestRouteProduceRetriesAfterNon2xxResponse(t *testing.T) {
	var firstCalls int
	remoteFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCalls++
		if got := r.URL.Query().Get("partition"); got != "1" {
			t.Fatalf("partition query = %q, want %q", got, "1")
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"busy"}`))
	}))
	defer remoteFail.Close()

	remoteOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("partition"); got != "2" {
			t.Fatalf("partition query = %q, want %q", got, "2")
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer remoteOK.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 3}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote-1", Addr: remoteFail.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote-2", Addr: remoteOK.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-1", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-remote-2", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 1}, nil)
	router.peer = fakePeerClient{produceFn: func(_ context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
		if addr == remoteFail.Listener.Addr().String() {
			firstCalls++
			if req.Partition != 1 {
				t.Fatalf("partition = %d, want 1", req.Partition)
			}
			return nodewire.Response{Status: http.StatusServiceUnavailable, Body: []byte(`{"error":"busy"}`)}, nil
		}
		if addr != remoteOK.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remoteOK.Listener.Addr().String())
		}
		if req.Partition != 2 {
			t.Fatalf("partition = %d, want 2", req.Partition)
		}
		return nodewire.Response{Status: http.StatusCreated, Body: []byte(`{"ok":true}`)}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":{"id":1}}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"message":{"id":1}}`))
	if !forwarded {
		t.Fatal("RouteProduce() = false, want true")
	}
	if firstCalls != 1 {
		t.Fatalf("firstCalls = %d, want %d", firstCalls, 1)
	}
	if res.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusCreated)
	}
	if got := res.Body.String(); got != `{"ok":true}` {
		t.Fatalf("body = %q, want %q", got, `{"ok":true}`)
	}
}

func TestRouteProduceReturnsFalseWhenAllResponsesAreNon2xx(t *testing.T) {
	remote1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer remote1.Close()
	remote2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer remote2.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote-0", Addr: remote1.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote-1", Addr: remote2.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote-0", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-1", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 0}, nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"message":{"id":1}}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"message":{"id":1}}`))
	if forwarded {
		t.Fatal("RouteProduce() = true, want false")
	}
}

func unreachableAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	return addr
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
	router.peer = fakePeerClient{consumeFn: func(_ context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if !req.HasPartition || req.Partition != 1 {
			t.Fatalf("partition request = %+v, want partition 1", req)
		}
		if req.WaitNanos != int64(time.Second) {
			t.Fatalf("wait = %s, want 1s", time.Duration(req.WaitNanos))
		}
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)
	forwarded, localPartition := router.RouteConsume(context.Background(), res, req, "orders", new(1))
	if !forwarded {
		t.Fatal("RouteConsume() = false, want true")
	}
	if localPartition != nil {
		t.Fatalf("RouteConsume() local partition = %d, want nil", *localPartition)
	}
	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusNoContent)
	}
}

func TestRouteConsumeReturnsLocalPartitionWithoutRemoteProbe(t *testing.T) {
	var probed []int
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := strconv.Atoi(r.URL.Query().Get("partition"))
		if err != nil {
			t.Fatalf("partition query parse error = %v", err)
		}
		if got := r.URL.Query().Get("wait"); got != "0s" {
			t.Fatalf("wait query = %q, want %q", got, "0s")
		}
		probed = append(probed, p)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-self", ""); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)

	forwarded, localPartition := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if forwarded {
		t.Fatal("RouteConsume() forwarded = true, want false")
	}
	if localPartition == nil || *localPartition != 1 {
		t.Fatalf("RouteConsume() local partition = %+v, want 1", localPartition)
	}
	if len(probed) != 0 {
		t.Fatalf("probed partitions = %v, want none", probed)
	}
}

func TestRouteConsumeReturnsNoContentWhenOnlyRemotePartitionsAreEmpty(t *testing.T) {
	var probes int
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("partition"); got != "" {
			t.Fatalf("partition query = %q, want empty", got)
		}
		if got := r.URL.Query().Get("local_only"); got != "1" {
			t.Fatalf("local_only query = %q, want 1", got)
		}
		if got := r.URL.Query().Get("wait"); got != "0s" {
			t.Fatalf("wait query = %q, want 0s", got)
		}
		probes++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{consumeFn: func(_ context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if !req.HasPartition || req.Partition != 0 {
			t.Fatalf("partition request = %+v, want partition 0", req)
		}
		if !req.LocalOnly || req.WaitNanos != 0 {
			t.Fatalf("local-only request = %+v, want local_only wait=0", req)
		}
		probes++
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)

	forwarded, localPartition := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if !forwarded {
		t.Fatal("RouteConsume() forwarded = false, want true")
	}
	if localPartition != nil {
		t.Fatalf("RouteConsume() local partition = %d, want nil", *localPartition)
	}
	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if probes != 1 {
		t.Fatalf("remote probes = %d, want 1", probes)
	}
}

func TestRouteConsumeRemoteTreatsNotOwnerProbeAsEmpty(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{consumeFn: func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusMisdirectedRequest, Body: []byte(`{"error":"not owner"}`)}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)

	forwarded, hadCandidates := router.RouteConsumeRemote(context.Background(), res, req, "orders")
	if forwarded {
		t.Fatal("RouteConsumeRemote() forwarded = true, want false")
	}
	if !hadCandidates {
		t.Fatal("RouteConsumeRemote() hadCandidates = false, want true")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("response was written with status %d, want untouched recorder status 200", res.Code)
	}
}

func TestRouteConsumeRemoteStopsAfterFirstDeliveredProbe(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	for _, member := range []metastore.Member{
		{ID: "node-remote-a", Addr: "remote-a.example:7942", Status: metastore.MemberAlive},
		{ID: "node-remote-b", Addr: "remote-b.example:7942", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, member); err != nil {
			t.Fatalf("RegisterMember(%s) error = %v", member.ID, err)
		}
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote-a", ""); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-b", ""); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	var calls []string
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{consumeFn: func(_ context.Context, addr string, _ nodewire.ConsumeRequest) (nodewire.Response, error) {
		calls = append(calls, addr)
		if addr != "remote-a.example:7942" {
			t.Fatalf("unexpected second remote consume probe to %q after first delivered", addr)
		}
		return nodewire.Response{
			Status:      http.StatusOK,
			ContentType: "application/json",
			Body:        []byte(`{"topic":"orders","partition":0,"offset":0,"payload":{"id":1},"receipt_handle":"h1"}`),
		}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)

	forwarded, hadCandidates := router.RouteConsumeRemote(context.Background(), res, req, "orders")
	if !forwarded {
		t.Fatal("RouteConsumeRemote() forwarded = false, want true")
	}
	if !hadCandidates {
		t.Fatal("RouteConsumeRemote() hadCandidates = false, want true")
	}
	if len(calls) != 1 || calls[0] != "remote-a.example:7942" {
		t.Fatalf("remote consume probes = %v, want only remote-a", calls)
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestRouteConsumeRemoteProbesEachRemoteOwnerOnce(t *testing.T) {
	var remoteAProbes int
	remoteA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("partition"); got != "" {
			t.Fatalf("remote A partition query = %q, want empty", got)
		}
		if got := r.URL.Query().Get("local_only"); got != "1" {
			t.Fatalf("remote A local_only query = %q, want 1", got)
		}
		remoteAProbes++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer remoteA.Close()

	var remoteBProbes int
	remoteB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("partition"); got != "" {
			t.Fatalf("remote B partition query = %q, want empty", got)
		}
		if got := r.URL.Query().Get("local_only"); got != "1" {
			t.Fatalf("remote B local_only query = %q, want 1", got)
		}
		remoteBProbes++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer remoteB.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 6}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	for _, member := range []metastore.Member{
		{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive},
		{ID: "node-remote-a", Addr: remoteA.Listener.Addr().String(), Status: metastore.MemberAlive},
		{ID: "node-remote-b", Addr: remoteB.Listener.Addr().String(), Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, member); err != nil {
			t.Fatalf("RegisterMember(%s) error = %v", member.ID, err)
		}
	}
	for _, assignment := range []struct {
		partition int
		ownerID   string
	}{
		{partition: 0, ownerID: "node-self"},
		{partition: 1, ownerID: "node-self"},
		{partition: 2, ownerID: "node-remote-a"},
		{partition: 3, ownerID: "node-remote-a"},
		{partition: 4, ownerID: "node-remote-b"},
		{partition: 5, ownerID: "node-remote-b"},
	} {
		if err := store.AssignPartition(ctx, "orders", assignment.partition, assignment.ownerID, ""); err != nil {
			t.Fatalf("AssignPartition(%d) error = %v", assignment.partition, err)
		}
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{consumeFn: func(_ context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		if !req.LocalOnly {
			t.Fatalf("local_only = false, want true")
		}
		switch addr {
		case remoteA.Listener.Addr().String():
			if !req.HasPartition || req.Partition != 2 {
				t.Fatalf("remote A request = %+v, want partition 2", req)
			}
			remoteAProbes++
		case remoteB.Listener.Addr().String():
			if !req.HasPartition || req.Partition != 4 {
				t.Fatalf("remote B request = %+v, want partition 4", req)
			}
			remoteBProbes++
		default:
			t.Fatalf("unexpected addr %q", addr)
		}
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)

	forwarded, hadCandidates := router.RouteConsumeRemote(context.Background(), res, req, "orders")
	if forwarded {
		t.Fatal("RouteConsumeRemote() forwarded = true, want false")
	}
	if !hadCandidates {
		t.Fatal("RouteConsumeRemote() hadCandidates = false, want true")
	}
	if remoteAProbes != 1 || remoteBProbes != 1 {
		t.Fatalf("remote probes = A:%d B:%d, want A:1 B:1", remoteAProbes, remoteBProbes)
	}
}

func TestRemoteConsumeCandidatesLimitOneProbePerRemoteOwner(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 5}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	for _, member := range []metastore.Member{
		{ID: "node-remote-a", Addr: "remote-a.example:7942", Status: metastore.MemberAlive},
		{ID: "node-remote-b", Addr: "remote-b.example:7942", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, member); err != nil {
			t.Fatalf("RegisterMember(%s) error = %v", member.ID, err)
		}
	}
	for _, assignment := range []struct {
		partition int
		ownerID   string
	}{
		{partition: 0, ownerID: "node-remote-a"},
		{partition: 1, ownerID: "node-remote-a"},
		{partition: 2, ownerID: "node-remote-a"},
		{partition: 3, ownerID: "node-remote-b"},
		{partition: 4, ownerID: "node-remote-b"},
	} {
		if err := store.AssignPartition(ctx, "orders", assignment.partition, assignment.ownerID, ""); err != nil {
			t.Fatalf("AssignPartition(%d) error = %v", assignment.partition, err)
		}
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	candidates := router.remoteConsumeCandidates("orders")
	if len(candidates) != 2 {
		t.Fatalf("remoteConsumeCandidates() len = %d, want 2: %+v", len(candidates), candidates)
	}

	seen := map[string]bool{}
	partitions := map[string]int{}
	for _, candidate := range candidates {
		if seen[candidate.addr] {
			t.Fatalf("remoteConsumeCandidates() returned duplicate owner addr %q: %+v", candidate.addr, candidates)
		}
		seen[candidate.addr] = true
		partitions[candidate.addr] = candidate.partition
	}
	if !seen["remote-a.example:7942"] || !seen["remote-b.example:7942"] {
		t.Fatalf("remoteConsumeCandidates() owners = %v, want remote-a and remote-b", seen)
	}
	if partitions["remote-a.example:7942"] != 0 || partitions["remote-b.example:7942"] != 3 {
		t.Fatalf("remoteConsumeCandidates() partitions = %v, want remote-a:0 remote-b:3", partitions)
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
	router.peer = fakePeerClient{ackFn: func(_ context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if req.Topic != "orders" || req.ReceiptHandle != "h1" {
			t.Fatalf("ack request = %+v, want orders/h1", req)
		}
		return nodewire.Response{Status: http.StatusOK, Body: []byte("ok")}, nil
	}}
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

func TestRouteGetTopicMergesRemotePartitionStats(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("partition"); got != "1" {
			t.Fatalf("partition query = %q, want %q", got, "1")
		}
		_ = json.NewEncoder(w).Encode(topic.Details{
			Topic:      topic.Topic{Name: "orders", Partitions: 2},
			Partitions: []topic.PartitionStats{{Index: 1, NextOffset: 20}},
		})
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{topicPartitionStatsFn: func(_ context.Context, addr, topicName string, partition int) (topic.PartitionStats, error) {
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if topicName != "orders" || partition != 1 {
			t.Fatalf("stats request topic=%q partition=%d, want orders/1", topicName, partition)
		}
		return topic.PartitionStats{Index: 1, NextOffset: 20}, nil
	}}
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	details, err := router.RouteGetTopic(context.Background(), req, "orders", topic.Details{
		Topic:      topic.Topic{Name: "orders", Partitions: 2},
		Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 10}, {Index: 1, NextOffset: 0}},
	})
	if err != nil {
		t.Fatalf("RouteGetTopic() error = %v", err)
	}
	if len(details.Partitions) != 2 {
		t.Fatalf("len(Partitions) = %d, want 2", len(details.Partitions))
	}
	if details.Partitions[0].Index != 0 || details.Partitions[0].NextOffset != 10 {
		t.Fatalf("partition 0 = %+v", details.Partitions[0])
	}
	if details.Partitions[1].Index != 1 || details.Partitions[1].NextOffset != 20 {
		t.Fatalf("partition 1 = %+v", details.Partitions[1])
	}
}

func TestRouteGetTopicReturnsErrorWhenRemoteOwnerMissing(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{topicPartitionStatsFn: func(context.Context, string, string, int) (topic.PartitionStats, error) {
		return topic.PartitionStats{}, context.DeadlineExceeded
	}}
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	_, err := router.RouteGetTopic(context.Background(), req, "orders", topic.Details{
		Topic:      topic.Topic{Name: "orders", Partitions: 1},
		Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 1}},
	})
	if err == nil {
		t.Fatal("RouteGetTopic() error = nil, want error")
	}
}

func TestRouteGetTopicKeepsLocalPartitionsLocal(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{topicPartitionStatsFn: func(context.Context, string, string, int) (topic.PartitionStats, error) {
		return topic.PartitionStats{Index: 9, NextOffset: 20}, nil
	}}
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	details, err := router.RouteGetTopic(context.Background(), req, "orders", topic.Details{
		Topic:      topic.Topic{Name: "orders", Partitions: 1},
		Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 7}},
	})
	if err != nil {
		t.Fatalf("RouteGetTopic() error = %v", err)
	}
	if len(details.Partitions) != 1 || details.Partitions[0].NextOffset != 7 {
		t.Fatalf("RouteGetTopic() partitions = %+v", details.Partitions)
	}
}

func TestRouteGetTopicReturnsErrorWhenRemoteStatusIsNon2xx(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	_, err := router.RouteGetTopic(context.Background(), req, "orders", topic.Details{
		Topic:      topic.Topic{Name: "orders", Partitions: 1},
		Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 0}},
	})
	if err == nil {
		t.Fatal("RouteGetTopic() error = nil, want error")
	}
}

func TestRouteGetTopicReturnsErrorWhenRemotePayloadHasWrongPartition(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(topic.Details{
			Topic:      topic.Topic{Name: "orders", Partitions: 1},
			Partitions: []topic.PartitionStats{{Index: 9, NextOffset: 20}},
		})
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	_, err := router.RouteGetTopic(context.Background(), req, "orders", topic.Details{
		Topic:      topic.Topic{Name: "orders", Partitions: 1},
		Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 0}},
	})
	if err == nil {
		t.Fatal("RouteGetTopic() error = nil, want error")
	}
}

func TestRouteGetTopicReturnsErrorWhenRemotePayloadHasMultiplePartitions(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(topic.Details{
			Topic:      topic.Topic{Name: "orders", Partitions: 2},
			Partitions: []topic.PartitionStats{{Index: 0}, {Index: 1}},
		})
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote", ""); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{topicPartitionStatsFn: func(context.Context, string, string, int) (topic.PartitionStats, error) {
		return topic.PartitionStats{}, context.DeadlineExceeded
	}}
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	_, err := router.RouteGetTopic(context.Background(), req, "orders", topic.Details{
		Topic:      topic.Topic{Name: "orders", Partitions: 1},
		Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 0}},
	})
	if err == nil {
		t.Fatal("RouteGetTopic() error = nil, want error")
	}
}

func TestRouteCreateTopicReturnsFalseWhenLeaderMemberCannotBeResolved(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewReader([]byte(`{"name":"orders"}`)))

	forwarded := router.RouteCreateTopic(context.Background(), res, req, []byte(`{"name":"orders"}`))
	if forwarded {
		t.Fatal("RouteCreateTopic() = true, want false")
	}
}

func TestRouteCreateTopicForwardsWhenLeaderMemberUsesExactLeaderAddress(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{createTopicFn: func(context.Context, string, []byte) (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusCreated}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewReader([]byte(`{"name":"orders"}`)))

	forwarded := router.RouteCreateTopic(context.Background(), res, req, []byte(`{"name":"orders"}`))
	if !forwarded {
		t.Fatal("RouteCreateTopic() = false, want true")
	}
}

func TestRouteCreateTopicUsesMemberHTTPAddrWhenClusterAddrMatchesLeader(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	var gotAddr string
	leaderHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer leaderHTTP.Close()

	leaderHTTPAddr := strings.TrimPrefix(leaderHTTP.URL, "http://")
	if err := store.RegisterMember(ctx, metastore.Member{
		ID:          "node-leader",
		Addr:        leaderHTTPAddr,
		ClusterAddr: store.LeaderAddr(),
		Status:      metastore.MemberAlive,
	}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{createTopicFn: func(_ context.Context, addr string, body []byte) (nodewire.Response, error) {
		gotAddr = addr
		if string(body) != `{"name":"orders"}` {
			t.Fatalf("body = %q, want create topic body", body)
		}
		return nodewire.Response{Status: http.StatusCreated}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewReader([]byte(`{"name":"orders"}`)))

	forwarded := router.RouteCreateTopic(context.Background(), res, req, []byte(`{"name":"orders"}`))
	if !forwarded {
		t.Fatal("RouteCreateTopic() = false, want true")
	}
	if res.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusCreated)
	}
	if gotAddr != leaderHTTPAddr {
		t.Fatalf("forwarded addr = %q, want %q", gotAddr, leaderHTTPAddr)
	}
}

func TestRouteAlterTopicReturnsFalseWhenLeaderMemberCannotBeResolved(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/v1/topics/orders", bytes.NewReader([]byte(`{"partitions":2}`)))
	forwarded := router.RouteAlterTopic(context.Background(), res, req, "orders", []byte(`{"partitions":2}`))
	if forwarded {
		t.Fatal("RouteAlterTopic() = true, want false")
	}
}

func TestRouteAlterTopicForwardsWhenLeaderMemberUsesExactLeaderAddress(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{alterTopicFn: func(context.Context, string, string, []byte) (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusOK}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPatch, "/v1/topics/orders", bytes.NewReader([]byte(`{"partitions":2}`)))

	forwarded := router.RouteAlterTopic(context.Background(), res, req, "orders", []byte(`{"partitions":2}`))
	if !forwarded {
		t.Fatal("RouteAlterTopic() = false, want true")
	}
}

func TestRouteDeleteTopicReturnsFalseWhenLeaderMemberCannotBeResolved(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)

	forwarded := router.RouteDeleteTopic(context.Background(), res, req, "orders")
	if forwarded {
		t.Fatal("RouteDeleteTopic() = true, want false")
	}
}

func TestRouteDeleteTopicReturnsFalseWhenLeaderMatchesSelf(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{deleteTopicFn: func(context.Context, string, string) (nodewire.Response, error) {
		return nodewire.Response{}, context.DeadlineExceeded
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)

	forwarded := router.RouteDeleteTopic(context.Background(), res, req, "orders")
	if forwarded {
		t.Fatal("RouteDeleteTopic() = true, want false")
	}
}

func TestRouteDeleteTopicReturnsFalseWhenMatchingLeaderMemberIsDead(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.MarkMemberDead(ctx, "node-dead"); err != nil {
		t.Fatalf("MarkMemberDead() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{deleteTopicFn: func(context.Context, string, string) (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)

	forwarded := router.RouteDeleteTopic(context.Background(), res, req, "orders")
	if forwarded {
		t.Fatal("RouteDeleteTopic() = true, want false")
	}
}

func TestRouteDeleteTopicReturnsFalseWhenLeaderAddressOnlyMatchesPort(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer leader.Close()

	store := newTestStore(t)
	ctx := context.Background()
	leaderAddr := ":" + leader.Listener.Addr().String()[strings.LastIndex(leader.Listener.Addr().String(), ":")+1:]
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: leaderAddr, Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)

	if router.RouteDeleteTopic(context.Background(), res, req, "orders") {
		t.Fatal("RouteDeleteTopic() unexpectedly forwarded")
	}
}

func TestRouteDeleteTopicForwardsWhenLeaderAddressMatchesExactly(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodDelete)
		}
		if r.URL.Path != "/v1/topics/orders" {
			t.Fatalf("path = %q, want %q", r.URL.Path, "/v1/topics/orders")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer leader.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)

	forwarded := router.RouteDeleteTopic(context.Background(), res, req, "orders")
	if !forwarded {
		t.Fatal("RouteDeleteTopic() = false, want true")
	}
	if res.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusBadGateway)
	}
}

func TestRouteDeleteTopicForwardsWhenLeaderMemberUsesExactLeaderAddress(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)

	forwarded := router.RouteDeleteTopic(context.Background(), res, req, "orders")
	if !forwarded {
		t.Fatal("RouteDeleteTopic() = false, want true")
	}
}

func TestRouteDeleteTopicReturnsFalseWhenLeaderOnlyMatchesForeignPort(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer leader.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-foreign", Addr: leader.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)

	if router.RouteDeleteTopic(context.Background(), res, req, "orders") {
		t.Fatal("RouteDeleteTopic() unexpectedly forwarded")
	}
}

func TestRouteDeleteTopicReturnsFalseWhenLeaderPortMatchIsSelf(t *testing.T) {
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer leader.Close()

	store := newTestStore(t)
	ctx := context.Background()
	leaderAddr := ":" + leader.Listener.Addr().String()[strings.LastIndex(leader.Listener.Addr().String(), ":")+1:]
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: leaderAddr, Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)

	if router.RouteDeleteTopic(context.Background(), res, req, "orders") {
		t.Fatal("RouteDeleteTopic() unexpectedly forwarded")
	}
}

func TestBroadcastDeleteTopicSkipsSelfAndDeadMembers(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodDelete)
		}
		if r.URL.Path != "/internal/v1/topics/orders" {
			t.Fatalf("path = %q, want %q", r.URL.Path, "/internal/v1/topics/orders")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "127.0.0.1:1", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: "127.0.0.1:2", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{purgeTopicFn: func(_ context.Context, addr, topicName string) (nodewire.Response, error) {
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if topicName != "orders" {
			t.Fatalf("topic = %q, want orders", topicName)
		}
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	if err := router.BroadcastDeleteTopic(context.Background(), "orders"); err != nil {
		t.Fatalf("BroadcastDeleteTopic() error = %v", err)
	}
}

func TestBroadcastDeleteTopicReturnsErrorOnRemoteFailure(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), nil)
	router.peer = fakePeerClient{purgeTopicFn: func(context.Context, string, string) (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusInternalServerError}, nil
	}}
	if err := router.BroadcastDeleteTopic(context.Background(), "orders"); err == nil {
		t.Fatal("BroadcastDeleteTopic() error = nil, want error")
	}
}

func TestRouteConsumePrefersLocalPartitionWithoutSnapshotRanking(t *testing.T) {
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
		t.Fatal("snapshot provider should not be called for queue consume routing")
		return nil, nil
	}))
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=1s", nil)

	forwarded, localPartition := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if forwarded {
		t.Fatal("RouteConsume() = true, want false")
	}
	if localPartition == nil || *localPartition != 0 {
		t.Fatalf("RouteConsume() local partition = %+v, want 0", localPartition)
	}
	if len(partitions) != 0 {
		t.Fatalf("probed partitions = %v, want none", partitions)
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

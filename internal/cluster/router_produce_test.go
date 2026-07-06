package cluster

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func TestNewRouter(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	if router == nil {
		t.Fatal("NewRouter() returned nil")
	}
}

func TestRouteProduceReturnsFalseWhenTopicMissing(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"id":1}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"id":1}`))
	if forwarded {
		t.Fatal("RouteProduce() = true, want false")
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
		if string(body) != `{"id":1}` {
			t.Fatalf("body = %q, want %q", body, `{"id":1}`)
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
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 1}, "")
	router.peer = fakePeerClient{produceFn: func(_ context.Context, addr string, req nodewire.ProduceRequest) (nodewire.Response, error) {
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if req.Partition != 1 {
			t.Fatalf("partition = %d, want 1", req.Partition)
		}
		if string(req.Payload) != `{"id":1}` {
			t.Fatalf("body = %q, want %q", req.Payload, `{"id":1}`)
		}
		return nodewire.Response{Status: http.StatusAccepted}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"id":1}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"id":1}`))
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
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-failed"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 1}, "")
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
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"id":1}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"id":1}`))
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote-0"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-1"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 0}, "")
	router.peer = fakePeerClient{produceFn: func(context.Context, string, nodewire.ProduceRequest) (nodewire.Response, error) {
		return nodewire.Response{}, context.DeadlineExceeded
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"id":1}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"id":1}`))
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-dead"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-failed"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 0}, "")
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
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"id":1}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"id":1}`))
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
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-1"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-remote-2"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 1}, "")
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
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"id":1}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"id":1}`))
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote-0"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-1"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", fixedPartitionManager{picked: 0}, "")
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/produce", bytes.NewBufferString(`{"id":1}`))

	forwarded := router.RouteProduce(context.Background(), res, req, "orders", "customer-1", []byte(`{"id":1}`))
	if forwarded {
		t.Fatal("RouteProduce() = true, want false")
	}
}

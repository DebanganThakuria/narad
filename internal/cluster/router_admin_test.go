package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

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
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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

func TestRouteCreateTopicFallsBackToLeaderIDWhenLeaderAddressDoesNotMatchMemberAddr(t *testing.T) {
	stores := newTestStoreCluster(t, "node-0", "node-1", "node-2")
	leaderID, leaderStore := waitForClusterLeader(t, stores)

	followerID := ""
	for id := range stores {
		if id != leaderID {
			followerID = id
			break
		}
	}
	if followerID == "" {
		t.Fatal("no follower found")
	}
	followerStore := stores[followerID]

	const leaderHTTPAddr = "leader.narad.svc.cluster.local:7942"
	if err := leaderStore.RegisterMember(context.Background(), metastore.Member{
		ID:          leaderID,
		Addr:        leaderHTTPAddr,
		ClusterAddr: "different-address-shape:7943",
		Status:      metastore.MemberAlive,
	}); err != nil {
		t.Fatalf("RegisterMember(%s) error = %v", leaderID, err)
	}
	waitForMember(t, followerStore, leaderID)

	var gotAddr string
	router := NewRouter(followerStore, followerID, partition.NewHashRoundRobin())
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
	if gotAddr != leaderHTTPAddr {
		t.Fatalf("forwarded addr = %q, want %q", gotAddr, leaderHTTPAddr)
	}
	if res.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusCreated)
	}
}

func TestRouteAlterTopicReturnsFalseWhenLeaderMemberCannotBeResolved(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)

	if router.RouteDeleteTopic(context.Background(), res, req, "orders") {
		t.Fatal("RouteDeleteTopic() unexpectedly forwarded")
	}
}

func TestBroadcastDeleteTopicSkipsSelfAndDeadMembers(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "127.0.0.1:1", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "127.0.0.1:2", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-dead", Addr: "127.0.0.1:3", Status: metastore.MemberDead}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	router.peer = fakePeerClient{purgeTopicFn: func(_ context.Context, addr, topicName string) (nodewire.Response, error) {
		if addr != "127.0.0.1:2" {
			t.Fatalf("addr = %q, want %q", addr, "127.0.0.1:2")
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
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "127.0.0.1:2", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	router.peer = fakePeerClient{purgeTopicFn: func(context.Context, string, string) (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusInternalServerError}, nil
	}}
	if err := router.BroadcastDeleteTopic(context.Background(), "orders"); err == nil {
		t.Fatal("BroadcastDeleteTopic() error = nil, want error")
	}
}

func TestBroadcastDeleteTopicAttemptsAllMembersDespiteFailure(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	for _, m := range []metastore.Member{
		{ID: "node-a", Addr: "127.0.0.1:2", Status: metastore.MemberAlive},
		{ID: "node-b", Addr: "127.0.0.1:3", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			t.Fatalf("RegisterMember(%s) error = %v", m.ID, err)
		}
	}

	var mu sync.Mutex
	attempted := map[string]bool{}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	router.peer = fakePeerClient{purgeTopicFn: func(_ context.Context, addr, _ string) (nodewire.Response, error) {
		mu.Lock()
		attempted[addr] = true
		mu.Unlock()
		if addr == "127.0.0.1:2" {
			// First member fails; the second must still be attempted.
			return nodewire.Response{}, errors.New("unreachable")
		}
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}

	err := router.BroadcastDeleteTopic(ctx, "orders")
	if err == nil {
		t.Fatal("BroadcastDeleteTopic() error = nil, want the failed member surfaced")
	}
	if !attempted["127.0.0.1:2"] || !attempted["127.0.0.1:3"] {
		t.Fatalf("attempted = %v, want both members attempted despite the first failing", attempted)
	}
}

// A create forward must carry a deadline covering the leader's startup
// create gate window (~60s of metastore catch-up plus sweep work): without
// one, the transport's short default reply timeout fires while the create
// still executes on the leader — the client sees 502, the retry sees 409.
func TestRouteCreateTopicForwardDeadlineCoversStartupGate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	var deadline time.Time
	var hasDeadline bool
	router.peer = fakePeerClient{createTopicFn: func(ctx context.Context, _ string, _ []byte) (nodewire.Response, error) {
		deadline, hasDeadline = ctx.Deadline()
		return nodewire.Response{Status: http.StatusCreated}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics", bytes.NewReader([]byte(`{"name":"orders"}`)))
	start := time.Now()
	forwarded := router.RouteCreateTopic(context.Background(), res, req, []byte(`{"name":"orders"}`))
	if !forwarded {
		t.Fatal("RouteCreateTopic() = false, want true")
	}
	if !hasDeadline {
		t.Fatal("create forward has no deadline; the transport fallback timeout would cut it short")
	}
	if remaining := deadline.Sub(start); remaining < time.Minute {
		t.Fatalf("create forward deadline is %s away, want at least the 60s startup gate window", remaining)
	}
}

// Each per-member purge RPC must budget the remote's purge execution on top
// of its replica apply wait, while staying bounded.
func TestBroadcastDeleteTopicDeadlineCoversPurgeExecution(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "127.0.0.1:2", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	var deadline time.Time
	var hasDeadline bool
	router.peer = fakePeerClient{purgeTopicFn: func(ctx context.Context, _, _ string) (nodewire.Response, error) {
		deadline, hasDeadline = ctx.Deadline()
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	start := time.Now()
	if err := router.BroadcastDeleteTopic(ctx, "orders"); err != nil {
		t.Fatalf("BroadcastDeleteTopic() error = %v", err)
	}
	if !hasDeadline {
		t.Fatal("purge RPC has no deadline, want a bounded one")
	}
	remaining := deadline.Sub(start)
	if remaining < purgeApplyWaitTimeout+purgeExecutionAllowance {
		t.Fatalf("purge deadline is %s away, want at least apply wait (%s) + execution allowance (%s)",
			remaining, purgeApplyWaitTimeout, purgeExecutionAllowance)
	}
	if remaining > purgeApplyWaitTimeout+purgeExecutionAllowance+longWaitRPCGrace+3*time.Second {
		t.Fatalf("purge deadline is %s away, want it bounded near the budgeted window", remaining)
	}
}

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	brokertopics "github.com/debanganthakuria/narad/internal/broker/topics"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/domain/user"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/platform/clusterrpc"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/platform/schema"
	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
	"github.com/debanganthakuria/narad/internal/security"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httptopics "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/topics"
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

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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

// A partition whose owner this node has no member record for is
// reported unavailable with liveness unknown; the rest of the topic is
// still answered.
func TestRouteGetTopicMarksAPartitionWhoseOwnerIsUnknown(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{topicPartitionStatsFn: func(context.Context, string, string, int) (topic.PartitionStats, error) {
		t.Error("asked an owner with no member record for stats")
		return topic.PartitionStats{}, context.DeadlineExceeded
	}}
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	details, err := router.RouteGetTopic(context.Background(), req, "orders", topic.Details{
		Topic:      topic.Topic{Name: "orders", Partitions: 1},
		Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 1}},
	})
	assertPartitionUnavailable(t, details, err, 0, "node-remote", topic.OwnerUnknown)
}

// assertPartitionUnavailable checks that a topic GET answered partially
// with partition p marked unavailable for the given owner and liveness.
func assertPartitionUnavailable(t *testing.T, details topic.Details, err error, p int, owner, liveness string) {
	t.Helper()
	if err != nil {
		t.Fatalf("RouteGetTopic() error = %v, want a partial answer", err)
	}
	if !details.Partial || p >= len(details.Partitions) {
		t.Fatalf("RouteGetTopic() = %+v, want partial with partition %d", details, p)
	}
	got := details.Partitions[p]
	want := topic.PartitionStats{Index: p, OwnerNode: owner, Status: topic.PartitionOwnerUnavailable, OwnerLiveness: liveness}
	if got != want {
		t.Fatalf("partition %d = %+v, want %+v", p, got, want)
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

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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

// A remote owner that answers with an error, a stats entry for another
// partition, or not at all is reported unreachable for that partition
// rather than failing the whole GET.
func TestRouteGetTopicMarksAPartitionUnreachableWhenItsOwnerFails(t *testing.T) {
	for name, answer := range map[string]func(context.Context, string, string, int) (topic.PartitionStats, error){
		"error status": func(context.Context, string, string, int) (topic.PartitionStats, error) {
			return topic.PartitionStats{}, errors.New("topic partition stats returned status 503")
		},
		"wrong partition": func(context.Context, string, string, int) (topic.PartitionStats, error) {
			return topic.PartitionStats{Index: 9, NextOffset: 20}, nil
		},
		"timeout": func(ctx context.Context, _, _ string, _ int) (topic.PartitionStats, error) {
			<-ctx.Done()
			return topic.PartitionStats{}, ctx.Err()
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			ctx := context.Background()
			if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
				t.Fatalf("CreateTopic() error = %v", err)
			}
			if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "127.0.0.1:2", Status: metastore.MemberAlive}); err != nil {
				t.Fatalf("RegisterMember() error = %v", err)
			}
			if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
				t.Fatalf("AssignPartition() error = %v", err)
			}
			router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
			router.peer = fakePeerClient{topicPartitionStatsFn: answer}
			req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
			req.SetPathValue("topic", "orders")
			start := time.Now()
			details, err := router.RouteGetTopic(context.Background(), req, "orders", topic.Details{
				Topic:      topic.Topic{Name: "orders", Partitions: 1},
				Partitions: []topic.PartitionStats{{Index: 0}},
			})
			assertPartitionUnavailable(t, details, err, 0, "node-remote", topic.OwnerUnreachable)
			if elapsed := time.Since(start); elapsed > topicStatsTimeout+time.Second {
				t.Fatalf("GET took %s, want the stats RPC bounded by %s", elapsed, topicStatsTimeout)
			}
		})
	}
}

func TestRouteCreateTopicReturnsFalseWhenLeaderMemberCannotBeResolved(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(followerStore, followerID, partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil)

	forwarded := router.RouteDeleteTopic(context.Background(), res, req, "orders")
	if !forwarded {
		t.Fatal("RouteDeleteTopic() = false, want true")
	}
	// A forward that reaches a known leader address but fails the RPC
	// (leader unreachable mid-election/partition) is retryable → 503.
	if res.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusServiceUnavailable)
	}
}

func TestRouteDeleteTopicForwardsWhenLeaderMemberUsesExactLeaderAddress(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
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

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{purgeTopicFn: func(_ context.Context, addr, topicName, _ string) (nodewire.Response, error) {
		if addr != "127.0.0.1:2" {
			t.Fatalf("addr = %q, want %q", addr, "127.0.0.1:2")
		}
		if topicName != "orders" {
			t.Fatalf("topic = %q, want orders", topicName)
		}
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	if err := router.BroadcastDeleteTopic(context.Background(), "orders", ""); err != nil {
		t.Fatalf("BroadcastDeleteTopic() error = %v", err)
	}
}

func TestBroadcastDeleteTopicReturnsErrorOnRemoteFailure(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "127.0.0.1:2", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{purgeTopicFn: func(context.Context, string, string, string) (nodewire.Response, error) {
		return nodewire.Response{Status: http.StatusInternalServerError}, nil
	}}
	if err := router.BroadcastDeleteTopic(context.Background(), "orders", ""); err == nil {
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{purgeTopicFn: func(_ context.Context, addr, _, _ string) (nodewire.Response, error) {
		mu.Lock()
		attempted[addr] = true
		mu.Unlock()
		if addr == "127.0.0.1:2" {
			// First member fails; the second must still be attempted.
			return nodewire.Response{}, errors.New("unreachable")
		}
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}

	err := router.BroadcastDeleteTopic(ctx, "orders", "")
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
// still executes on the leader — the client sees 503, the retry sees 409.
func TestRouteCreateTopicForwardDeadlineCoversStartupGate(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")

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

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	var deadline time.Time
	var hasDeadline bool
	router.peer = fakePeerClient{purgeTopicFn: func(ctx context.Context, _, _, _ string) (nodewire.Response, error) {
		deadline, hasDeadline = ctx.Deadline()
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	start := time.Now()
	if err := router.BroadcastDeleteTopic(ctx, "orders", ""); err != nil {
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

// Purges fan out concurrently under one shared deadline: three members
// that each take purgeDelay must cost about purgeDelay in total, not
// three times that, and every RPC must see the same deadline.
func TestBroadcastDeleteTopicFansOutConcurrentlyUnderOneDeadline(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	for _, m := range []metastore.Member{
		{ID: "node-a", Addr: "127.0.0.1:2", Status: metastore.MemberAlive},
		{ID: "node-b", Addr: "127.0.0.1:3", Status: metastore.MemberAlive},
		{ID: "node-c", Addr: "127.0.0.1:4", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			t.Fatalf("RegisterMember(%s) error = %v", m.ID, err)
		}
	}

	const purgeDelay = 200 * time.Millisecond
	var mu sync.Mutex
	var deadlines []time.Time
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{purgeTopicFn: func(ctx context.Context, _, _, _ string) (nodewire.Response, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("purge RPC has no deadline")
		}
		mu.Lock()
		deadlines = append(deadlines, deadline)
		mu.Unlock()
		time.Sleep(purgeDelay)
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}

	start := time.Now()
	if err := router.BroadcastDeleteTopic(ctx, "orders", ""); err != nil {
		t.Fatalf("BroadcastDeleteTopic() error = %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 2*purgeDelay {
		t.Fatalf("broadcast took %s for three %s purges, want them concurrent", elapsed, purgeDelay)
	}
	if len(deadlines) != 3 {
		t.Fatalf("purged %d members, want 3", len(deadlines))
	}
	for _, d := range deadlines[1:] {
		if !d.Equal(deadlines[0]) {
			t.Fatalf("purge deadlines differ (%v vs %v), want one shared deadline", deadlines[0], d)
		}
	}
}

// Failures are joined in member order, whichever purge finished first,
// so the logged message is stable.
func TestBroadcastDeleteTopicJoinsFailuresInMemberOrder(t *testing.T) {
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{purgeTopicFn: func(_ context.Context, addr, _, _ string) (nodewire.Response, error) {
		if addr == "127.0.0.1:2" {
			time.Sleep(50 * time.Millisecond) // node-a finishes last
		}
		return nodewire.Response{}, errors.New("unreachable")
	}}
	err := router.BroadcastDeleteTopic(ctx, "orders", "")
	if err == nil {
		t.Fatal("BroadcastDeleteTopic() error = nil, want both failures")
	}
	msg := err.Error()
	a, b := strings.Index(msg, "node-a"), strings.Index(msg, "node-b")
	if a < 0 || b < 0 || a > b {
		t.Fatalf("joined error = %q, want node-a before node-b", msg)
	}
}

// managerBroker is a leader's broker with a real topics Manager behind
// the topic writes, recording the request identity each one ran under.
type managerBroker struct {
	broker.Broker
	m *brokertopics.Manager

	mu         sync.Mutex
	identities []string
	deletes    int
}

func (b *managerBroker) record(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id, ok := security.IdentityFrom(ctx)
	if !ok {
		b.identities = append(b.identities, "<none>")
		return
	}
	b.identities = append(b.identities, id.Username)
}

func (b *managerBroker) seen() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.identities...)
}

func (b *managerBroker) GetTopic(ctx context.Context, name string) (topic.Topic, error) {
	return b.m.GetTopic(ctx, name)
}

func (b *managerBroker) CreateTopic(ctx context.Context, opts brokertopics.CreateOpts) (topic.Topic, error) {
	b.record(ctx)
	return b.m.CreateTopic(ctx, opts)
}

func (b *managerBroker) UpdateTopicRetention(ctx context.Context, name string, retentionMs int64) (topic.Topic, error) {
	b.record(ctx)
	return b.m.UpdateTopicRetention(ctx, name, retentionMs)
}

func (b *managerBroker) DeleteTopic(ctx context.Context, name string) error {
	b.record(ctx)
	err := b.m.DeleteTopic(ctx, name)
	if err == nil {
		b.mu.Lock()
		b.deletes++
		b.mu.Unlock()
	}
	return err
}

func (b *managerBroker) AttachChild(ctx context.Context, parent, child string, delayMs int64) error {
	b.record(ctx)
	return b.m.AttachChild(ctx, parent, child, delayMs)
}

func (b *managerBroker) DetachChild(ctx context.Context, parent, child string) error {
	b.record(ctx)
	return b.m.DetachChild(ctx, parent, child)
}

// loopbackFrames hands PeerClient requests straight to a leader's
// RPCServer, as the cluster transport would. With preActor set it
// answers like a 3.0.x leader: any payload carrying the trailing actor
// field is refused at decode, before anything runs.
type loopbackFrames struct {
	server   *RPCServer
	preActor bool

	mu       sync.Mutex
	payloads [][]byte
}

func (l *loopbackFrames) RequestOnLane(ctx context.Context, _ string, _ clusterrpc.Lane, _ clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	l.mu.Lock()
	l.payloads = append(l.payloads, append([]byte(nil), payload...))
	l.mu.Unlock()
	var res nodewire.Response
	if prefix, refused := preActorRefusal(payload); l.preActor && refused {
		res = errorResponse(http.StatusBadRequest, prefix+nodewire.TrailingPayloadError)
	} else {
		res = l.server.dispatch(ctx, requestKey{}, payload)
	}
	encoded, err := nodewire.EncodeResponse(res)
	if err != nil {
		return clusterwire.StreamFrame{}, err
	}
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: 1, Payload: encoded}, nil
}

func (l *loopbackFrames) RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, _ time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return l.RequestOnLane(ctx, addr, lane, frameType, payload)
}

func (l *loopbackFrames) sent() [][]byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([][]byte(nil), l.payloads...)
}

// preActorRefusal reports whether a 3.0.x leader would refuse payload
// for its trailing actor, and the prefix of its error text.
func preActorRefusal(payload []byte) (string, bool) {
	op, err := nodewire.OperationOf(payload)
	if err != nil {
		return "", false
	}
	switch op {
	case nodewire.OpDeleteTopic:
		req, err := nodewire.DecodeTopicNameRequest(payload, op)
		return "invalid delete topic request: ", err == nil && req.Actor != ""
	case nodewire.OpCreateTopic, nodewire.OpAlterTopic:
		req, err := nodewire.DecodeTopicBodyRequest(payload, op)
		prefix := "invalid create topic request: "
		if op == nodewire.OpAlterTopic {
			prefix = "invalid alter topic request: "
		}
		return prefix, err == nil && req.Actor != ""
	case nodewire.OpAttachChild, nodewire.OpDetachChild:
		req, err := nodewire.DecodeChildLinkRequest(payload, op)
		prefix := "invalid attach child request: "
		if op == nodewire.OpDetachChild {
			prefix = "invalid detach child request: "
		}
		return prefix, err == nil && req.Actor != ""
	}
	return "", false
}

// forwardingPair is a follower's router whose leader forwards land on a
// leader RPCServer backed by a real topics Manager over store.
func forwardingPair(t *testing.T, preActor bool) (*Router, *managerBroker, *loopbackFrames, *metastore.Store) {
	t.Helper()
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-leader", Addr: store.LeaderAddr(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	for _, name := range []string{"alice", "bob"} {
		u := user.User{Username: name, PasswordHash: []byte("x"), Grants: []user.Grant{{Action: user.ActionCreate, Patterns: []string{"*"}}}}
		if err := store.CreateUser(ctx, u); err != nil {
			t.Fatalf("CreateUser(%s): %v", name, err)
		}
	}
	dataDir := t.TempDir()
	m := brokertopics.NewManager(dataDir, store, nil, schema.NewJSONSchema(),
		consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
			return consumer.Caps{MaxInFlight: 8, MaxAckedAhead: 8}, nil
		}, nil),
		runtime.NewLogs(dataDir, storage.Options{}, store, nil),
		brokertopics.Config{DefaultPartitions: 3, MaxPartitions: 12, DefaultRetentionMs: 3_600_000, DefaultVisibilityTimeoutMs: 30_000, DefaultMaxInFlightPerPartition: 8, DefaultMaxAckedAheadPerPartition: 8},
		discardLogger(), "node-leader")
	br := &managerBroker{m: m}
	frames := &loopbackFrames{server: &RPCServer{broker: br, store: store, logger: discardLogger()}, preActor: preActor}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = &PeerClient{frames: frames}
	return router, br, frames, store
}

func asUser(name string) context.Context {
	return security.WithIdentity(context.Background(), user.User{Username: name, Grants: []user.Grant{{Action: user.ActionCreate, Patterns: []string{"*"}}}})
}

// A write a follower forwards runs on the leader as the caller (audit
// H1): the leader looks the caller up in its own replica and the
// Manager re-checks ownership against the topic as it stands there.
// Master forwarded no caller, so the leader ran every forwarded write
// unchecked and bob's forwarded delete removed alice's topic.
func TestForwardedTopicWriteRunsAsTheCaller(t *testing.T) {
	router, br, frames, store := forwardingPair(t, false)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 3, RetentionMs: 3_600_000, Owner: "alice"}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	rec := httptest.NewRecorder()
	router.RouteDeleteTopic(asUser("bob"), rec, httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil), "orders")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("bob's forwarded delete of alice's topic: status %d body %s, want 403", rec.Code, rec.Body)
	}
	if _, err := store.GetTopic(ctx, "orders"); err != nil {
		t.Fatalf("alice's topic after bob's delete: %v", err)
	}
	rec = httptest.NewRecorder()
	router.RouteAlterTopic(asUser("bob"), rec, nil, "orders", []byte(`{"retention_ms":7200000}`))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("bob's forwarded alter of alice's topic: status %d body %s, want 403", rec.Code, rec.Body)
	}

	rec = httptest.NewRecorder()
	router.RouteAlterTopic(asUser("alice"), rec, nil, "orders", []byte(`{"retention_ms":7200000}`))
	if rec.Code != http.StatusOK {
		t.Fatalf("alice's forwarded alter: status %d body %s, want 200", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	router.RouteCreateTopic(asUser("alice"), rec, nil, []byte(`{"name":"audit","owner":"alice"}`))
	if rec.Code != http.StatusCreated {
		t.Fatalf("alice's forwarded create: status %d body %s, want 201", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	router.RouteAttachChild(asUser("alice"), rec, nil, "orders", "audit", 0)
	if rec.Code != http.StatusOK {
		t.Fatalf("alice's forwarded attach: status %d body %s, want 200", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	router.RouteDetachChild(asUser("bob"), rec, nil, "orders", "audit")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("bob's forwarded detach of alice's link: status %d body %s, want 403", rec.Code, rec.Body)
	}

	// A caller the leader does not know is refused outright.
	rec = httptest.NewRecorder()
	router.RouteAlterTopic(asUser("mallory"), rec, nil, "orders", []byte(`{"retention_ms":7200000}`))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("an unknown caller's forwarded alter: status %d body %s, want 403", rec.Code, rec.Body)
	}

	want := []string{"bob", "bob", "alice", "alice", "alice", "bob"}
	if got := br.seen(); !slices.Equal(got, want) {
		t.Fatalf("identities the leader's broker ran under = %v, want %v", got, want)
	}
	if len(frames.sent()) == 0 {
		t.Fatal("nothing was forwarded")
	}
}

// Mid-roll the leader may still run 3.0.x, which refuses the trailing
// actor field at decode, before applying anything. The forwarder then
// resends the write once without it, so the write still lands, exactly
// once, checked only at the ingress as before the upgrade.
func TestForwardToAnOlderLeaderDropsTheCaller(t *testing.T) {
	router, br, frames, store := forwardingPair(t, true)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 3, Owner: "alice"}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	rec := httptest.NewRecorder()
	router.RouteDeleteTopic(asUser("alice"), rec, httptest.NewRequest(http.MethodDelete, "/v1/topics/orders", nil), "orders")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("forwarded delete to an older leader: status %d body %s, want 204", rec.Code, rec.Body)
	}
	if br.deletes != 1 {
		t.Fatalf("the delete ran %d times, want exactly once", br.deletes)
	}
	var deletes []nodewire.TopicNameRequest
	for _, p := range frames.sent() {
		if op, _ := nodewire.OperationOf(p); op == nodewire.OpDeleteTopic {
			req, err := nodewire.DecodeTopicNameRequest(p, op)
			if err != nil {
				t.Fatalf("decode forwarded delete: %v", err)
			}
			deletes = append(deletes, req)
		}
	}
	if len(deletes) != 2 || deletes[0].Actor != "alice" || deletes[1].Actor != "" {
		t.Fatalf("forwarded deletes = %+v, want one with the caller, then one resent without it", deletes)
	}
}

// scriptedFrames answers each request with the next scripted response
// and records the payloads.
type scriptedFrames struct {
	replies  []nodewire.Response
	payloads [][]byte
}

func (f *scriptedFrames) RequestOnLane(_ context.Context, _ string, _ clusterrpc.Lane, _ clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	f.payloads = append(f.payloads, append([]byte(nil), payload...))
	encoded, err := nodewire.EncodeResponse(f.replies[len(f.payloads)-1])
	if err != nil {
		return clusterwire.StreamFrame{}, err
	}
	return clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: 1, Payload: encoded}, nil
}

func (f *scriptedFrames) RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, _ time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return f.RequestOnLane(ctx, addr, lane, frameType, payload)
}

// Only an older leader's decode refusal of the actor field is resent
// without the caller: any other 400, even one whose text mentions
// "trailing", comes back as it is, so a write is never quietly re-run
// unchecked.
func TestCallerIsDroppedOnlyForAnOlderLeadersDecodeRefusal(t *testing.T) {
	ctx := context.Background()
	other := errorResponse(http.StatusBadRequest, `invalid json: schema property "trailing node rpc payload data"`)
	f := &scriptedFrames{replies: []nodewire.Response{other}}
	c := &PeerClient{frames: f}
	res, err := c.AlterTopic(ctx, "leader", "orders", []byte(`{}`), "alice")
	if err != nil || res.Status != http.StatusBadRequest || len(f.payloads) != 1 {
		t.Fatalf("alter answered %d (err %v) after %d sends; want the 400 returned after one send", res.Status, err, len(f.payloads))
	}

	refusal := errorResponse(http.StatusBadRequest, "invalid alter topic request: "+nodewire.TrailingPayloadError)
	f = &scriptedFrames{replies: []nodewire.Response{refusal, {Status: http.StatusOK}}}
	c = &PeerClient{frames: f}
	res, err = c.AlterTopic(ctx, "leader", "orders", []byte(`{}`), "alice")
	if err != nil || res.Status != http.StatusOK || len(f.payloads) != 2 {
		t.Fatalf("alter answered %d (err %v) after %d sends; want 200 after a resend", res.Status, err, len(f.payloads))
	}
	first, _ := nodewire.DecodeTopicBodyRequest(f.payloads[0], nodewire.OpAlterTopic)
	second, _ := nodewire.DecodeTopicBodyRequest(f.payloads[1], nodewire.OpAlterTopic)
	if first.Actor != "alice" || second.Actor != "" {
		t.Fatalf("actors sent = %q then %q, want alice then none", first.Actor, second.Actor)
	}
}

// purgeDeferredReply is a member's answer while its replica has not
// applied the delete yet.
func purgeDeferredReply() nodewire.Response {
	return nodewire.Response{
		Status:      http.StatusServiceUnavailable,
		ContentType: nodewire.ContentTypeJSON,
		Body:        []byte(`{"error":"this node's replica has not applied the topic delete yet","code":"purge_deferred"}` + "\n"),
	}
}

// The purge fan-out follows a delete that has already committed, so a
// client that disconnects or times out while it runs must not cancel
// the purge on the other members. Master derived the fan-out from the
// request's context: cancelling the request cancelled every member's
// purge, and their copies stayed until they restarted.
func TestPurgeBroadcastSurvivesACancelledRequest(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	for _, m := range []metastore.Member{
		{ID: "node-a", Addr: "127.0.0.1:2", Status: metastore.MemberAlive},
		{ID: "node-b", Addr: "127.0.0.1:3", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			t.Fatalf("RegisterMember(%s): %v", m.ID, err)
		}
	}
	reqCtx, cancelReq := context.WithCancel(ctx)
	var started sync.WaitGroup
	started.Add(2)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{purgeTopicFn: func(ctx context.Context, _, _, _ string) (nodewire.Response, error) {
		started.Done()
		select {
		case <-ctx.Done():
			return nodewire.Response{}, ctx.Err()
		case <-time.After(300 * time.Millisecond): // the member's purge work
		}
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	go func() {
		started.Wait()
		cancelReq() // the client goes away mid-fan-out
	}()
	if err := router.BroadcastDeleteTopic(reqCtx, "orders", "000000000000000c"); err != nil {
		t.Fatalf("BroadcastDeleteTopic after the request was cancelled: %v, want every member purged", err)
	}
}

// A member whose replica had not applied the delete answers
// purge_deferred; the leader asks it again within the same budget, and
// a purge that then runs is a success. Master counted the first answer
// as the member's final word.
func TestPurgeBroadcastRetriesADeferredMember(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-a", Addr: "127.0.0.1:2", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember: %v", err)
	}
	var mu sync.Mutex
	var calls int
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{purgeTopicFn: func(context.Context, string, string, string) (nodewire.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return purgeDeferredReply(), nil
		}
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	if err := router.BroadcastDeleteTopic(ctx, "orders", "000000000000000d"); err != nil {
		t.Fatalf("BroadcastDeleteTopic: %v, want the deferred member purged on its retry", err)
	}
	if calls != 2 {
		t.Fatalf("purge requests = %d, want 2 (deferred, then purged)", calls)
	}
}

// A member that still owes the purge when the fan-out gives up is
// logged once, at error, naming the topic, the incarnation and the
// member, after at most three attempts.
func TestUnfinishedPurgeIsLoggedAtError(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	for _, m := range []metastore.Member{
		{ID: "node-a", Addr: "127.0.0.1:2", Status: metastore.MemberAlive},
		{ID: "node-b", Addr: "127.0.0.1:3", Status: metastore.MemberAlive},
	} {
		if err := store.RegisterMember(ctx, m); err != nil {
			t.Fatalf("RegisterMember(%s): %v", m.ID, err)
		}
	}
	var mu sync.Mutex
	calls := map[string]int{}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	var logs bytes.Buffer
	router.SetLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	router.peer = fakePeerClient{purgeTopicFn: func(_ context.Context, addr, _, _ string) (nodewire.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		calls[addr]++
		if addr == "127.0.0.1:2" {
			return purgeDeferredReply(), nil
		}
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	if err := router.BroadcastDeleteTopic(ctx, "orders", "000000000000000e"); err == nil {
		t.Fatal("BroadcastDeleteTopic = nil, want the member that never purged reported")
	}
	if calls["127.0.0.1:2"] != 3 || calls["127.0.0.1:3"] != 1 {
		t.Fatalf("purge requests per member = %v, want 3 to the deferring member and 1 to the other", calls)
	}
	var lines []map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var line map[string]any
		if json.Unmarshal(raw, &line) == nil && line["level"] == "ERROR" {
			lines = append(lines, line)
		}
	}
	if len(lines) != 1 {
		t.Fatalf("error lines = %d, want 1:\n%s", len(lines), logs.String())
	}
	line := lines[0]
	members, _ := line["members"].([]any)
	if line["topic"] != "orders" || line["incarnation"] != "000000000000000e" || len(members) != 1 || members[0] != "node-a" {
		t.Fatalf("error line = %v, want topic orders, incarnation 000000000000000e and members [node-a]", line)
	}
}

// cancellingFrames lets the leader run a forwarded write to completion
// and then has the client give up before the reply reaches the ingress.
type cancellingFrames struct {
	leader *loopbackFrames
	cancel context.CancelFunc
}

func (f *cancellingFrames) RequestOnLane(ctx context.Context, addr string, lane clusterrpc.Lane, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	if _, err := f.leader.RequestOnLane(context.Background(), addr, lane, frameType, payload); err != nil {
		return clusterwire.StreamFrame{}, err
	}
	f.cancel()
	<-ctx.Done()
	return clusterwire.StreamFrame{}, ctx.Err()
}

func (f *cancellingFrames) RequestOnLaneTimeout(ctx context.Context, addr string, lane clusterrpc.Lane, _ time.Duration, frameType clusterwire.StreamFrameType, payload []byte) (clusterwire.StreamFrame, error) {
	return f.RequestOnLane(ctx, addr, lane, frameType, payload)
}

// A forwarded delete that the leader applied, but whose reply never
// reached the ingress because the client went away, is audited as
// outcome=unknown, never as rejected or failed: an audit query for
// changes that happened must not miss it. Master wrote no audit line at
// all.
func TestForwardWithoutTheLeadersAnswerIsAuditedAsUnknown(t *testing.T) {
	router, br, frames, store := forwardingPair(t, false)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "00000000000000f1", Partitions: 3, Owner: "alice"}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	reqCtx, cancel := context.WithCancel(asUser("alice"))
	defer cancel()
	router.peer = &PeerClient{frames: &cancellingFrames{leader: frames, cancel: cancel}}

	var logs bytes.Buffer
	set := handlers.New(handlers.Deps{
		Broker: br,
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		Router: router,
	})
	req := httptest.NewRequestWithContext(reqCtx, http.MethodDelete, "/v1/topics/orders", nil)
	req.SetPathValue("topic", "orders")
	rec := httptest.NewRecorder()
	httptopics.Delete(set).ServeHTTP(rec, req)

	if br.deletes != 1 {
		t.Fatalf("the leader deleted %d times, want 1 (the delete committed)", br.deletes)
	}
	var audit map[string]any
	for _, raw := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		var line map[string]any
		if json.Unmarshal(raw, &line) == nil && line["component"] == "audit" && line["event"] == "topic.delete" {
			audit = line
		}
	}
	if audit == nil {
		t.Fatalf("no topic.delete audit line (client got %d):\n%s", rec.Code, logs.String())
	}
	if audit["outcome"] != "unknown" || audit["actor"] != "alice" || audit["target"] != "orders" {
		t.Fatalf("audit line = %v, want actor alice, target orders, outcome unknown", audit)
	}
}

// retentionRefusedBroker is a leader's broker whose retention changes
// all end in err.
type retentionRefusedBroker struct {
	*managerBroker
	err error
}

func (b *retentionRefusedBroker) UpdateTopicRetention(context.Context, string, int64) (topic.Topic, error) {
	return topic.Topic{}, b.err
}

// A leader that lost its leadership after appending a forwarded change
// answers 503, but a later leader may still commit the entry, so the
// node that forwarded it audits the change as unknown, as it does a
// forward that got no reply. A 503 the leader decided before anything
// was appended (its barrier failed) stays failed.
func TestForwardedLeadershipLossIsAuditedAsUnknown(t *testing.T) {
	router, br, frames, store := forwardingPair(t, false)
	if err := store.CreateTopic(context.Background(), topic.Topic{Name: "orders", ID: "00000000000000f2", Partitions: 3, Owner: "alice"}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	leader := &retentionRefusedBroker{managerBroker: br}
	frames.server.broker = leader
	var logs bytes.Buffer
	set := handlers.New(handlers.Deps{
		Broker: br,
		Logger: slog.New(slog.NewJSONHandler(&logs, nil)),
		Router: router,
	})
	alterOutcome := func() (int, string, any) {
		logs.Reset()
		req := httptest.NewRequestWithContext(asUser("alice"), http.MethodPatch, "/v1/topics/orders", strings.NewReader(`{"retention_ms":7200000}`))
		req.SetPathValue("topic", "orders")
		rec := httptest.NewRecorder()
		httptopics.Alter(set).ServeHTTP(rec, req)
		for _, raw := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
			var line map[string]any
			if json.Unmarshal(raw, &line) == nil && line["component"] == "audit" && line["event"] == "topic.alter" {
				return rec.Code, rec.Body.String(), line["outcome"]
			}
		}
		t.Fatalf("no topic.alter audit line (client got %d):\n%s", rec.Code, logs.String())
		return 0, "", nil
	}

	leader.err = fmt.Errorf("%w: %w: leadership lost while committing log", errs.ErrUnavailable, errs.ErrOutcomeUnknown)
	if code, body, outcome := alterOutcome(); code != http.StatusServiceUnavailable || outcome != "unknown" || !strings.Contains(body, errs.ErrOutcomeUnknown.Error()) {
		t.Fatalf("leadership lost on the leader: client got %d %q, audit outcome %v; want 503 saying the change may still apply, outcome unknown", code, body, outcome)
	}
	leader.err = fmt.Errorf("%w: leader barrier: node is not the leader", errs.ErrUnavailable)
	if code, _, outcome := alterOutcome(); code != http.StatusServiceUnavailable || outcome != "failed" {
		t.Fatalf("barrier failed on the leader: client got %d, audit outcome %v; want 503, outcome failed", code, outcome)
	}
}

package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	nodewire "github.com/debanganthakuria/narad/internal/protocol/node"
)

func TestRouteConsumeReturnsFalseWhenTopicMissing(t *testing.T) {
	store := newTestStore(t)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", nil)

	forwarded := router.RouteAck(context.Background(), res, req, "orders", consumer.Handle{Partition: 0, Offset: 1, Nonce: 2})
	if forwarded {
		t.Fatal("RouteAck() = true, want false")
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
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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

// A forwarded long-poll must carry an explicit deadline covering the full
// requested wait: with no deadline, the peer transport applies its short
// default reply timeout, which would cut long polls short.
func TestRouteConsumePinnedLongPollDeadlineCoversWait(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	var deadline time.Time
	var hasDeadline bool
	router.peer = fakePeerClient{consumeFn: func(ctx context.Context, _ string, _ nodewire.ConsumeRequest) (nodewire.Response, error) {
		deadline, hasDeadline = ctx.Deadline()
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=30s", nil)
	start := time.Now()
	forwarded, _ := router.RouteConsume(context.Background(), res, req, "orders", new(1))
	if !forwarded {
		t.Fatal("RouteConsume() = false, want true")
	}
	if !hasDeadline {
		t.Fatal("forwarded long-poll has no deadline; the transport fallback timeout would cut it short")
	}
	if remaining := deadline.Sub(start); remaining < 30*time.Second {
		t.Fatalf("forwarded deadline is %s away, want at least the 30s wait", remaining)
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-self"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	router.peer = fakePeerClient{consumeFn: func(_ context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		if req.HasPartition {
			t.Fatalf("partition request = %+v, want unpinned local-only scan", req)
		}
		if !req.LocalOnly || req.WaitNanos != 0 {
			t.Fatalf("local-only request = %+v, want local_only wait=0", req)
		}
		probes++
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=0s", nil)

	start := time.Now()
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
		t.Fatalf("remote probes = %d, want 1 immediate probe for wait=0", probes)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("wait=0 consume took %s, want an immediate 204", elapsed)
	}
}

// remoteOnlyConsumeRouter builds a router whose topic has every partition
// owned by a remote member, so queue-style consumes can only be satisfied by
// probing remote owners.
func remoteOnlyConsumeRouter(t *testing.T, consumeFn func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error)) *Router {
	t.Helper()
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	router.consumeReprobeInterval = 10 * time.Millisecond
	router.peer = fakePeerClient{consumeFn: consumeFn}
	return router
}

// A queue-style long-poll on a node that owns no partitions of the topic
// must honor the wait budget: when the initial remote probes come back
// empty, the router keeps re-probing the owners and returns a message that
// materializes later instead of an immediate 204.
func TestRouteConsumeRemoteLongPollDeliversLateMessage(t *testing.T) {
	var probes int
	router := remoteOnlyConsumeRouter(t, func(_ context.Context, _ string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		if !req.LocalOnly || req.WaitNanos != 0 {
			t.Fatalf("remote probe request = %+v, want non-blocking local-only scan", req)
		}
		probes++
		if probes == 1 {
			return nodewire.Response{Status: http.StatusNoContent}, nil
		}
		return nodewire.Response{Status: http.StatusOK, ContentType: "application/json", Body: []byte(`{"offset":7}`)}, nil
	})
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=5s", nil)

	start := time.Now()
	forwarded, localPartition := router.RouteConsume(context.Background(), res, req, "orders", nil)
	if !forwarded {
		t.Fatal("RouteConsume() forwarded = false, want true")
	}
	if localPartition != nil {
		t.Fatalf("RouteConsume() local partition = %d, want nil", *localPartition)
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusOK)
	}
	if got := res.Body.String(); got != `{"offset":7}` {
		t.Fatalf("body = %q, want %q", got, `{"offset":7}`)
	}
	if probes != 2 {
		t.Fatalf("remote probes = %d, want 2 (empty then delivered)", probes)
	}
	if elapsed := time.Since(start); elapsed >= 5*time.Second {
		t.Fatalf("long-poll took the full %s budget despite an early message", elapsed)
	}
}

// When every re-probe stays empty, the long-poll must consume the wait
// budget before answering 204 rather than 204ing on the first empty round.
func TestRouteConsumeRemoteLongPollReturnsNoContentAfterWaitBudget(t *testing.T) {
	var probes int
	router := remoteOnlyConsumeRouter(t, func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		probes++
		return nodewire.Response{Status: http.StatusNoContent}, nil
	})
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=100ms", nil)

	start := time.Now()
	forwarded, localPartition := router.RouteConsume(context.Background(), res, req, "orders", nil)
	elapsed := time.Since(start)
	if !forwarded {
		t.Fatal("RouteConsume() forwarded = false, want true")
	}
	if localPartition != nil {
		t.Fatalf("RouteConsume() local partition = %d, want nil", *localPartition)
	}
	if res.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusNoContent)
	}
	if probes < 2 {
		t.Fatalf("remote probes = %d, want re-probes beyond the initial round", probes)
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("long-poll answered 204 after %s, want the full 100ms wait budget", elapsed)
	}
}

// Cancelling the request context must stop the remote re-probe loop well
// before the wait budget expires.
func TestRouteConsumeRemoteLongPollStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var probes int
	router := remoteOnlyConsumeRouter(t, func(context.Context, string, nodewire.ConsumeRequest) (nodewire.Response, error) {
		probes++
		cancel()
		return nodewire.Response{Status: http.StatusNoContent}, nil
	})
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=30s", nil)

	start := time.Now()
	forwarded, localPartition := router.RouteConsume(ctx, res, req, "orders", nil)
	elapsed := time.Since(start)
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
		t.Fatalf("remote probes = %d, want 1 before cancellation stopped the loop", probes)
	}
	if elapsed >= 5*time.Second {
		t.Fatalf("cancelled long-poll returned after %s, want well before the 30s budget", elapsed)
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	router.peer = fakePeerClient{consumeFn: func(_ context.Context, _ string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		if !req.LocalOnly || req.HasPartition || req.WaitNanos != 0 {
			t.Fatalf("remote probe request = %+v, want unpinned local-only scan", req)
		}
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote-a"); err != nil {
		t.Fatalf("AssignPartition(0) error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-b"); err != nil {
		t.Fatalf("AssignPartition(1) error = %v", err)
	}

	var calls []string
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	router.peer = fakePeerClient{consumeFn: func(_ context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		if !req.LocalOnly || req.HasPartition || req.WaitNanos != 0 {
			t.Fatalf("remote probe request = %+v, want unpinned local-only scan", req)
		}
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
		if err := store.AssignPartition(ctx, "orders", assignment.partition, assignment.ownerID); err != nil {
			t.Fatalf("AssignPartition(%d) error = %v", assignment.partition, err)
		}
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	router.peer = fakePeerClient{consumeFn: func(_ context.Context, addr string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		if !req.LocalOnly {
			t.Fatalf("local_only = false, want true")
		}
		if req.HasPartition {
			t.Fatalf("remote probe request = %+v, want unpinned local-only scan", req)
		}
		switch addr {
		case remoteA.Listener.Addr().String():
			remoteAProbes++
		case remoteB.Listener.Addr().String():
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
		if err := store.AssignPartition(ctx, "orders", assignment.partition, assignment.ownerID); err != nil {
			t.Fatalf("AssignPartition(%d) error = %v", assignment.partition, err)
		}
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	candidates := router.remoteConsumeCandidates("orders")
	if len(candidates) != 2 {
		t.Fatalf("remoteConsumeCandidates() len = %d, want 2: %+v", len(candidates), candidates)
	}

	seen := map[string]bool{}
	for _, addr := range candidates {
		if seen[addr] {
			t.Fatalf("remoteConsumeCandidates() returned duplicate owner addr %q: %+v", addr, candidates)
		}
		seen[addr] = true
	}
	if !seen["remote-a.example:7942"] || !seen["remote-b.example:7942"] {
		t.Fatalf("remoteConsumeCandidates() owners = %v, want remote-a and remote-b", seen)
	}
}

func TestRouteAckForwardsHandleToRemoteOwner(t *testing.T) {
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want %s", r.Method, http.MethodPost)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer remote.Close()

	store := newTestStore(t)
	ctx := context.Background()
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: remote.Listener.Addr().String(), Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	router.peer = fakePeerClient{ackFn: func(_ context.Context, addr string, req nodewire.AckRequest) (nodewire.Response, error) {
		if addr != remote.Listener.Addr().String() {
			t.Fatalf("addr = %q, want %q", addr, remote.Listener.Addr().String())
		}
		want := nodewire.AckRequest{Topic: "orders", Partition: 0, Offset: 1, Nonce: 2}
		if req != want {
			t.Fatalf("ack request = %+v, want %+v", req, want)
		}
		return nodewire.Response{Status: http.StatusOK, Body: []byte("ok")}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/topics/orders/ack", nil)

	forwarded := router.RouteAck(context.Background(), res, req, "orders", consumer.Handle{Partition: 0, Offset: 1, Nonce: 2})
	if !forwarded {
		t.Fatal("RouteAck() = false, want true")
	}
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusOK)
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
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote-1"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 2, "node-remote-2"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
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

// A client-supplied ?wait= must not stretch a forwarded long-poll (and the
// RPC deadline derived from it) arbitrarily: the router clamps the parsed
// wait to its max consume wait before forwarding.
func TestRouteConsumePinnedLongPollClampsExcessiveWait(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())

	var gotWait time.Duration
	var deadline time.Time
	var hasDeadline bool
	router.peer = fakePeerClient{consumeFn: func(ctx context.Context, _ string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		gotWait = time.Duration(req.WaitNanos)
		deadline, hasDeadline = ctx.Deadline()
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=24h", nil)
	start := time.Now()
	forwarded, _ := router.RouteConsume(context.Background(), res, req, "orders", new(1))
	if !forwarded {
		t.Fatal("RouteConsume() = false, want true")
	}
	if gotWait != defaultMaxConsumeWait {
		t.Fatalf("forwarded wait = %s, want clamped to %s", gotWait, defaultMaxConsumeWait)
	}
	if !hasDeadline {
		t.Fatal("forwarded long-poll has no deadline")
	}
	if remaining := deadline.Sub(start); remaining > defaultMaxConsumeWait+longWaitRPCGrace+3*time.Second {
		t.Fatalf("forwarded deadline is %s away, want at most wait ceiling + grace", remaining)
	}
}

// SetMaxConsumeWait wires the configured ceiling; the clamp must honor it.
func TestRouteConsumePinnedLongPollHonorsConfiguredMaxWait(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: 2}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 1, "node-remote"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin())
	router.SetMaxConsumeWait(10 * time.Second)

	var gotWait time.Duration
	router.peer = fakePeerClient{consumeFn: func(_ context.Context, _ string, req nodewire.ConsumeRequest) (nodewire.Response, error) {
		gotWait = time.Duration(req.WaitNanos)
		return nodewire.Response{Status: http.StatusNoContent}, nil
	}}
	res := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders/consume?wait=24h", nil)
	forwarded, _ := router.RouteConsume(context.Background(), res, req, "orders", new(1))
	if !forwarded {
		t.Fatal("RouteConsume() = false, want true")
	}
	if gotWait != 10*time.Second {
		t.Fatalf("forwarded wait = %s, want clamped to configured 10s", gotWait)
	}
}

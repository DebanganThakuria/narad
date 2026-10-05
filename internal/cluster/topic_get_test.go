package cluster

// Audit finding 1.7: a topic GET used to pay one sequential round trip
// per remote partition (about 86 for a 108-partition topic on 5 nodes).
// The remote fetches now run concurrently, and the merged result is
// still complete and in partition order.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
	"github.com/debanganthakuria/narad/internal/transport/httpserver/handlers"
	httptopics "github.com/debanganthakuria/narad/internal/transport/httpserver/handlers/topics"
)

func TestRouteGetTopicFetchesRemotePartitionsConcurrently(t *testing.T) {
	const remotePartitions = 6
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: remotePartitions + 1}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.RegisterMember(ctx, metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive}); err != nil {
		t.Fatalf("RegisterMember() error = %v", err)
	}
	if err := store.AssignPartition(ctx, "orders", 0, "node-self"); err != nil {
		t.Fatalf("AssignPartition() error = %v", err)
	}
	for p := 1; p <= remotePartitions; p++ {
		if err := store.AssignPartition(ctx, "orders", p, "node-remote"); err != nil {
			t.Fatalf("AssignPartition(%d) error = %v", p, err)
		}
	}

	// Every remote call blocks until ALL remote calls are in flight: a
	// sequential router would deadlock here and hit the timeout.
	var inFlight sync.WaitGroup
	inFlight.Add(remotePartitions)
	allInFlight := make(chan struct{})
	go func() { inFlight.Wait(); close(allInFlight) }()

	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	router.peer = fakePeerClient{topicPartitionStatsFn: func(ctx context.Context, _, _ string, p int) (topic.PartitionStats, error) {
		inFlight.Done()
		select {
		case <-allInFlight:
		case <-ctx.Done():
			return topic.PartitionStats{}, ctx.Err()
		}
		return topic.PartitionStats{Index: p, NextOffset: int64(100 * p)}, nil
	}}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil)
	details, err := router.RouteGetTopic(callCtx, req, "orders", topic.Details{
		Topic:      topic.Topic{Name: "orders", Partitions: remotePartitions + 1},
		Partitions: []topic.PartitionStats{{Index: 0, NextOffset: 10}},
	})
	if err != nil {
		t.Fatalf("RouteGetTopic() error = %v (remote fetches were not concurrent?)", err)
	}
	if len(details.Partitions) != remotePartitions+1 {
		t.Fatalf("len(Partitions) = %d, want %d", len(details.Partitions), remotePartitions+1)
	}
	for i, ps := range details.Partitions {
		if ps.Index != i {
			t.Fatalf("Partitions[%d].Index = %d, want %d", i, ps.Index, i)
		}
		wantNext := int64(100 * i)
		wantOwner := "node-remote"
		if i == 0 {
			wantNext, wantOwner = 10, "node-self"
		}
		if ps.NextOffset != wantNext || ps.OwnerNode != wantOwner {
			t.Fatalf("Partitions[%d] = %+v, want next=%d owner=%s", i, ps, wantNext, wantOwner)
		}
	}
}

// getTopicFixture is a topic whose partitions are owned as listed (an
// owner of "" leaves the partition unassigned), over a store holding the
// given members.
func getTopicFixture(t *testing.T, partitions int, owners map[int]string, members ...metastore.Member) *metastore.Store {
	t.Helper()
	store := newTestStore(t)
	ctx := context.Background()
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "00000000000000c1", Partitions: partitions}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for _, m := range members {
		if err := store.RegisterMember(ctx, m); err != nil {
			t.Fatalf("RegisterMember(%s): %v", m.ID, err)
		}
	}
	for p, owner := range owners {
		if err := store.AssignPartition(ctx, "orders", p, owner); err != nil {
			t.Fatalf("AssignPartition(%d): %v", p, err)
		}
	}
	return store
}

// localDetails is the describe of an n-partition topic on a node that
// owns partition 0, whose high watermark is 10.
func localDetails(n int) topic.Details {
	d := topic.Details{Topic: topic.Topic{Name: "orders", ID: "00000000000000c1", Partitions: n}}
	for p := range n {
		d.Partitions = append(d.Partitions, topic.PartitionStats{Index: p})
	}
	d.Partitions[0].HighWatermark = 10
	return d
}

// A topic GET with a partition owner down answers with the stats it
// could read, one entry per partition, and marks the rest: a dead
// owner, an owner whose stats RPC fails, an owner the node has no
// address for, and a partition with no owner yet each get a zero
// placeholder with status owner_unavailable and the owner's liveness,
// and the topic is partial. Master failed the whole GET with
// ErrNotPartitionOwner (421) as soon as one owner was down, so the
// operator lost every partition's stats exactly when a node died.
func TestGetTopicWithAnOwnerDownIsPartial(t *testing.T) {
	store := getTopicFixture(t, 6,
		map[int]string{0: "node-self", 1: "node-live", 2: "node-dead", 3: "node-silent", 5: "node-gone"},
		metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive},
		metastore.Member{ID: "node-live", Addr: "live.example:7942", Status: metastore.MemberAlive},
		metastore.Member{ID: "node-dead", Addr: "dead.example:7942", Status: metastore.MemberDead},
		metastore.Member{ID: "node-silent", Addr: "silent.example:7942", Status: metastore.MemberAlive},
	)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	var mu sync.Mutex
	asked := map[string]int{}
	router.peer = fakePeerClient{topicPartitionStatsFn: func(_ context.Context, addr, _ string, p int) (topic.PartitionStats, error) {
		mu.Lock()
		asked[addr]++
		mu.Unlock()
		if addr == "silent.example:7942" {
			return topic.PartitionStats{}, context.DeadlineExceeded
		}
		return topic.PartitionStats{Index: p, HighWatermark: 20}, nil
	}}

	details, err := router.RouteGetTopic(context.Background(), httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil), "orders", localDetails(6))
	if err != nil {
		t.Fatalf("RouteGetTopic with owners down: %v, want the partial stats", err)
	}
	if !details.Partial || len(details.Partitions) != 6 {
		t.Fatalf("partial = %v with %d partitions, want partial with 6", details.Partial, len(details.Partitions))
	}
	type want struct {
		status, liveness, owner string
		hwm                     int64
	}
	for p, w := range map[int]want{
		0: {topic.PartitionStatusOK, "", "node-self", 10},
		1: {topic.PartitionStatusOK, "", "node-live", 20},
		2: {topic.PartitionOwnerUnavailable, topic.OwnerDead, "node-dead", 0},
		3: {topic.PartitionOwnerUnavailable, topic.OwnerUnreachable, "node-silent", 0},
		4: {topic.PartitionOwnerUnavailable, topic.OwnerUnassigned, "", 0},
		5: {topic.PartitionOwnerUnavailable, topic.OwnerUnknown, "node-gone", 0},
	} {
		got := details.Partitions[p]
		if got.Index != p || got.Status != w.status || got.OwnerLiveness != w.liveness || got.OwnerNode != w.owner || got.HighWatermark != w.hwm {
			t.Fatalf("partition %d = %+v, want status %s, liveness %q, owner %q, hwm %d", p, got, w.status, w.liveness, w.owner, w.hwm)
		}
	}
	if asked["dead.example:7942"] != 0 {
		t.Fatal("asked a dead owner for stats")
	}
}

// Assignment rows at or beyond the topic's partition count (left by an
// earlier incarnation) are ignored: the answer has exactly one entry per
// partition and asks nobody about the extra rows. Master merged one
// entry per assignment row, so the topic over-reported.
func TestGetTopicIgnoresOutOfRangeAssignmentRows(t *testing.T) {
	store := getTopicFixture(t, 3,
		map[int]string{0: "node-self", 1: "node-self", 2: "node-remote"},
		metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive},
		metastore.Member{ID: "node-remote", Addr: "remote.example:7942", Status: metastore.MemberAlive},
	)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	var asked int
	router.peer = fakePeerClient{topicPartitionStatsFn: func(_ context.Context, _, _ string, p int) (topic.PartitionStats, error) {
		asked++
		return topic.PartitionStats{Index: p, HighWatermark: 99}, nil
	}}

	// The record says two partitions; the row for partition 2 is stale.
	details, err := router.RouteGetTopic(context.Background(), httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil), "orders", localDetails(2))
	if err != nil {
		t.Fatalf("RouteGetTopic: %v", err)
	}
	if len(details.Partitions) != 2 || details.Partial || asked != 0 {
		t.Fatalf("partitions = %+v, partial %v, remote asks %d; want the 2 local partitions only", details.Partitions, details.Partial, asked)
	}
	for p, got := range details.Partitions {
		if got.Index != p || got.Status != topic.PartitionStatusOK || got.OwnerNode != "node-self" {
			t.Fatalf("partition %d = %+v, want ok and owned here", p, got)
		}
	}
}

// A failure to read this node's own metadata is a 503 to retry, never a
// 421 telling the client it asked the wrong node. (Master already
// answered 503 here, through the assignment read's catch-up gate; this
// pins the contract for the member read the partial answer adds.)
func TestGetTopicStoreFailureIs503(t *testing.T) {
	store := getTopicFixture(t, 1, map[int]string{0: "node-self"},
		metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive})
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	_, err := router.RouteGetTopic(context.Background(), httptest.NewRequest(http.MethodGet, "/v1/topics/orders", nil), "orders", localDetails(1))
	if !errors.Is(err, errs.ErrUnavailable) || errors.Is(err, errs.ErrNotPartitionOwner) {
		t.Fatalf("RouteGetTopic over a closed store = %v, want an ErrUnavailable (503)", err)
	}
}

// getTopicBroker is a node's broker as the topic GET handler sees it.
type getTopicBroker struct {
	broker.Broker
	details topic.Details
}

func (b *getTopicBroker) GetTopicDetails(context.Context, string) (topic.Details, error) {
	return b.details, nil
}

// End to end through the HTTP handler: with one partition's owner dead
// the GET answers 200 with partial true and a status per partition, and
// a ?partition= query for the dead one answers that entry, marked.
// Master answered 421 to both.
func TestGetTopicAnswersPartialWhenAnOwnerIsDown(t *testing.T) {
	store := getTopicFixture(t, 2, map[int]string{0: "node-self", 1: "node-dead"},
		metastore.Member{ID: "node-self", Addr: "self.example:7942", Status: metastore.MemberAlive},
		metastore.Member{ID: "node-dead", Addr: "dead.example:7942", Status: metastore.MemberDead},
	)
	router := NewRouter(store, "node-self", partition.NewHashRoundRobin(), "")
	set := handlers.New(handlers.Deps{Broker: &getTopicBroker{details: localDetails(2)}, Logger: discardLogger(), Router: router})

	get := func(query string) map[string]any {
		req := httptest.NewRequest(http.MethodGet, "/v1/topics/orders"+query, nil)
		req.SetPathValue("topic", "orders")
		rec := httptest.NewRecorder()
		httptopics.Get(set).ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET%s = %d %s, want 200", query, rec.Code, rec.Body)
		}
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return body
	}
	body := get("")
	stats, _ := body["partition_stats"].([]any)
	if body["partial"] != true || len(stats) != 2 {
		t.Fatalf("GET body = %v, want partial with 2 partitions", body)
	}
	p0, _ := stats[0].(map[string]any)
	p1, _ := stats[1].(map[string]any)
	if p0["status"] != "ok" || p0["high_watermark"] != float64(10) {
		t.Fatalf("partition 0 = %v, want ok with high watermark 10", p0)
	}
	if p1["status"] != "owner_unavailable" || p1["owner_liveness"] != "dead" || p1["owner_node"] != "node-dead" {
		t.Fatalf("partition 1 = %v, want owner_unavailable, dead, owned by node-dead", p1)
	}

	body = get("?partition=1")
	stats, _ = body["partition_stats"].([]any)
	if body["partial"] != true || len(stats) != 1 {
		t.Fatalf("GET ?partition=1 body = %v, want partial with the one entry", body)
	}
	if one, _ := stats[0].(map[string]any); one["index"] != float64(1) || one["status"] != "owner_unavailable" {
		t.Fatalf("GET ?partition=1 entry = %v, want partition 1 marked owner_unavailable", one)
	}
	body = get("?partition=0")
	if _, partial := body["partial"]; partial {
		t.Fatalf("GET ?partition=0 body = %v, want no partial flag for a live partition", body)
	}
}

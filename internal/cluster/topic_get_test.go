package cluster

// Audit finding 1.7: a topic GET used to pay one sequential round trip
// per remote partition (about 86 for a 108-partition topic on 5 nodes).
// The remote fetches now run concurrently, and the merged result is
// still complete and in partition order.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
	"github.com/debanganthakuria/narad/internal/platform/partition"
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

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/metastore"
)

// wp10bGatedStore is a real store whose AssignPartition, as the
// controller calls it, waits for release; entered is closed on the first
// call. That pins the controller between reading a topic's assignments
// and writing them.
type wp10bGatedStore struct {
	*metastore.Store
	entered chan struct{}
	release chan struct{}
	first   bool
}

func (g *wp10bGatedStore) AssignPartition(ctx context.Context, topicName string, partition int, ownerID string) error {
	if !g.first {
		g.first = true
		close(g.entered)
	}
	<-g.release
	return g.Store.AssignPartition(ctx, topicName, partition, ownerID)
}

func wp10bLeaderStore(t *testing.T) *metastore.Store {
	t.Helper()
	s, err := metastore.New(metastore.Config{NodeID: "narad-0", DataDir: t.TempDir(), BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("metastore.New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for !s.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for raft leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return s
}

func wp10bOwners(t *testing.T, s *metastore.Store, topicName string) map[int]string {
	t.Helper()
	assignments, err := s.ListAssignments(topicName)
	if err != nil {
		t.Fatalf("ListAssignments: %v", err)
	}
	out := make(map[int]string, len(assignments))
	for _, a := range assignments {
		out[a.Partition] = a.OwnerID
	}
	return out
}

// A controller sweep and a topic create both place a topic's unassigned
// partitions by reading the assignments and then writing owners, and an
// assignment write replaces whatever is on record. Here the sweep read
// the member table when only narad-1 had registered and the create read
// it after narad-2 had too, the same shape as a create racing the
// out-of-cycle pass on a cold cluster. Unordered, the create placed
// every partition and the sweep then moved half of them to another
// node; a node could have committed records to a partition in between.
// No partition may ever change owner here.
func TestWP10BSweepAndCreateNeverOverwriteEachOther(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s := wp10bLeaderStore(t)
	for _, id := range []string{"narad-0", "narad-1"} {
		if err := s.RegisterMember(ctx, metastore.Member{ID: id, Addr: id + ":7943", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
			t.Fatalf("RegisterMember %s: %v", id, err)
		}
	}
	const partitions = 8
	if err := s.CreateTopic(ctx, topic.Topic{Name: "orders", Partitions: partitions}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	gated := &wp10bGatedStore{Store: s, entered: make(chan struct{}), release: make(chan struct{})}
	c := &Controller{store: gated, cfg: Config{}.withDefaults()}
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		c.reconcileAssignments(ctx)
	}()
	select {
	case <-gated.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("controller sweep never tried to assign")
	}

	// The sweep has read the assignments (none) and is about to write.
	// Another member arrives, and a create places the same partitions.
	if err := s.RegisterMember(ctx, metastore.Member{ID: "narad-2", Addr: "narad-2:7943", Status: metastore.MemberAlive, LastHeartbeat: time.Now().Unix()}); err != nil {
		t.Fatalf("RegisterMember narad-2: %v", err)
	}
	createDone := make(chan error, 1)
	go func() { createDone <- s.AssignNewPartitions(ctx, "orders", 0, partitions) }()
	select {
	case err := <-createDone:
		createDone <- err
	case <-time.After(time.Second):
		// Waiting for the sweep to finish, as it should.
	}
	before := wp10bOwners(t, s, "orders")

	close(gated.release)
	<-sweepDone
	if err := <-createDone; err != nil {
		t.Fatalf("AssignNewPartitions: %v", err)
	}
	after := wp10bOwners(t, s, "orders")

	if len(after) != partitions {
		t.Fatalf("assignments after both = %v, want all %d partitions", after, partitions)
	}
	for p, owner := range before {
		if after[p] != owner {
			t.Fatalf("partition %d changed owner %s -> %s: the sweep overwrote the create's placement (before %v, after %v)", p, owner, after[p], before, after)
		}
	}
}

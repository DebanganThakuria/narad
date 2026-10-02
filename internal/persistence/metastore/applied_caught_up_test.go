package metastore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

// TestAppliedCaughtUpWaitsForFSM pins AppliedCaughtUp to the FSM's own
// durable applied index rather than Raft's applied_index: Raft advances
// its number when a batch is queued for the FSM, before the entries are
// in bbolt, and fsm_pending does not count the batch being applied. A
// store whose FSM has not yet applied what Raft has handed it must not
// report caught up, or the ownership latch sets on a stale view (a
// restart replaying the log from index 1 did exactly that and committed
// produce records into a partition copy the node no longer owned).
func TestAppliedCaughtUpWaitsForFSM(t *testing.T) {
	s, err := New(Config{
		NodeID:        "fsm-0",
		DataDir:       t.TempDir(),
		BindAddr:      "127.0.0.1:0",
		AdvertiseAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	deadline := time.Now().Add(10 * time.Second)
	for !s.AppliedCaughtUp() {
		if time.Now().After(deadline) {
			t.Fatal("single-node store never caught up")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := s.CreateTopic(context.Background(), topic.Topic{Name: "orders", Partitions: 1}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for !s.AppliedCaughtUp() {
		if time.Now().After(deadline) {
			t.Fatal("store never caught up after a write")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Pretend the FSM is one entry behind what Raft has queued for it.
	durable := s.fsm.applied.Load()
	if durable == 0 {
		t.Fatal("fsm applied index is 0 after a committed write")
	}
	s.fsm.applied.Store(durable - 1)
	if s.AppliedCaughtUp() {
		t.Fatal("AppliedCaughtUp reported true while the FSM had not applied the last committed entry")
	}
	s.fsm.applied.Store(durable)
	if !s.AppliedCaughtUp() {
		t.Fatal("AppliedCaughtUp reported false once the FSM had applied everything")
	}
}

// A node restored from a snapshot whose image carries no applied index
// (one written by 3.0.x) reports caught up and ready without waiting for
// a new command. The FSM's own index is 0 after such a restore while
// Raft keeps trailing entries behind the snapshot, and the coverage walk
// used to go down to 0, find a command the snapshot already covered, and
// read the replica as behind until some new command committed.
func TestReadyAfterRestartFromAnOldFormatSnapshot(t *testing.T) {
	ctx := context.Background()
	addr := freeAddr(t)
	cfg := Config{NodeID: "solo", DataDir: t.TempDir(), BindAddr: addr, AdvertiseAddr: addr}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if s != nil {
			_ = s.Close()
		}
	})
	waitUntil(t, 15*time.Second, "leader and caught up", func() bool { return s.IsLeader() && s.AppliedCaughtUp() })
	for i := range 3 {
		if err := s.CreateTopic(ctx, topic.Topic{Name: fmt.Sprintf("t-%d", i), Partitions: 1}); err != nil {
			t.Fatal(err)
		}
	}
	// The image Raft takes next carries no applied index, like a 3.0.x one.
	if err := s.fsm.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte("fsm_meta")); err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.r.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	s = nil

	s, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	waitUntil(t, 15*time.Second, "leader", s.IsLeader)
	waitUntil(t, 3*time.Second, "caught up and ready with no new command", func() bool {
		return s.AppliedCaughtUp() && s.ClusterReady() == nil
	})
	if _, err := s.GetTopic(ctx, "t-2"); err != nil {
		t.Fatalf("restored replica: %v", err)
	}
}

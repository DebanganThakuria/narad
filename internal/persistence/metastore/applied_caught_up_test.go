package metastore

import (
	"context"
	"testing"
	"time"

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

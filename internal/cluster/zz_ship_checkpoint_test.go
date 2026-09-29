package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// A power loss can bring back an older checkpoint while the WAL segment
// unlinks done behind a newer one survive. A dispatcher that restarts
// from a checkpoint more than its read-ahead below the oldest segment
// used to stall for good: every record accepted afterwards was answered
// 202 and never committed.
func TestProduceDispatcherResumesFromCheckpointBelowOldestSegment(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	dir := t.TempDir()

	manager := newDispatchIngressManagerAt(t, dir)
	const total = 300
	for i := range total {
		if _, err := manager.AcceptProduce(context.Background(), "orders", "k", 0, fmt.Appendf(nil, `{"id":%d}`, i)); err != nil {
			t.Fatalf("AcceptProduce(%d): %v", i, err)
		}
	}
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{BatchSize: 8})
	for range 400 {
		processed, err := dispatcher.DispatchAvailable(context.Background())
		if err != nil {
			t.Fatalf("DispatchAvailable: %v", err)
		}
		if processed == 0 {
			break
		}
	}
	if got := len(committer.committed()); got != total {
		t.Fatalf("first run committed %d, want %d", got, total)
	}
	// Close syncs the checkpoint, so the compaction a restart sees is
	// the one a clean run leaves; then the power loss brings back a
	// value far below the oldest segment (BatchSize 8 reads 128 ahead).
	if err := manager.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var stale [8]byte
	binary.BigEndian.PutUint64(stale[:], 8)
	if err := os.WriteFile(filepath.Join(dir, "ingress", "produce", "checkpoint"), stale[:], 0o644); err != nil {
		t.Fatal(err)
	}

	reopened := newDispatchIngressManagerAt(t, dir)
	const fresh = 20
	for i := range fresh {
		if _, err := reopened.AcceptProduce(context.Background(), "orders", "k", 0, fmt.Appendf(nil, `{"new":%d}`, i)); err != nil {
			t.Fatalf("AcceptProduce new(%d): %v", i, err)
		}
	}
	committer2 := &fakeProduceCommitter{}
	dispatcher2 := NewProduceDispatcher(reopened, store, "node-self", committer2, nil, nil, ProduceDispatcherConfig{BatchSize: 8})
	for range 200 {
		if _, err := dispatcher2.DispatchAvailable(context.Background()); err != nil {
			t.Fatalf("DispatchAvailable after restart: %v", err)
		}
	}
	newSeen := 0
	for _, r := range committer2.committed() {
		if len(r.Payload) > 0 && r.Payload[2] == 'n' {
			newSeen++
		}
	}
	if newSeen != fresh {
		ckpt, _ := reopened.LoadProduceCheckpoint()
		t.Fatalf("after restart dispatched %d of %d new records (checkpoint %d, durable next %d)", newSeen, fresh, ckpt, reopened.DurableProduceNext())
	}
}

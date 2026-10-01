package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
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
	// Compaction waits for the checkpoint's sync: let it land and let
	// idle passes compact behind it, as a node that stops producing does.
	walDir := filepath.Join(dir, "ingress", "produce")
	shipDrainCompaction(t, dispatcher, walDir)
	oldest := shipOldestWALSeq(t, walDir)
	// BatchSize 8 reads 128 ahead of the checkpoint: the stale value must
	// sit further below the oldest segment than that, or the test would
	// pass without the raise it pins.
	if oldest < 8+128 {
		t.Fatalf("oldest WAL seq after compaction = %d, want >= %d for this test", oldest, 8+128)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The power loss brings back a value far below the oldest segment.
	var stale [8]byte
	binary.BigEndian.PutUint64(stale[:], 8)
	if err := os.WriteFile(filepath.Join(walDir, "checkpoint"), stale[:], 0o644); err != nil {
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

// shipDrainCompaction runs idle passes until they have compacted the WAL
// at walDir down to its active segment, which they can do only once the
// checkpoint's deferred sync has landed (250 ms after the last store,
// plus the sync, which a busy disk can stretch). Every record is
// dispatched by then, so nothing else may stay.
func shipDrainCompaction(t *testing.T, dispatcher *ProduceDispatcher, walDir string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := dispatcher.DispatchAvailable(context.Background()); err != nil {
			t.Fatalf("idle DispatchAvailable: %v", err)
		}
		if len(shipWALSegments(t, walDir)) == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("idle passes never compacted the WAL down to its active segment: segments %v", shipWALSegments(t, walDir))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// shipOldestWALSeq is the first seq still in the WAL at dir.
func shipOldestWALSeq(t *testing.T, dir string) uint64 {
	t.Helper()
	first, found := uint64(0), false
	if err := wal.Replay(dir, 0, 0, func(r wal.Record) error {
		if !found {
			first, found = r.ID.Seq, true
		}
		return nil
	}); err != nil {
		t.Fatalf("wal.Replay: %v", err)
	}
	if !found {
		t.Fatal("WAL holds no record")
	}
	return first
}

// shipWALSegments lists the segment bases in the WAL at dir.
func shipWALSegments(t *testing.T, dir string) []uint64 {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var bases []uint64
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".wal")
		if !ok {
			continue
		}
		base, err := strconv.ParseUint(name, 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, base)
	}
	return bases
}

// Compaction stops at the synced checkpoint. A dispatcher whose node
// stops producing must still compact behind its last checkpoint once
// that checkpoint's sync lands, not wait for the next produce.
func TestProduceDispatcherIdlePassesCompactBehindTheLastCheckpoint(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	dir := t.TempDir()
	manager := newDispatchIngressManagerAt(t, dir)
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(manager, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{BatchSize: 8})
	accept := func(n int) {
		for i := range n {
			if _, err := manager.AcceptProduce(context.Background(), "orders", "k", 0, fmt.Appendf(nil, `{"id":%d}`, i)); err != nil {
				t.Fatalf("AcceptProduce(%d): %v", i, err)
			}
		}
		for range 100 {
			processed, err := dispatcher.DispatchAvailable(context.Background())
			if err != nil {
				t.Fatalf("DispatchAvailable: %v", err)
			}
			if processed == 0 {
				break
			}
		}
	}
	accept(3) // the first store creates the checkpoint file, synced inline
	accept(120)
	walDir := filepath.Join(dir, "ingress", "produce")
	// Every sealed segment is wholly below the stored, synced checkpoint:
	// idle passes must compact down to the active one.
	shipDrainCompaction(t, dispatcher, walDir)
}

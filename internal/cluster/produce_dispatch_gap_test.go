package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

// A crash on a disk whose fsync lied can lose the tail of a sealed WAL
// segment while the segment rolled after it keeps its records: the WAL
// then has a hole in its seq space (see the WAL package's
// TestFaultWALLyingFsyncCrash). The records in the hole are gone (the
// disk's fault), and the dispatcher must not wait for them: a
// checkpoint parked below the hole would, once the lookahead horizon
// past it filled up, stop dispatching every later record on the node
// while producers kept getting 202s. The scan window starts at the
// first seq the replay actually sees, so a pass whose checkpoint lands
// on the hole is followed by one that starts past it. This pins that
// property.
func TestProduceDispatcherSkipsWALGap(t *testing.T) {
	store := newTestStore(t)
	seedProduceDispatchTopic(t, store, "node-self")
	dir := t.TempDir()

	manager := newDispatchIngressManagerAt(t, dir)
	const total = 60
	for i := range total {
		if _, err := manager.AcceptProduce(context.Background(), "orders", "k", 0, fmt.Appendf(nil, `{"id":%d}`, i)); err != nil {
			t.Fatalf("AcceptProduce(%d): %v", i, err)
		}
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Cut the last record out of the first (sealed) segment: the hole.
	walDir := filepath.Join(dir, "ingress", "produce")
	var firstSegmentLast wal.Record
	var segments int
	var lastBase uint64
	err := wal.Replay(walDir, 0, 0, func(r wal.Record) error {
		if segments == 0 || r.ID.SegmentBase != lastBase {
			segments++
			lastBase = r.ID.SegmentBase
		}
		if segments == 1 {
			firstSegmentLast = r
		}
		return nil
	})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if segments < 2 {
		t.Fatalf("test needs at least two WAL segments, got %d", segments)
	}
	segPath := filepath.Join(walDir, fmt.Sprintf("%020d.wal", firstSegmentLast.ID.SegmentBase))
	if err := os.Truncate(segPath, firstSegmentLast.ID.Offset); err != nil {
		t.Fatalf("truncate %s: %v", segPath, err)
	}
	holeSeq := firstSegmentLast.ID.Seq

	reopened := newDispatchIngressManagerAt(t, dir)
	committer := &fakeProduceCommitter{}
	dispatcher := NewProduceDispatcher(reopened, store, "node-self", committer, nil, nil, ProduceDispatcherConfig{BatchSize: 8})

	// Drain until nothing moves: the checkpoint must end up past the
	// hole, not parked on it.
	for range 40 {
		processed, err := dispatcher.DispatchAvailable(context.Background())
		if err != nil {
			t.Fatalf("DispatchAvailable: %v", err)
		}
		if processed == 0 {
			break
		}
	}

	committed := committer.committed()
	if len(committed) != total-1 {
		t.Fatalf("committed %d records, want %d (every record the WAL still holds)", len(committed), total-1)
	}
	for _, r := range committed {
		if r.WAL.Seq == holeSeq {
			t.Fatalf("committed the record in the hole (seq %d), which the WAL no longer holds", holeSeq)
		}
	}
	checkpoint, err := reopened.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint: %v", err)
	}
	if checkpoint != total {
		t.Fatalf("checkpoint = %d, want %d: the checkpoint must step over the hole at seq %d", checkpoint, total, holeSeq)
	}
}

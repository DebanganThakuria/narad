package ingress

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func acceptN(t *testing.T, m *Manager, n int) {
	t.Helper()
	for i := range n {
		payload := fmt.Appendf(nil, `{"id":%d,"pad":"%080d"}`, i, i)
		if _, err := m.AcceptProduce(context.Background(), "orders", "k", 0, payload); err != nil {
			t.Fatalf("AcceptProduce(%d) error = %v", i, err)
		}
	}
}

func walFirstSeq(t *testing.T, m *Manager) uint64 {
	t.Helper()
	first, err := m.log.FirstSeq()
	if err != nil {
		t.Fatalf("FirstSeq() error = %v", err)
	}
	return first
}

// A checkpoint that is written but not yet synced must not let the WAL
// be compacted behind it: after a power loss the file can come back
// holding the older, synced value while the unlinked segments stay
// gone.
func TestCompactProduceBeforeStopsAtSyncedCheckpoint(t *testing.T) {
	m, err := OpenManager(t.TempDir(), testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	defer m.Close()
	acceptN(t, m, 60)

	// The first store creates the file and syncs it inline.
	if err := m.StoreProduceCheckpoint(10); err != nil {
		t.Fatalf("StoreProduceCheckpoint(10) error = %v", err)
	}
	m.checkpoint.delay = time.Hour // keep the next store unsynced
	if err := m.StoreProduceCheckpoint(60); err != nil {
		t.Fatalf("StoreProduceCheckpoint(60) error = %v", err)
	}
	if _, err := m.CompactProduceBefore(60); err != nil {
		t.Fatalf("CompactProduceBefore() error = %v", err)
	}
	if first := walFirstSeq(t, m); first > 10 {
		t.Fatalf("oldest WAL seq after compacting behind an unsynced checkpoint = %d, want <= 10 (the synced checkpoint)", first)
	}

	// Once the flush lands, compaction catches up.
	m.checkpoint.flush()
	if _, err := m.CompactProduceBefore(60); err != nil {
		t.Fatalf("CompactProduceBefore() after flush error = %v", err)
	}
	if first := walFirstSeq(t, m); first <= 10 {
		t.Fatalf("oldest WAL seq after the checkpoint synced = %d, want > 10", first)
	}
}

// A checkpoint below the oldest WAL segment (a disk that lost the
// checkpoint's last write while the segment unlinks survived) is raised
// to that segment at open, so the dispatcher does not face a gap wider
// than it reads ahead.
func TestOpenManagerRaisesCheckpointBelowOldestSegment(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	acceptN(t, m, 60)
	if err := m.StoreProduceCheckpoint(60); err != nil {
		t.Fatalf("StoreProduceCheckpoint() error = %v", err)
	}
	if _, err := m.CompactProduceBefore(60); err != nil {
		t.Fatalf("CompactProduceBefore() error = %v", err)
	}
	oldest := walFirstSeq(t, m)
	if oldest <= 8 {
		t.Fatalf("oldest WAL seq after compaction = %d, want > 8 for this test", oldest)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var stale [8]byte
	binary.BigEndian.PutUint64(stale[:], 8)
	checkpointPath := filepath.Join(produceWALDir(dir), produceCheckpointFile)
	if err := os.WriteFile(checkpointPath, stale[:], 0o644); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() after stale checkpoint error = %v", err)
	}
	defer reopened.Close()
	from, to, raised := reopened.CheckpointRaised()
	if !raised || from != 8 || to != oldest {
		t.Fatalf("CheckpointRaised() = (%d, %d, %v), want (8, %d, true)", from, to, raised, oldest)
	}
	got, err := reopened.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if got != oldest {
		t.Fatalf("LoadProduceCheckpoint() = %d, want the raised %d", got, oldest)
	}
	if backlog := reopened.DispatchBacklog(); backlog != reopened.DurableProduceNext()-oldest {
		t.Fatalf("DispatchBacklog() = %d, want %d", backlog, reopened.DurableProduceNext()-oldest)
	}
}

// A checkpoint at or above the oldest segment is left as it is.
func TestOpenManagerKeepsCheckpointAtOldestSegment(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	acceptN(t, m, 20)
	if err := m.StoreProduceCheckpoint(5); err != nil {
		t.Fatalf("StoreProduceCheckpoint() error = %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	reopened, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	defer reopened.Close()
	if _, _, raised := reopened.CheckpointRaised(); raised {
		t.Fatal("CheckpointRaised() = true for a checkpoint inside the WAL")
	}
	if got, _ := reopened.LoadProduceCheckpoint(); got != 5 {
		t.Fatalf("LoadProduceCheckpoint() = %d, want 5", got)
	}
}

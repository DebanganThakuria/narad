package ingress

import (
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

// zzWP6CountCheckpointSyncs counts data syncs of the checkpoint file
// until the returned restore runs.
func zzWP6CountCheckpointSyncs(t *testing.T) (*atomic.Int64, func()) {
	t.Helper()
	var n atomic.Int64
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpSyncData && strings.HasSuffix(path, string(filepath.Separator)+produceCheckpointFile) {
			n.Add(1)
		}
		return nil
	})
	return &n, restore
}

// A progressing dispatch pass stores the checkpoint before its next
// pass can start. The store writes the value in place at once (a
// process crash keeps it in the page cache) but leaves the fdatasync to
// a background flush, so a burst of stores costs one device flush, not
// one each.
func TestZZWP6CheckpointStoreDefersTheDataSync(t *testing.T) {
	m, err := OpenManager(t.TempDir(), wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	// The first store creates the file and syncs it and its directory
	// once, inline, so the name is durable.
	if err := m.StoreProduceCheckpoint(0); err != nil {
		t.Fatal(err)
	}
	syncs, restore := zzWP6CountCheckpointSyncs(t)
	defer restore()

	const stores = 50
	for seq := uint64(1); seq <= stores; seq++ {
		if err := m.StoreProduceCheckpoint(seq); err != nil {
			t.Fatalf("StoreProduceCheckpoint(%d): %v", seq, err)
		}
		if got, err := m.LoadProduceCheckpoint(); err != nil || got != seq {
			t.Fatalf("LoadProduceCheckpoint after store(%d) = (%d, %v): the value must be written at once", seq, got, err)
		}
	}
	// At most the one deferred flush can have landed during the burst.
	if got := syncs.Load(); got > 1 {
		t.Fatalf("%d stores issued %d checkpoint data syncs, want them folded into a deferred one", stores, got)
	}
	// The deferred flush lands on its own.
	deadline := time.Now().Add(5 * time.Second)
	for syncs.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the deferred checkpoint sync never ran")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Close flushes a checkpoint that was written but not yet synced.
func TestZZWP6CheckpointCloseSyncsPendingWrite(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.StoreProduceCheckpoint(0); err != nil {
		t.Fatal(err)
	}
	syncs, restore := zzWP6CountCheckpointSyncs(t)
	defer restore()
	if err := m.StoreProduceCheckpoint(0); err != nil {
		t.Fatal(err)
	}
	before := syncs.Load()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if syncs.Load() == before {
		t.Fatal("Close left a written checkpoint unsynced")
	}
}

// A background sync that fails is not lost: the next store reports it,
// and the value is synced again once the disk recovers.
func TestZZWP6CheckpointBackgroundSyncFailureSurfaces(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.StoreProduceCheckpoint(0); err != nil {
		t.Fatal(err)
	}
	var failed atomic.Int64
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpSyncData && strings.HasSuffix(path, string(filepath.Separator)+produceCheckpointFile) {
			failed.Add(1)
			return errZZWP6Disk
		}
		return nil
	})
	if err := m.StoreProduceCheckpoint(0); err != nil {
		t.Fatalf("store: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for failed.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the deferred checkpoint sync never ran")
		}
		time.Sleep(5 * time.Millisecond)
	}
	restore()
	err = m.StoreProduceCheckpoint(0)
	if err == nil || !strings.Contains(err.Error(), errZZWP6Disk.Error()) {
		t.Fatalf("store after a failed background sync = %v, want the sync failure reported", err)
	}
	syncs, restoreCount := zzWP6CountCheckpointSyncs(t)
	defer restoreCount()
	if err := m.StoreProduceCheckpoint(0); err != nil {
		t.Fatalf("store after recovery: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for syncs.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the checkpoint was never synced again after the failure")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

type zzWP6Error string

func (e zzWP6Error) Error() string { return string(e) }

const errZZWP6Disk = zzWP6Error("zz wp6 injected checkpoint sync failure")

package ingress

import (
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// blockFirstCheckpointSync makes the first data sync of the checkpoint
// file block until release is called; later ones run as usual. entered
// is closed once the blocked sync has started. It returns the number of
// checkpoint data syncs seen so far.
func blockFirstCheckpointSync(t *testing.T) (entered <-chan struct{}, release func(), syncs *atomic.Int64) {
	t.Helper()
	in := make(chan struct{})
	gate := make(chan struct{})
	var n atomic.Int64
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op != syncfile.OpSyncData || !strings.HasSuffix(path, string(filepath.Separator)+produceCheckpointFile) {
			return nil
		}
		if n.Add(1) == 1 {
			close(in)
			<-gate
		}
		return nil
	})
	var released atomic.Bool
	release = func() {
		if released.CompareAndSwap(false, true) {
			close(gate)
		}
	}
	t.Cleanup(func() {
		release()
		restore()
	})
	return in, release, &n
}

// newSyncedCheckpointWriter returns a writer whose file exists and was
// synced by its first store, so later stores defer their sync to a
// background flush after delay.
func newSyncedCheckpointWriter(t *testing.T, delay time.Duration) *checkpointWriter {
	t.Helper()
	w := newCheckpointWriter(t.TempDir(), produceCheckpointFile)
	w.delay = delay
	if err := w.store(1); err != nil {
		t.Fatalf("store(1): %v", err)
	}
	if got := w.durable(); got != 1 {
		t.Fatalf("durable() after the creating store = %d, want 1", got)
	}
	return w
}

// A store that lands while a background flush is inside a slow sync is
// flushed once that sync returns, without waiting for another store: the
// flush it armed fires during the sync, finds it running and returns,
// so the finishing flush arms the next one. An idle dispatcher stores
// nothing more, and the value (and the WAL compaction behind it) would
// otherwise wait for Close.
func TestCheckpointFlushRearmsForStoreDuringSlowSync(t *testing.T) {
	w := newSyncedCheckpointWriter(t, 5*time.Millisecond)
	defer w.close()
	entered, release, syncs := blockFirstCheckpointSync(t)
	// Deferred after close, so it runs first: a failed assertion must
	// open the gate before close waits for the parked flush.
	defer release()

	if err := w.store(2); err != nil {
		t.Fatalf("store(2): %v", err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the background flush of store(2) never reached the sync")
	}
	if err := w.store(3); err != nil {
		t.Fatalf("store(3): %v", err)
	}
	// Wait for the flush store(3) armed to fire and find the sync running.
	deadline := time.Now().Add(5 * time.Second)
	for {
		w.mu.Lock()
		armed := w.flushArmed
		w.mu.Unlock()
		if !armed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the flush armed during the slow sync never fired")
		}
		time.Sleep(time.Millisecond)
	}
	release()

	for w.durable() != 3 {
		if time.Now().After(deadline) {
			t.Fatalf("durable() = %d after %d checkpoint syncs, want 3 synced without a further store", w.durable(), syncs.Load())
		}
		time.Sleep(time.Millisecond)
	}
}

// close waits for a background flush that is syncing the descriptor
// before closing it, and leaves the value that flush covered durable.
func TestCheckpointCloseWaitsForRunningFlush(t *testing.T) {
	w := newSyncedCheckpointWriter(t, 5*time.Millisecond)
	entered, release, _ := blockFirstCheckpointSync(t)

	if err := w.store(2); err != nil {
		t.Fatalf("store(2): %v", err)
	}
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- w.close() }()
	select {
	case err := <-closed:
		t.Fatalf("close returned (%v) while a background flush was syncing the descriptor", err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("close never returned after the background flush finished")
	}
	if got := w.durable(); got != 2 {
		t.Fatalf("durable() after close = %d, want 2", got)
	}
	if got, err := loadCheckpoint(w.path); err != nil || got != 2 {
		t.Fatalf("checkpoint on disk = (%d, %v), want 2", got, err)
	}
}

// close reports a final sync that failed, does not count the value as
// durable, and still releases the descriptor.
func TestCheckpointCloseReportsFailedFinalSync(t *testing.T) {
	w := newSyncedCheckpointWriter(t, time.Hour) // no background flush
	if err := w.store(2); err != nil {
		t.Fatalf("store(2): %v", err)
	}
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpSyncData && strings.HasSuffix(path, string(filepath.Separator)+produceCheckpointFile) {
			return errZZWP6Disk
		}
		return nil
	})
	defer restore()

	if err := w.close(); !errors.Is(err, errZZWP6Disk) {
		t.Fatalf("close = %v, want the failed sync reported", err)
	}
	if got := w.durable(); got != 1 {
		t.Fatalf("durable() after a failed final sync = %d, want 1 (the last synced value)", got)
	}
	if w.file != nil {
		t.Fatal("close kept the descriptor after a failed sync")
	}
	if err := w.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

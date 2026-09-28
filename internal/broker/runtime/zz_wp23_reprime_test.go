package runtime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// A prime that fails after it has opened the partition directory is
// retried on the next tick. If a move installed its copy over the
// directory in between, the retry must not prime the copy with the old
// shard's snapshots: the copy installed at 5 would recover the old
// shard's 19. The partition stays stranded until the move's reset
// Forgets it; then the new lineage's acks persist.
func TestZZWP23PrimeRetryAfterInstall(t *testing.T) {
	cases := []struct {
		name string
		// fail makes the first prime fail after it recorded the
		// directory; it returns the undo.
		fail func(r *zzWP23Rig) (undo func())
	}{
		{name: "open-failed", fail: func(r *zzWP23Rig) func() {
			path := filepath.Join(r.dir(0), storage.ConsumerAheadFileName)
			var failed atomic.Bool
			return syncfile.SetFaultHook(func(op syncfile.Op, p string) error {
				if op == syncfile.OpOpen && p == path && failed.CompareAndSwap(false, true) {
					return fmt.Errorf("injected: %w", os.ErrInvalid)
				}
				return nil
			})
		}},
		{name: "writeout-failed", fail: func(r *zzWP23Rig) func() {
			r.disk.failWriteOut = func(string) error { return fmt.Errorf("injected: %w", os.ErrInvalid) }
			return func() { r.disk.failWriteOut = nil }
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
			old := newZZWP23Shard(19)
			r.shards.set(0, old)
			undo := tc.fail(r)
			r.c.Commit("t", 0, 19)
			err := r.c.flush()
			undo()
			if err == nil {
				t.Fatal("setup: the prime did not fail")
			}
			installed := r.install(0, 5)
			old.ack(20)
			r.c.Commit("t", 0, old.state().frontier)
			if err := r.c.flush(); err != nil && !errors.Is(err, errOffsetDirGone) {
				t.Fatal(err)
			}
			r.untouched(0, installed, "retried prime")
			r.c.ioMu.Lock()
			_, stranded := r.c.stranded[offsetCommitKey{"t", 0}]
			r.c.ioMu.Unlock()
			if !stranded {
				t.Fatal("the partition is not stranded after its retried prime found another directory")
			}
			old.ack(21)
			r.c.Commit("t", 0, old.state().frontier)
			if err := r.c.flush(); err != nil {
				t.Fatal(err)
			}
			r.untouched(0, installed, "later tick")

			// The move's reset: DropPartition, then its Forget.
			r.shards.set(0, nil)
			r.c.Forget("t", 0)
			succ := newZZWP23Shard(5)
			r.shards.set(0, succ)
			succ.ack(6)
			r.c.Commit("t", 0, succ.state().frontier)
			if err := r.c.Close(); err != nil {
				t.Fatal(err)
			}
			if rec := zzWP23RecoverNew(t, r.dir(0)); rec.frontier != 6 {
				t.Fatalf("the new lineage's ack recovers %d after the reset, want 6", rec.frontier)
			}
		})
	}
}

package runtime

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// installBare renames an empty staged directory over partition p's, the
// way a move installs a copy whose source had no consumer state (no
// commit yet, so finalizeStaged writes neither file).
func (r *zzWP23Rig) installBare(p int) map[string][]byte {
	r.t.Helper()
	staged := filepath.Join(r.dataDir, "staged-bare")
	if err := os.MkdirAll(staged, 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := os.RemoveAll(r.dir(p)); err != nil {
		r.t.Fatal(err)
	}
	if err := os.Rename(staged, r.dir(p)); err != nil {
		r.t.Fatal(err)
	}
	return zzWP23Snapshot(r.t, r.dir(p))
}

// zzWP23InstallOnOpen installs a copy the first time the committer opens
// name in partition 0's directory by path: after every check it makes
// before that open, before the open itself.
func zzWP23InstallOnOpen(r *zzWP23Rig, name string, install func() map[string][]byte) (installed func() map[string][]byte, fired *atomic.Bool, restore func()) {
	path := filepath.Join(r.dir(0), name)
	var got map[string][]byte
	fired = &atomic.Bool{}
	restore = syncfile.SetFaultHook(func(op syncfile.Op, p string) error {
		if op == syncfile.OpOpen && p == path && fired.CompareAndSwap(false, true) {
			got = install()
		}
		return nil
	})
	return func() map[string][]byte { return got }, fired, restore
}

// A move installs its copy after the level's checks and before its open
// of consumer.offset, which then names the installed file. The level
// must write nothing into it: recovery takes the larger frontier of the
// two files, so the copy installed at 5 would recover the old shard's 20
// (or 21) and skip offsets it never acked.
func TestZZWP23InstallDuringLevelOpen(t *testing.T) {
	for _, name := range []string{"close", "writeout-30s-later"} {
		t.Run(name, func(t *testing.T) {
			r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
			old := r.warm(0, 20) // consumer.offset levelled to 19
			old.ack(20)
			r.c.Commit("t", 0, old.state().frontier)
			if err := r.c.flush(); err != nil {
				t.Fatal(err)
			}
			installed, fired, restore := zzWP23InstallOnOpen(r, storage.ConsumerOffsetFileName, func() map[string][]byte { return r.install(0, 5) })
			defer restore()
			if name == "close" {
				_ = r.c.Close()
			} else {
				old.ack(21)
				r.c.Commit("t", 0, old.state().frontier)
				_ = r.c.tickAt(time.Now().Add(consumerOffsetLevelEvery+time.Second), offsetTickFlush)
			}
			restore()
			if !fired.Load() {
				t.Fatal("setup: the level never opened consumer.offset")
			}
			r.untouched(0, installed(), name)
		})
	}
}

// The first level of a partition creates its consumer.offset. A move
// that installs a copy with no consumer state between the level's
// checks and that create must not find a consumer.offset created in it
// (holding the old shard's frontier, or empty): nothing the committer
// does for the old shard may add a file to another copy.
func TestZZWP23InstallDuringLevelCreate(t *testing.T) {
	r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
	old := newZZWP23Shard(-1)
	r.shards.set(0, old)
	for off := range int64(20) {
		old.ack(off)
	}
	installed, fired, restore := zzWP23InstallOnOpen(r, storage.ConsumerOffsetFileName, func() map[string][]byte { return r.installBare(0) })
	defer restore()
	r.c.Commit("t", 0, old.state().frontier)
	_ = r.c.flush()
	restore()
	if !fired.Load() {
		t.Fatal("setup: the first level never opened consumer.offset")
	}
	r.untouched(0, installed(), "first level")
	_ = r.c.Close()
	r.untouched(0, installed(), "close")
}

// The prime creates consumer.ahead when the partition has none. A move
// that installs a copy with no consumer state between the prime's
// directory open and that create must not find a consumer.ahead created
// in it.
func TestZZWP23InstallDuringPrimeCreate(t *testing.T) {
	r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
	old := newZZWP23Shard(19)
	r.shards.set(0, old)
	installed, fired, restore := zzWP23InstallOnOpen(r, storage.ConsumerAheadFileName, func() map[string][]byte { return r.installBare(0) })
	defer restore()
	r.c.Commit("t", 0, 19)
	_ = r.c.flush()
	restore()
	if !fired.Load() {
		t.Fatal("setup: the prime never opened consumer.ahead")
	}
	r.untouched(0, installed(), "prime")
	_ = r.c.Close()
	r.untouched(0, installed(), "close")
}

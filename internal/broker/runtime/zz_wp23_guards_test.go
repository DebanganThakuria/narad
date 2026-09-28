package runtime

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// zzWP23Untouched fails the test unless partition p's directory still
// holds exactly what the install wrote.
func (r *zzWP23Rig) untouched(p int, installed map[string][]byte, what string) {
	r.t.Helper()
	if got := zzWP23Snapshot(r.t, r.dir(p)); !zzWP23SameSnapshot(got, installed) {
		off, ok, _ := storage.ReadConsumerOffset(r.dir(p))
		r.t.Fatalf("%s: the old shard's state landed in the installed copy: consumer.offset %d (ok %v), recovers %d",
			what, off, ok, zzWP23RecoverNew(r.t, r.dir(p)).frontier)
	}
}

// A tick takes the old shard's snapshot; before the tick primes the
// partition, a move installs another lineage's copy and resets the
// partition (DropPartition, then its Forget). Forget's contract is that
// no snapshot taken before it is written, so the tick must skip the
// partition rather than prime the installed copy by path and write the
// old shard's frontier (20) into it: the new lineage (5) would then skip
// offsets 6 to 20, never acked there. Once the reset is over, the new
// lineage's own acks persist.
func TestZZWP23ForgetAfterSnapshot(t *testing.T) {
	for _, primed := range []bool{false, true} {
		name := "never-primed"
		if primed {
			name = "primed"
		}
		t.Run(name, func(t *testing.T) {
			r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
			var old *zzWP23Shard
			if primed {
				old = r.warm(0, 20)
			} else {
				old = newZZWP23Shard(19)
				r.shards.set(0, old)
			}
			var installed map[string][]byte
			var fired atomic.Bool
			r.c.SetAheadSource(func(topic string, p int) (int64, []int64, uint64, bool) {
				committed, offsets, version, ok := r.shards.source(topic, p)
				if ok && fired.CompareAndSwap(false, true) {
					installed = r.install(0, 5)
					r.shards.set(0, nil)
					r.c.Forget("t", 0)
				}
				return committed, offsets, version, ok
			})
			old.ack(30)
			old.ack(20)
			r.c.Commit("t", 0, old.state().frontier)
			if err := r.c.flush(); err != nil {
				t.Fatal(err)
			}
			if !fired.Load() {
				t.Fatal("setup: the tick took no snapshot")
			}
			r.untouched(0, installed, "tick")
			// The requeued commit finds no shard on the next tick.
			if err := r.c.flush(); err != nil {
				t.Fatal(err)
			}
			r.untouched(0, installed, "next tick")

			succ := newZZWP23Shard(5)
			r.shards.set(0, succ)
			succ.ack(6)
			r.c.Commit("t", 0, succ.state().frontier)
			if err := r.c.flush(); err != nil {
				t.Fatal(err)
			}
			if rec := zzWP23RecoverNew(t, r.dir(0)); rec.frontier != 6 {
				t.Fatalf("the new lineage's ack recovers %d, want 6", rec.frontier)
			}
		})
	}
}

// A move installs its copy between the prime's open of the partition
// directory and its open of consumer.ahead, which then names the
// installed file. The prime must see that the directory changed under
// it and write nothing: anchoring on the installed record and writing
// the old shard's frontier (19) into the window would make the copy
// installed at 5 recover 19.
func TestZZWP23InstallDuringPrimeOpen(t *testing.T) {
	r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
	old := newZZWP23Shard(19)
	r.shards.set(0, old)
	path := filepath.Join(r.dir(0), storage.ConsumerAheadFileName)
	var installed map[string][]byte
	var fired atomic.Bool
	restore := syncfile.SetFaultHook(func(op syncfile.Op, p string) error {
		if op == syncfile.OpOpen && p == path && fired.CompareAndSwap(false, true) {
			installed = r.install(0, 5)
		}
		return nil
	})
	defer restore()
	r.c.Commit("t", 0, 19)
	if err := r.c.flush(); err != nil {
		t.Fatal(err)
	}
	restore()
	if !fired.Load() {
		t.Fatal("setup: the prime never opened consumer.ahead")
	}
	r.untouched(0, installed, "prime")
}

// consumer.offset is levelled by path, on a writeout at most every 30s,
// on a quiet partition's phase tick, and for every partition at Close.
// A move that installed a copy under a primed partition and has not
// reset it yet must not receive the old shard's frontier (20) in its
// consumer.offset: recovery takes the larger of the two files, so the
// copy installed at 5 would recover 20.
func TestZZWP23LevelAfterInstall(t *testing.T) {
	cases := []struct {
		name string
		// quiet: the old shard's last ack is written out before the
		// install, so the level has no window beside it.
		quiet bool
		run   func(r *zzWP23Rig)
	}{
		{name: "close", run: func(r *zzWP23Rig) { _ = r.c.Close() }},
		{name: "writeout-30s-later", run: func(r *zzWP23Rig) {
			_ = r.c.tickAt(time.Now().Add(consumerOffsetLevelEvery+time.Second), offsetTickFlush)
		}},
		{name: "quiet/close", quiet: true, run: func(r *zzWP23Rig) { _ = r.c.Close() }},
		{name: "quiet/phase-tick-30s-later", quiet: true, run: func(r *zzWP23Rig) {
			// One of every run of D/T ticks is the partition's phase.
			at := time.Now().Add(consumerOffsetLevelEvery + time.Second)
			for i := range r.c.every {
				_ = r.c.tickAt(at.Add(time.Duration(i)*r.c.tick), offsetTickNormal)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
			old := r.warm(0, 20) // consumer.offset levelled to 19 just now
			old.ack(20)
			r.c.Commit("t", 0, old.state().frontier)
			if tc.quiet {
				if err := r.c.flush(); err != nil {
					t.Fatal(err)
				}
				if got := r.durableFrontier(0); got != 20 {
					t.Fatalf("setup: durable frontier %d, want 20", got)
				}
				if off, _, _ := storage.ReadConsumerOffset(r.dir(0)); off != 19 {
					t.Fatalf("setup: consumer.offset %d, want 19 (levelled less than 30s ago)", off)
				}
			}
			installed := r.install(0, 5)
			tc.run(r)
			r.untouched(0, installed, tc.name)
		})
	}
}

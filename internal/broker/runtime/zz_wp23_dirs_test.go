package runtime

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// zzWP23Snapshot reads every file in dir.
func zzWP23Snapshot(t testing.TB, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = b
	}
	return out
}

func zzWP23SameSnapshot(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for name, data := range a {
		if !bytes.Equal(data, b[name]) {
			return false
		}
	}
	return true
}

// zzWP23Install stages a copy of another lineage at frontier and
// renames it over partition p's directory, the way a move installs.
func (r *zzWP23Rig) install(p int, frontier int64) map[string][]byte {
	r.t.Helper()
	staged := filepath.Join(r.dataDir, fmt.Sprintf("staged-%d", p))
	if err := os.MkdirAll(staged, 0o755); err != nil {
		r.t.Fatal(err)
	}
	if err := storage.WriteConsumerOffset(staged, frontier); err != nil {
		r.t.Fatal(err)
	}
	if err := storage.WriteConsumerAhead(staged, 0, uint64(time.Now().UnixNano()), frontier, nil); err != nil {
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

// zzWP23Warm acks 0..n-1 on a fresh shard for partition p and writes it
// out, so the committer holds p primed with a descriptor.
func (r *zzWP23Rig) warm(p int, n int64) *zzWP23Shard {
	r.t.Helper()
	sh := newZZWP23Shard(-1)
	r.shards.set(p, sh)
	for off := range n {
		sh.ack(off)
	}
	r.c.Commit("t", p, sh.state().frontier)
	if err := r.c.flush(); err != nil {
		r.t.Fatal(err)
	}
	return sh
}

// CP10 and M2/M3: a move installs another lineage's copy over a
// partition directory the committer holds primed, while the old shard
// (frontier above the copy's) is still alive and acking. Whatever the
// committer holds, the installed files must stay byte for byte what
// the install wrote.
func TestZZWP23InstalledCopyStaysUntouched(t *testing.T) {
	cases := []struct {
		name string
		// forget: the move drops the shard and Forgets before the
		// install (G2), so the old shard's later commits find none.
		forget bool
		// evict: the partition's descriptor was evicted, so a write
		// reopens by path.
		evict bool
		// ticks the old shard keeps committing across.
		ticks int
	}{
		{name: "held-fd", ticks: 1},
		{name: "evicted-fd", evict: true, ticks: 1},
		{name: "forget-first/held-fd", forget: true, ticks: 5},
		{name: "forget-first/evicted-fd", forget: true, evict: true, ticks: 5},
		{name: "forget-first/never-primed", forget: true, ticks: 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newZZWP23Rig(t, zzWP23RigOpts{parts: 3, maxFDs: 1})
			var old *zzWP23Shard
			if tc.name != "forget-first/never-primed" {
				old = r.warm(0, 20)
			} else {
				old = newZZWP23Shard(19)
				r.shards.set(0, old)
			}
			if tc.evict {
				// Partition 1's prime takes the one descriptor from the
				// clean partition 0.
				r.warm(1, 3)
				r.c.ioMu.Lock()
				held := r.c.parts[offsetCommitKey{"t", 0}].f != nil
				r.c.ioMu.Unlock()
				if held {
					t.Fatal("setup: partition 0 still holds its descriptor")
				}
			}
			if tc.forget {
				r.shards.set(0, nil) // DropPartition, then its notifier
				r.c.Forget("t", 0)
			}
			installed := r.install(0, 5)
			for i := range tc.ticks {
				old.ack(int64(20 + i))
				r.c.Commit("t", 0, old.state().frontier)
				_ = r.c.flush()
				if got := zzWP23Snapshot(t, r.dir(0)); !zzWP23SameSnapshot(got, installed) {
					t.Fatalf("tick %d: the old shard's state landed in the installed copy", i)
				}
			}
			if rec := zzWP23RecoverNew(t, r.dir(0)); rec.frontier != 5 {
				t.Fatalf("installed copy recovers %d, want its own 5", rec.frontier)
			}
		})
	}
}

// CP10 (quarantine) and CP11: the partition directory is renamed away
// or removed under a committer holding it, before or between a
// snapshot and its write. The committer drops its state and never
// recreates the directory.
func TestZZWP23RemovedDirectoryIsNeverRecreated(t *testing.T) {
	for _, how := range []string{"quarantine", "remove", "remove-during-snapshot", "remove-before-prime"} {
		t.Run(how, func(t *testing.T) {
			r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
			var sh *zzWP23Shard
			if how == "remove-before-prime" {
				sh = newZZWP23Shard(3)
				r.shards.set(0, sh)
			} else {
				sh = r.warm(0, 10)
			}
			dir := r.dir(0)
			var quarantined map[string][]byte
			switch how {
			case "quarantine":
				if err := os.Rename(dir, dir+".quarantine"); err != nil {
					t.Fatal(err)
				}
				quarantined = zzWP23Snapshot(t, dir+".quarantine")
			case "remove", "remove-before-prime":
				if err := os.RemoveAll(dir); err != nil {
					t.Fatal(err)
				}
			case "remove-during-snapshot":
				r.c.SetAheadSource(func(topic string, p int) (int64, []int64, uint64, bool) {
					_ = os.RemoveAll(dir)
					return r.shards.source(topic, p)
				})
			}
			for i := range 3 {
				sh.ack(int64(10 + i))
				r.c.Commit("t", 0, sh.state().frontier)
				_ = r.c.flush()
			}
			if err := r.c.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partition directory recreated (stat err %v)", err)
			}
			r.c.ioMu.Lock()
			left := len(r.c.parts)
			r.c.ioMu.Unlock()
			if left != 0 {
				t.Fatalf("committer kept state for %d partitions of a removed directory", left)
			}
			if quarantined != nil {
				// Writes through the held descriptor may reach the
				// quarantined file (it is the same lineage), but only
				// acked values, and its slots stay valid.
				rec, ok, err := storage.ReadConsumerAhead(dir + ".quarantine")
				if err != nil || !ok || rec.Committed > sh.state().frontier {
					t.Fatalf("quarantined consumer.ahead = %+v ok %v err %v, acked frontier %d", rec, ok, err, sh.state().frontier)
				}
			}
		})
	}
}

// CP13: Forget racing a prime. The prime's by-path open is blocked;
// Forget must wait for it, and once Forget returns nothing the
// committer does may land in a directory installed after it.
func TestZZWP23ForgetWaitsForAPrime(t *testing.T) {
	r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
	sh := newZZWP23Shard(7)
	r.shards.set(0, sh)
	path := filepath.Join(r.dir(0), storage.ConsumerAheadFileName)
	entered, release := make(chan struct{}), make(chan struct{})
	var blocked atomic.Bool
	restore := syncfile.SetFaultHook(func(op syncfile.Op, p string) error {
		if op == syncfile.OpOpen && p == path && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
		return nil
	})
	defer restore()

	r.c.Commit("t", 0, 7)
	ticked := make(chan error, 1)
	go func() { ticked <- r.c.flush() }()
	<-entered
	forgot := make(chan struct{})
	go func() {
		r.shards.set(0, nil)
		r.c.Forget("t", 0)
		close(forgot)
	}()
	select {
	case <-forgot:
		t.Fatal("Forget returned while a prime's by-path open was in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-forgot
	installed := r.install(0, 2)
	if err := <-ticked; err != nil {
		t.Fatal(err)
	}
	sh.ack(8)
	r.c.Commit("t", 0, sh.state().frontier)
	_ = r.c.flush()
	if err := r.c.Close(); err != nil {
		t.Fatal(err)
	}
	if got := zzWP23Snapshot(t, r.dir(0)); !zzWP23SameSnapshot(got, installed) {
		t.Fatal("a write landed in the directory installed after Forget")
	}
}

// M5: a partition moves away and back twice within one committer
// lifetime (A to B to A). Each install is Forgotten before and after;
// each lineage's acks land in its own directory only, and every
// recovery is that lineage's.
func TestZZWP23MoveAwayAndBack(t *testing.T) {
	r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
	sh := r.warm(0, 30)
	for round, copyAt := range []int64{12, 50} {
		r.shards.set(0, nil)
		r.c.Forget("t", 0)
		r.install(0, copyAt)
		r.c.Forget("t", 0)
		// The next shard recovers from the installed files.
		rec := zzWP23RecoverNew(t, r.dir(0))
		if rec.frontier != copyAt {
			t.Fatalf("round %d: installed copy recovers %d, want %d", round, rec.frontier, copyAt)
		}
		// A late commit of the dropped shard finds no shard.
		sh.ack(sh.state().frontier + 1)
		r.c.Commit("t", 0, sh.state().frontier)
		_ = r.c.flush()
		if rec := zzWP23RecoverNew(t, r.dir(0)); rec.frontier != copyAt {
			t.Fatalf("round %d: the dropped shard's commit moved the copy to %d", round, rec.frontier)
		}
		sh = newZZWP23Shard(copyAt)
		r.shards.set(0, sh)
		for i := range int64(3) {
			sh.ack(copyAt + 1 + i)
		}
		r.c.Commit("t", 0, sh.state().frontier)
		if err := r.c.flush(); err != nil {
			t.Fatal(err)
		}
		if rec := zzWP23RecoverNew(t, r.dir(0)); rec.frontier != copyAt+3 {
			t.Fatalf("round %d: lineage's own acks recover %d, want %d", round, rec.frontier, copyAt+3)
		}
	}
}

// M6: a topic is purged and recreated while the committer holds its
// partitions' descriptors. The successor recovers -1 whatever the old
// shards' late commits do.
func TestZZWP23PurgeAndRecreate(t *testing.T) {
	r := newZZWP23Rig(t, zzWP23RigOpts{parts: 2})
	olds := []*zzWP23Shard{r.warm(0, 40), r.warm(1, 40)}
	// DropTopic, the notifier's Forgets, then the purge.
	for p := range 2 {
		r.shards.set(p, nil)
		r.c.Forget("t", p)
	}
	if err := os.RemoveAll(storage.TopicDir(r.dataDir, "t")); err != nil {
		t.Fatal(err)
	}
	for p := range 2 {
		if err := os.MkdirAll(r.dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for p, old := range olds {
		old.ack(41)
		r.c.Commit("t", p, old.state().frontier)
	}
	_ = r.c.flush()
	for p := range 2 {
		if rec := zzWP23RecoverNew(t, r.dir(p)); rec.frontier != -1 {
			t.Fatalf("successor partition %d recovers %d, want -1", p, rec.frontier)
		}
	}
	// The successor's own acks persist.
	succ := newZZWP23Shard(-1)
	r.shards.set(0, succ)
	succ.ack(0)
	r.c.Commit("t", 0, 0)
	if err := r.c.Close(); err != nil {
		t.Fatal(err)
	}
	if rec := zzWP23RecoverNew(t, r.dir(0)); rec.frontier != 0 {
		t.Fatalf("successor recovers %d after its first ack, want 0", rec.frontier)
	}
}

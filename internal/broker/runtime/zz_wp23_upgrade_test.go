package runtime

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP23Fixtures are the shapes v3.0.1 leaves a partition's consumer
// state in, laid down with the storage writers v3.0.1 used.
var zzWP23Fixtures = []struct {
	name string
	make func(t testing.TB, dir string)
	// frontier is what v3.0.1 recovers from it.
	frontier int64
}{
	{
		// A move's install: slot 0 only, a 4 KiB file.
		name: "mover-4KiB",
		make: func(t testing.TB, dir string) {
			zzWP23Must(t, storage.WriteConsumerOffset(dir, 30))
			zzWP23Must(t, storage.WriteConsumerAhead(dir, 0, 100, 30, []int64{33, 35}))
		},
		frontier: 30,
	},
	{
		name: "8KiB-slot1-newest",
		make: func(t testing.TB, dir string) {
			zzWP23Must(t, storage.WriteConsumerOffset(dir, 10))
			zzWP23Must(t, storage.WriteConsumerAhead(dir, 0, 100, 20, []int64{22}))
			zzWP23Must(t, storage.WriteConsumerAhead(dir, 1, 200, 40, []int64{42, 44}))
		},
		frontier: 40,
	},
	{
		name: "offset-only",
		make: func(t testing.TB, dir string) {
			zzWP23Must(t, storage.WriteConsumerOffset(dir, 17))
		},
		frontier: 17,
	},
	{
		// A crash between create and first write.
		name: "empty-offset",
		make: func(t testing.TB, dir string) {
			zzWP23Must(t, os.WriteFile(filepath.Join(dir, storage.ConsumerOffsetFileName), nil, 0o600))
		},
		frontier: -1,
	},
	{
		name:     "no-files",
		make:     func(testing.TB, string) {},
		frontier: -1,
	},
}

func zzWP23Must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// U1: a v3.0.1 data directory needs no upgrade step. Before any tick
// the broker recovers what v3.0.1 recovers; the first tick extends
// consumer.ahead to both slots and writes it out once before writing
// it, anchoring on v3.0.1's newest record; and after it both readers
// agree again.
func TestZZWP23UpgradeFromV301(t *testing.T) {
	for _, fx := range zzWP23Fixtures {
		t.Run(fx.name, func(t *testing.T) {
			r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1, fixture: func(dataDir string) {
				fx.make(t, storage.TopicPartitionDir(dataDir, "t", 0))
			}})
			dir := r.dir(0)
			ahead := zzWP23ImageOf(t, filepath.Join(dir, storage.ConsumerAheadFileName))
			offset := zzWP23ImageOf(t, filepath.Join(dir, storage.ConsumerOffsetFileName))
			old := zzWP23RecoverV301(ahead, offset)
			now := zzWP23RecoverNew(t, dir)
			if old.frontier != fx.frontier || now.frontier != fx.frontier {
				t.Fatalf("recovered v3.0.1 %d, broker %d, want %d", old.frontier, now.frontier, fx.frontier)
			}

			// The shard the broker recovers, then one ack.
			sh := zzWP23RecoveredShard(t, dir)
			r.shards.set(0, sh)
			r.crashAt = func(p offsetPoint, _ offsetCommitKey) bool {
				if p == offsetPointWrite {
					r.disk.mu.Lock()
					r.disk.ops = append(r.disk.ops, "write")
					r.disk.mu.Unlock()
				}
				return false
			}
			sh.ack(fx.frontier + 1)
			r.c.Commit("t", 0, sh.state().frontier)
			if err := r.tick(); err != nil {
				t.Fatal(err)
			}
			r.disk.mu.Lock()
			ops := r.disk.ops
			r.disk.mu.Unlock()
			first := slices.Index(ops, "write")
			if first < 0 || slices.Index(ops[first+1:], "write") >= 0 {
				t.Fatalf("first tick ops %v, want one window write", ops)
			}
			before := 0
			for _, op := range ops[:first] {
				if strings.HasPrefix(op, "writeout ") && strings.HasSuffix(op, storage.ConsumerAheadFileName) {
					before++
				}
			}
			if before != 1 {
				t.Fatalf("first tick ops %v: consumer.ahead written out %d times before its first write, want once (the prime)", ops, before)
			}
			info, err := os.Stat(filepath.Join(dir, storage.ConsumerAheadFileName))
			if err != nil || info.Size() != 2*storage.ConsumerAheadSlotSize {
				t.Fatalf("consumer.ahead after the first tick: %v %v, want both slots", info, err)
			}
			rec, _, _ := storage.ReadConsumerAhead(dir)
			if fx.name == "8KiB-slot1-newest" && rec.Slot != 0 {
				t.Fatalf("first write landed in slot %d, over v3.0.1's newest record in slot 1", rec.Slot)
			}
			if fx.name == "mover-4KiB" && rec.Slot != 1 {
				t.Fatalf("first write landed in slot %d, over the mover's record in slot 0", rec.Slot)
			}
			ahead = zzWP23ImageOf(t, filepath.Join(dir, storage.ConsumerAheadFileName))
			offset = zzWP23ImageOf(t, filepath.Join(dir, storage.ConsumerOffsetFileName))
			want := sh.state().frontier
			if got := zzWP23RecoverV301(ahead, offset); got.frontier != want {
				t.Fatalf("v3.0.1 recovers %d after the first tick, want %d", got.frontier, want)
			}
			if got := zzWP23RecoverNew(t, dir); got.frontier != want {
				t.Fatalf("broker recovers %d after the first tick, want %d", got.frontier, want)
			}
			r.checkAll(1)
		})
	}
}

// CP7: a crash while the prime extends or writes out a v3.0.1 file (a
// mover's 4 KiB one among them): every power-loss image recovers what
// v3.0.1 wrote, never less.
func TestZZWP23CrashDuringPrime(t *testing.T) {
	for _, fx := range zzWP23Fixtures {
		for _, point := range []offsetPoint{offsetPointPrimed, offsetPointPrimeOut} {
			for _, darwin := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/point=%d/darwin=%v", fx.name, point, darwin), func(t *testing.T) {
					r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1, darwin: darwin, fixture: func(dataDir string) {
						fx.make(t, storage.TopicPartitionDir(dataDir, "t", 0))
					}})
					r.crashAt = func(p offsetPoint, _ offsetCommitKey) bool { return p == point }
					sh := zzWP23RecoveredShard(t, r.dir(0))
					r.shards.set(0, sh)
					sh.ack(fx.frontier + 1)
					r.c.Commit("t", 0, sh.state().frontier)
					if err := r.tick(); !errors.Is(err, errOffsetCrash) {
						t.Fatalf("tick err %v, want the crash", err)
					}
					r.checkImages(0, sh.state(), fx.frontier)
				})
			}
		}
	}
}

func zzWP23ImageOf(t testing.TB, path string) zzWP23Image {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return zzWP23Image{}
	}
	if err != nil {
		t.Fatal(err)
	}
	return zzWP23Image{data: b, present: true}
}

// The default durability interval and its tick.
func TestZZWP23Cadences(t *testing.T) {
	for _, tc := range []struct {
		interval, d, tick time.Duration
		every             uint64
	}{
		{0, time.Second, 100 * time.Millisecond, 10},
		{time.Second, time.Second, 100 * time.Millisecond, 10},
		{100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond, 1},
		{150 * time.Millisecond, 150 * time.Millisecond, 100 * time.Millisecond, 1},
		{10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond, 1},
		{60 * time.Second, 60 * time.Second, 100 * time.Millisecond, 600},
	} {
		c := newConsumerOffsetCommitter(t.TempDir(), tc.interval, nil, committerOptions{manual: true})
		if c.durable != tc.d || c.tick != tc.tick || c.every != tc.every {
			t.Errorf("interval %v: D %v T %v every %d, want %v %v %d", tc.interval, c.durable, c.tick, c.every, tc.d, tc.tick, tc.every)
		}
	}
}

// zzWP23RecoveredShard is the shard the broker recovers from dir.
func zzWP23RecoveredShard(t testing.TB, dir string) *zzWP23Shard {
	t.Helper()
	rec := zzWP23RecoverNew(t, dir)
	sh := newZZWP23Shard(rec.frontier)
	for _, off := range rec.ahead {
		sh.acked.ahead[off] = true
	}
	return sh
}

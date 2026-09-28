package runtime

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// P1: the platform primitives. They work on an open file (the
// directory sync on a directory) and fail on a closed one; on Linux the writeout is syncfile.SyncData (fdatasync)
// and the device flush a no-op, on macOS the writeout is fsync(2) and
// the flush syncfile.Sync (F_FULLFSYNC, falling back to fsync where
// unsupported), each consulting the syncfile fault hook where it goes
// through syncfile.
func TestZZWP23Primitives(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "f"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := offsetWriteOut(f); err != nil {
		t.Fatalf("writeout: %v", err)
	}
	if err := offsetFlushDevice(f); err != nil {
		t.Fatalf("flush device: %v", err)
	}
	dir, err := os.Open(filepath.Dir(f.Name()))
	if err != nil {
		t.Fatal(err)
	}
	if err := offsetSyncDir(dir); err != nil {
		t.Fatalf("sync directory: %v", err)
	}
	_ = dir.Close()

	restore := syncfile.SetFaultHook(func(syncfile.Op, string) error { return syscall.EIO })
	writeErr, flushErr := offsetWriteOut(f), offsetFlushDevice(f)
	restore()
	switch runtime.GOOS {
	case "darwin":
		if writeErr != nil || !errors.Is(flushErr, syscall.EIO) {
			t.Fatalf("darwin with a failing hook: writeout %v (want nil: plain fsync), flush %v (want EIO: F_FULLFSYNC through syncfile)", writeErr, flushErr)
		}
	default:
		if !errors.Is(writeErr, syscall.EIO) || flushErr != nil {
			t.Fatalf("with a failing hook: writeout %v (want EIO: fdatasync through syncfile), flush %v (want nil: no-op)", writeErr, flushErr)
		}
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := offsetWriteOut(f); err == nil {
		t.Fatal("writeout of a closed file succeeded")
	}
	if runtime.GOOS == "darwin" {
		if err := offsetFlushDevice(f); !errors.Is(err, os.ErrClosed) {
			t.Fatalf("flush device of a closed file: %v, want os.ErrClosed", err)
		}
	}
	if n := offsetFDCap(); n < 1 || n > consumerOffsetMaxFDs {
		t.Fatalf("descriptor cap %d, want 1 to %d", n, consumerOffsetMaxFDs)
	}
}

// zzWP23SyncCounts runs parts partitions, every one acked before every
// tick, for ticks ticks of the rig's clock, and returns the rig after
// checking that no writeout ran between two writes of one tick.
func zzWP23SyncCounts(t *testing.T, o zzWP23RigOpts, ticks int) (*zzWP23Rig, []int) {
	t.Helper()
	r := newZZWP23Rig(t, o)
	r.crashAt = func(p offsetPoint, _ offsetCommitKey) bool {
		if p == offsetPointWrite {
			r.disk.mu.Lock()
			r.disk.ops = append(r.disk.ops, "write")
			r.disk.mu.Unlock()
		}
		return false
	}
	shards := make([]*zzWP23Shard, o.parts)
	for p := range shards {
		shards[p] = newZZWP23Shard(-1)
		r.shards.set(p, shards[p])
	}
	perTick := make([]int, 0, ticks)
	for i := range ticks {
		r.disk.mu.Lock()
		r.disk.ops = append(r.disk.ops, "tick")
		r.disk.mu.Unlock()
		for p, sh := range shards {
			sh.ack(int64(i))
			r.c.Commit("t", p, sh.state().frontier)
		}
		if err := r.tick(); err != nil {
			t.Fatal(err)
		}
		perTick = append(perTick, r.c.lastStats.wroteOut)
		// Every dirty partition is within D (plus the tick) of its
		// last durable point.
		r.c.ioMu.Lock()
		for key, st := range r.c.parts {
			if !st.dirtySince.IsZero() && r.now.Sub(st.dirtySince) > r.c.durable+r.c.tick {
				r.c.ioMu.Unlock()
				t.Fatalf("tick %d: partition %v dirty for %v, past D+T", i, key, r.now.Sub(st.dirtySince))
			}
		}
		r.c.ioMu.Unlock()
	}
	// Within a tick, once a write happened no sync may come before the
	// tick's last write: the write phase is sync-free (the prime's
	// writeouts come before it).
	r.disk.mu.Lock()
	defer r.disk.mu.Unlock()
	wrote, synced := false, false
	for i, op := range r.disk.ops {
		switch {
		case op == "tick":
			wrote, synced = false, false
		case op == "write":
			if synced {
				t.Fatalf("op %d: a write after a sync within one tick: %v", i, r.disk.ops[max(0, i-5):i+1])
			}
			wrote = true
		case strings.HasPrefix(op, "writeout ") || op == "flush":
			synced = wrote
		}
	}
	return r, perTick
}

// P2: sync counts. 64 partitions acked before every tick for 5s of
// ticks. At the default D=1s each is written out about once a second
// (64/s), the device is flushed at most once a tick (10/s), and the
// writeouts are spread over the ticks rather than bunched on one. At
// D=100ms, today's window, every partition is written out every tick.
func TestZZWP23SyncCounts(t *testing.T) {
	const parts, ticks = 64, 50
	for _, darwin := range []bool{false, true} {
		r, perTick := zzWP23SyncCounts(t, zzWP23RigOpts{parts: parts, darwin: darwin}, ticks)
		// The first tick primes every partition (one more flush, for the
		// primes); count from the second.
		windows, flushes := 0, r.disk.flushes-zzWP23FirstTicksFlushes(r, 1)
		for _, n := range perTick[1:] {
			windows += n
		}
		seconds := float64(ticks-1) * r.c.tick.Seconds()
		if rate := float64(windows) / seconds; rate < 64*0.85 || rate > 64*1.15 {
			t.Errorf("darwin=%v D=1s: %.1f window writeouts/s, want 64 (±15%%)", darwin, rate)
		}
		if rate := float64(flushes) / seconds; rate > 10 {
			t.Errorf("darwin=%v D=1s: %.1f device flushes/s, want at most 10", darwin, rate)
		}
		if peak := slices.Max(perTick[1:]); peak > parts/3 {
			t.Errorf("darwin=%v D=1s: %d partitions written out on one tick, want them spread over the interval (per tick %v)", darwin, peak, perTick)
		}

		r, perTick = zzWP23SyncCounts(t, zzWP23RigOpts{parts: parts, darwin: darwin, interval: 100 * time.Millisecond}, 20)
		for i, n := range perTick {
			if n != parts {
				t.Fatalf("darwin=%v D=100ms: tick %d wrote out %d partitions, want all %d", darwin, i, n, parts)
			}
		}
		if r.disk.flushes > 20+1 {
			t.Errorf("darwin=%v D=100ms: %d device flushes over 20 ticks, want at most one a tick", darwin, r.disk.flushes)
		}
	}

	// The burst hook, for A/B runs: every partition on one tick.
	_, perTick := zzWP23SyncCounts(t, zzWP23RigOpts{parts: parts, burst: true}, 30)
	bursts := 0
	for _, n := range perTick[1:] {
		if n == parts {
			bursts++
		} else if n != 0 {
			t.Fatalf("burst: a tick wrote out %d of %d partitions (per tick %v)", n, parts, perTick)
		}
	}
	if bursts < 2 {
		t.Fatalf("burst: %d full writeouts over 30 ticks, want one an interval", bursts)
	}
}

// P3: with the descriptor cap at 4 and 10 partitions acked every tick,
// the committer never holds more than 4 descriptors and every frontier
// still reaches disk.
func TestZZWP23DescriptorCap(t *testing.T) {
	const parts = 10
	r := newZZWP23Rig(t, zzWP23RigOpts{parts: parts, maxFDs: 4})
	shards := make([]*zzWP23Shard, parts)
	for p := range shards {
		shards[p] = newZZWP23Shard(-1)
		r.shards.set(p, shards[p])
	}
	for i := range 40 {
		for p, sh := range shards {
			if (i+p)%3 != 0 {
				sh.ack(int64(i))
				r.c.Commit("t", p, sh.state().frontier)
			}
		}
		if err := r.tick(); err != nil {
			t.Fatal(err)
		}
		r.c.ioMu.Lock()
		held := 0
		for _, st := range r.c.parts {
			if st.f != nil {
				held++
			}
		}
		counted := r.c.held
		r.c.ioMu.Unlock()
		if held > 4 || held != counted {
			t.Fatalf("tick %d: %d descriptors held (counted %d), cap 4", i, held, counted)
		}
	}
	if err := r.c.Close(); err != nil {
		t.Fatal(err)
	}
	for p, sh := range shards {
		if got := zzWP23RecoverNew(t, r.dir(p)); got.frontier != sh.state().frontier {
			t.Fatalf("partition %d recovers %d after Close, want %d", p, got.frontier, sh.state().frontier)
		}
		want := sh.state().frontier
		if got, ok, _ := storage.ReadConsumerOffset(r.dir(p)); ok != (want >= 0) || (ok && got != want) {
			t.Fatalf("partition %d consumer.offset %d (ok %v) after Close, want %d", p, got, ok, want)
		}
	}
}

// zzWP23FirstTicksFlushes counts the device flushes of the first n ticks.
func zzWP23FirstTicksFlushes(r *zzWP23Rig, n int) int {
	r.disk.mu.Lock()
	defer r.disk.mu.Unlock()
	ticks, flushes := 0, 0
	for _, op := range r.disk.ops {
		if op == "tick" {
			if ticks++; ticks > n {
				break
			}
		}
		if op == "flush" {
			flushes++
		}
	}
	return flushes
}

// consumer.offset is levelled with a writeout at most every 30s while
// a partition is acked, and once more on its phase after it goes
// quiet, so it trails consumer.ahead by at most about 30s plus D.
func TestZZWP23LevelCadence(t *testing.T) {
	r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1})
	sh := newZZWP23Shard(-1)
	r.shards.set(0, sh)
	levels := func() int {
		r.disk.mu.Lock()
		defer r.disk.mu.Unlock()
		n := 0
		for _, op := range r.disk.ops {
			if strings.HasPrefix(op, "writeout ") && strings.HasSuffix(op, storage.ConsumerOffsetFileName) {
				n++
			}
		}
		return n
	}
	// 45s of acks on every tick.
	for i := range 450 {
		sh.ack(int64(i))
		r.c.Commit("t", 0, sh.state().frontier)
		if err := r.tick(); err != nil {
			t.Fatal(err)
		}
	}
	if n := levels(); n < 2 || n > 3 {
		t.Fatalf("%d consumer.offset levels over 45s of acks, want one at the first writeout and one about 30s later", n)
	}
	// Quiet: within 30s plus D it catches up with the frontier.
	for range 310 {
		if err := r.tick(); err != nil {
			t.Fatal(err)
		}
	}
	if got, _, _ := storage.ReadConsumerOffset(r.dir(0)); got != sh.state().frontier {
		t.Fatalf("consumer.offset %d 31s after the last ack, want the frontier %d", got, sh.state().frontier)
	}
}

// Partitions whose writes wait more than twice D for their writeout
// are logged, at most once a minute: a power loss would then redeliver
// more than the setting says.
func TestZZWP23WarnsWhenWritesWaitPastTheInterval(t *testing.T) {
	// 8 partitions whose writeouts take 30ms each on a 20ms interval.
	sink := zzWP16LoggedLoop(t, 8, 20*time.Millisecond, 30*time.Millisecond, time.Second)
	late := sink.records(t, "WARN", "consumer offsets wait longer than their durability interval")
	if len(late) != 1 {
		t.Fatalf("%d durable-age warnings, want exactly 1 (once a minute)", len(late))
	}
	if late[0]["durability_interval"] != float64(20*time.Millisecond) {
		t.Errorf("warning attributes %v, want durability_interval=20ms", late[0])
	}
}

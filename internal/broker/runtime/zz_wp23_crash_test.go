package runtime

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// zzWP23Workload drives a rig for ticks ticks: before each, every
// partition acks a few offsets out of order just above its frontier
// and commits. It stops at the first tick that hits the rig's crash
// point and reports whether one did.
func zzWP23Workload(r *zzWP23Rig, parts, ticks int, seed uint64) bool {
	rng := rand.New(rand.NewPCG(seed, 23))
	shards := make([]*zzWP23Shard, parts)
	for p := range parts {
		shards[p] = newZZWP23Shard(-1)
		r.shards.set(p, shards[p])
	}
	for range ticks {
		for p, sh := range shards {
			base := sh.state().frontier
			for range 3 {
				sh.ack(base + 1 + rng.Int64N(6))
			}
			r.c.Commit("t", p, sh.state().frontier)
		}
		if err := r.tick(); errors.Is(err, errOffsetCrash) {
			return true
		}
	}
	return false
}

// checkAnchors pins the rule the two slots rest on: while a slot is a
// partition's anchor its bytes on stable storage are the bytes the
// file holds, so no write ever touched it.
func (r *zzWP23Rig) checkAnchors() {
	r.t.Helper()
	r.c.ioMu.Lock()
	defer r.c.ioMu.Unlock()
	for key, st := range r.c.parts {
		if st.anchor < 0 || st.syncing || st.dead {
			continue
		}
		path := filepath.Join(st.dir, storage.ConsumerAheadFileName)
		r.disk.mu.Lock()
		durable, ok := r.disk.durable[path]
		r.disk.mu.Unlock()
		if !ok || r.disk.lie {
			continue
		}
		cur, err := os.ReadFile(path)
		if err != nil {
			r.t.Fatal(err)
		}
		lo, hi := st.anchor*storage.ConsumerAheadSlotSize, (st.anchor+1)*storage.ConsumerAheadSlotSize
		if !bytes.Equal(zzWP23Range(durable, lo, hi), zzWP23Range(cur, lo, hi)) {
			r.t.Fatalf("partition %v: anchor slot %d was written while it was the anchor", key, st.anchor)
		}
	}
}

// checkAll checks every partition's power-loss images against its
// shard's acked state and the committer's last anchor.
func (r *zzWP23Rig) checkAll(parts int) int {
	r.t.Helper()
	r.checkAnchors()
	n := 0
	for p := range parts {
		r.shards.mu.Lock()
		sh := r.shards.shards[p]
		r.shards.mu.Unlock()
		n += r.checkImages(p, sh.state(), r.durableFrontier(p))
	}
	return n
}

// TestZZWP23CrashPoints is CP1-CP5, CP7, CP9 and CP12: a crash (process
// death, then power loss) at every step of a tick and of Close, on both
// disk models and at the default and the 100ms durability interval.
// Every power-loss image of every partition must recover only acked
// offsets, with v3.0.1's reader as well as the broker's, and never
// below the last anchor the committer made durable.
func TestZZWP23CrashPoints(t *testing.T) {
	const parts, ticks = 3, 14
	points := []offsetPoint{
		offsetPointPrimed, offsetPointPrimeOut, offsetPointWrite, offsetPointWritten,
		offsetPointLevel, offsetPointWrittenOut, offsetPointFlushed,
	}
	for _, darwin := range []bool{false, true} {
		for _, interval := range []time.Duration{time.Second, 100 * time.Millisecond} {
			for _, point := range points {
				name := fmt.Sprintf("darwin=%v/D=%v/point=%d", darwin, interval, point)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					crashes, images := 0, 0
					// Every occurrence of the point in the same workload.
					for nth := 1; ; nth++ {
						r := newZZWP23Rig(t, zzWP23RigOpts{parts: parts, interval: interval, darwin: darwin})
						seen := 0
						r.crashAt = func(p offsetPoint, _ offsetCommitKey) bool {
							if p != point {
								return false
							}
							seen++
							return seen == nth
						}
						if !zzWP23Workload(r, parts, ticks, 1) {
							// Past the last occurrence during the run: crash
							// inside Close instead (CP9), then stop.
							if err := r.c.Close(); !errors.Is(err, errOffsetCrash) {
								r.abandon()
								break
							}
						}
						crashes++
						images += r.checkAll(parts)
						r.abandon()
					}
					if crashes == 0 {
						t.Fatalf("point %d never reached", point)
					}
					t.Logf("%d crashes, %d images", crashes, images)
				})
			}
		}
	}
}

// TestZZWP23CrashRestartPowerLoss is CP6: a process crash leaves a
// window in the page cache that no writeout covered; the next process
// anchors on it, so it must write the file out before any write to the
// other slot, the only durable record. A power loss before the new
// process's first writeout then recovers at least that window.
func TestZZWP23CrashRestartPowerLoss(t *testing.T) {
	for _, darwin := range []bool{false, true} {
		t.Run(fmt.Sprintf("darwin=%v", darwin), func(t *testing.T) {
			r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1, darwin: darwin})
			sh := newZZWP23Shard(-1)
			r.shards.set(0, sh)
			for i := range 12 {
				sh.ack(int64(i))
				r.c.Commit("t", 0, sh.state().frontier)
				if err := r.tick(); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.c.flush(); err != nil {
				t.Fatal(err)
			}
			// Ack until a tick writes a window that is not its partition's
			// writeout tick: written into the page cache, never written out.
			for next := int64(12); !r.dirty(0); next++ {
				sh.ack(next)
				r.c.Commit("t", 0, sh.state().frontier)
				if err := r.tick(); err != nil {
					t.Fatal(err)
				}
			}
			windowed := sh.state()
			r.abandon() // process crash: the page cache keeps the window

			// A new process over the same disk.
			var order []string
			crashed := false
			c2 := newConsumerOffsetCommitter(r.dataDir, time.Second, nil, committerOptions{
				io: offsetIO{
					writeOut: func(f *os.File) error {
						order = append(order, "writeout")
						return r.disk.writeOut(f)
					},
					flushDevice: r.disk.flushDevice,
					syncDir:     r.disk.syncDir,
				},
				manual: true,
				crash: func(p offsetPoint, _ offsetCommitKey) bool {
					if p == offsetPointWrite {
						order = append(order, "write")
						crashed = true
						return true
					}
					return false
				},
			})
			c2.SetAheadSource(r.shards.source)
			sh.ack(13)
			c2.Commit("t", 0, sh.state().frontier)
			if err := c2.tickAt(r.now.Add(time.Second), offsetTickNormal); !errors.Is(err, errOffsetCrash) || !crashed {
				t.Fatalf("tick err %v, want the crash at the first write", err)
			}
			if len(order) < 2 || order[0] != "writeout" || order[len(order)-1] != "write" {
				t.Fatalf("new process order %v, want the prime's writeout before its first write", order)
			}
			r.c = c2
			// Power loss now: the floor is the window the prime anchored on.
			r.checkImages(0, sh.state(), windowed.frontier)
			r.abandon()
		})
	}
}

// TestZZWP23SyncErrorsNeverTouchTheAnchor is CP8: a writeout that
// fails, then a device flush that fails, leave the anchor alone (fsync
// errors mean the page cache is not to be trusted, not that the old
// record is gone); the next tick rewrites the window in full even with
// nothing new acked, and a power loss at any point recovers at least
// the anchor.
func TestZZWP23SyncErrorsNeverTouchTheAnchor(t *testing.T) {
	for _, darwin := range []bool{false, true} {
		t.Run(fmt.Sprintf("darwin=%v", darwin), func(t *testing.T) {
			r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1, darwin: darwin})
			sh := newZZWP23Shard(-1)
			r.shards.set(0, sh)
			ackTick := func(off int64) error {
				sh.ack(off)
				r.c.Commit("t", 0, sh.state().frontier)
				return r.c.flush()
			}
			for i := range 4 {
				if err := ackTick(int64(i)); err != nil {
					t.Fatal(err)
				}
			}
			anchor := r.durableFrontier(0)
			path := filepath.Join(r.dir(0), storage.ConsumerAheadFileName)
			writes := zzWP23CountWrites(t, path)

			r.disk.failWriteOut = func(p string) error {
				if p == path {
					return fmt.Errorf("injected: %w", os.ErrInvalid)
				}
				return nil
			}
			if err := ackTick(4); err == nil {
				t.Fatal("flush with a failing writeout reported no error")
			}
			r.checkAnchors()
			r.checkImages(0, sh.state(), anchor)
			if got := r.durableFrontier(0); got != anchor {
				t.Fatalf("anchor moved to %d after a failed writeout, want %d", got, anchor)
			}

			// The next tick rewrites the window although nothing changed.
			r.disk.failWriteOut = nil
			r.disk.failFlush = func() error { return fmt.Errorf("injected flush: %w", os.ErrInvalid) }
			before := writes()
			if err := r.c.flush(); err == nil && darwin {
				t.Fatal("flush with a failing device flush reported no error")
			}
			if writes() == before {
				t.Fatal("the tick after a failed writeout did not rewrite the window")
			}
			r.checkAnchors()
			r.checkImages(0, sh.state(), anchor)

			r.disk.failFlush = nil
			if err := r.c.flush(); err != nil {
				t.Fatal(err)
			}
			if got := r.durableFrontier(0); got != sh.state().frontier {
				t.Fatalf("anchor %d after recovery from the errors, want %d", got, sh.state().frontier)
			}
			r.checkAll(1)
		})
	}
}

// TestZZWP23LyingDisk is CP14: a device that acknowledges every sync
// and keeps nothing. Nothing can hold a floor then, but every image
// must still recover only acked offsets.
func TestZZWP23LyingDisk(t *testing.T) {
	r := newZZWP23Rig(t, zzWP23RigOpts{parts: 3, darwin: true})
	r.disk.lie = true
	zzWP23Workload(r, 3, 20, 7)
	if n := r.checkAll(3); n == 0 {
		t.Fatal("no images checked")
	}
}

// zzWP23CountWrites counts the writes to path through the syncfile
// seam. The hook is process-wide: callers must not run in parallel.
func zzWP23CountWrites(t *testing.T, path string) func() int {
	t.Helper()
	var n atomic.Int64
	restore := syncfile.SetFaultHook(func(op syncfile.Op, p string) error {
		if op == syncfile.OpWrite && p == path {
			n.Add(1)
		}
		return nil
	})
	t.Cleanup(restore)
	return func() int { return int(n.Load()) }
}

// A failed prime writeout leaves the anchor's pages untrusted (a
// kernel may mark them clean without writing them back): the next
// prime writes the anchor's bytes back, unchanged, before it writes
// the file out again, and only then does the tick write the window.
func TestZZWP23FailedPrimeRewritesTheAnchor(t *testing.T) {
	fx := zzWP23Fixtures[1] // 8 KiB, slot 1 newest
	r := newZZWP23Rig(t, zzWP23RigOpts{parts: 1, fixture: func(dataDir string) {
		fx.make(t, storage.TopicPartitionDir(dataDir, "t", 0))
	}})
	path := filepath.Join(r.dir(0), storage.ConsumerAheadFileName)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	restore := syncfile.SetFaultHook(func(op syncfile.Op, p string) error {
		if op == syncfile.OpWrite && p == path {
			r.disk.mu.Lock()
			r.disk.ops = append(r.disk.ops, "write")
			r.disk.mu.Unlock()
		}
		return nil
	})
	defer restore()
	sh := zzWP23RecoveredShard(t, r.dir(0))
	r.shards.set(0, sh)
	sh.ack(fx.frontier + 1)
	r.c.Commit("t", 0, sh.state().frontier)
	r.disk.failWriteOut = func(string) error { return fmt.Errorf("injected: %w", os.ErrInvalid) }
	if err := r.tick(); err == nil {
		t.Fatal("tick with a failing prime writeout reported no error")
	}
	r.disk.failWriteOut = nil
	r.disk.mu.Lock()
	r.disk.ops = nil
	r.disk.mu.Unlock()
	if err := r.tick(); err != nil {
		t.Fatal(err)
	}
	r.disk.mu.Lock()
	ops := slices.Clone(r.disk.ops)
	r.disk.mu.Unlock()
	if len(ops) < 4 || ops[0] != "write" || ops[1] != "writeout "+path || ops[2] != "flush" || ops[3] != "write" {
		t.Fatalf("re-prime ops %v, want the anchor written back, the file written out and flushed, then the window", ops)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before[storage.ConsumerAheadSlotSize:], after[storage.ConsumerAheadSlotSize:]) {
		t.Fatal("the anchor slot's bytes changed")
	}
	r.checkAll(1)
}

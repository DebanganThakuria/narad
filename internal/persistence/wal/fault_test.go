package wal

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile/faulttest"
)

// Disk-fault tests for the ingress WAL, driven through the syncfile
// fault-injection seam. The contract under test is the one the 202
// promises: a record whose Append returned nil is replayed after any
// restart, a record whose Append failed may or may not be (the caller
// retries), and nothing that was never appended is ever replayed.
// After a write or fsync failure the log refuses every later append
// (the failure is latched) rather than acking bytes stacked on top of
// a region of unknown durability, and a reopen recovers cleanly.

// faultLoadResult is what a concurrent append load observed.
type faultLoadResult struct {
	acked     map[string]RecordID // Append returned nil
	failed    map[string]error    // Append returned an error
	submitted map[string]bool     // every payload handed to Append
}

// runAppendLoad runs workers goroutines that each append perRound
// records, stopping a worker at its first error (a latched log fails
// everything after it anyway).
func runAppendLoad(t *testing.T, l *Log, workers, perWorker int) faultLoadResult {
	t.Helper()
	res := faultLoadResult{
		acked:     make(map[string]RecordID),
		failed:    make(map[string]error),
		submitted: make(map[string]bool),
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				payload := fmt.Sprintf("w%02d-i%04d-%s", w, i, strings.Repeat("x", 40+(i%50)))
				mu.Lock()
				res.submitted[payload] = true
				mu.Unlock()
				id, err := l.Append(context.Background(), []byte(payload))
				mu.Lock()
				if err != nil {
					res.failed[payload] = err
				} else {
					res.acked[payload] = id
				}
				mu.Unlock()
				if err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
	return res
}

// replayAll reads every record in dir from disk, in order, and checks
// the seqs are strictly increasing; with dense set it also requires
// them to be consecutive (an honest disk never leaves a gap).
func replayAll(t *testing.T, dir string, dense bool) []Record {
	t.Helper()
	var out []Record
	err := Replay(dir, 0, 0, func(r Record) error {
		out = append(out, r)
		return nil
	})
	if err != nil {
		t.Fatalf("replay %s: %v", dir, err)
	}
	for i := 1; i < len(out); i++ {
		prev, cur := out[i-1].ID.Seq, out[i].ID.Seq
		if cur <= prev {
			t.Fatalf("replay seqs not increasing: %d then %d", prev, cur)
		}
		if dense && cur != prev+1 {
			t.Fatalf("replay seqs not dense: %d then %d", prev, cur)
		}
	}
	return out
}

// assertReplayContract checks the acked-implies-replayed and
// replayed-implies-submitted halves of the contract.
func assertReplayContract(t *testing.T, records []Record, res faultLoadResult) {
	t.Helper()
	seen := make(map[string]int, len(records))
	for _, r := range records {
		p := string(r.Payload)
		if !res.submitted[p] {
			t.Fatalf("replayed a record that was never appended: %q", p)
		}
		seen[p]++
		if seen[p] > 1 {
			t.Fatalf("record replayed twice: %q", p)
		}
	}
	for p := range res.acked {
		if seen[p] == 0 {
			t.Fatalf("acked record missing after recovery: %q", p)
		}
	}
}

func faultOptions() Options {
	return Options{SegmentBytes: 48 << 10, MaxRecord: 1 << 20}
}

func reopenWAL(t *testing.T, dir string) *Log {
	t.Helper()
	l, err := Open(dir, faultOptions())
	if err != nil {
		t.Fatalf("reopen %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// Nth fdatasync on a WAL segment fails with EIO under concurrent
// appends: every append in that batch (and every later one) fails, the
// acked prefix survives a reopen, nothing unappended appears.
func TestFaultWALSyncFailsUnderLoad(t *testing.T) {
	dir := t.TempDir()
	inj := faulttest.New(t)
	rule := inj.FailNth(syncfile.OpSyncData, segmentSuffix, 6, syscall.EIO)

	l, err := Open(dir, faultOptions())
	if err != nil {
		t.Fatal(err)
	}
	res := runAppendLoad(t, l, 12, 200)
	if rule.Fired() != 1 {
		t.Fatalf("sync fault fired %d times, want 1", rule.Fired())
	}
	if len(res.failed) == 0 {
		t.Fatal("no append observed the sync failure")
	}
	for p, err := range res.failed {
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("append %q failed with %v, want the EIO to reach the caller", p, err)
		}
	}
	// Latched: the disk is "fine" again but the log must still refuse.
	if _, err := l.Append(context.Background(), []byte("after-fault")); !errors.Is(err, syscall.EIO) {
		t.Fatalf("append after a latched sync failure: %v, want the latched EIO", err)
	}
	if err := l.Close(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Close must surface the latched error, got %v", err)
	}

	records := replayAll(t, dir, true)
	assertReplayContract(t, records, res)
	t.Logf("acked=%d failed=%d replayed=%d", len(res.acked), len(res.failed), len(records))

	// A reopen recovers cleanly and accepts appends again.
	l2 := reopenWAL(t, dir)
	if _, err := l2.Append(context.Background(), []byte("post-reopen")); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if err := l2.Close(); err != nil {
		t.Fatalf("Close after reopen: %v", err)
	}
	after := replayAll(t, dir, true)
	if len(after) != len(records)+1 || string(after[len(after)-1].Payload) != "post-reopen" {
		t.Fatalf("replay after reopen: %d records, want %d ending in post-reopen", len(after), len(records)+1)
	}
}

// The write itself fails mid-batch (ENOSPC): same contract as an fsync
// failure, and the partial frame that a short write may have left is
// never replayed.
func TestFaultWALWriteFailsMidBatch(t *testing.T) {
	dir := t.TempDir()
	inj := faulttest.New(t)
	rule := inj.FailNth(syncfile.OpWrite, segmentSuffix, 9, syscall.ENOSPC)

	l, err := Open(dir, faultOptions())
	if err != nil {
		t.Fatal(err)
	}
	res := runAppendLoad(t, l, 12, 200)
	if rule.Fired() != 1 {
		t.Fatalf("write fault fired %d times, want 1", rule.Fired())
	}
	if len(res.failed) == 0 {
		t.Fatal("no append observed the write failure")
	}
	for p, err := range res.failed {
		if !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("append %q failed with %v, want ENOSPC", p, err)
		}
	}
	if _, err := l.Append(context.Background(), []byte("after-fault")); err == nil {
		t.Fatal("append after a write failure succeeded; the failure must latch")
	}
	_ = l.Close()

	records := replayAll(t, dir, true)
	assertReplayContract(t, records, res)

	l2 := reopenWAL(t, dir)
	if _, err := l2.Append(context.Background(), []byte("post-reopen")); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	res.submitted["post-reopen"] = true
	res.acked["post-reopen"] = RecordID{}
	assertReplayContract(t, replayAll(t, dir, true), res)
}

// A segment roll fails: creating the next segment (ENOSPC) or fsyncing
// the directory that holds it (EIO). The append that needed the roll
// fails, nothing acked before it is lost, and a reopen carries on from
// the sealed segments.
func TestFaultWALRollFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		op   syncfile.Op
		err  error
	}{
		{"create-enospc", syncfile.OpOpen, syscall.ENOSPC},
		{"dirsync-eio", syncfile.OpSync, syscall.EIO},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			inj := faulttest.New(t)
			l, err := Open(dir, faultOptions())
			if err != nil {
				t.Fatal(err)
			}
			// Installed after Open so the first segment's creation and
			// directory sync are not the calls that fail.
			rule := inj.FailNth(tc.op, dir, 1, tc.err)

			res := runAppendLoad(t, l, 8, 300)
			if rule.Fired() != 1 {
				t.Fatalf("roll fault fired %d times, want 1", rule.Fired())
			}
			if len(res.failed) == 0 {
				t.Fatal("no append observed the roll failure")
			}
			// The disk is fine again: the next roll must succeed without
			// a restart. Before the fix rollLocked closed the active
			// file before creating its successor, so a failed create
			// left a closed descriptor as the active file and every
			// later append failed (and latched the log) until a reopen.
			if _, err := l.Append(context.Background(), []byte("after-roll-fault")); err != nil {
				t.Fatalf("append after a transient roll failure: %v (the log needed a restart)", err)
			}
			res.submitted["after-roll-fault"] = true
			res.acked["after-roll-fault"] = RecordID{}
			if err := l.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			records := replayAll(t, dir, true)
			assertReplayContract(t, records, res)

			l2 := reopenWAL(t, dir)
			for i := range 200 {
				if _, err := l2.Append(context.Background(), fmt.Appendf(nil, "post-reopen-%03d-%s", i, strings.Repeat("y", 200))); err != nil {
					t.Fatalf("append %d after reopen: %v", i, err)
				}
			}
			if err := l2.Close(); err != nil {
				t.Fatal(err)
			}
			after := replayAll(t, dir, true)
			if len(after) != len(records)+200 {
				t.Fatalf("replay after reopen: %d records, want %d", len(after), len(records)+200)
			}
		})
	}
}

// walSegmentPath is the on-disk file of a RecordID's segment.
func walSegmentPath(dir string, id RecordID) string {
	return segmentPath(dir, id.SegmentBase)
}

// The disk lies: fsync returns success without writing, in windows of
// two honest then five lying calls. The load is then "crashed" by truncating every segment
// back to its last honest length, and recovery must replay exactly the
// honest prefix: every acked record whose frame lies below its
// segment's honest length, in dense seq order, and nothing else.
//
// What is lost here is every record acked on the strength of a lying
// fsync. That is a hardware (or virtualisation) fault the software
// cannot detect; the WAL's job is to lose nothing more and to never
// replay a torn or fabricated record.
func TestFaultWALLyingFsyncCrash(t *testing.T) {
	dir := t.TempDir()
	inj := faulttest.New(t)
	l, err := Open(dir, faultOptions())
	if err != nil {
		t.Fatal(err)
	}
	inj.LieSyncs(dir, 2, 5, true)

	res := runAppendLoad(t, l, 8, 250)
	if len(res.failed) != 0 {
		t.Fatalf("appends failed under a lying disk: %v", res.failed)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if inj.Lies() == 0 || inj.HonestSyncs() == 0 {
		t.Fatalf("lies=%d honest=%d: the run needs both", inj.Lies(), inj.HonestSyncs())
	}

	// The oracle: an acked record survived iff its whole frame is below
	// the honest length of its segment and the segment's name survived.
	durable := make(map[string]RecordID)
	for p, id := range res.acked {
		snap, ok := inj.Honest(walSegmentPath(dir, id))
		if ok && id.Offset+frameHeaderSize+int64(len(p)) <= snap.Len {
			durable[p] = id
		}
	}
	if err := inj.Crash(dir); err != nil {
		t.Fatalf("crash: %v", err)
	}

	// Seqs need not be dense any more: a sealed segment whose final
	// sync lied loses its tail while the segment rolled after it kept
	// its (honestly synced) records, so the WAL now has a hole in the
	// middle. The records in the hole are the lying disk's loss; the
	// dispatcher must step over the hole rather than wait forever for
	// seqs that no longer exist (see TestProduceDispatcherSkipsWALGap).
	pristine := copyDir(t, dir)
	records := replayAll(t, dir, false)
	assertReplayContract(t, records, faultLoadResult{acked: durable, submitted: res.submitted})
	if len(records) != len(durable) {
		t.Fatalf("replayed %d records, want exactly the %d durable ones", len(records), len(durable))
	}
	t.Logf("acked=%d durable=%d lost-to-lying-fsync=%d lies=%d honest=%d",
		len(res.acked), len(durable), len(res.acked)-len(durable), inj.Lies(), inj.HonestSyncs())

	// A reopen truncates nothing extra and keeps accepting appends.
	l2 := reopenWAL(t, dir)
	if _, err := l2.Append(context.Background(), []byte("post-crash")); err != nil {
		t.Fatalf("append after crash recovery: %v", err)
	}
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}
	after := replayAll(t, dir, false)
	if len(after) != len(durable)+1 {
		t.Fatalf("replay after reopen: %d, want %d", len(after), len(durable)+1)
	}

	// Torn tails on top of the crash: the active segment's tail is cut
	// inside its last record, extended with garbage, or zero-filled.
	// Recovery must never panic, must replay a dense prefix of the
	// durable records, and must never hand back a torn or fabricated
	// record. Then the log must accept appends and survive another
	// reopen.
	rng := rand.New(rand.NewPCG(7, 11))
	for _, tc := range []struct {
		name string
		hurt func(path string) error
	}{
		{"truncate-mid-record", func(p string) error { return faulttest.TruncateTail(p, int64(1+rng.IntN(60))) }},
		{"append-garbage", func(p string) error { return faulttest.AppendGarbage(p, 300, rng.Uint64()) }},
		{"zero-fill-tail", func(p string) error { return faulttest.ZeroTail(p, int64(1+rng.IntN(60))) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			work := copyDir(t, pristine)
			segs, err := listSegments(work)
			if err != nil || len(segs) == 0 {
				t.Fatalf("list segments: %v", err)
			}
			if err := tc.hurt(segs[len(segs)-1].path); err != nil {
				t.Fatal(err)
			}
			l3 := reopenWAL(t, work)
			got := replayAll(t, work, false)
			assertReplayContract(t, got, faultLoadResult{acked: map[string]RecordID{}, submitted: res.submitted})
			// A dense prefix of the durable set, in seq order.
			bySeq := make([]RecordID, 0, len(durable))
			for _, id := range durable {
				bySeq = append(bySeq, id)
			}
			sort.Slice(bySeq, func(i, j int) bool { return bySeq[i].Seq < bySeq[j].Seq })
			if len(got) > len(bySeq) || len(got) < len(bySeq)-1 {
				t.Fatalf("%s: replayed %d records, want %d or %d (at most the last record lost)", tc.name, len(got), len(bySeq)-1, len(bySeq))
			}
			for i, r := range got {
				if r.ID.Seq != bySeq[i].Seq {
					t.Fatalf("%s: record %d has seq %d, want %d", tc.name, i, r.ID.Seq, bySeq[i].Seq)
				}
			}
			if _, err := l3.Append(context.Background(), []byte("after-torn-tail")); err != nil {
				t.Fatalf("%s: append after torn-tail recovery: %v", tc.name, err)
			}
			if err := l3.Close(); err != nil {
				t.Fatal(err)
			}
			again := replayAll(t, work, false)
			if len(again) != len(got)+1 {
				t.Fatalf("%s: second recovery replayed %d, want %d", tc.name, len(again), len(got)+1)
			}
			if got := string(again[len(again)-1].Payload); got != "after-torn-tail" {
				t.Fatalf("%s: last record %q", tc.name, got)
			}
		})
	}
}

// copyDir copies the regular files of src into a fresh temp dir.
func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

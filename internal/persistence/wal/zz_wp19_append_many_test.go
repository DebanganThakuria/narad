package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile/faulttest"
)

// AppendManyWith stages a batch of records under one hold of the append
// lock so the batch shares one group commit. These tests hold it to the
// contract single appends already keep: the bytes on disk are exactly
// those of N single appends (so replay and recovery cannot tell them
// apart), a batch never spans a segment inside a frame, a crash keeps a
// prefix of it and never a gap, and an error acks nothing.

// zzWP19Fill appends record first+i's payload (zzWP5Payload).
func zzWP19Fill(first int) func(i int, dst []byte) []byte {
	return func(i int, dst []byte) []byte { return append(dst, zzWP5Payload(first+i)...) }
}

// zzWP19Sizes is the payload sizes of records first..first+count-1.
func zzWP19Sizes(first, count int) []int {
	sizes := make([]int, count)
	for i := range sizes {
		sizes[i] = len(zzWP5Payload(first + i))
	}
	return sizes
}

// zzWP19AppendBatch appends records first..first+count-1 as one batch.
func zzWP19AppendBatch(t *testing.T, l *Log, first, count int) []RecordID {
	t.Helper()
	ids, err := l.AppendManyWith(context.Background(), zzWP19Sizes(first, count), zzWP19Fill(first))
	if err != nil {
		t.Fatalf("AppendManyWith(%d records from %d): %v", count, first, err)
	}
	if len(ids) != count {
		t.Fatalf("AppendManyWith returned %d ids, want %d", len(ids), count)
	}
	return ids
}

// zzWP19Ops counts file operations on the segments of one directory.
type zzWP19Ops struct {
	syncs, writes atomic.Int64
}

// zzWP19CountOps counts the data syncs and writes of dir's segments
// until the returned restore is called.
func zzWP19CountOps(dir string) (*zzWP19Ops, func()) {
	ops := &zzWP19Ops{}
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if filepath.Dir(path) != dir || filepath.Ext(path) != segmentSuffix {
			return nil
		}
		switch op {
		case syncfile.OpSyncData:
			ops.syncs.Add(1)
		case syncfile.OpWrite:
			ops.writes.Add(1)
		}
		return nil
	})
	return ops, restore
}

// A log written in batches is byte for byte the log the same records
// make appended one at a time, rolls included, and every batch record
// gets the ID its single append got.
func TestZZWP19AppendManyWithMatchesSingleAppends(t *testing.T) {
	opts := Options{SegmentBytes: 4 << 10, MaxRecord: 4 << 10, Prealloc: PreallocOff}
	singleDir, batchDir := t.TempDir(), t.TempDir()
	single, err := Open(singleDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	batched, err := Open(batchDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 150
	var want []RecordID
	for i := range n {
		id, err := single.Append(context.Background(), zzWP5Payload(i))
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, id)
	}
	var got []RecordID
	sizes := []int{1, 7, 30, 3, 50, 2, 19}
	for next, k := 0, 0; next < n; k++ {
		count := min(sizes[k%len(sizes)], n-next)
		got = append(got, zzWP19AppendBatch(t, batched, next, count)...)
		next += count
	}
	if err := single.Close(); err != nil {
		t.Fatal(err)
	}
	if err := batched.Close(); err != nil {
		t.Fatal(err)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record %d: batch id %+v, single append id %+v", i, got[i], want[i])
		}
	}
	singleFiles, batchFiles := zzWP5ReadDir(t, singleDir), zzWP5ReadDir(t, batchDir)
	if len(singleFiles) < 5 || len(singleFiles) != len(batchFiles) {
		t.Fatalf("%d segments appended singly, %d in batches; want the same, at least 5", len(singleFiles), len(batchFiles))
	}
	for name, data := range singleFiles {
		if !bytes.Equal(batchFiles[name], data) {
			t.Fatalf("segment %s differs: %d bytes appended singly, %d in batches", name, len(data), len(batchFiles[name]))
		}
	}
	zzWP5ReplayAll(t, batchDir, n)
}

// A batch staged into an idle log costs one write and one data sync,
// however many records it holds.
func TestZZWP19AppendManyWithOneGroupCommit(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	next := 0
	for _, count := range []int{1, 10, 100, 250} {
		ops, restore := zzWP19CountOps(dir)
		zzWP19AppendBatch(t, l, next, count)
		restore()
		next += count
		if ops.syncs.Load() != 1 || ops.writes.Load() != 1 {
			t.Fatalf("a batch of %d cost %d writes and %d data syncs, want 1 and 1", count, ops.writes.Load(), ops.syncs.Load())
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	zzWP5ReplayAll(t, dir, next)
}

// Batches and single appends racing each other: each batch's records
// hold consecutive seqs (nothing is staged between them), everything
// acked replays exactly once, and the seq space stays dense.
func TestZZWP19AppendManyWithConcurrentAppendsStayContiguous(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 16 << 10, Prealloc: PreallocOff})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	payloads := map[uint64]string{}
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 60 {
				p := fmt.Sprintf("single-w%d-i%d-%s", w, i, strings.Repeat("s", i%40))
				id, err := l.Append(context.Background(), []byte(p))
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				payloads[id.Seq] = p
				mu.Unlock()
			}
		})
	}
	for w := range 4 {
		wg.Go(func() {
			for b := range 15 {
				count := 1 + (w*7+b*13)%40
				batch := make([]string, count)
				sizes := make([]int, count)
				for i := range batch {
					batch[i] = fmt.Sprintf("batch-w%d-b%d-r%d-%s", w, b, i, strings.Repeat("b", (i*5)%60))
					sizes[i] = len(batch[i])
				}
				ids, err := l.AppendManyWith(context.Background(), sizes, func(i int, dst []byte) []byte { return append(dst, batch[i]...) })
				if err != nil {
					t.Error(err)
					return
				}
				for i := 1; i < len(ids); i++ {
					if ids[i].Seq != ids[i-1].Seq+1 {
						t.Errorf("batch w%d b%d: record %d at seq %d after seq %d", w, b, i, ids[i].Seq, ids[i-1].Seq)
					}
				}
				mu.Lock()
				for i, id := range ids {
					payloads[id.Seq] = batch[i]
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	records := replayAll(t, dir, true)
	if len(records) != len(payloads) {
		t.Fatalf("replayed %d records, acked %d", len(records), len(payloads))
	}
	for _, r := range records {
		if payloads[r.ID.Seq] != string(r.Payload) {
			t.Fatalf("seq %d replayed %q, acked %q", r.ID.Seq, r.Payload, payloads[r.ID.Seq])
		}
	}
}

// A batch bigger than the room left in the active segment rolls it
// between two records, as single appends would: every sealed segment is
// whole frames up to at most SegmentBytes, the IDs name where each record
// really is, and the records staged before each roll share its inline
// sync, so the batch costs one sync per segment it touches.
func TestZZWP19AppendManyWithRollsBetweenRecords(t *testing.T) {
	for _, prealloc := range []SegmentPrealloc{PreallocOff, PreallocOn} {
		t.Run(fmt.Sprintf("prealloc=%v", prealloc == PreallocOn), func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{SegmentBytes: 4 << 10, MaxRecord: 4 << 10, Prealloc: prealloc}
			l, err := Open(dir, opts)
			if err != nil {
				t.Fatal(err)
			}
			for i := range 5 {
				if _, err := l.Append(context.Background(), zzWP5Payload(i)); err != nil {
					t.Fatal(err)
				}
			}
			ops, restore := zzWP19CountOps(dir)
			ids := zzWP19AppendBatch(t, l, 5, 60)
			restore()
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}

			segments := map[uint64]bool{}
			for i, id := range ids {
				segments[id.SegmentBase] = true
				if id.Seq != uint64(5+i) {
					t.Fatalf("record %d at seq %d, want %d", i, id.Seq, 5+i)
				}
				if i > 0 && id.SegmentBase == ids[i-1].SegmentBase {
					if want := ids[i-1].Offset + frameHeaderSize + int64(len(zzWP5Payload(4+i))); id.Offset != want {
						t.Fatalf("record %d at offset %d, want %d", i, id.Offset, want)
					}
				} else if i > 0 && (id.Offset != 0 || id.SegmentBase != id.Seq) {
					t.Fatalf("record %d opens segment %d at offset %d, want its own seq at offset 0", i, id.SegmentBase, id.Offset)
				}
			}
			if len(segments) < 4 {
				t.Fatalf("the batch touched %d segments, want at least 4", len(segments))
			}
			if got := ops.syncs.Load(); got != int64(len(segments)) {
				t.Fatalf("a batch across %d segments cost %d data syncs, want one per segment", len(segments), got)
			}
			all, err := listSegments(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range all[:len(all)-1] {
				size := zzWP5FileSize(t, s.path)
				if size > opts.SegmentBytes || zzWP5DataEnd(t, s.path) != size {
					t.Fatalf("sealed segment %s: %d bytes, whole frames to %d, SegmentBytes %d", filepath.Base(s.path), size, zzWP5DataEnd(t, s.path), opts.SegmentBytes)
				}
			}
			zzWP5ReplayAll(t, dir, 65)
		})
	}
}

// A crash while a batch was being written to a segment that grows by
// appending leaves the file cut anywhere inside it. Every cut, at and
// around each frame boundary, must reopen with the acked records and
// the batch's whole leading frames (durable, never acked: at least once)
// and nothing after them, with no gap, and appending must carry on from
// there.
func TestZZWP19AppendManyWithTornBatchReplaysPrefix(t *testing.T) {
	opts := Options{SegmentBytes: 1 << 20, MaxRecord: 4 << 10, Prealloc: PreallocOff}
	base := t.TempDir()
	l, err := Open(base, opts)
	if err != nil {
		t.Fatal(err)
	}
	const acked, count = 5, 12
	for i := range acked {
		if _, err := l.Append(context.Background(), zzWP5Payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	ids := zzWP19AppendBatch(t, l, acked, count)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	files := zzWP5ReadDir(t, base)
	if len(files) != 1 {
		t.Fatalf("%d segments, want 1", len(files))
	}
	name := filepath.Base(segmentPath(base, 0))
	full := files[name]

	var cuts []int
	for k := range count + 1 {
		start := len(full)
		if k < count {
			start = int(ids[k].Offset)
		}
		for _, d := range []int{-1, 0, 1, frameHeaderSize - 1, frameHeaderSize, frameHeaderSize + 1} {
			if cut := start + d; cut >= int(ids[0].Offset) && cut <= len(full) {
				cuts = append(cuts, cut)
			}
		}
	}
	for _, cut := range cuts {
		whole := 0
		for whole < count && int(ids[whole].Offset)+frameHeaderSize+len(zzWP5Payload(acked+whole)) <= cut {
			whole++
		}
		dir := zzWP5CrashDir(t, map[string][]byte{name: full[:cut]})
		l, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("cut at %d: Open: %v", cut, err)
		}
		want := acked + whole
		if got := l.NextSeq(); got != uint64(want) {
			t.Fatalf("cut at %d: NextSeq = %d, want %d (%d whole batch frames)", cut, got, want, whole)
		}
		if _, err := l.Append(context.Background(), zzWP5Payload(want)); err != nil {
			t.Fatalf("cut at %d: append after recovery: %v", cut, err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		zzWP5ReplayAll(t, dir, want+1)
	}
}

// The same for a batch written into a prepared segment, where a crash
// can keep any subset of the write's sectors or pages (see
// TestZZWP5PreallocCrashModelTornWrite): recovery keeps the batch's
// whole leading frames and nothing after its first damaged one.
func TestZZWP19AppendManyWithTornBatchInPreparedSegment(t *testing.T) {
	opts := zzWP5PreallocOptions()
	opts.SegmentBytes = 64 << 10
	base := t.TempDir()
	l, err := Open(base, opts)
	if err != nil {
		t.Fatal(err)
	}
	n := zzWP5AppendThroughRolls(t, l, 0, 1)
	l.mu.Lock()
	active, prepared := segmentPath(base, l.segmentBase), l.activePrepared
	l.mu.Unlock()
	if !prepared {
		t.Fatal("the active segment is not prepared")
	}
	before, err := os.ReadFile(active)
	if err != nil {
		t.Fatal(err)
	}
	const count = 40
	ids := zzWP19AppendBatch(t, l, n, count)
	after, err := os.ReadFile(active)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	files := zzWP5ReadDir(t, base)
	name := filepath.Base(active)
	start := int(ids[0].Offset)
	batch := after[start : int(ids[count-1].Offset)+frameHeaderSize+len(zzWP5Payload(n+count-1))]
	if !bytes.Equal(batch, zzWP5Batch(n, count)) {
		t.Fatal("the batch on disk is not the frames single appends would have written")
	}

	reopen := opts
	reopen.Prealloc = PreallocOff
	rng := rand.New(rand.NewPCG(19, 5))
	holes := 0
	const trials = 200
	for trial := range trials {
		unit := zzWP5Sector
		if trial%2 == 1 {
			unit = 4096
		}
		p := rng.Float64()
		image := zzWP5TearImage(before, after, start, start+len(batch), unit, func() bool { return rng.Float64() < p })
		intact := zzWP5IntactFrames(image, batch, start)
		if zzWP5HoleThenValid(image, batch, start) {
			holes++
		}
		crash := make(map[string][]byte, len(files))
		for k, v := range files {
			crash[k] = v
		}
		crash[name] = image
		dir := zzWP5CrashDir(t, crash)
		l, err := Open(dir, reopen)
		if err != nil {
			t.Fatalf("trial %d (%d-byte units, keep %.2f): Open: %v", trial, unit, p, err)
		}
		want := n + intact
		if got := l.NextSeq(); got != uint64(want) {
			t.Fatalf("trial %d: NextSeq = %d, want %d (%d acked, %d leading frames whole)", trial, got, want, n, intact)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		zzWP5ReplayAll(t, dir, want)
	}
	if holes < trials/10 {
		t.Fatalf("only %d of %d trials tore a hole in front of whole frames", holes, trials)
	}
}

// A fill that writes the wrong length, or panics, part way through a
// batch withdraws the records staged before it: none is written, their
// seqs are handed back, and the log goes on as if the batch never came.
func TestZZWP19AppendManyWithBadFillWithdraws(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 1 << 20, Prealloc: PreallocOff})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	for i := range 3 {
		if _, err := l.Append(context.Background(), zzWP5Payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	short := func(i int, dst []byte) []byte {
		if i == 6 {
			return append(dst, "short"...)
		}
		return zzWP19Fill(3)(i, dst)
	}
	if _, err := l.AppendManyWith(context.Background(), zzWP19Sizes(3, 10), short); err == nil || !strings.Contains(err.Error(), "record 6") {
		t.Fatalf("short fill error = %v, want one naming record 6", err)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("a panicking fill did not panic")
			}
		}()
		_, _ = l.AppendManyWith(context.Background(), zzWP19Sizes(3, 10), func(i int, dst []byte) []byte {
			if i == 4 {
				panic("fill")
			}
			return zzWP19Fill(3)(i, dst)
		})
	}()
	if got := l.NextSeq(); got != 3 {
		t.Fatalf("NextSeq = %d after withdrawn batches, want 3", got)
	}
	ids := zzWP19AppendBatch(t, l, 3, 4)
	if ids[0].Seq != 3 || ids[0].Offset != int64(3*frameHeaderSize+len(zzWP5Payload(0))+len(zzWP5Payload(1))+len(zzWP5Payload(2))) {
		t.Fatalf("the batch after withdrawn ones starts at %+v", ids[0])
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	zzWP5ReplayAll(t, dir, 7)
}

// A fill that fails after a roll inside the batch withdraws only what was
// staged since the roll: the records synced before it stay (durable,
// never acked), and the seq space goes on from them without a gap.
func TestZZWP19AppendManyWithBadFillAfterRoll(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 4 << 10, MaxRecord: 4 << 10, Prealloc: PreallocOff})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	const count = 40
	bad := func(i int, dst []byte) []byte {
		if i == count-1 {
			return append(dst, "short"...)
		}
		return zzWP19Fill(0)(i, dst)
	}
	if _, err := l.AppendManyWith(context.Background(), zzWP19Sizes(0, count), bad); err == nil {
		t.Fatal("a short fill after a roll succeeded")
	}
	kept := int(l.NextSeq())
	if kept == 0 || kept >= count-1 {
		t.Fatalf("NextSeq = %d: want the records synced before the last roll, and only them", kept)
	}
	zzWP19AppendBatch(t, l, kept, 3)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	zzWP5ReplayAll(t, dir, kept+3)
}

// Bad arguments are refused before anything is staged.
func TestZZWP19AppendManyWithRefusesBadArguments(t *testing.T) {
	l, err := Open(t.TempDir(), Options{MaxRecord: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	fill := func(i int, dst []byte) []byte { return append(dst, 'x') }
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		sizes []int
		fill  func(int, []byte) []byte
	}{
		{"no records", context.Background(), nil, fill},
		{"empty record", context.Background(), []int{1, 0, 1}, fill},
		{"record over MaxRecord", context.Background(), []int{1, 1<<10 + 1}, fill},
		{"nil fill", context.Background(), []int{1}, nil},
		{"cancelled", cancelled, []int{1}, fill},
	} {
		if ids, err := l.AppendManyWith(tc.ctx, tc.sizes, tc.fill); err == nil || ids != nil {
			t.Fatalf("%s: ids %v, error %v; want an error", tc.name, ids, err)
		}
	}
	if _, err := l.AppendManyWith(cancelled, []int{1}, fill); !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled context reported %v, want context.Canceled", err)
	}
	if got := l.NextSeq(); got != 0 {
		t.Fatalf("NextSeq = %d after refused batches, want 0", got)
	}
}

// A failed sync fails the whole batch, latches the log, and acks none of
// it; a reopen replays only what was acked before.
func TestZZWP19AppendManyWithSyncFailure(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, faultOptions())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 4 {
		if _, err := l.Append(context.Background(), zzWP5Payload(i)); err != nil {
			t.Fatal(err)
		}
	}
	inj := faulttest.New(t)
	inj.FailNth(syncfile.OpSyncData, dir, 1, syscall.EIO)
	if ids, err := l.AppendManyWith(context.Background(), zzWP19Sizes(4, 20), zzWP19Fill(4)); err == nil || ids != nil {
		t.Fatalf("AppendManyWith over a failed sync: ids %v, error %v", ids, err)
	}
	if l.Err() == nil {
		t.Fatal("a failed sync did not latch the log")
	}
	if _, err := l.AppendManyWith(context.Background(), zzWP19Sizes(24, 2), zzWP19Fill(24)); err == nil {
		t.Fatal("a latched log took a batch")
	}
	_ = l.Close()
	// The failed write reached the file (only its sync failed), so the
	// batch may replay whole; what matters is the acked prefix and no gap.
	records := replayAll(t, dir, true)
	if len(records) < 4 {
		t.Fatalf("replayed %d records, want the 4 acked ones at least", len(records))
	}
	for i, r := range records {
		if !bytes.Equal(r.Payload, zzWP5Payload(i)) {
			t.Fatalf("record %d = %.20q", i, r.Payload)
		}
	}
}

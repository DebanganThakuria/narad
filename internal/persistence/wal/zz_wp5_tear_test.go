package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// Crash recovery of prepared segments. A prepared segment is overwritten
// in place, so its size never changes, and a power loss during a group
// commit's sync can leave any subset of the unsynced write's sectors on
// disk: a hole of the zeros they overwrote in front of valid frames of
// the same write. None of those frames was acked (their sync never
// returned), so recovery must truncate at the hole. Open used to read
// the hole as corruption of synced data and refuse to start, and the
// node crash-looped until someone truncated the segment by hand.

// zzWP5Sector is the smallest unit a device persists whole.
const zzWP5Sector = 512

// zzWP5TearImage returns a copy of before in which each unit-aligned
// block of [from, to) that keep picks holds after's bytes there instead:
// the file as a crash during the sync of the write that turned before
// into after can leave it.
func zzWP5TearImage(before, after []byte, from, to, unit int, keep func() bool) []byte {
	image := bytes.Clone(before)
	for block := from / unit * unit; block < to; block += unit {
		if keep() {
			lo, hi := max(block, from), min(block+unit, to)
			copy(image[lo:hi], after[lo:hi])
		}
	}
	return image
}

// zzWP5CrashDir writes files (base name to contents) into a new
// directory, as the disk holds them after a crash.
func zzWP5CrashDir(t *testing.T, files map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// zzWP5ReadDir reads every segment of dir.
func zzWP5ReadDir(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files, err := zzWP5ReadSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// zzWP5ReadSegments reads every segment of dir, by base name.
func zzWP5ReadSegments(dir string) (map[string][]byte, error) {
	segments, err := listSegments(dir)
	if err != nil {
		return nil, err
	}
	files := make(map[string][]byte, len(segments))
	for _, s := range segments {
		data, err := os.ReadFile(s.path)
		if err != nil {
			return nil, err
		}
		files[filepath.Base(s.path)] = data
	}
	return files, nil
}

// zzWP5Batch encodes records first..first+count-1 as one staged batch.
func zzWP5Batch(first, count int) []byte {
	var batch []byte
	for i := first; i < first+count; i++ {
		batch = appendFrame(batch, uint64(i), zzWP5Payload(i))
	}
	return batch
}

// zzWP5IntactFrames counts the frames of batch, laid down at offset at,
// that image holds whole from the first one on.
func zzWP5IntactFrames(image, batch []byte, at int) int {
	intact := 0
	for off := 0; off < len(batch); intact++ {
		end := off + frameHeaderSize + int(binary.BigEndian.Uint32(batch[off+4:off+8]))
		if !bytes.Equal(image[at+off:at+end], batch[off:end]) {
			break
		}
		off = end
	}
	return intact
}

// zzWP5HoleThenValid reports whether image holds a whole frame of batch
// after one it does not: the state recovery used to refuse.
func zzWP5HoleThenValid(image, batch []byte, at int) bool {
	damaged := false
	for off := 0; off < len(batch); {
		end := off + frameHeaderSize + int(binary.BigEndian.Uint32(batch[off+4:off+8]))
		whole := bytes.Equal(image[at+off:at+end], batch[off:end])
		if whole && damaged {
			return true
		}
		damaged = damaged || !whole
		off = end
	}
	return false
}

// The on-disk state after the device persisted pages 2 to 4 of an
// unsynced 12 KiB batch in a prepared segment but not page 1: acked
// frames, the preparer's zeros, then valid frames of the torn batch.
// Open must truncate at the zeros and keep every acked record.
func TestZZWP5PreallocTornBatchHoleThenValidOpens(t *testing.T) {
	dir := t.TempDir()
	n, active := zzWP5PreparedLog(t, dir)
	end := zzWP5DataEnd(t, active)

	var batch []byte
	for i := 0; len(batch) < 3*4096 && int(end)+len(batch) < zzWP5SegmentBytes-512; i++ {
		batch = appendFrame(batch, uint64(n+i), zzWP5Payload(n+i))
	}
	page := (end + 4095) &^ 4095
	if page == end {
		page += 4096
	}
	f, err := os.OpenFile(active, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(batch[page-end:], page); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	l, err := Open(dir, zzWP5PreallocOptions())
	if err != nil {
		t.Fatalf("Open after a crash that tore only an unacked batch: %v", err)
	}
	if got := l.NextSeq(); got != uint64(n) {
		t.Fatalf("NextSeq = %d, want %d: the torn batch must not come back", got, n)
	}
	if size := zzWP5FileSize(t, active); size != end {
		t.Fatalf("active segment %d bytes, want truncated at the hole, %d", size, end)
	}
	if _, err := l.Append(context.Background(), zzWP5Payload(n)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	zzWP5ReplayAll(t, dir, n+1)
}

// The crash model for prepared segments: a write of whole frames, no
// larger than the segment's write limit, into the zeros after the acked
// data, of which any subset of 512-byte sectors or 4 KiB pages reached
// the disk. Every such state must open, keep every acked record, keep
// the torn write's leading frames that did reach the disk whole (they
// are durable, never acked, and may come back, as after any crash) and
// nothing after its first damaged frame, and go on appending.
func TestZZWP5PreallocCrashModelTornWrite(t *testing.T) {
	opts := zzWP5PreallocOptions()
	opts.SegmentBytes = 64 << 10
	opts.preparedWriteLimit = 16 << 10
	base := t.TempDir()
	n, active := zzWP5PreparedLogWith(t, base, opts)
	end := int(zzWP5DataEnd(t, active))
	files := zzWP5ReadDir(t, base)
	name := filepath.Base(active)
	before := files[name]

	rng := rand.New(rand.NewPCG(1, 2))
	reopen := opts
	reopen.Prealloc = PreallocOff
	holes := 0
	const trials = 300
	for trial := range trials {
		// A run the writer could issue: whole frames up to the limit.
		count := 1
		for count < 200 && len(zzWP5Batch(n, count+1)) <= int(opts.preparedWriteLimit) {
			count++
		}
		count = 1 + rng.IntN(count)
		batch := zzWP5Batch(n, count)
		after := bytes.Clone(before)
		copy(after[end:], batch)
		unit := zzWP5Sector
		if trial%2 == 1 {
			unit = 4096
		}
		p := rng.Float64()
		image := zzWP5TearImage(before, after, end, end+len(batch), unit, func() bool { return rng.Float64() < p })
		intact := zzWP5IntactFrames(image, batch, end)
		if zzWP5HoleThenValid(image, batch, end) {
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
			t.Fatalf("trial %d (%d frames, %d-byte units, keep %.2f): Open: %v", trial, count, unit, p, err)
		}
		want := n + intact
		if got := l.NextSeq(); got != uint64(want) {
			t.Fatalf("trial %d: NextSeq = %d, want %d (%d acked, %d leading frames whole)", trial, got, want, n, intact)
		}
		if trial%25 == 0 {
			if _, err := l.Append(context.Background(), zzWP5Payload(want)); err != nil {
				t.Fatal(err)
			}
			want++
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		zzWP5ReplayAll(t, dir, want)
	}
	if holes < trials/10 {
		t.Fatalf("only %d of %d trials tore a hole in front of whole frames: the model did not reach the case", holes, trials)
	}
	t.Logf("%d of %d torn writes left whole frames after a hole", holes, trials)
}

// The same crash model driven by the real group commit under load: the
// hook snapshots the active prepared segment around one of the log's
// own writes, fails that write's sync (so none of its records is acked
// and the log latches), and every subset of that write's sectors or
// pages the disk could have kept must recover with the acked records
// intact, seqs dense, and nothing that was never appended. The hook also
// checks the invariant recovery rests on: a segment never has two writes
// in flight without a sync between them, and no write into a prepared
// segment is larger than its limit unless it is a single frame.
func TestZZWP5PreallocCrashDuringGroupCommit(t *testing.T) {
	opts := zzWP5PreallocOptions()
	opts.SegmentBytes = 64 << 10
	opts.preparedWriteLimit = 1 << 10
	dir := t.TempDir()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	primed := zzWP5AppendThroughRolls(t, l, 0, 1)

	preparedSize := opts.SegmentBytes + prepTrailerSize
	var mu sync.Mutex
	var problems []string
	unsynced := make(map[string]bool)
	preparedWrites := 0
	var target string
	var before, after []byte
	var others map[string][]byte
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if filepath.Dir(path) != dir || filepath.Ext(path) != segmentSuffix {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		switch op {
		case syncfile.OpWrite:
			if unsynced[path] {
				problems = append(problems, fmt.Sprintf("second write into %s before a sync", filepath.Base(path)))
			}
			unsynced[path] = true
			info, err := os.Stat(path)
			if err != nil || info.Size() != preparedSize {
				return nil
			}
			preparedWrites++
			if preparedWrites >= 60 && after == nil {
				target = path
				if before, err = os.ReadFile(path); err != nil {
					problems = append(problems, err.Error())
				}
			}
		case syncfile.OpSyncData:
			unsynced[path] = false
			if path != target || after != nil {
				return nil
			}
			written, err := os.ReadFile(path)
			if err != nil {
				problems = append(problems, err.Error())
				return nil
			}
			// Crash a write that spans sectors and frames, close to the
			// limit, so that its tears include holes in front of whole
			// frames; let smaller ones sync.
			changed := 0
			for i := range written {
				if written[i] != before[i] {
					changed++
				}
			}
			if changed < int(opts.preparedWriteLimit)*3/4 {
				return nil
			}
			after = written
			if others, err = zzWP5ReadSegments(dir); err != nil {
				problems = append(problems, err.Error())
			}
			delete(others, filepath.Base(path))
			return syscall.EIO
		}
		return nil
	})
	res := runAppendLoad(t, l, 32, 120)
	restore()
	_ = l.Close()
	for i := range primed {
		res.submitted[string(zzWP5Payload(i))] = true
		res.acked[string(zzWP5Payload(i))] = RecordID{}
	}
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	if after == nil {
		t.Fatalf("the load finished before the hook crashed a write (%d prepared writes)", preparedWrites)
	}
	if len(res.failed) == 0 {
		t.Fatal("no append observed the failed sync")
	}

	// The write that was in flight: from the end of the synced data to
	// the last byte it changed (frames end in payload bytes, never zero).
	from := 0
	for from < len(before) && before[from] == after[from] {
		from++
	}
	to := len(after)
	for to > from && before[to-1] == after[to-1] {
		to--
	}
	frames, runEnd := 0, from
	for runEnd < to {
		runEnd += frameHeaderSize + int(binary.BigEndian.Uint32(after[runEnd+4:runEnd+8]))
		frames++
	}
	if runEnd != to {
		t.Fatalf("the crashed write [%d,%d) does not end on a frame boundary (%d)", from, to, runEnd)
	}
	if int64(to-from) > opts.preparedWriteLimit && frames > 1 {
		t.Fatalf("the crashed write is %d bytes in %d frames, over the %d-byte limit", to-from, frames, opts.preparedWriteLimit)
	}
	t.Logf("crashed write: %d bytes, %d frames, at %d; %d acked, %d failed", to-from, frames, from, len(res.acked), len(res.failed))

	rng := rand.New(rand.NewPCG(3, 4))
	name := filepath.Base(target)
	holes := 0
	for trial := range 200 {
		unit := zzWP5Sector
		if trial%2 == 1 {
			unit = 4096
		}
		p := rng.Float64()
		crash := make(map[string][]byte, len(others)+1)
		for k, v := range others {
			crash[k] = v
		}
		image := zzWP5TearImage(before, after, from, to, unit, func() bool { return rng.Float64() < p })
		if zzWP5HoleThenValid(image, after[from:to], from) {
			holes++
		}
		crash[name] = image
		crashDir := zzWP5CrashDir(t, crash)
		reopened, err := Open(crashDir, faultOptions())
		if err != nil {
			t.Fatalf("trial %d (%d-byte units, keep %.2f): Open: %v", trial, unit, p, err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
		assertReplayContract(t, replayAll(t, crashDir, true), res)
	}
	if holes == 0 {
		t.Fatal("no tear of the crashed write left whole frames after a hole: the test did not reach the case")
	}
	t.Logf("%d of 200 tears left whole frames after a hole", holes)
}

// What stays a loud failure in a prepared segment: damage that a torn
// write of at most the segment's limit cannot explain, or a hole in
// front of valid frames in a segment that does not end in the prepared
// trailer. Each case changes one thing about a torn write that opens;
// the file must be left as it was.
func TestZZWP5PreallocTearOutsideTheModelFailsOpen(t *testing.T) {
	opts := zzWP5PreallocOptions()
	const limit = 4 << 10
	opts.preparedWriteLimit = limit
	base := t.TempDir()
	n, active := zzWP5PreparedLogWith(t, base, opts)
	end := int(zzWP5DataEnd(t, active))
	files := zzWP5ReadDir(t, base)
	name := filepath.Base(active)
	before := files[name]
	size := len(before)
	window := end + limit
	if window+1024 > int(opts.SegmentBytes) {
		t.Fatalf("data end %d leaves no room past the %d-byte window", end, limit)
	}
	// tear lays batch down at end and zeroes the rest of the first
	// sector it touches: a write whose first sector never reached the
	// disk.
	tear := func(batch []byte) []byte {
		image := bytes.Clone(before)
		copy(image[end:], batch)
		clear(image[end : end+zzWP5Sector-end%zzWP5Sector])
		if !zzWP5HoleThenValid(image, batch, end) {
			t.Fatal("the torn image has no whole frame after its hole")
		}
		return image
	}
	batch := zzWP5Batch(n, 8)

	for _, tc := range []struct {
		name  string
		image func() []byte
		fails bool
	}{
		{"torn write", func() []byte { return tear(batch) }, false},
		{"frame across the window end", func() []byte {
			// Continues the run, but ends past the window; its
			// payload's tail is zeros, so the bytes past the window
			// stay zero and only the frame's extent gives it away.
			payload := append([]byte("tail"), make([]byte, 400)...)
			frame := appendFrame(nil, uint64(n+8), payload)
			image := tear(batch)
			copy(image[window-100:], frame)
			return image
		}, true},
		{"non-zero past the window", func() []byte {
			image := tear(batch)
			image[window+100] = 'x'
			return image
		}, true},
		{"seq jump after the hole", func() []byte {
			jumped := appendFrame(nil, uint64(n), zzWP5Payload(n))
			for i := 1; i < 8; i++ {
				jumped = appendFrame(jumped, uint64(n+50+i), zzWP5Payload(n+i))
			}
			return tear(jumped)
		}, true},
		{"older seq after the hole", func() []byte {
			older := appendFrame(nil, uint64(n), zzWP5Payload(n))
			older = appendFrame(older, uint64(n-1), zzWP5Payload(n-1))
			older = appendFrame(older, uint64(n-1), zzWP5Payload(n+2))
			return tear(older)
		}, true},
		{"trailer limit below the tear", func() []byte {
			image := tear(batch)
			binary.BigEndian.PutUint64(image[size-8:], 64)
			return image
		}, true},
		{"no trailer", func() []byte {
			image := tear(batch)
			clear(image[size-prepTrailerSize:])
			return image
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			image := tc.image()
			crash := make(map[string][]byte, len(files))
			for k, v := range files {
				crash[k] = v
			}
			crash[name] = image
			dir := zzWP5CrashDir(t, crash)
			l, err := Open(dir, zzWP5PreallocOptions())
			if !tc.fails {
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				if got := l.NextSeq(); got != uint64(n) {
					t.Fatalf("NextSeq = %d, want %d", got, n)
				}
				if err := l.Close(); err != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, errCorruptFrame) {
				if err == nil {
					_ = l.Close()
				}
				t.Fatalf("Open = %v, want a corruption failure", err)
			}
			got, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, image) {
				t.Fatal("the failed Open changed the segment")
			}
		})
	}
}

// A buffer longer than the limit goes out in runs of whole frames, each
// written and synced before the next is written, and lands on disk
// byte for byte; without a limit it is one write and one sync.
func TestZZWP5PreparedWriteRuns(t *testing.T) {
	var buffer []byte
	var frameEnds []int
	for i := range 40 {
		buffer = appendFrame(buffer, uint64(i), bytes.Repeat([]byte{byte('a' + i%26)}, 30+(i*37)%400))
		frameEnds = append(frameEnds, len(buffer))
	}
	// One frame larger than any limit below goes alone.
	buffer = appendFrame(buffer, 40, bytes.Repeat([]byte("z"), 3000))
	frameEnds = append(frameEnds, len(buffer))
	isFrameEnd := func(off int) bool {
		for _, e := range frameEnds {
			if e == off {
				return true
			}
		}
		return false
	}

	for _, limit := range []int64{0, 1, 512, 1000, 2048, int64(len(buffer))} {
		t.Run(fmt.Sprint("limit=", limit), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "00000000000000000000.wal")
			file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			var ops []syncfile.Op
			var sizes []int64
			restore := syncfile.SetFaultHook(func(op syncfile.Op, p string) error {
				if p != path {
					return nil
				}
				ops = append(ops, op)
				if op == syncfile.OpSyncData {
					info, err := os.Stat(p)
					if err != nil {
						return err
					}
					sizes = append(sizes, info.Size())
				}
				return nil
			})
			var l Log
			err = l.writeAndSyncFileOps(file, buffer, limit)
			restore()
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, buffer) {
				t.Fatal("the file does not hold the buffer")
			}
			for i, op := range ops {
				if want := []syncfile.Op{syncfile.OpWrite, syncfile.OpSyncData}[i%2]; op != want {
					t.Fatalf("op %d is %v, want %v: runs must alternate write and sync", i, op, want)
				}
			}
			if limit == 0 || limit >= int64(len(buffer)) {
				if len(sizes) != 1 {
					t.Fatalf("%d syncs, want one", len(sizes))
				}
				return
			}
			prev := int64(0)
			for _, synced := range sizes {
				if !isFrameEnd(int(synced)) {
					t.Fatalf("a run ends at %d, inside a frame", synced)
				}
				run := synced - prev
				single := false
				for i, e := range frameEnds {
					start := 0
					if i > 0 {
						start = frameEnds[i-1]
					}
					if int64(start) == prev && int64(e) == synced {
						single = true
					}
				}
				if run > limit && !single {
					t.Fatalf("a run of %d bytes and several frames, over the %d limit", run, limit)
				}
				prev = synced
			}
			if prev != int64(len(buffer)) {
				t.Fatalf("runs end at %d, want %d", prev, len(buffer))
			}
		})
	}
}

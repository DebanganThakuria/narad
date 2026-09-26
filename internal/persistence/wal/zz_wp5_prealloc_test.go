package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile/faulttest"
)

// Tests for prepared (zero-filled) segments; see SegmentPrealloc.

const zzWP5SegmentBytes = 16 << 10

func zzWP5PreallocOptions() Options {
	return Options{SegmentBytes: zzWP5SegmentBytes, MaxRecord: 4 << 10, Prealloc: PreallocOn}
}

// zzWP5Payload is record i's payload: distinct, 150-280 bytes.
func zzWP5Payload(i int) []byte {
	return fmt.Appendf(nil, "p-%06d-%s", i, bytes.Repeat([]byte{byte('a' + i%26)}, 140+i%131))
}

// zzWP5WaitSpare waits until the preparer has a spare ready.
func zzWP5WaitSpare(t *testing.T, l *Log) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		l.mu.Lock()
		ready := l.spareReady
		l.mu.Unlock()
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the preparer never readied a spare segment")
		}
		time.Sleep(time.Millisecond)
	}
}

// zzWP5AppendThroughRolls appends records from next on, one at a time,
// until the log has rolled rolls more times, waiting for the spare
// before each roll so every roll uses a prepared segment. It returns the
// next record index.
func zzWP5AppendThroughRolls(t *testing.T, l *Log, next, rolls int) int {
	t.Helper()
	for rolled := 0; rolled < rolls; {
		l.mu.Lock()
		base := l.segmentBase
		nearFull := l.segmentSize+int64(frameHeaderSize+300) > l.opts.SegmentBytes
		l.mu.Unlock()
		if nearFull {
			zzWP5WaitSpare(t, l)
		}
		id, err := l.Append(context.Background(), zzWP5Payload(next))
		if err != nil {
			t.Fatalf("append %d: %v", next, err)
		}
		next++
		if id.SegmentBase != base {
			rolled++
		}
	}
	return next
}

// zzWP5ReplayAll replays dir from the start and checks record i is
// zzWP5Payload(i) with seq i.
func zzWP5ReplayAll(t *testing.T, dir string, want int) {
	t.Helper()
	got := 0
	if err := Replay(dir, 0, 0, func(r Record) error {
		if r.ID.Seq != uint64(got) || !bytes.Equal(r.Payload, zzWP5Payload(got)) {
			return fmt.Errorf("record %d: seq %d payload %.20q", got, r.ID.Seq, r.Payload)
		}
		got++
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if got != want {
		t.Fatalf("replayed %d records, want %d", got, want)
	}
}

func zzWP5FileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

// A roll on the append path brings in the prepared segment: the new
// segment is SegmentBytes long before anything is written to it, so
// appends overwrite rather than extend it (the file size never changes,
// which is what keeps each group commit's data sync out of the file
// system journal). The roll that seals it trims it back to its data
// first, so no sealed segment ever has a zero tail (a binary that
// predates preparation would refuse to open one). The spare's file never
// shows up as a segment, and everything replays.
func TestZZWP5PreallocRollUsesPreparedSegment(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, zzWP5PreallocOptions())
	if err != nil {
		t.Fatal(err)
	}
	n := zzWP5AppendThroughRolls(t, l, 0, 1)

	l.mu.Lock()
	active, prepared := l.segmentBase, l.activePrepared
	l.mu.Unlock()
	if !prepared {
		t.Fatal("the roll did not use the prepared segment")
	}
	activePath := segmentPath(dir, active)
	if size := zzWP5FileSize(t, activePath); size != zzWP5SegmentBytes {
		t.Fatalf("prepared active segment is %d bytes, want %d", size, zzWP5SegmentBytes)
	}
	for range 10 {
		if _, err := l.Append(context.Background(), zzWP5Payload(n)); err != nil {
			t.Fatal(err)
		}
		n++
		if size := zzWP5FileSize(t, activePath); size != zzWP5SegmentBytes {
			t.Fatalf("an append changed the prepared segment's size to %d", size)
		}
	}

	// Seal it: by the time the successor exists it is trimmed.
	n = zzWP5AppendThroughRolls(t, l, n, 1)
	if size, end := zzWP5FileSize(t, activePath), zzWP5DataEnd(t, activePath); size != end {
		t.Fatalf("sealed prepared segment is %d bytes, want trimmed to its data end %d", size, end)
	}
	segments, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range segments {
		if filepath.Base(s.path) == prepFileName {
			t.Fatal("the spare is listed as a segment")
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, prepFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Close left the spare behind: %v", err)
	}
	zzWP5ReplayAll(t, dir, n)
}

// A replay of the open log must not parse bytes past the last sync. In a
// prepared segment those bytes are zeros or a frame still being copied
// into the page cache, not an end of file, so a concurrent reader that
// ran into a half-written frame reported corruption and failed the
// dispatcher's pass. The sync hook below runs after a batch is written
// and before it is synced, plants a torn frame past it, and replays:
// the log's replay must stop at the synced size, while the directory
// replay (which has no synced size) walks into the unsynced batch.
func TestZZWP5PreallocLiveReplayStopsAtSyncedSize(t *testing.T) {
	dir := t.TempDir()
	opts := zzWP5PreallocOptions()
	opts.SegmentBytes = 64 << 10
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	n := zzWP5AppendThroughRolls(t, l, 0, 1)

	var hookErr error
	var inHook bool
	var mu sync.Mutex
	armed := true
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		mu.Lock()
		defer mu.Unlock()
		if op != syncfile.OpSyncData || filepath.Ext(path) != segmentSuffix || !armed || inHook {
			return nil
		}
		armed = false
		inHook = true
		defer func() { inHook = false }()
		// The batch holding record n is written, not synced. Plant a torn
		// frame (magic and garbage) right after it.
		l.mu.Lock()
		end := l.segmentSize
		durable := l.durableSize
		l.mu.Unlock()
		f, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			hookErr = err
			return nil
		}
		defer f.Close()
		torn := []byte{0x4e, 0x57, 0x41, 0x4c, 0xff, 0xff, 0x00, 0x01, 0x02, 0x03}
		if _, err := f.WriteAt(torn, end); err != nil {
			hookErr = err
			return nil
		}
		defer func() { _, _ = f.WriteAt(make([]byte, len(torn)), end) }()

		var seen []uint64
		if err := l.ReplayFromCursor(Cursor{}, func(r Record, _ Cursor) error {
			seen = append(seen, r.ID.Seq)
			return nil
		}); err != nil {
			hookErr = fmt.Errorf("live replay during a sync: %w", err)
			return nil
		}
		if len(seen) != n || (n > 0 && seen[n-1] != uint64(n-1)) {
			hookErr = fmt.Errorf("live replay saw %d records (synced size %d), want the %d synced ones", len(seen), durable, n)
			return nil
		}
		if err := Replay(dir, 0, 0, func(Record) error { return nil }); err == nil {
			hookErr = errors.New("directory replay did not trip over the torn frame: the test did not plant it past the unsynced batch")
		}
		return nil
	})
	defer restore()
	if _, err := l.Append(context.Background(), zzWP5Payload(n)); err != nil {
		t.Fatal(err)
	}
	restore()
	mu.Lock()
	defer mu.Unlock()
	if armed {
		t.Fatal("the sync hook never ran")
	}
	if hookErr != nil {
		t.Fatal(hookErr)
	}
}

// Appenders and a reader racing through many prepared rolls: every live
// replay must succeed and see a dense prefix of what was acked.
func TestZZWP5PreallocConcurrentAppendAndLiveReplay(t *testing.T) {
	dir := t.TempDir()
	opts := zzWP5PreallocOptions()
	opts.SegmentBytes = 8 << 10
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	const writers, perWriter = 8, 150
	var acked sync.Map
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			for i := range perWriter {
				p := fmt.Appendf(nil, "w%d-%04d-%s", w, i, bytes.Repeat([]byte("q"), 40+i%90))
				id, err := l.Append(context.Background(), p)
				if err != nil {
					t.Errorf("append: %v", err)
					return
				}
				acked.Store(id.Seq, string(p))
			}
		})
	}
	stop := make(chan struct{})
	readerDone := make(chan error, 1)
	go func() {
		passes := 0
		for {
			select {
			case <-stop:
				if passes == 0 {
					readerDone <- errors.New("no replay pass completed")
					return
				}
				readerDone <- nil
				return
			default:
			}
			var prev uint64
			first := true
			err := l.ReplayFromCursor(Cursor{}, func(r Record, _ Cursor) error {
				if !first && r.ID.Seq != prev+1 {
					return fmt.Errorf("seq %d after %d", r.ID.Seq, prev)
				}
				first, prev = false, r.ID.Seq
				return nil
			})
			if err != nil {
				readerDone <- err
				return
			}
			passes++
		}
	}()
	wg.Wait()
	close(stop)
	if err := <-readerDone; err != nil {
		t.Fatalf("live replay during appends: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	got := 0
	if err := Replay(dir, 0, 0, func(r Record) error {
		want, ok := acked.Load(r.ID.Seq)
		if !ok || want.(string) != string(r.Payload) {
			return fmt.Errorf("seq %d: payload %.20q not the acked one", r.ID.Seq, r.Payload)
		}
		got++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got != writers*perWriter {
		t.Fatalf("replayed %d, want %d", got, writers*perWriter)
	}
}

// zzWP5PreparedLog leaves dir with a prepared active segment holding
// records and its zero tail, as a process that stopped (or crashed)
// would. It returns the number of records and the active segment's path.
func zzWP5PreparedLog(t *testing.T, dir string) (int, string) {
	t.Helper()
	l, err := Open(dir, zzWP5PreallocOptions())
	if err != nil {
		t.Fatal(err)
	}
	n := zzWP5AppendThroughRolls(t, l, 0, 1)
	for range 5 {
		if _, err := l.Append(context.Background(), zzWP5Payload(n)); err != nil {
			t.Fatal(err)
		}
		n++
	}
	l.mu.Lock()
	active := segmentPath(dir, l.segmentBase)
	l.mu.Unlock()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if size := zzWP5FileSize(t, active); size != zzWP5SegmentBytes {
		t.Fatalf("active segment %d bytes, want a prepared %d", size, zzWP5SegmentBytes)
	}
	return n, active
}

// zzWP5DataEnd is the end of the last frame in a segment file.
func zzWP5DataEnd(t *testing.T, path string) int64 {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var end int64
	for end+frameHeaderSize <= int64(len(data)) && bytes.Equal(data[end:end+4], []byte("NWAL")) {
		end += frameHeaderSize + int64(uint32(data[end+4])<<24|uint32(data[end+5])<<16|uint32(data[end+6])<<8|uint32(data[end+7]))
	}
	return end
}

// Reopening a log whose active segment was prepared: the zero tail is
// the clean end of data, every record replays, the active segment is
// truncated to its data (as any active segment is), and appends go on.
func TestZZWP5PreallocReopenPreparedActiveSegment(t *testing.T) {
	for _, prealloc := range []SegmentPrealloc{PreallocOn, PreallocOff} {
		t.Run(fmt.Sprint("prealloc=", prealloc == PreallocOn), func(t *testing.T) {
			dir := t.TempDir()
			n, active := zzWP5PreparedLog(t, dir)
			end := zzWP5DataEnd(t, active)
			zzWP5ReplayAll(t, dir, n)

			opts := zzWP5PreallocOptions()
			opts.Prealloc = prealloc
			l, err := Open(dir, opts)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if got := l.NextSeq(); got != uint64(n) {
				t.Fatalf("NextSeq = %d, want %d", got, n)
			}
			if size := zzWP5FileSize(t, active); size != end {
				t.Fatalf("active segment %d bytes after reopen, want truncated to its data %d", size, end)
			}
			for range 3 {
				if _, err := l.Append(context.Background(), zzWP5Payload(n)); err != nil {
					t.Fatal(err)
				}
				n++
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			zzWP5ReplayAll(t, dir, n)
		})
	}
}

// A crash mid-write in a prepared segment leaves a torn frame followed
// by zeros. Recovery treats it as the torn tail it is.
func TestZZWP5PreallocTornTailInPreparedSegment(t *testing.T) {
	dir := t.TempDir()
	n, active := zzWP5PreparedLog(t, dir)
	end := zzWP5DataEnd(t, active)
	f, err := os.OpenFile(active, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Half a frame: magic, a length, a seq, no valid CRC or payload.
	torn := []byte{0x4e, 0x57, 0x41, 0x4c, 0, 0, 0, 90, 0, 0, 0, 0, 0, 0, 0, byte(n), 1, 2, 3, 4, 'x', 'y'}
	if _, err := f.WriteAt(torn, end); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	l, err := Open(dir, zzWP5PreallocOptions())
	if err != nil {
		t.Fatalf("reopen over a torn tail: %v", err)
	}
	if size := zzWP5FileSize(t, active); size != end {
		t.Fatalf("active segment %d bytes, want the torn tail cut at %d", size, end)
	}
	if _, err := l.Append(context.Background(), zzWP5Payload(n)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	zzWP5ReplayAll(t, dir, n+1)
}

// Zeros end the active segment's data only if nothing valid follows
// them. A zeroed frame with acked frames after it is lost data, and
// recovery must fail loudly rather than truncate the later frames away,
// in the active and in a sealed segment alike.
func TestZZWP5PreallocZeroedFrameBeforeValidDataFailsOpen(t *testing.T) {
	for _, sealed := range []bool{false, true} {
		t.Run(fmt.Sprint("sealed=", sealed), func(t *testing.T) {
			dir := t.TempDir()
			n, active := zzWP5PreparedLog(t, dir)
			if sealed {
				// A later (empty) segment makes the prepared one sealed.
				if err := os.WriteFile(segmentPath(dir, uint64(n)), nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			// Zero the first frame's header; frames after it stay valid.
			f, err := os.OpenFile(active, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt(make([]byte, frameHeaderSize), 0); err != nil {
				t.Fatal(err)
			}
			_ = f.Close()
			before := zzWP5FileSize(t, active)

			_, err = Open(dir, zzWP5PreallocOptions())
			if !errors.Is(err, errCorruptFrame) {
				t.Fatalf("Open over a zeroed frame followed by valid frames = %v, want a corruption failure", err)
			}
			if size := zzWP5FileSize(t, active); size != before {
				t.Fatalf("the failed Open changed the segment from %d to %d bytes", before, size)
			}
		})
	}
}

// Rolls trim a prepared segment before sealing it, so a sealed segment
// that ends in zeros is never something this code leaves behind, and
// recovery keeps treating it the way it always has (and the way a
// binary that predates preparation does): as corruption of synced data,
// a loud Open failure rather than a silent truncation of what may have
// been acked records. Offline replay still reads the frames before it.
func TestZZWP5PreallocSealedZeroTailFailsOpen(t *testing.T) {
	dir := t.TempDir()
	n, active := zzWP5PreparedLog(t, dir)
	if err := os.WriteFile(segmentPath(dir, uint64(n)), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	zzWP5ReplayAll(t, dir, n)
	before := zzWP5FileSize(t, active)
	if _, err := Open(dir, zzWP5PreallocOptions()); !errors.Is(err, errCorruptFrame) {
		t.Fatalf("Open with a sealed segment ending in zeros = %v, want a corruption failure", err)
	}
	if size := zzWP5FileSize(t, active); size != before {
		t.Fatalf("the failed Open changed the segment from %d to %d bytes", before, size)
	}
}

// A spare left by a stop mid-preparation is discarded at Open: it may be
// partly written, and it is never mistaken for a segment.
func TestZZWP5PreallocStaleSpareDiscarded(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, prepFileName), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := Open(dir, zzWP5PreallocOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, prepFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale spare survived Open: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

// Faults in preparation, in bringing the spare in, and in trimming the
// prepared segment a roll seals. A preparation that fails (a full disk)
// must cost no append: the roll creates an empty segment as before. A
// failed rename likewise falls back to a create. A failure to open the
// renamed segment or to trim the sealed one fails the append that
// rolled, like a failed create, and the next append rolls cleanly.
// Nothing acked is ever lost.
func TestZZWP5PreallocFaults(t *testing.T) {
	for _, tc := range []struct {
		name  string
		op    syncfile.Op
		match func(dir string) func(string) bool
		// spareFirst waits for a ready spare before each roll, so the
		// fault hits bringing it in rather than a plain create.
		spareFirst bool
		wantFails  bool
	}{
		{"prepare-enospc", syncfile.OpWrite, func(dir string) func(string) bool {
			return func(p string) bool { return p == filepath.Join(dir, prepFileName) }
		}, false, false},
		{"rename-eio", syncfile.OpRename, func(dir string) func(string) bool {
			return func(p string) bool { return filepath.Dir(p) == dir }
		}, true, false},
		{"open-prepared-eio", syncfile.OpOpen, func(dir string) func(string) bool {
			return func(p string) bool { return filepath.Dir(p) == dir && filepath.Ext(p) == segmentSuffix }
		}, true, true},
		{"trim-eio", syncfile.OpTruncate, func(dir string) func(string) bool {
			return func(p string) bool { return filepath.Dir(p) == dir && filepath.Ext(p) == segmentSuffix }
		}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			inj := faulttest.New(t)
			l, err := Open(dir, zzWP5PreallocOptions())
			if err != nil {
				t.Fatal(err)
			}
			rule := inj.FailNthFunc(tc.op, tc.match(dir), 1, syscall.ENOSPC)
			acked := 0
			failed := 0
			for i := 0; acked < 180; i++ {
				if tc.spareFirst && rule.Fired() == 0 {
					l.mu.Lock()
					nearFull := l.segmentSize+int64(frameHeaderSize+300) > l.opts.SegmentBytes
					l.mu.Unlock()
					if nearFull {
						zzWP5WaitSpare(t, l)
					}
				}
				_, err := l.Append(context.Background(), zzWP5Payload(acked))
				if err != nil {
					failed++
					if failed > 1 {
						t.Fatalf("append %d failed again after one fault: %v", i, err)
					}
					continue
				}
				acked++
			}
			if rule.Fired() != 1 {
				t.Fatalf("fault fired %d times, want 1", rule.Fired())
			}
			if tc.wantFails != (failed == 1) {
				t.Fatalf("%d appends failed, want failure=%v", failed, tc.wantFails)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			zzWP5ReplayAll(t, dir, acked)
		})
	}
}

// zzWP5CheckOlderBinaryReadable asserts the layout a binary that
// predates preparation can open without losing anything: every sealed
// segment holds valid frames back to back up to its end of file (such a
// binary reads anything else in a sealed segment as corruption and
// refuses to start), and the last segment holds valid frames followed by
// nothing but zeros (which such a binary truncates as a torn tail).
func zzWP5CheckOlderBinaryReadable(t *testing.T, dir string) {
	t.Helper()
	segments, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range segments {
		data, err := os.ReadFile(s.path)
		if err != nil {
			t.Fatal(err)
		}
		end := zzWP5DataEnd(t, s.path)
		if i < len(segments)-1 {
			if end != int64(len(data)) {
				t.Fatalf("sealed segment %s: %d bytes of frames, %d bytes of file: an older binary would refuse to open it", filepath.Base(s.path), end, len(data))
			}
			continue
		}
		if tail := data[end:]; len(bytes.Trim(tail, "\x00")) != 0 {
			t.Fatalf("last segment %s: non-zero bytes after its frames", filepath.Base(s.path))
		}
	}
}

// Whenever the process could crash (after any acked append, across
// prepared rolls) the directory must stay openable by the binary before
// segment preparation, with every acked record intact. That is what
// makes the feature safe to roll back.
func TestZZWP5PreallocDirectoryStaysReadableByOlderBinaries(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, zzWP5PreallocOptions())
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range 4 {
		n = zzWP5AppendThroughRolls(t, l, n, 1)
		zzWP5CheckOlderBinaryReadable(t, dir)
		for range 3 {
			if _, err := l.Append(context.Background(), zzWP5Payload(n)); err != nil {
				t.Fatal(err)
			}
			n++
			zzWP5CheckOlderBinaryReadable(t, dir)
		}
	}
	l.mu.Lock()
	prepared := l.activePrepared
	l.mu.Unlock()
	if !prepared {
		t.Fatal("the active segment is not a prepared one: the test did not exercise preparation")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	zzWP5CheckOlderBinaryReadable(t, dir)
	zzWP5ReplayAll(t, dir, n)
}

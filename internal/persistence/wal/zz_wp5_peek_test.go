package wal

import (
	"errors"
	"os"
	"testing"
)

// zzWP5PeekLog appends n records to a fresh open log.
func zzWP5PeekLog(t *testing.T, n int) (*Log, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	zzWP5AppendConcurrently(t, l, n)
	return l, dir
}

// A peek sees every record's id and resume cursor before its payload is
// read; the records it skips are never allocated and never reach fn, the
// others arrive intact. The cursors match what a full replay reports.
func TestZZWP5ReplayPeekSkipsBeforeReading(t *testing.T) {
	const n = 512
	l, _ := zzWP5PeekLog(t, n)
	want := map[uint64]Cursor{}
	if err := l.ReplayFromCursor(Cursor{}, func(r Record, next Cursor) error {
		want[r.ID.Seq] = next
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	peeked, delivered := 0, 0
	err := l.ReplayFromCursorPeek(Cursor{}, func(id RecordID, next Cursor) (bool, error) {
		if next != want[id.Seq] {
			return false, errors.New("peek cursor differs from the replay cursor")
		}
		peeked++
		return id.Seq%8 != 0, nil
	}, func(r Record, next Cursor) error {
		if r.ID.Seq%8 != 0 || next != want[r.ID.Seq] {
			return errors.New("fn got a skipped record or a wrong cursor")
		}
		delivered++
		return nil
	})
	if err != nil || peeked != n || delivered != n/8 {
		t.Fatalf("peek replay: err=%v peeked=%d delivered=%d", err, peeked, delivered)
	}

	allocs := testing.AllocsPerRun(20, func() {
		if err := l.ReplayFromCursorPeek(Cursor{}, func(id RecordID, _ Cursor) (bool, error) {
			return id.Seq%8 != 0, nil
		}, func(Record, Cursor) error { return nil }); err != nil {
			t.Fatal(err)
		}
	})
	// The delivered eighth allocates its payloads; the fixed per-pass
	// cost is a handful of objects.
	if max := float64(n/8 + 16); allocs > max {
		t.Fatalf("peek replay allocated %.0f objects per pass, want at most %.0f: skipped records must not be allocated", allocs, max)
	}
}

// Records below the cursor's seq are passed over the same way.
func TestZZWP5ReplayBelowFromIsNotAllocated(t *testing.T) {
	const n = 512
	l, dir := zzWP5PeekLog(t, n)
	for _, replay := range []func(fn func(Record, Cursor) error) error{
		func(fn func(Record, Cursor) error) error { return l.ReplayFromCursor(Cursor{Seq: n - 8}, fn) },
		func(fn func(Record, Cursor) error) error { return ReplayFromCursor(dir, Cursor{Seq: n - 8}, 0, fn) },
	} {
		allocs := testing.AllocsPerRun(20, func() {
			count := 0
			if err := replay(func(Record, Cursor) error { count++; return nil }); err != nil || count != 8 {
				t.Fatalf("replay: %v count=%d", err, count)
			}
		})
		// 8 payloads plus the fixed cost of a pass (directory listing,
		// open); a record below the cursor used to cost its payload too.
		if allocs > 8+32 {
			t.Fatalf("replaying the last 8 of %d records allocated %.0f objects", n, allocs)
		}
	}
}

// Skipping does not skip verification: a corrupt payload is reported
// whether or not anyone wanted the record.
func TestZZWP5ReplayPeekStillVerifiesSkippedRecords(t *testing.T) {
	const n = 64
	l, dir := zzWP5PeekLog(t, n)
	var victim RecordID
	if err := l.ReplayFromCursor(Cursor{}, func(r Record, _ Cursor) error {
		if r.ID.Seq == 10 {
			victim = r.ID
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(segmentPath(dir, victim.SegmentBase), os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{'#'}, victim.Offset+frameHeaderSize+3); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	err = l.ReplayFromCursorPeek(Cursor{}, func(RecordID, Cursor) (bool, error) { return true, nil }, func(Record, Cursor) error {
		return errors.New("fn called for a skipped record")
	})
	if !errors.Is(err, errCorruptFrame) {
		t.Fatalf("peek replay over a corrupt skipped record = %v, want a checksum failure", err)
	}
	if err := l.ReplayFromCursor(Cursor{Seq: 11}, func(Record, Cursor) error { return nil }); !errors.Is(err, errCorruptFrame) {
		t.Fatalf("replay past a corrupt record below the cursor seq = %v, want a checksum failure", err)
	}
}

// A peek error stops the replay and comes back unchanged.
func TestZZWP5ReplayPeekErrorStops(t *testing.T) {
	l, _ := zzWP5PeekLog(t, 32)
	stop := errors.New("stop")
	calls := 0
	err := l.ReplayFromCursorPeek(Cursor{}, func(id RecordID, _ Cursor) (bool, error) {
		calls++
		if id.Seq == 5 {
			return false, stop
		}
		return false, nil
	}, func(Record, Cursor) error { return nil })
	if !errors.Is(err, stop) || calls != 6 {
		t.Fatalf("err=%v calls=%d, want the peek's error after 6 calls", err, calls)
	}
}

// A sealed segment can end in a torn frame (its final sync lied and the
// crash kept part of it). Peek promises complete frames, so it must not
// be asked about that one.
func TestZZWP5ReplayPeekNeverSeesATornFrame(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 2 << 10, Prealloc: PreallocOff}
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	zzWP5AppendConcurrently(t, l, 40)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	segments, err := listSegments(dir)
	if err != nil || len(segments) < 2 {
		t.Fatalf("segments %v: %v", segments, err)
	}
	// Cut the first segment inside its last frame.
	first := segments[0].path
	info, err := os.Stat(first)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(first, info.Size()-5); err != nil {
		t.Fatal(err)
	}
	var lastWhole uint64
	if err := ReplayFromCursor(dir, Cursor{}, 0, func(r Record, _ Cursor) error {
		if r.ID.SegmentBase == segments[0].base {
			lastWhole = r.ID.Seq
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	torn := lastWhole + 1

	l, err = Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.ReplayFromCursorPeek(Cursor{}, func(id RecordID, _ Cursor) (bool, error) {
		if id.Seq == torn && id.SegmentBase == segments[0].base {
			return false, errors.New("peek was asked about the torn frame")
		}
		return false, nil
	}, func(Record, Cursor) error { return nil }); err != nil {
		t.Fatal(err)
	}
}

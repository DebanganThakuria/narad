package wal

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

// zzWP5AppendConcurrently appends n distinct records from 16 goroutines
// so the log batches them; record i's payload is zzWP5Record(i).
func zzWP5AppendConcurrently(t *testing.T, l *Log, n int) {
	t.Helper()
	const workers = 16
	errs := make(chan error, workers)
	for w := range workers {
		go func() {
			for i := w; i < n; i += workers {
				if _, err := l.Append(context.Background(), zzWP5Record(i)); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	for range workers {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func zzWP5Record(i int) []byte {
	return fmt.Appendf(nil, "rec-%05d-%s", i, bytes.Repeat([]byte("z"), 100+i%13))
}

// Replay allocated the frame header once per record (it escaped through
// the io.Reader call) and a 64 KiB read buffer per segment on every
// call; the dispatcher replays every few milliseconds. The payload, which
// callers keep, is now the only per-record allocation.
func TestZZWP5ReplayAllocatesOnlyThePayload(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Options{SegmentBytes: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	const n = 512
	zzWP5AppendConcurrently(t, l, n)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(20, func() {
		count := 0
		if err := ReplayFromCursor(dir, Cursor{}, 0, func(r Record, _ Cursor) error {
			if !bytes.HasPrefix(r.Payload, []byte("rec-")) {
				return fmt.Errorf("record %d: payload %q", r.ID.Seq, r.Payload)
			}
			count++
			return nil
		}); err != nil || count != n {
			t.Fatalf("replay: %v count=%d", err, count)
		}
	})
	if perRecord := allocs / n; perRecord > 1.1 {
		t.Fatalf("replay allocates %.2f objects per record (%.0f per pass of %d), want only the payload", perRecord, allocs, n)
	}
}

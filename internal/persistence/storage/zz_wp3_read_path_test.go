package storage

import (
	"path/filepath"
	"testing"
	"time"
)

// A read below the durable tail is served from the segment files and
// never touches the write buffer or the flushing snapshot, whose mutexes
// the appenders and the flusher hold: with both held by someone else the
// read still completes.
func TestWP3ReadBelowDurableTailSkipsWriteBufferLocks(t *testing.T) {
	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	first, _ := wp3CommitBatch(t, l, wp3Records(7, 190))

	l.buffer.mu.Lock()
	l.flushingMu.Lock()
	unlocked := false
	unlock := func() {
		if !unlocked {
			unlocked = true
			l.flushingMu.Unlock()
			l.buffer.mu.Unlock()
		}
	}
	defer unlock()

	done := make(chan error, 1)
	go func() {
		_, err := l.ReadShared(first + 3)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ReadShared: %v", err)
		}
	case <-time.After(2 * time.Second):
		unlock()
		<-done
		t.Fatal("ReadShared of a durable offset waited on the write buffer / flushing snapshot mutexes")
	}
}

// A read served by the frame cache allocates nothing.
func TestWP3CachedReadAllocatesNothing(t *testing.T) {
	l, err := NewLog(filepath.Join(t.TempDir(), "p0"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	first, _ := wp3CommitBatch(t, l, wp3Records(7, 190))
	if _, err := l.ReadShared(first); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(200, func() {
		if _, err := l.ReadShared(first + 2); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("cached ReadShared allocates %.1f objects per call, want 0", allocs)
	}
}

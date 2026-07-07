package storage

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestAppendCloseRaceDoesNotLoseAckedRecord schedule-forces the
// Append/Close race via appendGateHook: the append passes the closed
// check, then Close runs concurrently. Close must block on the append
// gate until the buffer push completes, so the acked record is picked
// up by the flusher's final drain instead of being silently dropped
// (and its offset reassigned after reopen).
func TestAppendCloseRaceDoesNotLoseAckedRecord(t *testing.T) {
	dir := t.TempDir()
	l, err := NewLog(dir, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}

	closeErr := make(chan error, 1)
	var once sync.Once
	appendGateHook = func() {
		once.Do(func() {
			go func() { closeErr <- l.Close() }()
			// Give Close time to CAS closed=true and reach the append
			// gate; it must park there until this append's push lands.
			time.Sleep(50 * time.Millisecond)
		})
	}
	t.Cleanup(func() { appendGateHook = nil })

	payload := []byte("must-survive-close")
	off, err := l.Append(payload)
	appendGateHook = nil
	if err != nil {
		// The append observed closed=false; it must not fail.
		t.Fatalf("Append() error = %v, want success", err)
	}
	if err := <-closeErr; err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Appends that arrive after Close must lose cleanly with ErrLogClosed.
	if _, err := l.Append([]byte("late")); !errors.Is(err, ErrLogClosed) {
		t.Fatalf("Append() after Close error = %v, want %v", err, ErrLogClosed)
	}
	if _, _, err := l.AppendBatch([][]byte{[]byte("late")}); !errors.Is(err, ErrLogClosed) {
		t.Fatalf("AppendBatch() after Close error = %v, want %v", err, ErrLogClosed)
	}

	reopened, err := NewLog(dir, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("NewLog(reopen) error = %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if got := reopened.NextOffset(); got != off+1 {
		t.Fatalf("NextOffset() after reopen = %d, want %d (acked record was lost)", got, off+1)
	}
	got, err := reopened.Read(off)
	if err != nil {
		t.Fatalf("Read(%d) after reopen error = %v", off, err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("Read(%d) after reopen = %q, want %q", off, got, payload)
	}
}

// TestAppendCloseRaceConcurrentHammer is a non-deterministic companion
// to the schedule-forced test above: every Append that returns success
// while Close races it must be present after reopen.
func TestAppendCloseRaceConcurrentHammer(t *testing.T) {
	for round := range 20 {
		dir := t.TempDir()
		l, err := NewLog(dir, slowFlushOpts(t, nil))
		if err != nil {
			t.Fatalf("round %d: NewLog() error = %v", round, err)
		}

		var maxAcked atomic.Int64
		maxAcked.Store(-1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				off, err := l.Append([]byte("rec"))
				if err != nil {
					return
				}
				maxAcked.Store(off) // offsets are monotonically increasing
			}
		}()

		time.Sleep(time.Duration(round%5) * 100 * time.Microsecond)
		if err := l.Close(); err != nil {
			t.Fatalf("round %d: Close() error = %v", round, err)
		}
		<-done

		reopened, err := NewLog(dir, slowFlushOpts(t, nil))
		if err != nil {
			t.Fatalf("round %d: NewLog(reopen) error = %v", round, err)
		}
		if got, want := reopened.NextOffset(), maxAcked.Load()+1; got != want {
			t.Fatalf("round %d: NextOffset() after reopen = %d, want %d (acked records lost)", round, got, want)
		}
		if err := reopened.Close(); err != nil {
			t.Fatalf("round %d: Close(reopen) error = %v", round, err)
		}
	}
}

// TestNotifyBroadcastWakesAllWaiters pins the broadcast semantics of
// NotifyC: a single AdvanceHighWatermark must wake EVERY blocked
// waiter, not just one.
func TestNotifyBroadcastWakesAllWaiters(t *testing.T) {
	l, err := NewLog(t.TempDir(), slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	defer func() { _ = l.Close() }()

	if _, _, err := l.AppendBatch([][]byte{[]byte("a"), []byte("b")}); err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}

	const waiters = 4
	var wg sync.WaitGroup
	woken := make(chan struct{}, waiters)
	for range waiters {
		ch := l.NotifyC() // fetched BEFORE the advance, as consumers do
		wg.Go(func() {
			select {
			case <-ch:
				woken <- struct{}{}
			case <-time.After(2 * time.Second):
			}
		})
	}

	if err := l.AdvanceHighWatermark(2); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}
	wg.Wait()
	if len(woken) != waiters {
		t.Fatalf("woken waiters = %d, want %d (notify is not a broadcast)", len(woken), waiters)
	}
}

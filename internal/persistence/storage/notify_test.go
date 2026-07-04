package storage

import "testing"

// notifyChanIdentity returns the current broadcast channel WITHOUT
// registering as a waiter (unlike NotifyC, which sets the
// waiter-presence flag).
func notifyChanIdentity(l *Log) chan struct{} {
	l.notifyMu.Lock()
	defer l.notifyMu.Unlock()
	return l.notify
}

// TestNotifyAllNoopsWithoutWaiters pins the zero-cost no-waiter path:
// Append, AppendBatch, AdvanceHighWatermark, and Wake must not close or
// replace the broadcast channel (i.e. must not allocate) when nobody
// fetched it via NotifyC — and must still broadcast once a waiter has.
func TestNotifyAllNoopsWithoutWaiters(t *testing.T) {
	l, err := NewLog(t.TempDir(), DefaultOptions())
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	before := notifyChanIdentity(l)

	if _, err := l.Append([]byte("a")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, _, err := l.AppendBatch([][]byte{[]byte("b"), []byte("c")}); err != nil {
		t.Fatalf("AppendBatch() error = %v", err)
	}
	if err := l.AdvanceHighWatermark(3); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}
	l.Wake()

	after := notifyChanIdentity(l)
	if before != after {
		t.Fatal("append/HWM/Wake with no registered waiters replaced the notify channel; want no-op")
	}
	select {
	case <-before:
		t.Fatal("notify channel closed with no registered waiters")
	default:
	}

	// Broadcast semantics must be intact once a waiter registers: the
	// fetched channel is closed synchronously by the next notification
	// and a fresh channel installed for the next round.
	w1 := l.NotifyC()
	w2 := l.NotifyC()
	if _, err := l.Append([]byte("d")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	select {
	case <-w1:
	default:
		t.Fatal("Append() did not broadcast to the first registered waiter")
	}
	select {
	case <-w2:
	default:
		t.Fatal("Append() did not broadcast to the second registered waiter")
	}
	if notifyChanIdentity(l) == before {
		t.Fatal("broadcast did not install a fresh notify channel")
	}
}

// TestWakeBroadcastsToRegisteredWaiter pins Wake() specifically: it is
// the purger/ack release path's wake-up, so with a waiter registered it
// must close the fetched channel even though no log state changed.
func TestWakeBroadcastsToRegisteredWaiter(t *testing.T) {
	l, err := NewLog(t.TempDir(), DefaultOptions())
	if err != nil {
		t.Fatalf("NewLog() error = %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	w := l.NotifyC()
	l.Wake()
	select {
	case <-w:
	default:
		t.Fatal("Wake() did not broadcast to a registered waiter")
	}
}

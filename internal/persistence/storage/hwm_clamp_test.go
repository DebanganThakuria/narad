package storage

import "testing"

// syncHighWatermark must never persist a visibility boundary past the
// durably-fsynced tail. The commit path upholds this by advancing the HWM
// only after Sync()+VerifyDurable, so the clamp is a no-op in practice; this
// verifies the guard itself, independent of caller discipline.
func TestSyncHighWatermarkClampsToDurableTail(t *testing.T) {
	l, err := NewLog(testLogPath(t), slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	defer l.Close()

	// Simulate an HWM raised ahead of the durable tail.
	l.durableTail.Store(3)
	l.highWatermark.Store(10)

	if err := l.syncHighWatermark(true); err != nil {
		t.Fatalf("syncHighWatermark: %v", err)
	}

	if got := l.persistedHWM.Load(); got != 3 {
		t.Fatalf("persistedHWM = %d, want 3 (clamped to durableTail)", got)
	}
	persisted, err := l.PersistedHighWatermark()
	if err != nil {
		t.Fatalf("PersistedHighWatermark: %v", err)
	}
	if persisted != 3 {
		t.Fatalf("persisted hwm file = %d, want 3", persisted)
	}
}

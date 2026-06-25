package storage

import (
	"fmt"
	"os"
	"testing"
)

// The high-watermark is persisted in place (fixed 8-byte overwrite + fsync, no
// temp file or rename) on every forced sync. This verifies that mechanism is
// durable mid-run — without a clean Close — and that the file never grows or
// leaves stale tail bytes across repeated overwrites, so a restart restores the
// exact last-persisted value. (Per-commit persistence of the visible boundary
// is a durability requirement: a record once exposed must stay exposed across a
// crash; the in-place write only makes that persist cheap, not less frequent.)
func TestHighWatermarkInPlacePersistDurableMidRun(t *testing.T) {
	path := testLogPath(t)
	l, err := NewLog(path, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}

	var want int64
	for i := range 5 {
		if _, err := l.Append(fmt.Appendf(nil, "rec-%d", i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		want = int64(i + 1)
		if err := l.AdvanceHighWatermark(want); err != nil {
			t.Fatalf("AdvanceHighWatermark(%d): %v", want, err)
		}
		// Forced sync persists the current HWM in place.
		if err := l.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
	}

	// Fixed-size file: an in-place 8-byte overwrite must never grow the file or
	// leave a stale tail (which loadHighWatermark would reject as != 8 bytes).
	info, err := os.Stat(hwmFilePath(path))
	if err != nil {
		t.Fatalf("stat hwm: %v", err)
	}
	if info.Size() != 8 {
		t.Fatalf("hwm file size = %d, want 8", info.Size())
	}

	// Durable mid-run: the persisted value is readable from disk without a
	// clean Close (i.e. it would survive a crash here).
	persisted, err := l.PersistedHighWatermark()
	if err != nil {
		t.Fatalf("PersistedHighWatermark: %v", err)
	}
	if persisted != want {
		t.Fatalf("persisted HWM = %d, want %d", persisted, want)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Restart restores the exact persisted HWM.
	l2, err := NewLog(path, slowFlushOpts(t, nil))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if got := l2.HighWatermark(); got != want {
		t.Fatalf("HWM after restart = %d, want %d", got, want)
	}
}

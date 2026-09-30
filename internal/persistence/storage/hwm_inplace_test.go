package storage

import (
	"fmt"
	"os"
	"testing"
)

// The high-watermark file is emptied before a log's first advance and
// written in place (fixed 8-byte overwrite + fsync, no temp file or
// rename) at Close. This verifies that mid-run, without a clean Close, a
// restart would still recover every exposed record; that Close leaves
// exactly 8 bytes (never a grown file or a stale tail, which
// loadHighWatermark would reject), fsyncing the directory for a file
// the log created; and that a restart restores the exact value.
func TestHighWatermarkInPlacePersistDurableMidRun(t *testing.T) {
	path := testLogPath(t)
	var want int64
	for life := range 2 {
		l, err := NewLog(path, slowFlushOpts(t, nil))
		if err != nil {
			t.Fatalf("NewLog: %v", err)
		}
		if got := l.HighWatermark(); got != want {
			t.Fatalf("life %d: HWM after restart = %d, want %d", life, got, want)
		}
		for i := range 5 {
			if _, err := l.Append(fmt.Appendf(nil, "rec-%d-%d", life, i)); err != nil {
				t.Fatalf("Append %d: %v", i, err)
			}
			if err := l.Sync(); err != nil {
				t.Fatalf("Sync %d: %v", i, err)
			}
			want++
			if err := l.AdvanceHighWatermark(want); err != nil {
				t.Fatalf("AdvanceHighWatermark(%d): %v", want, err)
			}
		}

		// Durable mid-run: what a restart would recover right now covers
		// every exposed record (i.e. it would survive a crash here),
		// because the released file defers to the record tail.
		if info, err := os.Stat(hwmFilePath(path)); err != nil || info.Size() != 0 {
			t.Fatalf("life %d: hwm file mid-run = (%v, %v), want an empty file", life, info, err)
		}
		persisted, err := l.PersistedHighWatermark()
		if err != nil {
			t.Fatalf("PersistedHighWatermark: %v", err)
		}
		if persisted != want {
			t.Fatalf("life %d: persisted HWM = %d, want %d", life, persisted, want)
		}
		if err := l.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// In the first life the release CREATED the file, and a new name
		// is only durable once the parent directory has been fsynced;
		// Close does that when it writes the boundary (otherwise a crash
		// can lose the file and recovery would expose the hidden tail).
		l.hwmMu.Lock()
		dirSynced := l.hwmDirSynced
		l.hwmMu.Unlock()
		if !dirSynced {
			t.Fatalf("life %d: partition dir not fsynced for the hwm file", life)
		}
		info, err := os.Stat(hwmFilePath(path))
		if err != nil {
			t.Fatalf("stat hwm: %v", err)
		}
		if info.Size() != 8 {
			t.Fatalf("life %d: hwm file size = %d, want 8", life, info.Size())
		}
		if got, ok, err := ReadPersistedHighWatermark(path); err != nil || !ok || got != want {
			t.Fatalf("life %d: persisted after Close = (%d, %v, %v), want %d", life, got, ok, err, want)
		}
	}
}

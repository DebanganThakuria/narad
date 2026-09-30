package syncfile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// A SyncData may run while another goroutine closes the file: the
// consumer offset committer writes a tick's files out without its lock,
// and a Forget closes a partition's held descriptor under it. The sync
// must hold a reference on the descriptor for the call, so the close
// neither races with reading it nor frees the number for another open
// to reuse mid-flush; a sync that loses the race reports os.ErrClosed.
// On Linux, SyncData read the descriptor with f.Fd(), which the race
// detector reports against Close, and a closed file gave EBADF.
func TestZZShipSyncDataConcurrentClose(t *testing.T) {
	dir := t.TempDir()
	for i := range 200 {
		f, err := os.Create(filepath.Join(dir, "data"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write([]byte("chunk")); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- SyncData(f) }()
		_ = f.Close()
		if err := <-done; err != nil && !errors.Is(err, os.ErrClosed) {
			t.Fatalf("round %d: SyncData racing Close = %v, want nil or os.ErrClosed", i, err)
		}
	}
}

// A closed file keeps os.File.Sync's error shape on every platform,
// which callers match with errors.Is(err, os.ErrClosed) (storage's
// segment close, the committer's device flush).
func TestZZShipSyncDataClosedFileIsErrClosed(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	err = SyncData(f)
	var pathErr *os.PathError
	if !errors.Is(err, os.ErrClosed) || !errors.As(err, &pathErr) || pathErr.Op != "sync" {
		t.Fatalf("SyncData on a closed file = %v, want *os.PathError{Op: sync} wrapping os.ErrClosed", err)
	}
}

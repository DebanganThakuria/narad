package ingress

import (
	"os"
	"path/filepath"
	"testing"
)

// The checkpoint is overwritten in place through a held descriptor: the
// file stays exactly 8 bytes, no temp files are left, and every stored
// value reads back. An empty file (crash between create and first
// write) reads as 0, the same as a missing file.
func TestCheckpointInPlaceStoreAndLoad(t *testing.T) {
	dir := t.TempDir()
	w := newCheckpointWriter(dir, produceCheckpointFile)
	path := filepath.Join(dir, produceCheckpointFile)

	for _, seq := range []uint64{1, 5, 4, 1 << 40} {
		if err := w.store(seq); err != nil {
			t.Fatalf("store(%d): %v", seq, err)
		}
		got, err := loadCheckpoint(path)
		if err != nil || got != seq {
			t.Fatalf("load after store(%d) = (%d, %v)", seq, got, err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != produceCheckpointFile {
		t.Fatalf("dir entries = %d, want only the checkpoint file", len(entries))
	}
	if info, _ := os.Stat(path); info.Size() != 8 {
		t.Fatalf("size = %d, want 8", info.Size())
	}
	if err := w.close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen path (a fresh writer over an existing file) keeps working.
	w2 := newCheckpointWriter(dir, produceCheckpointFile)
	if err := w2.store(77); err != nil {
		t.Fatalf("store after reopen: %v", err)
	}
	if got, _ := loadCheckpoint(path); got != 77 {
		t.Fatalf("load after reopen = %d, want 77", got)
	}
	_ = w2.close()

	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got, err := loadCheckpoint(path); err != nil || got != 0 {
		t.Fatalf("empty checkpoint = (%d, %v), want (0, nil)", got, err)
	}
	if err := os.WriteFile(path, []byte{1, 2, 3}, 0o644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}
	if _, err := loadCheckpoint(path); err == nil {
		t.Fatal("3-byte checkpoint must be rejected")
	}
}

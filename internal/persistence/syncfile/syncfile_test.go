package syncfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncDataAppendedBytesReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	for i := 0; i < 3; i++ {
		if _, err := f.Write([]byte("chunk")); err != nil {
			t.Fatal(err)
		}
		if err := SyncData(f); err != nil {
			t.Fatalf("SyncData after append %d: %v", i, err)
		}
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "chunkchunkchunk" {
		t.Fatalf("read back %q", got)
	}
}

func TestSyncDataClosedFileErrors(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "closed")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := SyncData(f); err == nil {
		t.Fatal("SyncData on closed file: want error, got nil")
	}
}

// Benchmarks: on Linux these show the fdatasync vs fsync gap that
// motivated the package; on other platforms both call the same
// primitive and should match.
func BenchmarkSyncData(b *testing.B) { benchSync(b, SyncData) }
func BenchmarkFullSync(b *testing.B) { benchSync(b, (*os.File).Sync) }

func benchSync(b *testing.B, sync func(*os.File) error) {
	f, err := os.OpenFile(filepath.Join(b.TempDir(), "bench"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		b.Fatal(err)
	}
	defer f.Close()
	payload := make([]byte, 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Write(payload); err != nil {
			b.Fatal(err)
		}
		if err := sync(f); err != nil {
			b.Fatal(err)
		}
	}
}

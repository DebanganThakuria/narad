package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// assertPrivate fails when path is readable by anyone but its owner.
func assertPrivate(t *testing.T, path string) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("%s has mode %o, want no group/other bits", path, perm)
	}
}

// The WAL directory, its first segment, and every rolled segment are
// private to the broker's user: the WAL holds every accepted payload.
func TestWALFilesAndDirectoryArePrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ingress", "produce")
	opts := testOptions()
	opts.SegmentBytes = 64 // roll after a couple of records
	log, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for range 8 {
		if _, err := log.Append(context.Background(), []byte("payload-payload-payload")); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	assertPrivate(t, filepath.Dir(dir))
	assertPrivate(t, dir)
	segments, err := listSegments(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) < 2 {
		t.Fatalf("segments = %d, want a rolled segment too", len(segments))
	}
	for _, s := range segments {
		assertPrivate(t, s.path)
	}
}

package metrics

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, path string, n int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, n), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The incremental scanner must report exactly what a full walk reports
// across the lifecycle of a data directory: sealed segments cached,
// active segments re-stat'ed, deletions noticed, new files counted, tmp
// files skipped.
func TestDirSizeScannerMatchesFullWalk(t *testing.T) {
	root := t.TempDir()
	p0 := filepath.Join(root, "topics", "orders", "p00000")
	writeFile(t, filepath.Join(p0, "00000000000000000000.log"), 1000)
	writeFile(t, filepath.Join(p0, "00000000000000000500.log"), 300) // active
	writeFile(t, filepath.Join(p0, "hwm"), 8)
	writeFile(t, filepath.Join(p0, "consumer.offset"), 8)
	writeFile(t, filepath.Join(p0, "consumer.offset.123.tmp"), 8) // skipped
	wal := filepath.Join(root, "ingress", "produce")
	writeFile(t, filepath.Join(wal, "00000000000000000000.wal"), 2000)
	writeFile(t, filepath.Join(wal, "00000000000000000900.wal"), 100)
	writeFile(t, filepath.Join(root, "raft", "raft.db"), 4096)

	s := newDirSizeScanner()
	check := func(step string) {
		t.Helper()
		want, err := dirSizeBytes(root)
		if err != nil {
			t.Fatalf("%s: walk: %v", step, err)
		}
		got, err := s.scan(root)
		if err != nil {
			t.Fatalf("%s: scan: %v", step, err)
		}
		if got != want {
			t.Fatalf("%s: scanner = %d, full walk = %d", step, got, want)
		}
	}
	check("initial")
	if len(s.sealed) != 2 {
		t.Fatalf("sealed cache = %d entries, want 2 (one .log, one .wal)", len(s.sealed))
	}

	// Active segment grows: must be re-stat'ed, not cached.
	writeFile(t, filepath.Join(p0, "00000000000000000500.log"), 700)
	check("active grew")

	// A roll seals the previous active segment and adds a new active one.
	writeFile(t, filepath.Join(p0, "00000000000000000900.log"), 50)
	check("rolled")
	if len(s.sealed) != 3 {
		t.Fatalf("sealed cache = %d entries, want 3", len(s.sealed))
	}

	// Retention deletes the oldest sealed segment; compaction deletes a WAL segment.
	if err := os.Remove(filepath.Join(p0, "00000000000000000000.log")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(wal, "00000000000000000000.wal")); err != nil {
		t.Fatal(err)
	}
	check("deleted")
	if len(s.sealed) != 1 {
		t.Fatalf("sealed cache = %d entries after deletions, want 1", len(s.sealed))
	}

	// A whole partition directory disappears (topic delete) and a new one appears.
	if err := os.RemoveAll(p0); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, "topics", "events", "p00001", "00000000000000000000.log"), 42)
	check("topic churn")

	if _, err := s.scan(filepath.Join(root, "missing")); err != nil {
		t.Fatalf("missing root must scan as empty, got %v", err)
	}
}

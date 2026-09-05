package storage

import (
	"path/filepath"
	"testing"
)

// StatPartitionDir must agree with the open log's own counters and
// must describe a never-written or closed partition without creating
// anything.
func TestStatPartitionDirMatchesOpenLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "p00000")

	empty, err := StatPartitionDir(dir)
	if err != nil {
		t.Fatalf("StatPartitionDir(missing dir): %v", err)
	}
	if empty != (PartitionDirStats{}) {
		t.Fatalf("stats of a missing partition dir = %+v, want zero", empty)
	}

	log, err := NewLog(dir, Options{})
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	for i := range 5 {
		if _, err := log.Append([]byte{byte(i)}); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
	}
	if err := log.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := log.AdvanceHighWatermark(5); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	wantSegments, wantOldest, wantSize := log.SegmentCount(), log.OldestOffset(), log.SizeBytes()
	wantAt, ok := log.OldestSegmentAt()
	if !ok {
		t.Fatal("OldestSegmentAt not available on an open log with data")
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, err := StatPartitionDir(dir)
	if err != nil {
		t.Fatalf("StatPartitionDir: %v", err)
	}
	want := PartitionDirStats{
		Segments: wantSegments, OldestOffset: wantOldest, SizeBytes: wantSize,
		OldestSegmentAt: wantAt, HighWatermark: 5,
	}
	if got != want {
		t.Fatalf("StatPartitionDir = %+v, want %+v", got, want)
	}
}

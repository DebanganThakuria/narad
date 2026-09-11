package storage

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestConsumerAheadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	if _, ok, err := ReadConsumerAhead(dir); err != nil || ok {
		t.Fatalf("ReadConsumerAhead(empty dir) = ok %v, err %v; want none", ok, err)
	}
	// Unsorted, duplicated, and one entry at the frontier that must drop.
	if err := WriteConsumerAhead(dir, 0, 7, 10, []int64{15, 12, 12, 10, 40}); err != nil {
		t.Fatalf("WriteConsumerAhead() error = %v", err)
	}
	rec, ok, err := ReadConsumerAhead(dir)
	if err != nil || !ok {
		t.Fatalf("ReadConsumerAhead() = ok %v, err %v", ok, err)
	}
	if rec.Seq != 7 || rec.Committed != 10 || !slices.Equal(rec.Offsets, []int64{12, 15, 40}) {
		t.Fatalf("recovered %+v, want seq 7 committed 10 offsets [12 15 40]", rec)
	}
	if n, err := consumerAheadFileSize(dir); err != nil || n != consumerAheadSlotSize {
		t.Fatalf("file size after one write = %d, %v; want one slot", n, err)
	}
}

func TestConsumerAheadNewestSlotWins(t *testing.T) {
	dir := t.TempDir()
	if err := WriteConsumerAhead(dir, 0, 1, 0, []int64{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := WriteConsumerAhead(dir, 1, 2, 5, []int64{9}); err != nil {
		t.Fatal(err)
	}
	rec, ok, _ := ReadConsumerAhead(dir)
	if !ok || rec.Seq != 2 || !slices.Equal(rec.Offsets, []int64{9}) {
		t.Fatalf("recovered %+v, want the seq-2 record", rec)
	}
	// A third write goes back to slot 0 and must win over slot 1.
	if err := WriteConsumerAhead(dir, 2, 3, 20, []int64{21, 30}); err != nil {
		t.Fatal(err)
	}
	rec, ok, _ = ReadConsumerAhead(dir)
	if !ok || rec.Seq != 3 || rec.Committed != 20 || !slices.Equal(rec.Offsets, []int64{21, 30}) {
		t.Fatalf("recovered %+v, want the seq-3 record", rec)
	}
}

// TestConsumerAheadTornWriteFallsBack pins the crash story: a slot
// damaged mid-write fails its checksum and the reader uses the other one.
func TestConsumerAheadTornWriteFallsBack(t *testing.T) {
	dir := t.TempDir()
	if err := WriteConsumerAhead(dir, 0, 1, 0, []int64{3, 4}); err != nil {
		t.Fatal(err)
	}
	if err := WriteConsumerAhead(dir, 1, 2, 2, []int64{5}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, consumerAheadFileName)
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Flip one payload byte of the newest slot (slot 1).
	if _, err := f.WriteAt([]byte{0xff}, consumerAheadSlotSize+consumerAheadHdrLen); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	rec, ok, err := ReadConsumerAhead(dir)
	if err != nil || !ok {
		t.Fatalf("ReadConsumerAhead() = ok %v, err %v", ok, err)
	}
	if rec.Seq != 1 || !slices.Equal(rec.Offsets, []int64{3, 4}) {
		t.Fatalf("recovered %+v, want the intact seq-1 record", rec)
	}
	// Both slots damaged: reads as none, never as an error.
	if err := os.WriteFile(path, make([]byte, 2*consumerAheadSlotSize), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := ReadConsumerAhead(dir); err != nil || ok {
		t.Fatalf("garbage file: ok %v err %v, want none", ok, err)
	}
}

// TestConsumerAheadOverflowKeepsLowest pins the truncation rule: a set
// that does not fit one slot keeps its lowest offsets, so the frontier
// hole and everything right above it recover, and only the far tail
// costs duplicates.
func TestConsumerAheadOverflowKeepsLowest(t *testing.T) {
	// Deltas of 200 need two varint bytes each: ~2030 fit in a slot.
	offsets := make([]int64, 0, 5000)
	for i := range 5000 {
		offsets = append(offsets, 1000+int64(i)*200)
	}
	rec, ok := decodeConsumerAheadSlot(EncodeConsumerAhead(9, 0, offsets))
	if !ok {
		t.Fatal("encoded slot does not decode")
	}
	if len(rec.Offsets) == 0 || len(rec.Offsets) >= len(offsets) {
		t.Fatalf("kept %d of %d offsets, want a strict prefix", len(rec.Offsets), len(offsets))
	}
	if !slices.Equal(rec.Offsets, offsets[:len(rec.Offsets)]) {
		t.Fatal("kept offsets are not the lowest prefix")
	}
	if len(rec.Offsets) < 2000 {
		t.Fatalf("kept only %d offsets; the slot should hold about 2000 two-byte deltas", len(rec.Offsets))
	}
}

func TestConsumerAheadEmptyRecordAndMissingDir(t *testing.T) {
	dir := t.TempDir()
	if err := WriteConsumerAhead(dir, 0, 1, 3, nil); err != nil {
		t.Fatal(err)
	}
	rec, ok, err := ReadConsumerAhead(dir)
	if err != nil || !ok || rec.Committed != 3 || len(rec.Offsets) != 0 {
		t.Fatalf("empty record: %+v ok %v err %v", rec, ok, err)
	}
	if err := WriteConsumerAhead(filepath.Join(dir, "gone"), 0, 2, 0, []int64{1}); err != ErrPartitionDirMissing {
		t.Fatalf("missing dir error = %v, want ErrPartitionDirMissing", err)
	}
}

package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func zzWP11BCursorBytes(t *testing.T, dir, child string, c FanoutCursor) []byte {
	t.Helper()
	if err := WriteFanoutCursorIfPartitionDirExists(dir, child, c); err != nil {
		t.Fatalf("write cursor %+v: %v", c, err)
	}
	buf, err := os.ReadFile(filepath.Join(dir, fanoutCursorFileName(child)))
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

// A reader that catches an in-place cursor write half done sees the
// start of one record and the rest of another. That mix must never read
// back as a valid cursor at an offset nobody wrote (98332 and 99100
// splice to 99132, past the true position: a silent skip for whoever
// resumes from it). Every splice of two same-length records either
// reads as one of them or fails as corrupt, and a move refuses to ship
// it.
func TestZZWP11BFanoutCursorSplicedRecordIsNeverAValidCursor(t *testing.T) {
	dir := t.TempDir()
	a := FanoutCursor{Epoch: "0123456789abcdef", NextOffset: 98332}
	b := FanoutCursor{Epoch: "0123456789abcdef", NextOffset: 99100}
	bufA := zzWP11BCursorBytes(t, dir, "child", a)
	bufB := zzWP11BCursorBytes(t, dir, "child", b)
	if len(bufA) != len(bufB) {
		t.Fatalf("records for the same epoch differ in length: %d vs %d", len(bufA), len(bufB))
	}
	first, last := -1, -1
	for i := range bufA {
		if bufA[i] != bufB[i] {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	if first < 0 {
		t.Fatal("records for different offsets are identical")
	}
	path := filepath.Join(dir, fanoutCursorFileName("child"))
	for _, pair := range [][2][]byte{{bufA, bufB}, {bufB, bufA}} {
		for k := first; k <= last+1; k++ {
			spliced := append(append([]byte{}, pair[0][:k]...), pair[1][k:]...)
			if err := os.WriteFile(path, spliced, 0o600); err != nil {
				t.Fatal(err)
			}
			got, ok, err := ReadFanoutCursor(dir, "child")
			if err == nil && ok && got != a && got != b {
				t.Fatalf("splice at byte %d reads as %+v: a cursor nobody wrote (wrote %d and %d)", k, got, a.NextOffset, b.NextOffset)
			}
			if err != nil || !ok {
				if files, lerr := ListFanoutCursorFiles(dir); lerr == nil {
					t.Fatalf("splice at byte %d: ReadFanoutCursor failed (%v) but ListFanoutCursorFiles shipped %q", k, err, files[0].Data)
				}
			}
		}
	}
}

// BenchmarkZZWP11BFanoutCursorRead measures ReadFanoutCursor.
func BenchmarkZZWP11BFanoutCursorRead(b *testing.B) {
	dir := b.TempDir()
	if err := WriteFanoutCursorIfPartitionDirExists(dir, "child", FanoutCursor{Epoch: "0123456789abcdef", NextOffset: 123456}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, ok, err := ReadFanoutCursor(dir, "child"); err != nil || !ok {
			b.Fatal(ok, err)
		}
	}
}

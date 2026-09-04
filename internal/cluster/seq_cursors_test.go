package cluster

import (
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

func TestSeqCursorsDenseAndSparse(t *testing.T) {
	var c seqCursors
	if _, ok := c.get(3); ok {
		t.Fatal("empty seqCursors must report missing")
	}
	c.set(10, wal.Cursor{Seq: 11})
	c.set(11, wal.Cursor{Seq: 12})
	c.set(14, wal.Cursor{Seq: 15}) // gap: 12 and 13 never scanned
	for seq, want := range map[uint64]uint64{10: 11, 11: 12, 14: 15} {
		got, ok := c.get(seq)
		if !ok || got.Seq != want {
			t.Fatalf("get(%d) = (%+v, %v), want seq %d", seq, got, ok, want)
		}
	}
	for _, seq := range []uint64{9, 12, 13, 15} {
		if _, ok := c.get(seq); ok {
			t.Fatalf("get(%d) must report missing", seq)
		}
	}
	c.set(5, wal.Cursor{Seq: 6}) // below base: ignored, never happens in a monotonic scan
	if _, ok := c.get(5); ok {
		t.Fatal("seq below base must not be stored")
	}
}

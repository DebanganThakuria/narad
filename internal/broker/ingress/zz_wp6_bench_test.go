package ingress

import (
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

// BenchmarkZZWP6StoreProduceCheckpoint is the checkpoint write a
// progressing dispatch pass makes before its next pass can start.
func BenchmarkZZWP6StoreProduceCheckpoint(b *testing.B) {
	m, err := OpenManager(b.TempDir(), wal.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer m.Close()
	var seq uint64
	b.ReportAllocs()
	for b.Loop() {
		seq++
		if err := m.StoreProduceCheckpoint(seq); err != nil {
			b.Fatal(err)
		}
	}
}

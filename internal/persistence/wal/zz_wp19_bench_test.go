package wal

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

// BenchmarkZZWP19AppendManyWith is one appender staging batches of 300
// byte records: every call waits for one group commit, so ns/record is
// that commit divided by the batch size.
func BenchmarkZZWP19AppendManyWith(b *testing.B) {
	for _, size := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			l, err := Open(b.TempDir(), Options{SegmentBytes: 1 << 30})
			if err != nil {
				b.Fatal(err)
			}
			payload := bytes.Repeat([]byte("m"), 300)
			sizes := make([]int, size)
			for i := range sizes {
				sizes[i] = len(payload)
			}
			fill := func(_ int, dst []byte) []byte { return append(dst, payload...) }
			b.ReportAllocs()
			for b.Loop() {
				if _, err := l.AppendManyWith(context.Background(), sizes, fill); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/record")
			if err := l.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

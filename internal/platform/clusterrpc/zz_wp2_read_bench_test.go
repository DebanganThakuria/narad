package clusterrpc

import (
	"bufio"
	"io"
	"sync/atomic"
	"testing"

	"github.com/debanganthakuria/narad/internal/protocol/clusterwire"
)

// zzWP2Repeat yields the same bytes forever, a fixed number per Read
// (like a stream handing over what arrived in one packet).
type zzWP2Repeat struct {
	data  []byte
	pos   int
	chunk int
}

func (r *zzWP2Repeat) Read(p []byte) (int, error) {
	p = p[:min(len(p), r.chunk)]
	n := 0
	for n < len(p) {
		c := copy(p[n:], r.data[r.pos:])
		n += c
		r.pos = (r.pos + c) % len(r.data)
	}
	return n, nil
}

// The client read path with and without the read counter the liveness
// probe uses: the counter must be noise next to reading a frame.
func BenchmarkZZWP2ClientFrameRead(b *testing.B) {
	frame := clusterwire.AppendStreamFrame(nil, clusterwire.StreamFrame{Type: clusterwire.StreamFrameNodeReply, RequestID: 7, Payload: make([]byte, 16)})
	for _, tc := range []struct {
		name string
		wrap func(io.Reader) io.Reader
	}{
		{"plain", func(r io.Reader) io.Reader { return r }},
		{"counted", func(r io.Reader) io.Reader {
			var reads atomic.Uint64
			return countingReader{r: r, reads: &reads}
		}},
	} {
		// One frame per underlying read: the counter's worst case.
		b.Run(tc.name, func(b *testing.B) {
			reader := bufio.NewReader(tc.wrap(&zzWP2Repeat{data: frame, chunk: len(frame)}))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := clusterwire.ReadStreamFrame(reader, clusterwire.MaxStreamFramePayloadBytes); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

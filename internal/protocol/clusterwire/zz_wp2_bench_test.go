package clusterwire

import (
	"bufio"
	"testing"
)

// zzWP2RepeatReader yields the same bytes forever, so a benchmark can
// read an unbounded run of identical frames.
type zzWP2RepeatReader struct {
	data []byte
	pos  int
}

func (r *zzWP2RepeatReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		c := copy(p[n:], r.data[r.pos:])
		n += c
		r.pos = (r.pos + c) % len(r.data)
	}
	return n, nil
}

func benchmarkZZWP2ReadStreamFrame(b *testing.B, payloadBytes int) {
	frame := AppendStreamFrame(nil, StreamFrame{Type: StreamFrameNodeReply, RequestID: 7, Payload: make([]byte, payloadBytes)})
	reader := bufio.NewReader(&zzWP2RepeatReader{data: frame})
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	for b.Loop() {
		if _, err := ReadStreamFrame(reader, MaxStreamFramePayloadBytes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZZWP2ReadStreamFrameBufio(b *testing.B) {
	b.Run("small", func(b *testing.B) { benchmarkZZWP2ReadStreamFrame(b, 32) })
	b.Run("64KiB", func(b *testing.B) { benchmarkZZWP2ReadStreamFrame(b, 64<<10) })
}

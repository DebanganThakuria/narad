package clusterwire

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

// Reading through a *bufio.Reader parses the header in the reader's
// buffer: the frame's payload is the only allocation.
func TestZZWP2BufferedReadAllocatesOnlyThePayload(t *testing.T) {
	if zzWP2Race {
		t.Skip("allocation counts differ under the race detector")
	}
	frame := AppendStreamFrame(nil, StreamFrame{Type: StreamFrameNodeReply, RequestID: 7, Payload: []byte("reply")})
	reader := bufio.NewReader(&zzWP2RepeatReader{data: frame})
	got := testing.AllocsPerRun(100, func() {
		if _, err := ReadStreamFrame(reader, MaxStreamFramePayloadBytes); err != nil {
			t.Fatal(err)
		}
	})
	if got != 1 {
		t.Fatalf("buffered ReadStreamFrame allocs = %v, want 1 (the payload)", got)
	}
}

// zzWP2ReadAll reads frames from r until the first error.
func zzWP2ReadAll(r io.Reader, maxPayload int) ([]StreamFrame, error) {
	var frames []StreamFrame
	for {
		frame, err := ReadStreamFrame(r, maxPayload)
		if err != nil {
			return frames, err
		}
		frames = append(frames, frame)
	}
}

// FuzzZZWP2BufferedReadMatchesUnbuffered: the buffered header path must
// read exactly the frames, and fail with exactly the errors (io.EOF
// between frames, io.ErrUnexpectedEOF inside one, the same rejections),
// as the plain io.ReadFull path, whatever the buffer size and wherever
// the refills fall.
func FuzzZZWP2BufferedReadMatchesUnbuffered(f *testing.F) {
	two := append(frameBytes(StreamFrame{Type: StreamFrameNodeRequest, RequestID: 42, Payload: []byte("payload")}),
		frameBytes(StreamFrame{Type: StreamFramePing, RequestID: 7})...)
	f.Add(two, 0, 16)
	f.Add(two, 0, 20)
	f.Add(two, 0, 23)
	f.Add(two, 4, 4096)
	f.Add(two[:30], 0, 4096)
	f.Add(two[:10], 0, 32)
	f.Add([]byte{}, 0, 4096)
	f.Add([]byte("NRS2 not a header at all"), 0, 64)
	f.Add(frameBytes(StreamFrame{Type: StreamFrameNodeReply, Payload: make([]byte, 5000)}), 0, 4096)
	f.Fuzz(func(t *testing.T, data []byte, maxPayload, size int) {
		if size < 16 || size > 1<<16 {
			size = 4096
		}
		plain, plainErr := zzWP2ReadAll(bytes.NewReader(data), maxPayload)
		buffered, bufferedErr := zzWP2ReadAll(bufio.NewReaderSize(bytes.NewReader(data), size), maxPayload)
		if len(plain) != len(buffered) {
			t.Fatalf("plain read %d frames, buffered %d", len(plain), len(buffered))
		}
		for i := range plain {
			if !bytes.Equal(frameBytes(plain[i]), frameBytes(buffered[i])) {
				t.Fatalf("frame %d differs: plain %+v buffered %+v", i, plain[i], buffered[i])
			}
		}
		if plainErr.Error() != bufferedErr.Error() {
			t.Fatalf("plain read ended with %v, buffered with %v", plainErr, bufferedErr)
		}
	})
}

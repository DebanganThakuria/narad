package clusterwire

import (
	"bufio"
	"bytes"
	"errors"
	"testing"
)

// zzWP2WriteRecorder keeps every byte written and each Write's size; a
// failAt >= 0 makes the Write with that index fail after taking n bytes.
type zzWP2WriteRecorder struct {
	data   bytes.Buffer
	sizes  []int
	failAt int
	n      int
}

var errZZWP2Write = errors.New("write failed")

func (w *zzWP2WriteRecorder) Write(p []byte) (int, error) {
	w.sizes = append(w.sizes, len(p))
	if w.failAt == len(w.sizes)-1 {
		w.data.Write(p[:w.n])
		return w.n, errZZWP2Write
	}
	return w.data.Write(p)
}

// A frame that fits maxStaged goes out in one Write; a bigger one as its
// header and then its payload, without growing the staging buffer. The
// bytes on the wire are the same either way.
func TestZZWP2StagedWriteSplitsOnlyBigFrames(t *testing.T) {
	const maxStaged = 1 << 10
	for _, size := range []int{0, 100, maxStaged - streamFrameHeaderBytes, maxStaged - streamFrameHeaderBytes + 1, 64 << 10} {
		frame := StreamFrame{Type: StreamFrameNodeRequest, RequestID: 9, Payload: bytes.Repeat([]byte{'p'}, size)}
		w := &zzWP2WriteRecorder{failAt: -1}
		buf, n, err := WriteStreamFrameStaged(w, make([]byte, 0, 32), frame, maxStaged)
		if err != nil {
			t.Fatalf("payload %d: %v", size, err)
		}
		if want := AppendStreamFrame(nil, frame); !bytes.Equal(w.data.Bytes(), want) || n != len(want) {
			t.Fatalf("payload %d: wrote %d bytes (reported %d), want the %d-byte frame", size, w.data.Len(), n, len(want))
		}
		wantWrites := 1
		if streamFrameHeaderBytes+size > maxStaged {
			wantWrites = 2
			if cap(buf) != 32 {
				t.Fatalf("payload %d: staging buffer grew to %d for an unstaged frame", size, cap(buf))
			}
		}
		if len(w.sizes) != wantWrites {
			t.Fatalf("payload %d: %d writes %v, want %d", size, len(w.sizes), w.sizes, wantWrites)
		}
		got, err := ReadStreamFrame(bufio.NewReader(&w.data), MaxStreamFramePayloadBytes)
		if err != nil || got.RequestID != frame.RequestID || !bytes.Equal(got.Payload, frame.Payload) {
			t.Fatalf("payload %d: read back %+v, %v", size, got.RequestID, err)
		}
	}
}

// The byte count lets a caller tell a write that sent nothing from one
// cut off part-way: a failed header write reports what it took, a failed
// payload write adds the whole header.
func TestZZWP2StagedWriteReportsBytesWritten(t *testing.T) {
	frame := StreamFrame{Type: StreamFrameNodeRequest, RequestID: 1, Payload: make([]byte, 4096)}
	for _, tc := range []struct {
		failAt, n, want int
	}{
		{failAt: 0, n: 0, want: 0},
		{failAt: 0, n: 7, want: 7},
		{failAt: 1, n: 0, want: streamFrameHeaderBytes},
		{failAt: 1, n: 100, want: streamFrameHeaderBytes + 100},
	} {
		w := &zzWP2WriteRecorder{failAt: tc.failAt, n: tc.n}
		_, n, err := WriteStreamFrameStaged(w, nil, frame, 1024)
		if !errors.Is(err, errZZWP2Write) || n != tc.want {
			t.Fatalf("write %d failing after %d bytes: n = %d, err = %v; want n = %d", tc.failAt, tc.n, n, err, tc.want)
		}
	}
	if _, n, err := WriteStreamFrameStaged(&zzWP2WriteRecorder{failAt: -1}, nil, StreamFrame{Payload: make([]byte, MaxStreamFramePayloadBytes+1)}, 1024); err == nil || n != 0 {
		t.Fatalf("oversized frame: n = %d, err = %v; want a refusal with nothing written", n, err)
	}
}

// Writing a frame too big to stage allocates nothing: no buffer of its
// size, no copy.
func TestZZWP2StagedWriteDoesNotAllocateForBigFrames(t *testing.T) {
	if zzWP2Race {
		t.Skip("allocation counts differ under the race detector")
	}
	frame := StreamFrame{Type: StreamFrameNodeReply, RequestID: 3, Payload: make([]byte, 1<<20)}
	buf := make([]byte, 0, 64)
	got := testing.AllocsPerRun(20, func() {
		buf, _, _ = WriteStreamFrameStaged(zzWP2Discard{}, buf, frame, 256<<10)
	})
	if got != 0 {
		t.Fatalf("allocs per 1 MiB frame write = %v, want 0", got)
	}
}

type zzWP2Discard struct{}

func (zzWP2Discard) Write(p []byte) (int, error) { return len(p), nil }

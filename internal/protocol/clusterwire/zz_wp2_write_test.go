package clusterwire

import (
	"bufio"
	"bytes"
	"errors"
	"slices"
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

// zzWP2Lead is how much of a big frame's payload goes out with its
// header when the staging cap is maxStaged.
func zzWP2Lead(maxStaged int) int {
	return min(stagedLeadBytes, maxStaged-streamFrameHeaderBytes)
}

// A frame that fits maxStaged goes out in one Write; a bigger one as its
// header and the start of its payload, then the rest of the payload in
// place, without staging more than the lead. The bytes on the wire are
// the same either way.
func TestZZWP2StagedWriteSplitsOnlyBigFrames(t *testing.T) {
	for _, maxStaged := range []int{1 << 10, 64 << 10} {
		lead := zzWP2Lead(maxStaged)
		for _, size := range []int{0, 100, maxStaged - streamFrameHeaderBytes, maxStaged - streamFrameHeaderBytes + 1, 1 << 20} {
			frame := StreamFrame{Type: StreamFrameNodeRequest, RequestID: 9, Payload: bytes.Repeat([]byte{'p'}, size)}
			w := &zzWP2WriteRecorder{failAt: -1}
			buf, n, err := WriteStreamFrameStaged(w, make([]byte, 0, 32), frame, maxStaged)
			if err != nil {
				t.Fatalf("cap %d, payload %d: %v", maxStaged, size, err)
			}
			if want := AppendStreamFrame(nil, frame); !bytes.Equal(w.data.Bytes(), want) || n != len(want) {
				t.Fatalf("cap %d, payload %d: wrote %d bytes (reported %d), want the %d-byte frame", maxStaged, size, w.data.Len(), n, len(want))
			}
			wantSizes := []int{streamFrameHeaderBytes + size}
			if streamFrameHeaderBytes+size > maxStaged {
				wantSizes = []int{streamFrameHeaderBytes + lead, size - lead}
				if len(buf) != streamFrameHeaderBytes+lead {
					t.Fatalf("cap %d, payload %d: staged %d bytes of an unstaged frame, want header and lead (%d)", maxStaged, size, len(buf), streamFrameHeaderBytes+lead)
				}
			}
			if !slices.Equal(w.sizes, wantSizes) {
				t.Fatalf("cap %d, payload %d: writes %v, want %v", maxStaged, size, w.sizes, wantSizes)
			}
			got, err := ReadStreamFrame(bufio.NewReader(&w.data), MaxStreamFramePayloadBytes)
			if err != nil || got.RequestID != frame.RequestID || !bytes.Equal(got.Payload, frame.Payload) {
				t.Fatalf("cap %d, payload %d: read back %+v, %v", maxStaged, size, got.RequestID, err)
			}
		}
	}
}

// A big frame's first Write must not be small enough for a QUIC stream
// to buffer without sending (quic-go takes up to one 1452-byte packet),
// or a frame that made no progress would look cut off after its header.
func TestZZWP2StagedWriteLeadsWithMoreThanAPacket(t *testing.T) {
	const quicPacketBuffer = 1452
	frame := StreamFrame{Type: StreamFrameNodeRequest, RequestID: 1, Payload: make([]byte, 1<<20)}
	w := &zzWP2WriteRecorder{failAt: -1}
	if _, _, err := WriteStreamFrameStaged(w, nil, frame, 256<<10); err != nil {
		t.Fatal(err)
	}
	if len(w.sizes) != 2 || w.sizes[0] <= quicPacketBuffer {
		t.Fatalf("writes %v, want two with the first over %d bytes", w.sizes, quicPacketBuffer)
	}
}

// The byte count lets a caller tell a write that sent nothing from one
// cut off part-way: a failed first write reports what it took, a failed
// second write adds the whole header and lead.
func TestZZWP2StagedWriteReportsBytesWritten(t *testing.T) {
	const maxStaged = 1024
	first := streamFrameHeaderBytes + zzWP2Lead(maxStaged)
	frame := StreamFrame{Type: StreamFrameNodeRequest, RequestID: 1, Payload: make([]byte, 4096)}
	for _, tc := range []struct {
		failAt, n, want int
	}{
		{failAt: 0, n: 0, want: 0},
		{failAt: 0, n: 7, want: 7},
		{failAt: 1, n: 0, want: first},
		{failAt: 1, n: 100, want: first + 100},
	} {
		w := &zzWP2WriteRecorder{failAt: tc.failAt, n: tc.n}
		_, n, err := WriteStreamFrameStaged(w, nil, frame, maxStaged)
		if !errors.Is(err, errZZWP2Write) || n != tc.want {
			t.Fatalf("write %d failing after %d bytes: n = %d, err = %v; want n = %d", tc.failAt, tc.n, n, err, tc.want)
		}
	}
	if _, n, err := WriteStreamFrameStaged(&zzWP2WriteRecorder{failAt: -1}, nil, StreamFrame{Payload: make([]byte, MaxStreamFramePayloadBytes+1)}, maxStaged); err == nil || n != 0 {
		t.Fatalf("oversized frame: n = %d, err = %v; want a refusal with nothing written", n, err)
	}
}

// Writing a frame too big to stage allocates nothing: no buffer of its
// size, no copy of it (the lead is staged in the retained buffer).
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

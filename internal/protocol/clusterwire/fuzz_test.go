package clusterwire

// Fuzz targets for the stream framing. Frames are read off a QUIC
// stream by both the server and the client read loops, so a framing
// bug is reachable from any peer that completed the auth handshake
// (and, for the 64-byte pre-auth read, from any peer at all).

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"
)

func frameBytes(frame StreamFrame) []byte {
	return AppendStreamFrame(nil, frame)
}

// FuzzReadStreamFrame reads frames back to back from arbitrary bytes.
// Every frame that reads must be a prefix of the input, must re-encode
// to the bytes it was read from (reserved bytes aside), and must honour
// the payload cap; a short or corrupt header is an error, never a panic.
func FuzzReadStreamFrame(f *testing.F) {
	f.Add(frameBytes(StreamFrame{Type: StreamFrameNodeRequest, RequestID: 42, Payload: []byte("payload")}), 0)
	f.Add(frameBytes(StreamFrame{Type: StreamFramePing, RequestID: 7}), 0)
	f.Add(append(frameBytes(StreamFrame{Type: StreamFrameAuth, Payload: bytes.Repeat([]byte{1}, 32)}),
		frameBytes(StreamFrame{Type: StreamFrameCancel, RequestID: 1})...), 64)
	f.Add(frameBytes(StreamFrame{Type: StreamFrameError, RequestID: 3, Payload: []byte{0, 4, 'b', 'o', 'o', 'm'}}), 0)
	f.Add([]byte{}, 0)
	f.Add([]byte("NRS1"), 0)
	// A header claiming a 4 GiB payload with nothing behind it.
	f.Add([]byte{'N', 'R', 'S', '1', 1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0xff, 0xff, 0xff, 0xff}, 0)
	// A payload just over a small cap.
	f.Add(frameBytes(StreamFrame{Type: StreamFrameNodeReply, Payload: make([]byte, 65)}), 64)

	f.Fuzz(func(t *testing.T, data []byte, maxPayload int) {
		cap := maxPayload
		if cap <= 0 {
			cap = MaxStreamFramePayloadBytes
		}
		r := bytes.NewReader(data)
		consumed := 0
		for {
			frame, err := ReadStreamFrame(r, maxPayload)
			if err != nil {
				if len(data)-consumed >= streamFrameHeaderBytes {
					// A full header was available: the failure must have
					// been a real rejection, or a short payload.
					hdr := data[consumed : consumed+streamFrameHeaderBytes]
					claimed := int(binary.BigEndian.Uint32(hdr[16:20]))
					valid := binary.BigEndian.Uint32(hdr[0:4]) == streamMagic && hdr[4] == streamVersion && claimed <= cap
					if valid && len(data)-consumed-streamFrameHeaderBytes >= claimed {
						t.Fatalf("frame at %d rejected although complete and within cap: %v", consumed, err)
					}
				}
				return
			}
			if len(frame.Payload) > cap {
				t.Fatalf("payload of %d bytes exceeds cap %d", len(frame.Payload), cap)
			}
			n := streamFrameHeaderBytes + len(frame.Payload)
			if consumed+n > len(data) {
				t.Fatalf("frame claims %d bytes past the end of the input", consumed+n-len(data))
			}
			// The reserved bytes are ignored on read; everything else must
			// re-encode to exactly what was read.
			want := bytes.Clone(data[consumed : consumed+n])
			want[6], want[7] = 0, 0
			if got := frameBytes(frame); !bytes.Equal(got, want) {
				t.Fatalf("re-encoded frame differs from its wire bytes\n got %x\nwant %x", got, want)
			}
			if remaining := r.Len(); remaining != len(data)-consumed-n {
				t.Fatalf("reader consumed %d bytes, want %d", len(data)-consumed-remaining, n)
			}
			consumed += n
		}
	})
}

// FuzzStreamFrameRoundTrip writes a fuzzer-chosen frame and reads it back.
func FuzzStreamFrameRoundTrip(f *testing.F) {
	f.Add(uint8(StreamFrameNodeRequest), uint64(42), []byte("payload"))
	f.Add(uint8(StreamFramePing), uint64(0), []byte{})
	f.Add(uint8(255), uint64(math.MaxUint64), bytes.Repeat([]byte{0xff}, 1000))

	f.Fuzz(func(t *testing.T, typ uint8, id uint64, payload []byte) {
		want := StreamFrame{Type: StreamFrameType(typ), RequestID: id, Payload: payload}
		var buf bytes.Buffer
		if err := WriteStreamFrame(&buf, want); err != nil {
			t.Fatalf("WriteStreamFrame: %v", err)
		}
		if buf.Len() != streamFrameHeaderBytes+len(payload) {
			t.Fatalf("wrote %d bytes, want %d", buf.Len(), streamFrameHeaderBytes+len(payload))
		}
		got, err := ReadStreamFrame(&buf, 0)
		if err != nil {
			t.Fatalf("ReadStreamFrame: %v", err)
		}
		if got.Type != want.Type || got.RequestID != want.RequestID || !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("round trip\n got %+v\nwant %+v", got, want)
		}
		if buf.Len() != 0 {
			t.Fatalf("%d bytes left unread", buf.Len())
		}
		// Truncating the frame anywhere must fail with an EOF-class error.
		full := frameBytes(want)
		for _, cut := range []int{0, streamFrameHeaderBytes - 1, len(full) - 1} {
			if cut < 0 || cut >= len(full) {
				continue
			}
			if _, err := ReadStreamFrame(bytes.NewReader(full[:cut]), 0); err == nil {
				t.Fatalf("truncated frame (%d of %d bytes) was accepted", cut, len(full))
			} else if err != io.EOF && err != io.ErrUnexpectedEOF {
				t.Fatalf("truncated frame (%d of %d bytes): err = %v, want EOF", cut, len(full), err)
			}
		}
	})
}

// FuzzDecodeStreamError feeds arbitrary bytes to the error-payload
// decoder. A decoded message must be exactly the bytes after the length
// prefix, and its re-encoding must be the input: the encoding is
// canonical, so trailing bytes are a framing bug and must be rejected.
func FuzzDecodeStreamError(f *testing.F) {
	seed, _ := EncodeStreamError("boom")
	f.Add(seed)
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{0, 0})
	f.Add([]byte{0, 1})
	f.Add([]byte{0xff, 0xff})
	f.Add([]byte{0, 1, 'x', 'y'})

	f.Fuzz(func(t *testing.T, payload []byte) {
		got, err := DecodeStreamError(payload)
		if err != nil {
			return
		}
		if len(got.Message)+2 != len(payload) {
			t.Fatalf("decoded %d message bytes from a %d-byte payload", len(got.Message), len(payload))
		}
		re, _ := EncodeStreamError(got.Message)
		if !bytes.Equal(re, payload) {
			t.Fatalf("re-encode differs\n got %x\nwant %x", re, payload)
		}
	})
}

// FuzzStreamErrorRoundTrip encodes arbitrary messages; those longer than
// the 16-bit length field are truncated, everything else must survive.
func FuzzStreamErrorRoundTrip(f *testing.F) {
	f.Add("boom")
	f.Add("")
	f.Add(string(bytes.Repeat([]byte("x"), math.MaxUint16+1)))

	f.Fuzz(func(t *testing.T, message string) {
		payload, err := EncodeStreamError(message)
		if err != nil {
			t.Fatalf("EncodeStreamError: %v", err)
		}
		got, err := DecodeStreamError(payload)
		if err != nil {
			t.Fatalf("DecodeStreamError: %v", err)
		}
		want := message
		if len(want) > math.MaxUint16 {
			want = want[:math.MaxUint16]
		}
		if got.Message != want {
			t.Fatalf("round trip: got %d bytes, want %d", len(got.Message), len(want))
		}
	})
}

package clusterwire

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestStreamFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	want := StreamFrame{
		Type:      StreamFrameNodeRequest,
		RequestID: 42,
		Payload:   []byte("payload"),
	}
	if err := WriteStreamFrame(&buf, want); err != nil {
		t.Fatalf("WriteStreamFrame() error = %v", err)
	}
	got, err := ReadStreamFrame(&buf, 0)
	if err != nil {
		t.Fatalf("ReadStreamFrame() error = %v", err)
	}
	if got.Type != want.Type || got.RequestID != want.RequestID || string(got.Payload) != string(want.Payload) {
		t.Fatalf("frame = %+v, want %+v", got, want)
	}
}

func TestStreamFrameEmptyPayload(t *testing.T) {
	var buf bytes.Buffer
	want := StreamFrame{Type: StreamFramePing, RequestID: 7}
	if err := WriteStreamFrame(&buf, want); err != nil {
		t.Fatalf("WriteStreamFrame() error = %v", err)
	}
	got, err := ReadStreamFrame(&buf, 0)
	if err != nil {
		t.Fatalf("ReadStreamFrame() error = %v", err)
	}
	if got.Type != want.Type || got.RequestID != want.RequestID || len(got.Payload) != 0 {
		t.Fatalf("frame = %+v, want %+v", got, want)
	}
}

// countingWriter records how many Write calls it received and the bytes.
type countingWriter struct {
	bytes.Buffer
	writes int
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.Buffer.Write(p)
}

// legacyEncode is the pre-single-write layout: header then payload as two
// separate writes. The new encoder must produce identical bytes.
func legacyEncode(frame StreamFrame) []byte {
	var header [streamFrameHeaderBytes]byte
	binary.BigEndian.PutUint32(header[0:4], streamMagic)
	header[4] = streamVersion
	header[5] = byte(frame.Type)
	binary.BigEndian.PutUint64(header[8:16], frame.RequestID)
	binary.BigEndian.PutUint32(header[16:20], uint32(len(frame.Payload)))
	return append(append([]byte(nil), header[:]...), frame.Payload...)
}

func TestWriteStreamFrameIssuesOneWriteWithIdenticalBytes(t *testing.T) {
	for _, frame := range []StreamFrame{
		{Type: StreamFrameNodeRequest, RequestID: 42, Payload: []byte("payload")},
		{Type: StreamFramePing, RequestID: 7},
		{Type: StreamFrameNodeReply, RequestID: 1 << 40, Payload: bytes.Repeat([]byte("x"), 70000)},
	} {
		var w countingWriter
		if err := WriteStreamFrame(&w, frame); err != nil {
			t.Fatalf("WriteStreamFrame() error = %v", err)
		}
		if w.writes != 1 {
			t.Fatalf("frame type %d: writes = %d, want 1", frame.Type, w.writes)
		}
		if !bytes.Equal(w.Bytes(), legacyEncode(frame)) {
			t.Fatalf("frame type %d: bytes differ from the legacy header+payload layout", frame.Type)
		}
	}
}

func TestWriteStreamFrameIntoReusesBuffer(t *testing.T) {
	var w countingWriter
	buf := make([]byte, 0, 1024)
	frame := StreamFrame{Type: StreamFrameNodeRequest, RequestID: 3, Payload: []byte("abc")}
	out, err := WriteStreamFrameInto(&w, buf, frame)
	if err != nil {
		t.Fatalf("WriteStreamFrameInto() error = %v", err)
	}
	if &out[:1][0] != &buf[:1][0] {
		t.Fatal("WriteStreamFrameInto() reallocated a buffer that had room")
	}
	if !bytes.Equal(w.Bytes(), legacyEncode(frame)) {
		t.Fatal("WriteStreamFrameInto() bytes differ from the legacy layout")
	}
	got, err := ReadStreamFrame(bytes.NewReader(w.Bytes()), 0)
	if err != nil {
		t.Fatalf("ReadStreamFrame() error = %v", err)
	}
	if got.Type != frame.Type || got.RequestID != frame.RequestID || string(got.Payload) != "abc" {
		t.Fatalf("frame = %+v, want %+v", got, frame)
	}
	if _, err := WriteStreamFrameInto(&w, nil, StreamFrame{Payload: make([]byte, MaxStreamFramePayloadBytes+1)}); err == nil {
		t.Fatal("oversized payload accepted")
	}
}

func TestStreamErrorRoundTrip(t *testing.T) {
	payload, err := EncodeStreamError("boom")
	if err != nil {
		t.Fatalf("EncodeStreamError() error = %v", err)
	}
	got, err := DecodeStreamError(payload)
	if err != nil {
		t.Fatalf("DecodeStreamError() error = %v", err)
	}
	if got.Message != "boom" {
		t.Fatalf("message = %q, want %q", got.Message, "boom")
	}
}

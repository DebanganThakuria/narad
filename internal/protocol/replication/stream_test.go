package replication

import (
	"bytes"
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

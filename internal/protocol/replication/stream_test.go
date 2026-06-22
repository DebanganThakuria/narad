package replication

import (
	"bytes"
	"testing"
)

func TestStreamFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	want := StreamFrame{
		Type:      StreamFrameAppendBatch,
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

func TestStreamAppendBatchRoundTrip(t *testing.T) {
	payload, err := EncodeStreamAppendBatch("orders", 3, 99, [][]byte{
		[]byte(`{"id":1}`),
		[]byte(`{"id":2}`),
	})
	if err != nil {
		t.Fatalf("EncodeStreamAppendBatch() error = %v", err)
	}
	got, err := DecodeStreamAppendBatch(payload)
	if err != nil {
		t.Fatalf("DecodeStreamAppendBatch() error = %v", err)
	}
	if got.Topic != "orders" || got.Partition != 3 || got.BaseOffset != 99 {
		t.Fatalf("append metadata = %+v", got)
	}
	if len(got.Payloads) != 2 || string(got.Payloads[0]) != `{"id":1}` || string(got.Payloads[1]) != `{"id":2}` {
		t.Fatalf("append payloads = %q", got.Payloads)
	}
}

func TestStreamAppendMultiRoundTrip(t *testing.T) {
	payload, err := EncodeStreamAppendMulti([]StreamAppendGroup{
		{
			Topic:      "orders",
			Partition:  3,
			BaseOffset: 99,
			Payloads: [][]byte{
				[]byte(`{"id":1}`),
				[]byte(`{"id":2}`),
			},
		},
		{
			Topic:      "payments",
			Partition:  1,
			BaseOffset: 7,
			Payloads: [][]byte{
				[]byte(`{"id":3}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("EncodeStreamAppendMulti() error = %v", err)
	}
	got, err := DecodeStreamAppendMulti(payload)
	if err != nil {
		t.Fatalf("DecodeStreamAppendMulti() error = %v", err)
	}
	if len(got.Groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(got.Groups))
	}
	if got.Groups[0].Topic != "orders" || got.Groups[0].Partition != 3 || got.Groups[0].BaseOffset != 99 {
		t.Fatalf("first group metadata = %+v", got.Groups[0])
	}
	if string(got.Groups[0].Payloads[1]) != `{"id":2}` {
		t.Fatalf("first group second payload = %s", got.Groups[0].Payloads[1])
	}
	if got.Groups[1].Topic != "payments" || got.Groups[1].Partition != 1 || got.Groups[1].BaseOffset != 7 {
		t.Fatalf("second group metadata = %+v", got.Groups[1])
	}
	if string(got.Groups[1].Payloads[0]) != `{"id":3}` {
		t.Fatalf("second group first payload = %s", got.Groups[1].Payloads[0])
	}
}

func TestStreamAppendResultsRoundTrip(t *testing.T) {
	payload, err := EncodeStreamAppendResults([]StreamAppendResult{
		{NextOffset: 2, ReplicaNextOffset: -1},
		{NextOffset: -1, ReplicaNextOffset: 7, Message: "replicate offset mismatch"},
	})
	if err != nil {
		t.Fatalf("EncodeStreamAppendResults() error = %v", err)
	}
	got, err := DecodeStreamAppendResults(payload)
	if err != nil {
		t.Fatalf("DecodeStreamAppendResults() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("results = %d, want 2", len(got))
	}
	if got[0].NextOffset != 2 || got[0].ReplicaNextOffset != -1 || got[0].Message != "" {
		t.Fatalf("first result = %+v", got[0])
	}
	if got[1].NextOffset != -1 || got[1].ReplicaNextOffset != 7 || got[1].Message != "replicate offset mismatch" {
		t.Fatalf("second result = %+v", got[1])
	}
}

func TestStreamErrorRoundTripWithReplicaOffset(t *testing.T) {
	payload, err := EncodeStreamError(7, "replicate offset mismatch")
	if err != nil {
		t.Fatalf("EncodeStreamError() error = %v", err)
	}
	got, err := DecodeStreamError(payload)
	if err != nil {
		t.Fatalf("DecodeStreamError() error = %v", err)
	}
	if got.ReplicaNextOffset != 7 || got.Message != "replicate offset mismatch" {
		t.Fatalf("stream error = %+v", got)
	}
}

package node

import (
	"bytes"
	"testing"
)

// The two remote operations are appended to the op block; their values
// are part of the wire and must never move.
func TestRemoteOperationValues(t *testing.T) {
	if OpAckBatch != 32 {
		t.Fatalf("OpAckBatch = %d, want 32", OpAckBatch)
	}
	if OpRemoteWrite != 35 || OpRemoteCheck != 36 {
		t.Fatalf("OpRemoteWrite, OpRemoteCheck = %d, %d, want 35, 36", OpRemoteWrite, OpRemoteCheck)
	}
}

func TestRemoteWriteRequestRoundTrip(t *testing.T) {
	cases := []RemoteWriteRequest{
		{SubOp: RemoteSubCreate, Actor: "alice", RequestID: "0123456789abcdef", Body: []byte(`{"name":"b"}`)},
		{SubOp: RemoteSubTopicDelete},
		{SubOp: RemoteSubDetach, Actor: "bob", Body: []byte{}},
	}
	for _, want := range cases {
		payload, err := EncodeRemoteWriteRequest(want)
		if err != nil {
			t.Fatalf("encode %+v: %v", want, err)
		}
		if op, _ := OperationOf(payload); op != OpRemoteWrite {
			t.Fatalf("leading op = %d, want %d", op, OpRemoteWrite)
		}
		got, err := DecodeRemoteWriteRequest(payload)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.SubOp != want.SubOp || got.Actor != want.Actor || got.RequestID != want.RequestID || !bytes.Equal(got.Body, want.Body) {
			t.Fatalf("round trip = %+v, want %+v", got, want)
		}
		if _, err := DecodeRemoteWriteRequest(append(bytes.Clone(payload), 0)); err == nil {
			t.Fatal("decode accepted a trailing byte")
		}
		if _, err := DecodeRemoteWriteRequest(payload[:len(payload)-1]); err == nil {
			t.Fatal("decode accepted a truncated payload")
		}
	}
}

func TestRemoteCheckRequestRoundTrip(t *testing.T) {
	for _, mode := range []string{RemoteCheckRun, RemoteCheckStatus, RemoteCheckUnshipped} {
		want := RemoteCheckRequest{Mode: mode, RequestID: "fedcba9876543210", Body: []byte(`{"remote":"b"}`)}
		payload, err := EncodeRemoteCheckRequest(want)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		if op, _ := OperationOf(payload); op != OpRemoteCheck {
			t.Fatalf("leading op = %d, want %d", op, OpRemoteCheck)
		}
		got, err := DecodeRemoteCheckRequest(payload)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Mode != want.Mode || got.RequestID != want.RequestID || !bytes.Equal(got.Body, want.Body) {
			t.Fatalf("round trip = %+v, want %+v", got, want)
		}
	}
}

func TestRemoteRequestsRejectWrongOp(t *testing.T) {
	write, err := EncodeRemoteWriteRequest(RemoteWriteRequest{SubOp: RemoteSubCreate})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRemoteCheckRequest(write); err == nil {
		t.Fatal("DecodeRemoteCheckRequest accepted an OpRemoteWrite payload")
	}
	check, err := EncodeRemoteCheckRequest(RemoteCheckRequest{Mode: RemoteCheckStatus})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeRemoteWriteRequest(check); err == nil {
		t.Fatal("DecodeRemoteWriteRequest accepted an OpRemoteCheck payload")
	}
}

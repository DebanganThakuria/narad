package node

import (
	"bytes"
	"testing"
)

func TestUserRequestRoundTrip(t *testing.T) {
	ops := []Operation{OpCreateUser, OpUpdateUser, OpDeleteUser}
	want := UserRequest{Username: "alice", Body: []byte(`{"username":"alice"}`)}
	for _, op := range ops {
		payload, err := EncodeUserRequest(op, want)
		if err != nil {
			t.Fatalf("op %d encode: %v", op, err)
		}
		if gotOp, _ := OperationOf(payload); gotOp != op {
			t.Fatalf("leading op = %d, want %d", gotOp, op)
		}
		got, err := DecodeUserRequest(payload, op)
		if err != nil {
			t.Fatalf("op %d decode: %v", op, err)
		}
		if got.Username != want.Username || !bytes.Equal(got.Body, want.Body) {
			t.Fatalf("op %d roundtrip = %+v, want %+v", op, got, want)
		}
	}
}

func TestDecodeUserRequestRejectsWrongOp(t *testing.T) {
	payload, err := EncodeUserRequest(OpCreateUser, UserRequest{Username: "x"})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := DecodeUserRequest(payload, OpDeleteUser); err == nil {
		t.Fatal("decode with wrong op = nil, want error")
	}
}

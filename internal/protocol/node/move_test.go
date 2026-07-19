package node

import "testing"

func TestCompleteMoveRequestRoundTrip(t *testing.T) {
	in := CompleteMoveRequest{Topic: "orders", Partition: 3, ExpectedOwner: "narad-2", TargetID: "narad-3"}
	p, err := EncodeCompleteMoveRequest(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeCompleteMoveRequest(p)
	if err != nil || got != in {
		t.Fatalf("round trip = %+v (err %v), want %+v", got, err, in)
	}
	if _, err := DecodeAbortMoveRequest(p); err == nil {
		t.Fatal("complete-move payload decoded as abort-move")
	}
}

func TestAbortMoveRequestRoundTrip(t *testing.T) {
	in := AbortMoveRequest{Topic: "orders", Partition: 5, ExpectedTarget: "narad-1"}
	p, err := EncodeAbortMoveRequest(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeAbortMoveRequest(p)
	if err != nil || got != in {
		t.Fatalf("round trip = %+v (err %v), want %+v", got, err, in)
	}
}

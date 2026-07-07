package node

import "testing"

func TestChildLinkRequestRoundTrip(t *testing.T) {
	for _, op := range []Operation{OpAttachChild, OpDetachChild} {
		want := ChildLinkRequest{Parent: "orders", Child: "audit", DelayMs: 3_600_000}
		payload, err := EncodeChildLinkRequest(op, want)
		if err != nil {
			t.Fatalf("encode(%d): %v", op, err)
		}
		got, err := DecodeChildLinkRequest(payload, op)
		if err != nil {
			t.Fatalf("decode(%d): %v", op, err)
		}
		if got != want {
			t.Fatalf("round trip(%d) = %+v, want %+v", op, got, want)
		}
	}
}

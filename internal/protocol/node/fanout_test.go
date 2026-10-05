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

// Attach and detach carry the caller the same way (see
// TestTopicRequestsCarryAnOptionalActor).
func TestChildLinkRequestCarriesAnOptionalActor(t *testing.T) {
	for _, op := range []Operation{OpAttachChild, OpDetachChild} {
		for _, want := range []ChildLinkRequest{
			{Parent: "orders", Child: "audit", DelayMs: 5},
			{Parent: "orders", Child: "audit", DelayMs: 5, Actor: "alice"},
		} {
			payload, err := EncodeChildLinkRequest(op, want)
			if err != nil {
				t.Fatalf("encode %+v: %v", want, err)
			}
			got, err := DecodeChildLinkRequest(payload, op)
			if err != nil {
				t.Fatalf("decode %+v: %v", want, err)
			}
			if got != want {
				t.Fatalf("round trip = %+v, want %+v", got, want)
			}
			r, err := opReader(payload, op)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = r.string()
			_, _ = r.string()
			_, _ = r.i64()
			err = r.done()
			switch {
			case want.Actor == "" && err != nil:
				t.Fatalf("a pre-actor decoder refused an actor-less link: %v", err)
			case want.Actor != "" && (err == nil || err.Error() != TrailingPayloadError):
				t.Fatalf("a pre-actor decoder on an actor link = %v, want %q", err, TrailingPayloadError)
			}
		}
	}
}

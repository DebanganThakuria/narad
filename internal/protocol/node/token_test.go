package node

import (
	"testing"
	"time"
)

func TestTokenDeltaRoundTrip(t *testing.T) {
	want := TokenDelta{
		From: "node-a.example:7942",
		Add: []TokenRegistration{
			{Topic: "orders", TTLNanos: int64(28 * time.Second)},
			{Topic: "payments", TTLNanos: int64(5 * time.Second)},
		},
		Drop: []string{"shipments", "refunds"},
	}
	payload, err := EncodeTokenDelta(want)
	if err != nil {
		t.Fatalf("EncodeTokenDelta() error = %v", err)
	}
	got, err := DecodeTokenDelta(payload)
	if err != nil {
		t.Fatalf("DecodeTokenDelta() error = %v", err)
	}
	if got.From != want.From || len(got.Add) != 2 || len(got.Drop) != 2 {
		t.Fatalf("decoded %+v, want %+v", got, want)
	}
	for i, a := range want.Add {
		if got.Add[i] != a {
			t.Fatalf("add[%d] = %+v, want %+v", i, got.Add[i], a)
		}
	}
	for i, d := range want.Drop {
		if got.Drop[i] != d {
			t.Fatalf("drop[%d] = %q, want %q", i, got.Drop[i], d)
		}
	}
}

// An empty delta is a legitimate frame (a peer with nothing to say),
// and must not decode into nil-vs-empty confusion.
func TestTokenDeltaRoundTripEmpty(t *testing.T) {
	payload, err := EncodeTokenDelta(TokenDelta{From: "n1:1"})
	if err != nil {
		t.Fatalf("EncodeTokenDelta() error = %v", err)
	}
	got, err := DecodeTokenDelta(payload)
	if err != nil {
		t.Fatalf("DecodeTokenDelta() error = %v", err)
	}
	if got.From != "n1:1" || len(got.Add) != 0 || len(got.Drop) != 0 {
		t.Fatalf("decoded %+v, want an empty delta from n1:1", got)
	}
}

func TestTokenDeltaRejectsOversizedBatch(t *testing.T) {
	huge := TokenDelta{From: "n1:1", Add: make([]TokenRegistration, maxTokenBatch+1)}
	if _, err := EncodeTokenDelta(huge); err == nil {
		t.Fatal("EncodeTokenDelta() accepted a batch over the cap, want an error")
	}
}

func TestTokenNotifyRoundTrip(t *testing.T) {
	payload, err := EncodeTokenNotifyRequest(TokenNotifyRequest{From: "node-a:7942", Topic: "orders"})
	if err != nil {
		t.Fatalf("EncodeTokenNotifyRequest() error = %v", err)
	}
	got, err := DecodeTokenNotifyRequest(payload)
	if err != nil {
		t.Fatalf("DecodeTokenNotifyRequest() error = %v", err)
	}
	if got.From != "node-a:7942" || got.Topic != "orders" {
		t.Fatalf("decoded %+v, want node-a:7942/orders", got)
	}
}

// Both token frames must accept a payload with bytes they do not
// understand on the end, because that is a NEWER peer sending a field
// this build predates. Rejecting the tail would mean a mixed-version
// cluster stops exchanging tokens the moment the format grows, which is
// exactly the rolling-upgrade hole this protocol already has once.
func TestTokenFramesTolerateTrailingBytes(t *testing.T) {
	delta, err := EncodeTokenDelta(TokenDelta{
		From: "node-a:7942",
		Add:  []TokenRegistration{{Topic: "orders", TTLNanos: int64(time.Second)}},
	})
	if err != nil {
		t.Fatalf("EncodeTokenDelta() error = %v", err)
	}
	gotDelta, err := DecodeTokenDelta(append(delta, 0xde, 0xad, 0xbe, 0xef))
	if err != nil {
		t.Fatalf("DecodeTokenDelta() with a future field appended: error = %v", err)
	}
	if len(gotDelta.Add) != 1 || gotDelta.Add[0].Topic != "orders" {
		t.Fatalf("decoded %+v, want the orders registration intact", gotDelta)
	}

	notify, err := EncodeTokenNotifyRequest(TokenNotifyRequest{From: "node-a:7942", Topic: "orders"})
	if err != nil {
		t.Fatalf("EncodeTokenNotifyRequest() error = %v", err)
	}
	gotNotify, err := DecodeTokenNotifyRequest(append(notify, 0x01, 0x02))
	if err != nil {
		t.Fatalf("DecodeTokenNotifyRequest() with a future field appended: error = %v", err)
	}
	if gotNotify.From != "node-a:7942" || gotNotify.Topic != "orders" {
		t.Fatalf("decoded %+v, want node-a:7942/orders", gotNotify)
	}
}

// A verdict is one byte, and anything unrecognised must read as a pass:
// the owner then offers the record elsewhere instead of holding it for a
// claim that is not coming.
func TestTokenNotifyReplyDefaultsToPass(t *testing.T) {
	if !DecodeTokenNotifyReply(EncodeTokenNotifyReply(TokenNotifyReply{Claiming: true})).Claiming {
		t.Fatal("a claiming verdict did not survive the round trip")
	}
	for name, body := range map[string][]byte{
		"empty":     {},
		"zero":      {0},
		"garbage":   {9},
		"too long":  {1, 1},
		"nil slice": nil,
	} {
		if DecodeTokenNotifyReply(body).Claiming {
			t.Fatalf("%s body decoded as claiming, want a pass", name)
		}
	}
}

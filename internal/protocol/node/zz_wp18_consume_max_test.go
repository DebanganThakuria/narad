package node

import (
	"bytes"
	"strings"
	"testing"
)

// TestZZWP18ConsumeMaxIsOptionalOnTheWire pins the compatibility rule for
// Max: a single-record request (Max 0 or 1) is byte for byte what it was
// before Max existed, a batch request carries it after the Claim byte,
// and an owner that knows only Claim refuses a batch request as trailing
// data (the requester's cue to fall back to one record).
func TestZZWP18ConsumeMaxIsOptionalOnTheWire(t *testing.T) {
	encode := func(req ConsumeRequest) []byte {
		t.Helper()
		b, err := EncodeConsumeRequest(req)
		if err != nil {
			t.Fatalf("encode %+v: %v", req, err)
		}
		return b
	}
	plain := encode(ConsumeRequest{Topic: "orders", LocalOnly: true})
	claim := encode(ConsumeRequest{Topic: "orders", LocalOnly: true, Claim: true})
	for _, max := range []int{0, 1} {
		if got := encode(ConsumeRequest{Topic: "orders", LocalOnly: true, Max: max}); !bytes.Equal(got, plain) {
			t.Fatalf("Max %d changed the single-record encoding:\n got %x\nwant %x", max, got, plain)
		}
		if got := encode(ConsumeRequest{Topic: "orders", LocalOnly: true, Claim: true, Max: max}); !bytes.Equal(got, claim) {
			t.Fatalf("Max %d changed the claim encoding:\n got %x\nwant %x", max, got, claim)
		}
	}

	batch := encode(ConsumeRequest{Topic: "orders", LocalOnly: true, Max: 50})
	if len(batch) != len(plain)+5 || !bytes.Equal(batch[:len(plain)], plain) || batch[len(plain)] != 0 {
		t.Fatalf("batch encoding %x: want the plain request, a false Claim byte and 4 bytes of Max", batch)
	}
	got, err := DecodeConsumeRequest(batch)
	if err != nil || got.Max != 50 || got.Claim || !got.LocalOnly || got.Topic != "orders" {
		t.Fatalf("decode batch = %+v, %v; want Max 50, no Claim", got, err)
	}
	got, err = DecodeConsumeRequest(encode(ConsumeRequest{Topic: "orders", LocalOnly: true, Claim: true, Max: 7}))
	if err != nil || got.Max != 7 || !got.Claim {
		t.Fatalf("decode claim batch = %+v, %v; want Max 7 with Claim", got, err)
	}
	for _, payload := range [][]byte{plain, claim} {
		if got, err := DecodeConsumeRequest(payload); err != nil || got.Max != 0 {
			t.Fatalf("decode %x = %+v, %v; want Max 0", payload, got, err)
		}
	}

	// A decoder that knows Claim but not Max: it reads the Claim byte and
	// then finds data it does not expect.
	r, err := opReader(batch, OpConsume)
	if err != nil {
		t.Fatal(err)
	}
	r.pos = len(claim)
	if err := r.done(); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("an owner without Max would answer %v, want a trailing-data refusal", err)
	}

	if _, err := EncodeConsumeRequest(ConsumeRequest{Topic: "orders", Max: -1}); err == nil {
		t.Fatal("encoded a negative Max")
	}
	neg := append(bytes.Clone(claim[:len(claim)-1]), 0, 0xff, 0xff, 0xff, 0xff)
	if _, err := DecodeConsumeRequest(neg); err == nil {
		t.Fatal("decoded a negative Max")
	}
	one := append(bytes.Clone(claim[:len(claim)-1]), 0, 0, 0, 0, 1)
	if got, err := DecodeConsumeRequest(one); err != nil || got.Max != 0 {
		t.Fatalf("decode an explicit Max 1 = %+v, %v; want Max 0 (one record)", got, err)
	}
}

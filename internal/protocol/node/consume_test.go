package node

import "testing"

// TestConsumeRequestClaimIsOptionalOnTheWire pins the compatibility rule:
// Claim is written only when set and decodes as false when absent, so a
// request from an older peer (no trailing byte) still decodes.
func TestConsumeRequestClaimIsOptionalOnTheWire(t *testing.T) {
	plain, err := EncodeConsumeRequest(ConsumeRequest{Topic: "orders", LocalOnly: true})
	if err != nil {
		t.Fatalf("encode plain: %v", err)
	}
	claim, err := EncodeConsumeRequest(ConsumeRequest{Topic: "orders", LocalOnly: true, Claim: true})
	if err != nil {
		t.Fatalf("encode claim: %v", err)
	}
	if len(claim) != len(plain)+1 {
		t.Fatalf("claim adds %d bytes, want exactly 1", len(claim)-len(plain))
	}
	got, err := DecodeConsumeRequest(plain)
	if err != nil || got.Claim || !got.LocalOnly {
		t.Fatalf("decode plain = %+v, %v; want LocalOnly without Claim", got, err)
	}
	got, err = DecodeConsumeRequest(claim)
	if err != nil || !got.Claim || !got.LocalOnly {
		t.Fatalf("decode claim = %+v, %v; want LocalOnly with Claim", got, err)
	}
}

package node

import (
	"strings"
	"testing"
)

// A forget request survives the wire, and its decoder refuses a payload
// for another op, a truncated one, and one with bytes left over.
func TestForgetServerRequestRoundTripsAndRefusesTrailingData(t *testing.T) {
	for _, want := range []ForgetServerRequest{{ID: "narad-3"}, {ID: ""}} {
		payload, err := EncodeForgetServerRequest(want)
		if err != nil {
			t.Fatalf("encode %+v: %v", want, err)
		}
		got, err := DecodeForgetServerRequest(payload)
		if err != nil || got != want {
			t.Fatalf("round trip = %+v, %v; want %+v", got, err, want)
		}
	}

	payload, err := EncodeForgetServerRequest(ForgetServerRequest{ID: "narad-3"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeForgetServerRequest(append(append([]byte(nil), payload...), 0)); err == nil || !strings.Contains(err.Error(), TrailingPayloadError) {
		t.Fatalf("trailing byte: err = %v, want %q", err, TrailingPayloadError)
	}
	if _, err := DecodeForgetServerRequest(payload[:len(payload)-1]); err == nil {
		t.Fatal("decoded a truncated payload")
	}
	other, err := EncodeDecommissionRequest(DecommissionRequest{ID: "narad-3"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeForgetServerRequest(other); err == nil {
		t.Fatal("decoded a decommission payload as a forget")
	}
}

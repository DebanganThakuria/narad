package node

import "testing"

func TestDecommissionRequestRoundTrip(t *testing.T) {
	for _, tc := range []DecommissionRequest{
		{ID: "narad-3", Cancel: false},
		{ID: "narad-0", Cancel: true},
		{ID: "", Cancel: false},
	} {
		payload, err := EncodeDecommissionRequest(tc)
		if err != nil {
			t.Fatalf("encode %+v: %v", tc, err)
		}
		got, err := DecodeDecommissionRequest(payload)
		if err != nil {
			t.Fatalf("decode %+v: %v", tc, err)
		}
		if got != tc {
			t.Fatalf("round trip = %+v, want %+v", got, tc)
		}
	}
}

func TestDecodeDecommissionRejectsWrongOp(t *testing.T) {
	// A payload for a different op must not decode as decommission.
	other, _ := EncodePrepareHandoffRequest(PrepareHandoffRequest{Topic: "t", Partition: 0})
	if _, err := DecodeDecommissionRequest(other); err == nil {
		t.Fatal("decoded a non-decommission payload without error")
	}
}

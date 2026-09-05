package node

import "testing"

func TestJoinClusterRequestRoundTrip(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		in := JoinClusterRequest{ID: "narad-3", ClusterAddr: "narad-3.narad-headless:7943", Fresh: fresh}
		payload, err := EncodeJoinClusterRequest(in)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		out, err := DecodeJoinClusterRequest(payload)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if out != in {
			t.Fatalf("round trip = %+v, want %+v", out, in)
		}
	}
}

// A request from a node that predates the Fresh flag has no trailing
// byte; it must still decode, as Fresh=false (state unknown, so a
// removed ID stays refused).
func TestJoinClusterRequestDecodesLegacyPayloadWithoutFreshFlag(t *testing.T) {
	w := opWriter(OpJoinCluster, fieldLen("narad-3")+fieldLen("addr:7943"))
	if err := w.string("narad-3"); err != nil {
		t.Fatal(err)
	}
	if err := w.string("addr:7943"); err != nil {
		t.Fatal(err)
	}
	out, err := DecodeJoinClusterRequest(w.finish())
	if err != nil {
		t.Fatalf("decode legacy payload: %v", err)
	}
	if out.Fresh || out.ID != "narad-3" || out.ClusterAddr != "addr:7943" {
		t.Fatalf("legacy decode = %+v", out)
	}
}

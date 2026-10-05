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

// decodeJoinClusterRequestV301 is v3.0.1's decoder, field for field: a
// trailing optional Fresh flag, then done().
func decodeJoinClusterRequestV301(payload []byte) error {
	r, err := opReader(payload, OpJoinCluster)
	if err != nil {
		return err
	}
	for range 2 {
		if _, err := r.string(); err != nil {
			return err
		}
	}
	if r.remaining() > 0 {
		if _, err := r.bool(); err != nil {
			return err
		}
	}
	return r.done()
}

// A join carries the newest Raft entry type the joiner applies, so the
// leader can refuse a node older than every member. The field trails
// Fresh: a frame without it decodes as reporting nothing, a request
// without it is the v3.0.1 frame byte for byte, and a v3.0.1 leader
// refuses the longer frame in words the joiner matches.
func TestJoinClusterRequestCarriesEntryTypes(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		in := JoinClusterRequest{ID: "narad-3", ClusterAddr: "narad-3:7943", Fresh: fresh, EntryTypes: 23}
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
		err = decodeJoinClusterRequestV301(payload)
		if err == nil || err.Error() != TrailingPayloadError {
			t.Fatalf("v3.0.1 decoder on the new frame: err = %v, want %q", err, TrailingPayloadError)
		}

		legacy := in
		legacy.EntryTypes = 0
		bare, err := EncodeJoinClusterRequest(legacy)
		if err != nil {
			t.Fatalf("encode without entry types: %v", err)
		}
		if err := decodeJoinClusterRequestV301(bare); err != nil {
			t.Fatalf("v3.0.1 decoder refuses a request without entry types: %v", err)
		}
		if out, err := DecodeJoinClusterRequest(bare); err != nil || out != legacy {
			t.Fatalf("a frame without entry types decodes as %+v, %v; want %+v", out, err, legacy)
		}
	}
}

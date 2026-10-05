package node

import (
	"bytes"
	"testing"
)

// legacyMemberFrame writes an OpRegisterMember payload exactly as
// v3.0.1 does: no build, no entry types.
func legacyMemberFrame(t *testing.T, req MemberRequest) []byte {
	t.Helper()
	w := opWriter(OpRegisterMember, 0)
	for _, s := range []string{req.ID, req.Addr, req.ClusterAddr, req.Status} {
		if err := w.string(s); err != nil {
			t.Fatal(err)
		}
	}
	w.i64(req.LastHeartbeat)
	return w.finish()
}

// decodeMemberRequestV301 is v3.0.1's decoder, field for field: it ends
// with done(), so it refuses any field added since.
func decodeMemberRequestV301(payload []byte) error {
	r, err := opReader(payload, OpRegisterMember)
	if err != nil {
		return err
	}
	for range 4 {
		if _, err := r.string(); err != nil {
			return err
		}
	}
	if _, err := r.i64(); err != nil {
		return err
	}
	return r.done()
}

// A heartbeat carries the sender's build and the newest Raft entry type
// it applies, so the leader can tell which entry types every member
// knows. The fields trail the v3.0.1 frame: a v3.0.1 frame still
// decodes (as reporting nothing), a request that sets neither is the
// v3.0.1 frame byte for byte (the fallback a sender resends), and a
// v3.0.1 leader refuses the longer frame in words the sender matches.
func TestMemberRequestCarriesBuildAndEntryTypes(t *testing.T) {
	in := MemberRequest{
		ID: "narad-3", Addr: "narad-3:7942", ClusterAddr: "narad-3:7943", Status: "alive", LastHeartbeat: 1700000000,
		Build: "narad v3.1.0", EntryTypes: 23,
	}
	payload, err := EncodeMemberRequest(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeMemberRequest(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("round trip = %+v, want %+v", out, in)
	}

	// Either field alone still travels.
	for _, partial := range []MemberRequest{{ID: "a", Addr: "a:1", Build: "narad dev"}, {ID: "a", Addr: "a:1", EntryTypes: 22}} {
		payload, err := EncodeMemberRequest(partial)
		if err != nil {
			t.Fatalf("encode %+v: %v", partial, err)
		}
		if out, err := DecodeMemberRequest(payload); err != nil || out != partial {
			t.Fatalf("round trip of %+v = %+v, %v", partial, out, err)
		}
	}

	legacy := in
	legacy.Build, legacy.EntryTypes = "", 0
	old := legacyMemberFrame(t, legacy)
	out, err = DecodeMemberRequest(old)
	if err != nil {
		t.Fatalf("decode a v3.0.1 frame: %v", err)
	}
	if out != legacy {
		t.Fatalf("v3.0.1 frame decodes as %+v, want %+v (nothing reported)", out, legacy)
	}
	bare, err := EncodeMemberRequest(legacy)
	if err != nil {
		t.Fatalf("encode without the new fields: %v", err)
	}
	if !bytes.Equal(bare, old) {
		t.Fatalf("a request without build or entry types encodes as %x, want the v3.0.1 frame %x", bare, old)
	}

	if err := decodeMemberRequestV301(old); err != nil {
		t.Fatalf("v3.0.1 decoder refuses its own frame: %v", err)
	}
	err = decodeMemberRequestV301(payload)
	if err == nil || err.Error() != TrailingPayloadError {
		t.Fatalf("v3.0.1 decoder on the new frame: err = %v, want %q", err, TrailingPayloadError)
	}
}

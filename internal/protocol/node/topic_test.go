package node

import "testing"

// The incarnation ID rides a purge as an optional trailing field: a
// payload without it decodes as before (an older sender), and one with
// it round-trips.
func TestTopicNameRequestIncarnationRoundTrip(t *testing.T) {
	for _, req := range []TopicNameRequest{
		{Topic: "orders"},
		{Topic: "orders", ID: "0123456789abcdef"},
	} {
		payload, err := EncodeTopicNameRequest(OpPurgeTopic, req)
		if err != nil {
			t.Fatalf("Encode(%+v): %v", req, err)
		}
		got, err := DecodeTopicNameRequest(payload, OpPurgeTopic)
		if err != nil {
			t.Fatalf("Decode(%+v): %v", req, err)
		}
		if got != req {
			t.Fatalf("round trip = %+v, want %+v", got, req)
		}
	}
	// The name-only payload is byte-identical to the pre-ID encoding:
	// operation byte, then one length-prefixed string.
	payload, _ := EncodeTopicNameRequest(OpPurgeTopic, TopicNameRequest{Topic: "ab"})
	if want := []byte{byte(OpPurgeTopic), 0, 0, 0, 2, 'a', 'b'}; string(payload) != string(want) {
		t.Fatalf("name-only payload = %v, want %v", payload, want)
	}
}

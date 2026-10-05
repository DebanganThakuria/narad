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

// preActorDecode reads fields the way a decoder that predates the
// Actor field does: the known fields, then done().
func preActorDecode(t *testing.T, payload []byte, op Operation, fields func(*reader) error) error {
	t.Helper()
	r, err := opReader(payload, op)
	if err != nil {
		t.Fatalf("opReader: %v", err)
	}
	if err := fields(&r); err != nil {
		return err
	}
	return r.done()
}

// A forwarded create, alter or delete names the caller in an optional
// trailing Actor field (audit H1): absent, the payload is byte-for-byte
// the pre-actor one; present, it round-trips, and a decoder that
// predates the field refuses it with TrailingPayloadError, which is
// what the sender's fallback recognizes.
func TestTopicRequestsCarryAnOptionalActor(t *testing.T) {
	for _, op := range []Operation{OpCreateTopic, OpAlterTopic} {
		for _, req := range []TopicBodyRequest{
			{Topic: "orders", Body: []byte(`{}`)},
			{Topic: "orders", Body: []byte(`{}`), Actor: "alice"},
		} {
			payload, err := EncodeTopicBodyRequest(op, req)
			if err != nil {
				t.Fatalf("encode %+v: %v", req, err)
			}
			got, err := DecodeTopicBodyRequest(payload, op)
			if err != nil {
				t.Fatalf("decode %+v: %v", req, err)
			}
			if got.Topic != req.Topic || string(got.Body) != string(req.Body) || got.Actor != req.Actor {
				t.Fatalf("round trip = %+v, want %+v", got, req)
			}
			err = preActorDecode(t, payload, op, func(r *reader) error {
				if _, err := r.string(); err != nil {
					return err
				}
				_, err := r.bytes()
				return err
			})
			switch {
			case req.Actor == "" && err != nil:
				t.Fatalf("a pre-actor decoder refused an actor-less payload: %v", err)
			case req.Actor != "" && (err == nil || err.Error() != TrailingPayloadError):
				t.Fatalf("a pre-actor decoder on an actor payload = %v, want %q", err, TrailingPayloadError)
			}
		}
	}

	for _, req := range []TopicNameRequest{
		{Topic: "orders"},
		{Topic: "orders", Actor: "alice"},
		{Topic: "orders", ID: "0123456789abcdef", Actor: "alice"},
	} {
		payload, err := EncodeTopicNameRequest(OpDeleteTopic, req)
		if err != nil {
			t.Fatalf("encode %+v: %v", req, err)
		}
		got, err := DecodeTopicNameRequest(payload, OpDeleteTopic)
		if err != nil {
			t.Fatalf("decode %+v: %v", req, err)
		}
		if got != req {
			t.Fatalf("round trip = %+v, want %+v", got, req)
		}
		err = preActorDecode(t, payload, OpDeleteTopic, func(r *reader) error {
			if _, err := r.string(); err != nil {
				return err
			}
			if r.remaining() > 0 {
				_, err := r.string()
				return err
			}
			return nil
		})
		switch {
		case req.Actor == "" && err != nil:
			t.Fatalf("a pre-actor decoder refused an actor-less delete: %v", err)
		case req.Actor != "" && (err == nil || err.Error() != TrailingPayloadError):
			t.Fatalf("a pre-actor decoder on an actor delete = %v, want %q", err, TrailingPayloadError)
		}
	}
}

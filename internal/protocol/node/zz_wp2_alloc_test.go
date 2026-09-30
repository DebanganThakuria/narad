package node

import (
	"bytes"
	"testing"
)

// Decoding an inbound RPC keeps its reader on the stack: an ack decode
// allocates only the topic string, a consume decode likewise.
func TestZZWP2RequestDecodersDoNotHeapAllocateTheReader(t *testing.T) {
	if zzWP2Race {
		t.Skip("allocation counts differ under the race detector")
	}
	ack, err := EncodeAckRequest(AckRequest{Topic: "orders", Partition: 3, Offset: 12345, Nonce: 99})
	if err != nil {
		t.Fatal(err)
	}
	consume, err := EncodeConsumeRequest(ConsumeRequest{Topic: "orders", Partition: 3, HasPartition: true})
	if err != nil {
		t.Fatal(err)
	}
	for name, decode := range map[string]func(){
		"ack":     func() { _, _ = DecodeAckRequest(ack) },
		"consume": func() { _, _ = DecodeConsumeRequest(consume) },
	} {
		if got := testing.AllocsPerRun(100, decode); got != 1 {
			t.Errorf("%s decode allocs = %v, want 1 (the topic string)", name, got)
		}
	}
}

// Decoding a reply with a content type servers send allocates nothing;
// an unknown one still round-trips.
func TestZZWP2DecodeResponseInternsContentType(t *testing.T) {
	for _, contentType := range []string{"", ContentTypeJSON, contentTypeOctetStream, "text/x-custom"} {
		payload, err := EncodeResponse(Response{Status: 200, ContentType: contentType, Body: []byte("{}")})
		if err != nil {
			t.Fatal(err)
		}
		res, err := DecodeResponse(payload)
		if err != nil {
			t.Fatal(err)
		}
		if res.Status != 200 || res.ContentType != contentType || string(res.Body) != "{}" {
			t.Fatalf("round trip of %q = %+v", contentType, res)
		}
		if zzWP2Race || contentType == "text/x-custom" {
			continue
		}
		if got := testing.AllocsPerRun(100, func() { _, _ = DecodeResponse(payload) }); got != 0 {
			t.Errorf("DecodeResponse(%q) allocs = %v, want 0", contentType, got)
		}
	}
}

// AppendResponse writes the same bytes EncodeResponse does, after
// whatever dst already holds, and leaves dst alone on error.
func TestZZWP2AppendResponseMatchesEncode(t *testing.T) {
	res := Response{Status: 204, ContentType: ContentTypeJSON, Body: []byte(`{"ok":true}`)}
	want, err := EncodeResponse(res)
	if err != nil {
		t.Fatal(err)
	}
	prefix := []byte("hdr")
	got, err := AppendResponse(append([]byte(nil), prefix...), res)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, append(append([]byte(nil), prefix...), want...)) {
		t.Fatalf("AppendResponse = %x, want prefix + %x", got, want)
	}
	dst := []byte("keep")
	if out, err := AppendResponse(dst, Response{Status: -1}); err == nil || string(out) != "keep" {
		t.Fatalf("AppendResponse with a bad status = %q, %v; want dst unchanged and an error", out, err)
	}
	if out, err := EncodeResponse(Response{Status: 70000}); err == nil || out != nil {
		t.Fatalf("EncodeResponse with a bad status = %v, %v; want nil and an error", out, err)
	}
}

package node

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// The string and bytes field encoders must stay byte-identical: string
// avoids the []byte conversion but the wire layout (uint32 length prefix
// followed by the raw bytes) is shared with every decoder in the package.
func TestWriterStringMatchesBytesEncoding(t *testing.T) {
	for _, v := range []string{"", "a", "orders", "customer-1", "\x00\xff unicode ✓"} {
		ws := newWriter(0)
		if err := ws.string(v); err != nil {
			t.Fatalf("string(%q) error = %v", v, err)
		}
		wb := newWriter(0)
		if err := wb.bytes([]byte(v)); err != nil {
			t.Fatalf("bytes(%q) error = %v", v, err)
		}
		if !bytes.Equal(ws.finish(), wb.finish()) {
			t.Fatalf("string(%q) = %x, bytes = %x", v, ws.finish(), wb.finish())
		}
		want := make([]byte, 4+len(v))
		binary.BigEndian.PutUint32(want, uint32(len(v)))
		copy(want[4:], v)
		if !bytes.Equal(ws.finish(), want) {
			t.Fatalf("string(%q) = %x, want %x", v, ws.finish(), want)
		}
		if got := fieldLen(v); got != len(want) {
			t.Fatalf("fieldLen(%q) = %d, want %d", v, got, len(want))
		}
	}
}

// Every encoder sizes its buffer up front from fieldLen; a mismatch would
// not break correctness but would defeat the single-allocation design.
func TestEncodersAllocateExactCapacity(t *testing.T) {
	payload, err := EncodeAckRequest(AckRequest{Topic: "orders", Partition: 3, Offset: 9, Nonce: 11})
	if err != nil {
		t.Fatalf("EncodeAckRequest() error = %v", err)
	}
	if cap(payload) != len(payload) {
		t.Fatalf("ack payload cap = %d, len = %d, want exact sizing", cap(payload), len(payload))
	}
	res, err := EncodeResponse(Response{Status: 200, ContentType: ContentTypeJSON, Body: []byte(`{"ok":true}`)})
	if err != nil {
		t.Fatalf("EncodeResponse() error = %v", err)
	}
	if cap(res) != len(res) {
		t.Fatalf("response payload cap = %d, len = %d, want exact sizing", cap(res), len(res))
	}
}

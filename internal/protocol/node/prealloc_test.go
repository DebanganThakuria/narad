package node

import (
	"runtime"
	"testing"
)

// A batch header that claims 2^31-1 records in front of a payload that
// cannot hold them must fail without reserving memory for the claim.
// The decoder preallocates at most one record slot per
// minCommitProduceBytes of payload, so the bytes it allocates stay a
// small multiple of the bytes it was handed.
func TestDecodeCommitProduceBatchBoundsPreallocation(t *testing.T) {
	const payloadBytes = 1 << 20
	payload := make([]byte, 1+4+payloadBytes)
	payload[0] = byte(OpCommitProduceBatch)
	payload[1], payload[2], payload[3], payload[4] = 0x7f, 0xff, 0xff, 0xff
	// Every record starts with a topic length; zero-length topic, key
	// and payload with a 4-byte partition and 8-byte timestamp is 24
	// bytes per record, so the payload decodes as records until it runs
	// out and fails on the truncated tail.

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err := DecodeCommitProduceBatchRequest(payload)
	runtime.ReadMemStats(&after)
	if err == nil {
		t.Fatal("decode of an over-claimed batch succeeded")
	}
	allocated := after.TotalAlloc - before.TotalAlloc
	// The bound is 72 bytes of record header per 24 payload bytes (3x),
	// plus append growth; anything near the old 72x is a regression.
	if limit := uint64(payloadBytes * 8); allocated > limit {
		t.Fatalf("decode allocated %d bytes for a %d-byte payload (limit %d)", allocated, payloadBytes, limit)
	}
}

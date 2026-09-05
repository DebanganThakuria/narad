package node

import "testing"

func TestPartitionSegmentsRequestRoundTrip(t *testing.T) {
	in := PartitionSegmentsRequest{Topic: "orders", Partition: 7}
	b, err := EncodePartitionSegmentsRequest(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if op, _ := OperationOf(b); op != OpListPartitionSegments {
		t.Fatalf("op = %d, want OpListPartitionSegments", op)
	}
	out, err := DecodePartitionSegmentsRequest(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("round trip = %+v, want %+v", out, in)
	}
}

func TestFetchSegmentChunkRequestRoundTrip(t *testing.T) {
	in := FetchSegmentChunkRequest{Topic: "orders-2", Partition: 3, BaseOffset: 4096, At: 128, Length: 65536}
	b, err := EncodeFetchSegmentChunkRequest(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if op, _ := OperationOf(b); op != OpFetchSegmentChunk {
		t.Fatalf("op = %d, want OpFetchSegmentChunk", op)
	}
	out, err := DecodeFetchSegmentChunkRequest(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("round trip = %+v, want %+v", out, in)
	}
}

// A wrong-op payload must be rejected, not silently mis-decoded.
func TestPartitionTransferDecodeRejectsWrongOp(t *testing.T) {
	b, _ := EncodePartitionSegmentsRequest(PartitionSegmentsRequest{Topic: "t", Partition: 0})
	if _, err := DecodeFetchSegmentChunkRequest(b); err == nil {
		t.Fatal("decoding a segments request as a fetch request must fail")
	}
}

func TestPrepareHandoffRequestRoundTrip(t *testing.T) {
	in := PrepareHandoffRequest{Topic: "orders", Partition: 4, FreezeTTLNanos: 30_000_000_000}
	b, err := EncodePrepareHandoffRequest(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if op, _ := OperationOf(b); op != OpPrepareHandoff {
		t.Fatalf("op = %d, want OpPrepareHandoff", op)
	}
	out, err := DecodePrepareHandoffRequest(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != in {
		t.Fatalf("round trip = %+v, want %+v", out, in)
	}
}

// The freeze token is an optional trailing field: a request without it
// encodes exactly as before (so older owners keep decoding it), and one
// with it round-trips.
func TestPrepareHandoffRequestRoundTripWithAndWithoutToken(t *testing.T) {
	plain := PrepareHandoffRequest{Topic: "orders", Partition: 2, FreezeTTLNanos: 30_000_000_000}
	b, err := EncodePrepareHandoffRequest(plain)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if want := 1 + fieldLen("orders") + 4 + 8; len(b) != want {
		t.Fatalf("token-less encoding is %d bytes, want the pre-token size %d", len(b), want)
	}
	out, err := DecodePrepareHandoffRequest(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != plain {
		t.Fatalf("round trip = %+v, want %+v", out, plain)
	}

	fenced := PrepareHandoffRequest{Topic: "orders", Partition: 2, FreezeTTLNanos: 30_000_000_000, FreezeToken: "deadbeef01020304"}
	b, err = EncodePrepareHandoffRequest(fenced)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err = DecodePrepareHandoffRequest(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out != fenced {
		t.Fatalf("round trip = %+v, want %+v", out, fenced)
	}
}

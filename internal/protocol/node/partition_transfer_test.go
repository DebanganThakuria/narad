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

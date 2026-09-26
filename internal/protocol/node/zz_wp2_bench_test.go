package node

import "testing"

func BenchmarkZZWP2DecodeAckRequest(b *testing.B) {
	payload, err := EncodeAckRequest(AckRequest{Topic: "orders", Partition: 3, Offset: 12345, Nonce: 99})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodeAckRequest(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZZWP2DecodeConsumeRequest(b *testing.B) {
	payload, err := EncodeConsumeRequest(ConsumeRequest{Topic: "orders", Partition: 3, HasPartition: true, WaitNanos: 1e9})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodeConsumeRequest(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZZWP2DecodeCommitProduceBatchRequest(b *testing.B) {
	records := make([]CommitProduceRequest, 32)
	for i := range records {
		records[i] = CommitProduceRequest{Topic: "orders", Key: "k", TargetPartition: 3, Payload: make([]byte, 256), CreatedAtUnixMs: 1}
	}
	payload, err := EncodeCommitProduceBatchRequest(CommitProduceBatchRequest{Records: records})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodeCommitProduceBatchRequest(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZZWP2EncodeResponse(b *testing.B) {
	body := make([]byte, 512)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := EncodeResponse(Response{Status: 200, ContentType: ContentTypeJSON, Body: body}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkZZWP2DecodeResponse(b *testing.B) {
	payload, err := EncodeResponse(Response{Status: 200, ContentType: ContentTypeJSON, Body: make([]byte, 512)})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodeResponse(payload); err != nil {
			b.Fatal(err)
		}
	}
}

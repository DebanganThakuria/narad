package node

import "testing"

func zzWP6BatchRecords() []CommitProduceRequest {
	records := make([]CommitProduceRequest, 64)
	for i := range records {
		records[i] = CommitProduceRequest{Topic: "orders-run-1790424109845303", Key: "customer-42", TargetPartition: 3, Payload: make([]byte, 256), CreatedAtUnixMs: 1}
	}
	return records
}

// BenchmarkZZWP6EncodeCommitProduceBatchRequest is the dispatcher's
// per-commit encode of a 64-record batch.
func BenchmarkZZWP6EncodeCommitProduceBatchRequest(b *testing.B) {
	req := CommitProduceBatchRequest{Records: zzWP6BatchRecords()}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := EncodeCommitProduceBatchRequest(req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkZZWP6DecodeCommitProduceBatchRequest is the owner's decode of
// the same batch.
func BenchmarkZZWP6DecodeCommitProduceBatchRequest(b *testing.B) {
	payload, err := EncodeCommitProduceBatchRequest(CommitProduceBatchRequest{Records: zzWP6BatchRecords()})
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

func zzWP6BatchRecordsWithIDs() []CommitProduceRequest {
	records := zzWP6BatchRecords()
	for i := range records {
		records[i].TopicID = "6f1c2a9e0b7d4c33"
	}
	return records
}

// BenchmarkZZWP6EncodeCommitProduceBatchRequestWithIDs is the encode of
// the same batch carrying its records' topic incarnation.
func BenchmarkZZWP6EncodeCommitProduceBatchRequestWithIDs(b *testing.B) {
	req := CommitProduceBatchRequest{Records: zzWP6BatchRecordsWithIDs()}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := EncodeCommitProduceBatchRequest(req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkZZWP6DecodeCommitProduceBatchRequestWithIDs is its decode.
func BenchmarkZZWP6DecodeCommitProduceBatchRequestWithIDs(b *testing.B) {
	payload, err := EncodeCommitProduceBatchRequest(CommitProduceBatchRequest{Records: zzWP6BatchRecordsWithIDs()})
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

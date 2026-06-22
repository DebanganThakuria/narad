package node

import "testing"

func TestCommitProduceRequestRoundTrip(t *testing.T) {
	want := CommitProduceRequest{
		MessageID:       "message-1",
		Topic:           "orders",
		Key:             "customer-1",
		TargetPartition: 7,
		Payload:         []byte(`{"id":1}`),
		CreatedAtUnixMs: 123456,
	}

	encoded, err := EncodeCommitProduceRequest(want)
	if err != nil {
		t.Fatalf("EncodeCommitProduceRequest() error = %v", err)
	}
	got, err := DecodeCommitProduceRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeCommitProduceRequest() error = %v", err)
	}
	if got.MessageID != want.MessageID ||
		got.Topic != want.Topic ||
		got.Key != want.Key ||
		got.TargetPartition != want.TargetPartition ||
		string(got.Payload) != string(want.Payload) ||
		got.CreatedAtUnixMs != want.CreatedAtUnixMs {
		t.Fatalf("roundtrip = %+v, want %+v", got, want)
	}
}

func TestCommitProduceBatchRequestRoundTrip(t *testing.T) {
	want := CommitProduceBatchRequest{Records: []CommitProduceRequest{
		{
			MessageID:       "message-1",
			Topic:           "orders",
			Key:             "customer-1",
			TargetPartition: 7,
			Payload:         []byte(`{"id":1}`),
			CreatedAtUnixMs: 123456,
		},
		{
			MessageID:       "message-2",
			Topic:           "orders",
			Key:             "customer-2",
			TargetPartition: 7,
			Payload:         []byte(`{"id":2}`),
			CreatedAtUnixMs: 123457,
		},
	}}

	encoded, err := EncodeCommitProduceBatchRequest(want)
	if err != nil {
		t.Fatalf("EncodeCommitProduceBatchRequest() error = %v", err)
	}
	got, err := DecodeCommitProduceBatchRequest(encoded)
	if err != nil {
		t.Fatalf("DecodeCommitProduceBatchRequest() error = %v", err)
	}
	if len(got.Records) != len(want.Records) {
		t.Fatalf("roundtrip records = %d, want %d", len(got.Records), len(want.Records))
	}
	for i := range want.Records {
		if got.Records[i].MessageID != want.Records[i].MessageID ||
			got.Records[i].Topic != want.Records[i].Topic ||
			got.Records[i].Key != want.Records[i].Key ||
			got.Records[i].TargetPartition != want.Records[i].TargetPartition ||
			string(got.Records[i].Payload) != string(want.Records[i].Payload) ||
			got.Records[i].CreatedAtUnixMs != want.Records[i].CreatedAtUnixMs {
			t.Fatalf("roundtrip[%d] = %+v, want %+v", i, got.Records[i], want.Records[i])
		}
	}
}

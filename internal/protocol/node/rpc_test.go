package node

import (
	"strings"
	"testing"
)

func TestCommitProduceRequestRoundTrip(t *testing.T) {
	want := CommitProduceRequest{
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
	if got.Topic != want.Topic ||
		got.Key != want.Key ||
		got.TargetPartition != want.TargetPartition ||
		string(got.Payload) != string(want.Payload) ||
		got.CreatedAtUnixMs != want.CreatedAtUnixMs {
		t.Fatalf("roundtrip = %+v, want %+v", got, want)
	}
}

func TestCommitProduceRequestRejectsMalformedPayloads(t *testing.T) {
	valid := CommitProduceRequest{
		Topic:           "orders",
		Key:             "customer-1",
		TargetPartition: 7,
		Payload:         []byte(`{"id":1}`),
		CreatedAtUnixMs: 123456,
	}
	encoded, err := EncodeCommitProduceRequest(valid)
	if err != nil {
		t.Fatalf("EncodeCommitProduceRequest() error = %v", err)
	}

	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{
			name: "empty",
			data: nil,
			want: "EOF",
		},
		{
			name: "wrong op",
			data: []byte{byte(OpAck)},
			want: "unexpected operation",
		},
		{
			name: "truncated topic length",
			data: encoded[:3],
			want: "EOF",
		},
		{
			name: "truncated timestamp",
			data: encoded[:len(encoded)-1],
			want: "EOF",
		},
		{
			name: "trailing bytes",
			data: append(append([]byte(nil), encoded...), 0),
			want: "trailing node rpc payload data",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeCommitProduceRequest(tc.data)
			if err == nil {
				t.Fatal("DecodeCommitProduceRequest() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("DecodeCommitProduceRequest() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCommitProduceBatchRequestRoundTrip(t *testing.T) {
	want := CommitProduceBatchRequest{Records: []CommitProduceRequest{
		{
			Topic:           "orders",
			Key:             "customer-1",
			TargetPartition: 7,
			Payload:         []byte(`{"id":1}`),
			CreatedAtUnixMs: 123456,
		},
		{
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
		if got.Records[i].Topic != want.Records[i].Topic ||
			got.Records[i].Key != want.Records[i].Key ||
			got.Records[i].TargetPartition != want.Records[i].TargetPartition ||
			string(got.Records[i].Payload) != string(want.Records[i].Payload) ||
			got.Records[i].CreatedAtUnixMs != want.Records[i].CreatedAtUnixMs {
			t.Fatalf("roundtrip[%d] = %+v, want %+v", i, got.Records[i], want.Records[i])
		}
	}
}

func TestCommitProduceBatchRequestRejectsMalformedPayloads(t *testing.T) {
	valid := CommitProduceBatchRequest{Records: []CommitProduceRequest{
		{
			Topic:           "orders",
			Key:             "customer-1",
			TargetPartition: 7,
			Payload:         []byte(`{"id":1}`),
			CreatedAtUnixMs: 123456,
		},
	}}
	encoded, err := EncodeCommitProduceBatchRequest(valid)
	if err != nil {
		t.Fatalf("EncodeCommitProduceBatchRequest() error = %v", err)
	}

	negativeCount := opWriter(OpCommitProduceBatch, 4)
	negativeCount.i32(-1)
	hugeCount := opWriter(OpCommitProduceBatch, 4)
	hugeCount.i32(1 << 30)

	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{
			name: "empty",
			data: nil,
			want: "EOF",
		},
		{
			name: "wrong op",
			data: []byte{byte(OpAck)},
			want: "unexpected operation",
		},
		{
			name: "truncated count",
			data: []byte{byte(OpCommitProduceBatch), 0},
			want: "EOF",
		},
		{
			name: "negative count",
			data: negativeCount.finish(),
			want: "negative commit produce batch size",
		},
		{
			name: "huge count without records",
			data: hugeCount.finish(),
			want: "EOF",
		},
		{
			name: "truncated record",
			data: encoded[:len(encoded)-1],
			want: "EOF",
		},
		{
			name: "trailing bytes",
			data: append(append([]byte(nil), encoded...), 0),
			want: "trailing node rpc payload data",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeCommitProduceBatchRequest(tc.data)
			if err == nil {
				t.Fatal("DecodeCommitProduceBatchRequest() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("DecodeCommitProduceBatchRequest() error = %v, want %q", err, tc.want)
			}
		})
	}
}

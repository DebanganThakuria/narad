package node

// Fuzz targets for the node-RPC wire codec. A request payload arrives
// from a peer over an authenticated cluster stream and is decoded on a
// goroutine with no recover, so a decoder panic is a node crash. The
// invariants every target enforces:
//
//   - a decoder never panics on any input;
//   - a decoder never claims bytes it was not given (decoded strings and
//     byte slices are sub-slices of the payload, so their total length
//     is bounded by the payload length);
//   - a truncated payload and a payload with trailing garbage are both
//     rejected;
//   - a value that decoded is stable: re-encoding it and decoding again
//     yields the same value;
//   - every value an encoder accepts decodes back to itself exactly.

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

// decoder is one Decode* function wrapped to a uniform shape: the decoded
// value (for round-trip comparison), its re-encoding, and the error.
type decoder struct {
	name string
	// optionalTail marks decoders whose LAST field is optional on the
	// wire, so truncating a payload can yield a shorter but still valid
	// payload rather than an error.
	optionalTail bool
	// size is the total length of the decoded string/byte fields, used
	// for the no-invented-bytes check.
	decode func([]byte) (value any, size int, err error)
	encode func(value any) ([]byte, error)
}

var decoders = []decoder{
	{
		name: "Produce",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeProduceRequest(b)
			return v, len(v.Topic) + len(v.Key) + len(v.Payload), err
		},
		encode: func(v any) ([]byte, error) { return EncodeProduceRequest(v.(ProduceRequest)) },
	},
	{
		name: "CommitProduce",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeCommitProduceRequest(b)
			return v, len(v.Topic) + len(v.Key) + len(v.Payload), err
		},
		encode: func(v any) ([]byte, error) { return EncodeCommitProduceRequest(v.(CommitProduceRequest)) },
	},
	{
		name: "CommitProduceBatch",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeCommitProduceBatchRequest(b)
			size := 0
			for _, r := range v.Records {
				size += len(r.Topic) + len(r.Key) + len(r.Payload)
			}
			return v, size, err
		},
		encode: func(v any) ([]byte, error) {
			return EncodeCommitProduceBatchRequest(v.(CommitProduceBatchRequest))
		},
	},
	{
		name: "Consume",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeConsumeRequest(b)
			return v, len(v.Topic), err
		},
		encode:       func(v any) ([]byte, error) { return EncodeConsumeRequest(v.(ConsumeRequest)) },
		optionalTail: true, // the Claim flag is a trailing optional byte
	},
	{
		name: "Ack",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeAckRequest(b)
			return v, len(v.Topic), err
		},
		encode: func(v any) ([]byte, error) { return EncodeAckRequest(v.(AckRequest)) },
	},
	{
		name: "ExtendAck",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeExtendAckRequest(b)
			return v, len(v.Topic), err
		},
		encode: func(v any) ([]byte, error) { return EncodeExtendAckRequest(v.(AckRequest)) },
	},
	{
		name: "Nack",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeNackRequest(b)
			return v, len(v.Topic), err
		},
		encode: func(v any) ([]byte, error) { return EncodeNackRequest(v.(AckRequest)) },
	},
	{
		name: "CreateTopic",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeTopicBodyRequest(b, OpCreateTopic)
			return v, len(v.Topic) + len(v.Body), err
		},
		encode: func(v any) ([]byte, error) { return EncodeTopicBodyRequest(OpCreateTopic, v.(TopicBodyRequest)) },
	},
	{
		name: "AlterTopic",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeTopicBodyRequest(b, OpAlterTopic)
			return v, len(v.Topic) + len(v.Body), err
		},
		encode: func(v any) ([]byte, error) { return EncodeTopicBodyRequest(OpAlterTopic, v.(TopicBodyRequest)) },
	},
	{
		name:         "DeleteTopic",
		optionalTail: true,
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeTopicNameRequest(b, OpDeleteTopic)
			return v, len(v.Topic) + len(v.ID), err
		},
		encode: func(v any) ([]byte, error) { return EncodeTopicNameRequest(OpDeleteTopic, v.(TopicNameRequest)) },
	},
	{
		name:         "PurgeTopic",
		optionalTail: true,
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeTopicNameRequest(b, OpPurgeTopic)
			return v, len(v.Topic) + len(v.ID), err
		},
		encode: func(v any) ([]byte, error) { return EncodeTopicNameRequest(OpPurgeTopic, v.(TopicNameRequest)) },
	},
	{
		name:         "GetTopic",
		optionalTail: true,
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeTopicNameRequest(b, OpGetTopic)
			return v, len(v.Topic) + len(v.ID), err
		},
		encode: func(v any) ([]byte, error) { return EncodeTopicNameRequest(OpGetTopic, v.(TopicNameRequest)) },
	},
	{
		name: "TopicPartitionStats",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeTopicPartitionStatsRequest(b)
			return v, len(v.Topic), err
		},
		encode: func(v any) ([]byte, error) {
			return EncodeTopicPartitionStatsRequest(v.(TopicPartitionStatsRequest))
		},
	},
	{
		name: "CreateUser",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeUserRequest(b, OpCreateUser)
			return v, len(v.Username) + len(v.Body), err
		},
		encode: func(v any) ([]byte, error) { return EncodeUserRequest(OpCreateUser, v.(UserRequest)) },
	},
	{
		name: "UpdateUser",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeUserRequest(b, OpUpdateUser)
			return v, len(v.Username) + len(v.Body), err
		},
		encode: func(v any) ([]byte, error) { return EncodeUserRequest(OpUpdateUser, v.(UserRequest)) },
	},
	{
		name: "DeleteUser",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeUserRequest(b, OpDeleteUser)
			return v, len(v.Username) + len(v.Body), err
		},
		encode: func(v any) ([]byte, error) { return EncodeUserRequest(OpDeleteUser, v.(UserRequest)) },
	},
	{
		name: "RegisterMember",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeMemberRequest(b)
			return v, len(v.ID) + len(v.Addr) + len(v.ClusterAddr) + len(v.Status), err
		},
		encode: func(v any) ([]byte, error) { return EncodeMemberRequest(v.(MemberRequest)) },
	},
	{
		name:         "JoinCluster",
		optionalTail: true,
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeJoinClusterRequest(b)
			return v, len(v.ID) + len(v.ClusterAddr), err
		},
		encode: func(v any) ([]byte, error) { return EncodeJoinClusterRequest(v.(JoinClusterRequest)) },
	},
	{
		name: "AttachChild",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeChildLinkRequest(b, OpAttachChild)
			return v, len(v.Parent) + len(v.Child), err
		},
		encode: func(v any) ([]byte, error) { return EncodeChildLinkRequest(OpAttachChild, v.(ChildLinkRequest)) },
	},
	{
		name: "DetachChild",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeChildLinkRequest(b, OpDetachChild)
			return v, len(v.Parent) + len(v.Child), err
		},
		encode: func(v any) ([]byte, error) { return EncodeChildLinkRequest(OpDetachChild, v.(ChildLinkRequest)) },
	},
	{
		name: "FanoutCursors",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeTopicNameRequest(b, OpFanoutCursors)
			return v, len(v.Topic) + len(v.ID), err
		},
		optionalTail: true,
		encode:       func(v any) ([]byte, error) { return EncodeTopicNameRequest(OpFanoutCursors, v.(TopicNameRequest)) },
	},
	{
		name: "ListPartitionSegments",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodePartitionSegmentsRequest(b)
			return v, len(v.Topic), err
		},
		encode: func(v any) ([]byte, error) { return EncodePartitionSegmentsRequest(v.(PartitionSegmentsRequest)) },
	},
	{
		name: "FetchSegmentChunk",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeFetchSegmentChunkRequest(b)
			return v, len(v.Topic), err
		},
		encode: func(v any) ([]byte, error) { return EncodeFetchSegmentChunkRequest(v.(FetchSegmentChunkRequest)) },
	},
	{
		name:         "PrepareHandoff",
		optionalTail: true,
		decode: func(b []byte) (any, int, error) {
			v, err := DecodePrepareHandoffRequest(b)
			return v, len(v.Topic) + len(v.FreezeToken), err
		},
		encode: func(v any) ([]byte, error) { return EncodePrepareHandoffRequest(v.(PrepareHandoffRequest)) },
	},
	{
		name: "Decommission",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeDecommissionRequest(b)
			return v, len(v.ID), err
		},
		encode: func(v any) ([]byte, error) { return EncodeDecommissionRequest(v.(DecommissionRequest)) },
	},
	{
		name: "CompleteMove",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeCompleteMoveRequest(b)
			return v, len(v.Topic) + len(v.ExpectedOwner) + len(v.TargetID), err
		},
		encode: func(v any) ([]byte, error) { return EncodeCompleteMoveRequest(v.(CompleteMoveRequest)) },
	},
	{
		name: "AbortMove",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeAbortMoveRequest(b)
			return v, len(v.Topic) + len(v.ExpectedTarget), err
		},
		encode: func(v any) ([]byte, error) { return EncodeAbortMoveRequest(v.(AbortMoveRequest)) },
	},
	{
		name: "GetAssignment",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeGetAssignmentRequest(b)
			return v, len(v.Topic), err
		},
		encode: func(v any) ([]byte, error) { return EncodeGetAssignmentRequest(v.(GetAssignmentRequest)) },
	},
	{
		name: "AppliedIndex",
		decode: func(b []byte) (any, int, error) {
			return struct{}{}, 0, DecodeAppliedIndexRequest(b)
		},
		encode: func(any) ([]byte, error) { return EncodeAppliedIndexRequest(), nil },
	},
	{
		name: "Response",
		decode: func(b []byte) (any, int, error) {
			v, err := DecodeResponse(b)
			return v, len(v.ContentType) + len(v.Body), err
		},
		encode: func(v any) ([]byte, error) { return EncodeResponse(v.(Response)) },
	},
}

// seedPayloads are real encodings of one representative value per
// request type, plus the degenerate shapes a fuzzer should start from.
func seedPayloads(t testing.TB) [][]byte {
	must := func(b []byte, err error) []byte {
		if err != nil {
			t.Fatalf("seed encode: %v", err)
		}
		return b
	}
	seeds := [][]byte{
		{},
		{byte(OpProduce)},
		{0xff},
		must(EncodeProduceRequest(ProduceRequest{Topic: "orders", Key: "k1", Partition: 3, Payload: []byte("hello")})),
		must(EncodeCommitProduceRequest(CommitProduceRequest{Topic: "orders", Key: "k", TargetPartition: 1, Payload: []byte("p"), CreatedAtUnixMs: 1700000000000})),
		must(EncodeCommitProduceBatchRequest(CommitProduceBatchRequest{Records: []CommitProduceRequest{
			{Topic: "orders", Key: "a", TargetPartition: 0, Payload: []byte("x"), CreatedAtUnixMs: 1},
			{Topic: "orders", Key: "b", TargetPartition: 2, Payload: []byte("yy"), CreatedAtUnixMs: 2},
		}})),
		must(EncodeCommitProduceBatchRequest(CommitProduceBatchRequest{})),
		must(EncodeConsumeRequest(ConsumeRequest{Topic: "orders", Partition: 2, HasPartition: true, Offset: 10, HasOffset: true, WaitNanos: 5e9})),
		must(EncodeConsumeRequest(ConsumeRequest{Topic: "orders", LocalOnly: true})),
		must(EncodeConsumeRequest(ConsumeRequest{Topic: "orders", LocalOnly: true, Claim: true})),
		must(EncodeAckRequest(AckRequest{Topic: "orders", Partition: 1, Offset: 7, Nonce: 99})),
		must(EncodeExtendAckRequest(AckRequest{Topic: "orders", Partition: 1, Offset: 7, Nonce: 99})),
		must(EncodeNackRequest(AckRequest{Topic: "orders", Partition: 1, Offset: 7, Nonce: 99})),
		must(EncodeTopicBodyRequest(OpCreateTopic, TopicBodyRequest{Topic: "orders", Body: []byte(`{"name":"orders","partitions":4}`)})),
		must(EncodeTopicBodyRequest(OpAlterTopic, TopicBodyRequest{Topic: "orders", Body: []byte(`{"partitions":8}`)})),
		must(EncodeTopicNameRequest(OpDeleteTopic, TopicNameRequest{Topic: "orders"})),
		must(EncodeTopicNameRequest(OpPurgeTopic, TopicNameRequest{Topic: "orders", ID: "01HXYZ"})),
		must(EncodeTopicNameRequest(OpGetTopic, TopicNameRequest{Topic: "orders"})),
		must(EncodeTopicNameRequest(OpFanoutCursors, TopicNameRequest{Topic: "orders"})),
		must(EncodeTopicPartitionStatsRequest(TopicPartitionStatsRequest{Topic: "orders", Partition: 0})),
		must(EncodeUserRequest(OpCreateUser, UserRequest{Username: "alice", Body: []byte(`{"username":"alice"}`)})),
		must(EncodeUserRequest(OpUpdateUser, UserRequest{Username: "alice", Body: []byte(`{}`)})),
		must(EncodeUserRequest(OpDeleteUser, UserRequest{Username: "alice"})),
		must(EncodeMemberRequest(MemberRequest{ID: "n1", Addr: "10.0.0.1:7942", ClusterAddr: "10.0.0.1:7943", Status: "alive", LastHeartbeat: 1700000000})),
		must(EncodeJoinClusterRequest(JoinClusterRequest{ID: "n2", ClusterAddr: "10.0.0.2:7943", Fresh: true})),
		must(EncodeChildLinkRequest(OpAttachChild, ChildLinkRequest{Parent: "orders", Child: "orders-audit", DelayMs: 60000})),
		must(EncodeChildLinkRequest(OpDetachChild, ChildLinkRequest{Parent: "orders", Child: "orders-audit"})),
		must(EncodePartitionSegmentsRequest(PartitionSegmentsRequest{Topic: "orders", Partition: 1})),
		must(EncodeFetchSegmentChunkRequest(FetchSegmentChunkRequest{Topic: "orders", Partition: 1, BaseOffset: 4096, At: 128, Length: 65536})),
		must(EncodePrepareHandoffRequest(PrepareHandoffRequest{Topic: "orders", Partition: 1, FreezeTTLNanos: 3e10})),
		must(EncodePrepareHandoffRequest(PrepareHandoffRequest{Topic: "orders", Partition: 1, FreezeTTLNanos: 3e10, FreezeToken: "tok-1"})),
		must(EncodeDecommissionRequest(DecommissionRequest{ID: "n3", Cancel: true})),
		must(EncodeCompleteMoveRequest(CompleteMoveRequest{Topic: "orders", Partition: 2, ExpectedOwner: "n1", TargetID: "n2"})),
		must(EncodeAbortMoveRequest(AbortMoveRequest{Topic: "orders", Partition: 2, ExpectedTarget: "n2"})),
		must(EncodeGetAssignmentRequest(GetAssignmentRequest{Topic: "orders", Partition: 2})),
		EncodeAppliedIndexRequest(),
		must(EncodeResponse(Response{Status: 200, ContentType: ContentTypeJSON, Body: []byte(`{"ok":true}`)})),
		must(EncodeResponse(Response{Status: 65535})),
	}
	// A batch that claims far more records than the payload can hold:
	// the decoder must fail without allocating for the claimed count.
	seeds = append(seeds, []byte{byte(OpCommitProduceBatch), 0x7f, 0xff, 0xff, 0xff})
	return seeds
}

// FuzzDecodeAny feeds arbitrary bytes to EVERY decoder. Each must reject
// or decode cleanly; a decoded value must be a stable fixed point of
// encode/decode and must be rejected again once truncated or padded.
func FuzzDecodeAny(f *testing.F) {
	for _, seed := range seedPayloads(f) {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		// The decoders sub-slice the payload; a mutation-detecting copy
		// proves none of them writes into it.
		original := bytes.Clone(payload)
		for _, d := range decoders {
			value, size, err := d.decode(payload)
			if !bytes.Equal(payload, original) {
				t.Fatalf("%s: decoder mutated its input", d.name)
			}
			if err != nil {
				continue
			}
			if size > len(payload) {
				t.Fatalf("%s: decoded %d bytes of fields from a %d-byte payload", d.name, size, len(payload))
			}

			// Stable fixed point: encode(decode(p)) decodes to the same value.
			encoded, err := d.encode(value)
			if err != nil {
				t.Fatalf("%s: re-encode of a decoded value failed: %v", d.name, err)
			}
			again, _, err := d.decode(encoded)
			if err != nil {
				t.Fatalf("%s: re-decode of a re-encoded value failed: %v (payload %x, re-encoded %x)", d.name, err, payload, encoded)
			}
			if !reflect.DeepEqual(value, again) {
				t.Fatalf("%s: decode(encode(v)) != v\n v = %#v\n v'= %#v", d.name, value, again)
			}

			// Trailing garbage: one byte that is not a valid bool (so it
			// cannot be read as an optional trailing flag) must fail.
			padded := append(bytes.Clone(payload), 0xff)
			if _, _, err := d.decode(padded); err == nil {
				t.Fatalf("%s: accepted a payload with a trailing byte", d.name)
			}
			// Truncation: dropping the last byte must fail, unless the
			// last field is optional on the wire.
			if !d.optionalTail && len(payload) > 1 {
				if _, _, err := d.decode(payload[:len(payload)-1]); err == nil {
					t.Fatalf("%s: accepted a truncated payload", d.name)
				}
			}
		}

		// OperationOf never panics and agrees with the first byte.
		if op, err := OperationOf(payload); err == nil && op != Operation(payload[0]) {
			t.Fatalf("OperationOf = %d, want %d", op, payload[0])
		}
	})
}

// FuzzRoundTripMessaging encodes the data-plane requests (produce,
// commit, batch, consume, ack) from fuzzer-chosen fields and checks that
// decoding gives back exactly what was encoded. Partition is a Go int on
// the request and an int32 on the wire, so the encoder must refuse
// values it cannot carry rather than truncate them.
func FuzzRoundTripMessaging(f *testing.F) {
	f.Add("orders", "k1", 3, []byte("hello"), int64(1700000000000), int64(10), true, false, int64(5e9))
	f.Add("", "", 0, []byte{}, int64(0), int64(0), false, false, int64(0))
	f.Add("t", "k", -1, []byte{0}, int64(-1), int64(-1), true, true, int64(-1))
	f.Add("t", "k", math.MaxInt32, []byte(nil), int64(math.MaxInt64), int64(math.MinInt64), false, true, int64(math.MaxInt64))
	f.Add("t", "k", math.MaxInt32+1, []byte(nil), int64(0), int64(0), false, false, int64(0))
	f.Add("t", "k", math.MinInt32-1, []byte(nil), int64(0), int64(0), false, false, int64(0))

	f.Fuzz(func(t *testing.T, topic, key string, partition int, payload []byte, ts, offset int64, flagA, flagB bool, wait int64) {
		inRange := partition >= math.MinInt32 && partition <= math.MaxInt32

		checkPartition := func(name string, encoded []byte, err error) bool {
			if !inRange {
				if err == nil {
					t.Fatalf("%s: encoded partition %d, which does not fit the int32 wire field", name, partition)
				}
				return false
			}
			if err != nil {
				t.Fatalf("%s: encode error: %v", name, err)
			}
			return true
		}

		produce := ProduceRequest{Topic: topic, Key: key, Partition: partition, Payload: payload}
		if b, err := EncodeProduceRequest(produce); checkPartition("Produce", b, err) {
			got, err := DecodeProduceRequest(b)
			if err != nil {
				t.Fatalf("Produce: decode error: %v", err)
			}
			if !equalProduce(got, produce) {
				t.Fatalf("Produce: round trip\n got %#v\nwant %#v", got, produce)
			}
		}

		commit := CommitProduceRequest{Topic: topic, Key: key, TargetPartition: partition, Payload: payload, CreatedAtUnixMs: ts}
		if b, err := EncodeCommitProduceRequest(commit); checkPartition("CommitProduce", b, err) {
			got, err := DecodeCommitProduceRequest(b)
			if err != nil {
				t.Fatalf("CommitProduce: decode error: %v", err)
			}
			if !equalCommit(got, commit) {
				t.Fatalf("CommitProduce: round trip\n got %#v\nwant %#v", got, commit)
			}
		}

		batch := CommitProduceBatchRequest{Records: []CommitProduceRequest{commit, {Topic: key, Key: topic, Payload: payload}, commit}}
		if b, err := EncodeCommitProduceBatchRequest(batch); checkPartition("CommitProduceBatch", b, err) {
			got, err := DecodeCommitProduceBatchRequest(b)
			if err != nil {
				t.Fatalf("CommitProduceBatch: decode error: %v", err)
			}
			if len(got.Records) != len(batch.Records) {
				t.Fatalf("CommitProduceBatch: %d records, want %d", len(got.Records), len(batch.Records))
			}
			for i := range got.Records {
				if !equalCommit(got.Records[i], batch.Records[i]) {
					t.Fatalf("CommitProduceBatch: record %d\n got %#v\nwant %#v", i, got.Records[i], batch.Records[i])
				}
			}
		}

		consume := ConsumeRequest{Topic: topic, Partition: partition, HasPartition: flagA, Offset: offset, HasOffset: flagB, WaitNanos: wait, LocalOnly: flagA != flagB}
		if b, err := EncodeConsumeRequest(consume); checkPartition("Consume", b, err) {
			got, err := DecodeConsumeRequest(b)
			if err != nil {
				t.Fatalf("Consume: decode error: %v", err)
			}
			if got != consume {
				t.Fatalf("Consume: round trip\n got %#v\nwant %#v", got, consume)
			}
		}

		ack := AckRequest{Topic: topic, Partition: partition, Offset: offset, Nonce: ts}
		for _, c := range []struct {
			name   string
			encode func(AckRequest) ([]byte, error)
			decode func([]byte) (AckRequest, error)
		}{
			{"Ack", EncodeAckRequest, DecodeAckRequest},
			{"ExtendAck", EncodeExtendAckRequest, DecodeExtendAckRequest},
			{"Nack", EncodeNackRequest, DecodeNackRequest},
		} {
			if b, err := c.encode(ack); checkPartition(c.name, b, err) {
				got, err := c.decode(b)
				if err != nil {
					t.Fatalf("%s: decode error: %v", c.name, err)
				}
				if got != ack {
					t.Fatalf("%s: round trip\n got %#v\nwant %#v", c.name, got, ack)
				}
			}
		}
	})
}

// FuzzRoundTripControl does the same for the control-plane requests and
// the Response envelope.
func FuzzRoundTripControl(f *testing.F) {
	f.Add("orders", "01HXYZ", "n1", "n2", 3, int64(60000), true, []byte(`{"partitions":4}`), 200)
	f.Add("", "", "", "", 0, int64(0), false, []byte{}, 0)
	f.Add("t", "id", "a", "b", math.MaxInt32, int64(math.MinInt64), true, []byte(nil), 65535)
	f.Add("t", "id", "a", "b", math.MaxInt32+1, int64(0), false, []byte(nil), 65536)
	f.Add("t", "id", "a", "b", -1, int64(-1), false, []byte(nil), -1)

	f.Fuzz(func(t *testing.T, topic, id, a, b string, partition int, n int64, flag bool, body []byte, status int) {
		inRange := partition >= math.MinInt32 && partition <= math.MaxInt32
		check := func(name string, err error) bool {
			if !inRange {
				if err == nil {
					t.Fatalf("%s: encoded partition %d, which does not fit the int32 wire field", name, partition)
				}
				return false
			}
			if err != nil {
				t.Fatalf("%s: encode error: %v", name, err)
			}
			return true
		}
		mustDecode := func(name string, err error) {
			if err != nil {
				t.Fatalf("%s: decode error: %v", name, err)
			}
		}

		for _, op := range []Operation{OpCreateTopic, OpAlterTopic} {
			want := TopicBodyRequest{Topic: topic, Body: body}
			enc, err := EncodeTopicBodyRequest(op, want)
			mustDecode("TopicBody encode", err)
			got, err := DecodeTopicBodyRequest(enc, op)
			mustDecode("TopicBody", err)
			if got.Topic != want.Topic || !bytes.Equal(got.Body, want.Body) {
				t.Fatalf("TopicBody %d: round trip\n got %#v\nwant %#v", op, got, want)
			}
		}
		for _, op := range []Operation{OpDeleteTopic, OpPurgeTopic, OpGetTopic, OpFanoutCursors} {
			want := TopicNameRequest{Topic: topic, ID: id}
			enc, err := EncodeTopicNameRequest(op, want)
			mustDecode("TopicName encode", err)
			got, err := DecodeTopicNameRequest(enc, op)
			mustDecode("TopicName", err)
			if got != want {
				t.Fatalf("TopicName %d: round trip\n got %#v\nwant %#v", op, got, want)
			}
		}
		for _, op := range []Operation{OpCreateUser, OpUpdateUser, OpDeleteUser} {
			want := UserRequest{Username: a, Body: body}
			enc, err := EncodeUserRequest(op, want)
			mustDecode("User encode", err)
			got, err := DecodeUserRequest(enc, op)
			mustDecode("User", err)
			if got.Username != want.Username || !bytes.Equal(got.Body, want.Body) {
				t.Fatalf("User %d: round trip\n got %#v\nwant %#v", op, got, want)
			}
		}
		for _, op := range []Operation{OpAttachChild, OpDetachChild} {
			want := ChildLinkRequest{Parent: topic, Child: id, DelayMs: n}
			enc, err := EncodeChildLinkRequest(op, want)
			mustDecode("ChildLink encode", err)
			got, err := DecodeChildLinkRequest(enc, op)
			mustDecode("ChildLink", err)
			if got != want {
				t.Fatalf("ChildLink %d: round trip\n got %#v\nwant %#v", op, got, want)
			}
		}
		{
			want := MemberRequest{ID: a, Addr: b, ClusterAddr: id, Status: topic, LastHeartbeat: n}
			enc, err := EncodeMemberRequest(want)
			mustDecode("Member encode", err)
			got, err := DecodeMemberRequest(enc)
			mustDecode("Member", err)
			if got != want {
				t.Fatalf("Member: round trip\n got %#v\nwant %#v", got, want)
			}
		}
		{
			want := JoinClusterRequest{ID: a, ClusterAddr: b, Fresh: flag}
			enc, err := EncodeJoinClusterRequest(want)
			mustDecode("Join encode", err)
			got, err := DecodeJoinClusterRequest(enc)
			mustDecode("Join", err)
			if got != want {
				t.Fatalf("Join: round trip\n got %#v\nwant %#v", got, want)
			}
		}
		{
			want := DecommissionRequest{ID: a, Cancel: flag}
			enc, err := EncodeDecommissionRequest(want)
			mustDecode("Decommission encode", err)
			got, err := DecodeDecommissionRequest(enc)
			mustDecode("Decommission", err)
			if got != want {
				t.Fatalf("Decommission: round trip\n got %#v\nwant %#v", got, want)
			}
		}
		{
			want := TopicPartitionStatsRequest{Topic: topic, Partition: partition}
			if enc, err := EncodeTopicPartitionStatsRequest(want); check("TopicPartitionStats", err) {
				got, err := DecodeTopicPartitionStatsRequest(enc)
				mustDecode("TopicPartitionStats", err)
				if got != want {
					t.Fatalf("TopicPartitionStats: round trip\n got %#v\nwant %#v", got, want)
				}
			}
		}
		{
			want := PartitionSegmentsRequest{Topic: topic, Partition: partition}
			if enc, err := EncodePartitionSegmentsRequest(want); check("PartitionSegments", err) {
				got, err := DecodePartitionSegmentsRequest(enc)
				mustDecode("PartitionSegments", err)
				if got != want {
					t.Fatalf("PartitionSegments: round trip\n got %#v\nwant %#v", got, want)
				}
			}
		}
		{
			want := FetchSegmentChunkRequest{Topic: topic, Partition: partition, BaseOffset: n, At: -n, Length: n ^ 0x5a5a}
			if enc, err := EncodeFetchSegmentChunkRequest(want); check("FetchSegmentChunk", err) {
				got, err := DecodeFetchSegmentChunkRequest(enc)
				mustDecode("FetchSegmentChunk", err)
				if got != want {
					t.Fatalf("FetchSegmentChunk: round trip\n got %#v\nwant %#v", got, want)
				}
			}
		}
		{
			want := PrepareHandoffRequest{Topic: topic, Partition: partition, FreezeTTLNanos: n, FreezeToken: id}
			if enc, err := EncodePrepareHandoffRequest(want); check("PrepareHandoff", err) {
				got, err := DecodePrepareHandoffRequest(enc)
				mustDecode("PrepareHandoff", err)
				if got != want {
					t.Fatalf("PrepareHandoff: round trip\n got %#v\nwant %#v", got, want)
				}
			}
		}
		{
			want := CompleteMoveRequest{Topic: topic, Partition: partition, ExpectedOwner: a, TargetID: b}
			if enc, err := EncodeCompleteMoveRequest(want); check("CompleteMove", err) {
				got, err := DecodeCompleteMoveRequest(enc)
				mustDecode("CompleteMove", err)
				if got != want {
					t.Fatalf("CompleteMove: round trip\n got %#v\nwant %#v", got, want)
				}
			}
		}
		{
			want := AbortMoveRequest{Topic: topic, Partition: partition, ExpectedTarget: b}
			if enc, err := EncodeAbortMoveRequest(want); check("AbortMove", err) {
				got, err := DecodeAbortMoveRequest(enc)
				mustDecode("AbortMove", err)
				if got != want {
					t.Fatalf("AbortMove: round trip\n got %#v\nwant %#v", got, want)
				}
			}
		}
		{
			want := GetAssignmentRequest{Topic: topic, Partition: partition}
			if enc, err := EncodeGetAssignmentRequest(want); check("GetAssignment", err) {
				got, err := DecodeGetAssignmentRequest(enc)
				mustDecode("GetAssignment", err)
				if got != want {
					t.Fatalf("GetAssignment: round trip\n got %#v\nwant %#v", got, want)
				}
			}
		}
		{
			want := Response{Status: status, ContentType: a, Body: body}
			enc, err := EncodeResponse(want)
			if status < 0 || status > math.MaxUint16 {
				if err == nil {
					t.Fatalf("Response: encoded status %d, which does not fit the uint16 wire field", status)
				}
			} else {
				mustDecode("Response encode", err)
				got, err := DecodeResponse(enc)
				mustDecode("Response", err)
				if got.Status != want.Status || got.ContentType != want.ContentType || !bytes.Equal(got.Body, want.Body) {
					t.Fatalf("Response: round trip\n got %#v\nwant %#v", got, want)
				}
			}
		}
	})
}

func equalProduce(a, b ProduceRequest) bool {
	return a.Topic == b.Topic && a.Key == b.Key && a.Partition == b.Partition && bytes.Equal(a.Payload, b.Payload)
}

func equalCommit(a, b CommitProduceRequest) bool {
	return a.Topic == b.Topic && a.Key == b.Key && a.TargetPartition == b.TargetPartition &&
		bytes.Equal(a.Payload, b.Payload) && a.CreatedAtUnixMs == b.CreatedAtUnixMs
}

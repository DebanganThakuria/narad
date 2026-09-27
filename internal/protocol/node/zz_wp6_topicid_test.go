package node

import (
	"bytes"
	"strings"
	"testing"
)

// zzWP6LegacyDecodeBatch is DecodeCommitProduceBatchRequest as releases
// before topic IDs shipped it: the records, then nothing.
func zzWP6LegacyDecodeBatch(payload []byte) (CommitProduceBatchRequest, error) {
	r, err := opReader(payload, OpCommitProduceBatch)
	if err != nil {
		return CommitProduceBatchRequest{}, err
	}
	count, err := r.i32()
	if err != nil {
		return CommitProduceBatchRequest{}, err
	}
	var records []CommitProduceRequest
	for range int(count) {
		record, err := readCommitProduce(&r)
		if err != nil {
			return CommitProduceBatchRequest{}, err
		}
		records = append(records, record)
	}
	if err := r.done(); err != nil {
		return CommitProduceBatchRequest{}, err
	}
	return CommitProduceBatchRequest{Records: records}, nil
}

func zzWP6Records(ids ...string) []CommitProduceRequest {
	records := make([]CommitProduceRequest, len(ids))
	for i, id := range ids {
		records[i] = CommitProduceRequest{Topic: "orders", TopicID: id, Key: "k", TargetPartition: 2, Payload: []byte{byte('a' + i)}, CreatedAtUnixMs: int64(i)}
	}
	return records
}

// A batch carries each record's topic incarnation, in runs, and decodes
// back exactly, including records without one mixed in.
func TestZZWP6CommitProduceBatchCarriesTopicIDs(t *testing.T) {
	for _, ids := range [][]string{
		{"a"},
		{"a", "a", "a"},
		{"a", "a", "b", "", "b"},
		{"", "a"},
		{"a", ""},
	} {
		want := CommitProduceBatchRequest{Records: zzWP6Records(ids...)}
		encoded, err := EncodeCommitProduceBatchRequest(want)
		if err != nil {
			t.Fatalf("%q: encode: %v", ids, err)
		}
		got, err := DecodeCommitProduceBatchRequest(encoded)
		if err != nil {
			t.Fatalf("%q: decode: %v", ids, err)
		}
		if len(got.Records) != len(want.Records) {
			t.Fatalf("%q: %d records, want %d", ids, len(got.Records), len(want.Records))
		}
		for i := range want.Records {
			if !equalCommit(got.Records[i], want.Records[i]) {
				t.Fatalf("%q: record %d = %+v, want %+v", ids, i, got.Records[i], want.Records[i])
			}
		}
	}

	single := CommitProduceRequest{Topic: "orders", TopicID: "a", Key: "k", Payload: []byte("p"), CreatedAtUnixMs: 9}
	encoded, err := EncodeCommitProduceRequest(single)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCommitProduceRequest(encoded)
	if err != nil || !equalCommit(got, single) {
		t.Fatalf("single round trip = (%+v, %v), want %+v", got, err, single)
	}
}

// Mixed-version clusters: a batch with no topic IDs is byte for byte the
// frame older releases send and accept, and a batch with them is refused
// by an older owner with the "trailing" error the dispatcher's fallback
// keys on.
func TestZZWP6CommitProduceBatchOlderOwnerCompatibility(t *testing.T) {
	legacy := CommitProduceBatchRequest{Records: zzWP6Records("", "", "")}
	encoded, err := EncodeCommitProduceBatchRequest(legacy)
	if err != nil {
		t.Fatal(err)
	}
	w := opWriter(OpCommitProduceBatch, 0)
	w.i32(int32(len(legacy.Records)))
	for _, r := range legacy.Records {
		if err := writeCommitProduce(w, r); err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(encoded, w.finish()) {
		t.Fatal("a batch without topic ids no longer encodes as the legacy frame")
	}
	if _, err := zzWP6LegacyDecodeBatch(encoded); err != nil {
		t.Fatalf("legacy decoder refused a batch without topic ids: %v", err)
	}
	// And the new decoder reads a legacy frame with empty IDs.
	got, err := DecodeCommitProduceBatchRequest(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range got.Records {
		if r.TopicID != "" {
			t.Fatalf("record %d decoded topic id %q from a legacy frame", i, r.TopicID)
		}
	}

	withIDs, err := EncodeCommitProduceBatchRequest(CommitProduceBatchRequest{Records: zzWP6Records("a", "a")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = zzWP6LegacyDecodeBatch(withIDs)
	if err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("legacy decoder on a batch with topic ids = %v, want the trailing-data refusal", err)
	}
}

// One run carries a whole single-incarnation batch: the section costs a
// few bytes per batch, not per record.
func TestZZWP6CommitProduceBatchTopicIDSectionIsOneRun(t *testing.T) {
	plain, err := EncodeCommitProduceBatchRequest(CommitProduceBatchRequest{Records: zzWP6Records("", "", "", "")})
	if err != nil {
		t.Fatal(err)
	}
	withIDs, err := EncodeCommitProduceBatchRequest(CommitProduceBatchRequest{Records: zzWP6Records("abcdef0123456789", "abcdef0123456789", "abcdef0123456789", "abcdef0123456789")})
	if err != nil {
		t.Fatal(err)
	}
	if extra := len(withIDs) - len(plain); extra != 4+4+4+16 {
		t.Fatalf("topic-id section = %d bytes, want %d (one run)", extra, 4+4+4+16)
	}
}

func TestZZWP6CommitProduceBatchRejectsMalformedTopicIDs(t *testing.T) {
	base, err := EncodeCommitProduceBatchRequest(CommitProduceBatchRequest{Records: zzWP6Records("", "")})
	if err != nil {
		t.Fatal(err)
	}
	section := func(build func(w *writer)) []byte {
		w := &writer{buf: append([]byte(nil), base...)}
		build(w)
		return w.finish()
	}
	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{"negative runs", section(func(w *writer) { w.i32(-1) }), "runs"},
		{"more runs than records", section(func(w *writer) { w.i32(3) }), "runs"},
		{"zero-length run", section(func(w *writer) { w.i32(1); w.i32(0); _ = w.string("a") }), "run of 0"},
		{"run past the records", section(func(w *writer) { w.i32(1); w.i32(3); _ = w.string("a") }), "run of 3"},
		{"runs short of the records", section(func(w *writer) { w.i32(1); w.i32(1); _ = w.string("a") }), "cover 1 of 2"},
		{"truncated id", section(func(w *writer) { w.i32(1); w.i32(2); w.i32(5) }), "EOF"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeCommitProduceBatchRequest(tc.data)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("decode = %v, want an error containing %q", err, tc.want)
			}
		})
	}
}

func FuzzZZWP6CommitProduceBatchTopicIDs(f *testing.F) {
	f.Add("a", "b", uint8(3), uint8(1))
	f.Add("", "a", uint8(1), uint8(2))
	f.Fuzz(func(t *testing.T, id1, id2 string, n1, n2 uint8) {
		var records []CommitProduceRequest
		for range int(n1 % 8) {
			records = append(records, CommitProduceRequest{Topic: "t", TopicID: id1, Payload: []byte("p")})
		}
		for range int(n2 % 8) {
			records = append(records, CommitProduceRequest{Topic: "t", TopicID: id2, Payload: []byte("q")})
		}
		b, err := EncodeCommitProduceBatchRequest(CommitProduceBatchRequest{Records: records})
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeCommitProduceBatchRequest(b)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(got.Records) != len(records) {
			t.Fatalf("%d records, want %d", len(got.Records), len(records))
		}
		for i := range records {
			if !equalCommit(got.Records[i], records[i]) {
				t.Fatalf("record %d = %+v, want %+v", i, got.Records[i], records[i])
			}
		}
	})
}

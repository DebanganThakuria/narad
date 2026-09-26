package ingress

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

// zzWP5LegacyRecord is a record as binaries before format 2 wrote it,
// built by hand so the test does not depend on the encoder under test.
func zzWP5LegacyRecord(topic, key string, partition uint32, createdAt uint64, payload []byte) []byte {
	var b []byte
	b = append(b, 1)
	b = binary.BigEndian.AppendUint32(b, uint32(len(topic)))
	b = append(b, topic...)
	b = binary.BigEndian.AppendUint32(b, uint32(len(key)))
	b = append(b, key...)
	b = binary.BigEndian.AppendUint32(b, partition)
	b = binary.BigEndian.AppendUint64(b, createdAt)
	b = binary.BigEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

// A record carries the incarnation of the topic it was accepted
// against, so a delete and recreate of the same name while it waits in
// the WAL can be detected. It round-trips through format 2.
func TestZZWP5ProduceRecordCarriesTopicID(t *testing.T) {
	want := ProduceRecord{Topic: "orders", TopicID: "f76e7d4b965dd285", Key: "k", TargetPartition: 2, Payload: []byte(`{"id":42}`), CreatedAtUnixMs: 99}
	encoded, err := EncodeProduceRecord(want)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != produceRecordFormatV2 {
		t.Fatalf("format byte = %d, want 2", encoded[0])
	}
	if len(encoded) != produceRecordSize(want) {
		t.Fatalf("encoded %d bytes, produceRecordSize says %d", len(encoded), produceRecordSize(want))
	}
	got, err := DecodeProduceRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != want.Topic || got.TopicID != want.TopicID || got.Key != want.Key || got.TargetPartition != want.TargetPartition ||
		!bytes.Equal(got.Payload, want.Payload) || got.CreatedAtUnixMs != want.CreatedAtUnixMs {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

// Without a topic ID the encoder still writes format 1, byte for byte
// what older binaries wrote (so they can still read it), and the
// decoder keeps reading format 1 with an empty TopicID.
func TestZZWP5ProduceRecordWithoutTopicIDStaysFormat1(t *testing.T) {
	legacy := zzWP5LegacyRecord("orders", "k", 2, 99, []byte(`{"id":42}`))
	encoded, err := EncodeProduceRecord(ProduceRecord{Topic: "orders", Key: "k", TargetPartition: 2, Payload: []byte(`{"id":42}`), CreatedAtUnixMs: 99})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, legacy) {
		t.Fatalf("record without a topic ID encodes as %x, want the legacy format-1 bytes %x", encoded, legacy)
	}
	got, err := DecodeProduceRecord(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != "orders" || got.TopicID != "" || got.Key != "k" || got.TargetPartition != 2 || string(got.Payload) != `{"id":42}` || got.CreatedAtUnixMs != 99 {
		t.Fatalf("legacy decode = %+v", got)
	}
}

func TestZZWP5ProduceRecordFormat2RejectsMalformed(t *testing.T) {
	encoded, err := EncodeProduceRecord(ProduceRecord{Topic: "orders", TopicID: "id-1", Key: "k", Payload: []byte("p")})
	if err != nil {
		t.Fatal(err)
	}
	// Topic "orders" ends at 1+4+6 = 11; the ID length prefix follows.
	emptyID := append(append([]byte(nil), encoded[:11]...), 0, 0, 0, 0)
	emptyID = append(emptyID, encoded[11+4+len("id-1"):]...)
	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{"unknown format", append([]byte{3}, encoded[1:]...), "unsupported produce record format 3"},
		{"truncated topic id", encoded[:13], "EOF"},
		{"empty topic id", emptyID, "empty topic id"},
		{"trailing bytes", append(append([]byte(nil), encoded...), 0), "trailing bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeProduceRecord(tc.data); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("DecodeProduceRecord() error = %v, want %q", err, tc.want)
			}
		})
	}
}

// The accept path stamps the ID into the WAL, and replay (the
// dispatcher's path) hands it back; a WAL written by an older binary
// mixed with new records replays both.
func TestZZWP5AcceptProduceWithTopicIDReplays(t *testing.T) {
	dir := t.TempDir()
	m, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx := context.Background()
	if _, err := m.AcceptProduce(ctx, "orders", "k0", 0, []byte("legacy")); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AcceptProduceWithTopicID(ctx, "orders", "incarnation-2", "k1", 1, []byte("stamped")); err != nil {
		t.Fatal(err)
	}
	var got []ProduceRecord
	if err := m.ReplayProduceFromCursor(wal.Cursor{}, func(r ProduceRecord, _ wal.Cursor) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].TopicID != "" || string(got[0].Payload) != "legacy" ||
		got[1].TopicID != "incarnation-2" || got[1].Key != "k1" || string(got[1].Payload) != "stamped" {
		t.Fatalf("replayed %+v", got)
	}
}

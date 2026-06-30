package ingress

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

func TestProduceRecordEncodeDecodeRoundTrip(t *testing.T) {
	want := ProduceRecord{
		Topic:           "orders",
		Key:             "customer-1",
		TargetPartition: 7,
		Payload:         []byte(`{"id":1}`),
		CreatedAtUnixMs: 123456,
	}

	encoded, err := EncodeProduceRecord(want)
	if err != nil {
		t.Fatalf("EncodeProduceRecord() error = %v", err)
	}
	got, err := DecodeProduceRecord(encoded)
	if err != nil {
		t.Fatalf("DecodeProduceRecord() error = %v", err)
	}
	if got.Topic != want.Topic ||
		got.Key != want.Key ||
		got.TargetPartition != want.TargetPartition ||
		string(got.Payload) != string(want.Payload) ||
		got.CreatedAtUnixMs != want.CreatedAtUnixMs {
		t.Fatalf("roundtrip = %+v, want %+v", got, want)
	}
}

func TestProduceRecordEncodeRejectsInvalidRecords(t *testing.T) {
	valid := ProduceRecord{
		Topic:           "orders",
		Key:             "customer-1",
		TargetPartition: 0,
		Payload:         []byte(`{"id":1}`),
		CreatedAtUnixMs: 123456,
	}

	for _, tc := range []struct {
		name   string
		record ProduceRecord
		want   string
	}{
		{
			name:   "missing topic",
			record: ProduceRecord{Key: valid.Key, TargetPartition: valid.TargetPartition, Payload: valid.Payload, CreatedAtUnixMs: valid.CreatedAtUnixMs},
			want:   "topic required",
		},
		{
			name:   "negative partition",
			record: ProduceRecord{Topic: valid.Topic, Key: valid.Key, TargetPartition: -1, Payload: valid.Payload, CreatedAtUnixMs: valid.CreatedAtUnixMs},
			want:   "target partition must be >= 0",
		},
		{
			name:   "empty payload",
			record: ProduceRecord{Topic: valid.Topic, Key: valid.Key, TargetPartition: valid.TargetPartition, CreatedAtUnixMs: valid.CreatedAtUnixMs},
			want:   "payload required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EncodeProduceRecord(tc.record)
			if err == nil {
				t.Fatal("EncodeProduceRecord() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("EncodeProduceRecord() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestProduceRecordDecodeRejectsMalformedRecords(t *testing.T) {
	valid := ProduceRecord{
		Topic:           "orders",
		Key:             "customer-1",
		TargetPartition: 7,
		Payload:         []byte(`{"id":1}`),
		CreatedAtUnixMs: 123456,
	}
	encoded, err := EncodeProduceRecord(valid)
	if err != nil {
		t.Fatalf("EncodeProduceRecord() error = %v", err)
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
			name: "unsupported format",
			data: append([]byte{99}, encoded[1:]...),
			want: "unsupported produce record format 99",
		},
		{
			name: "truncated topic length",
			data: encoded[:3],
			want: "EOF",
		},
		{
			name: "truncated payload",
			data: encoded[:len(encoded)-1],
			want: "EOF",
		},
		{
			name: "trailing bytes",
			data: append(append([]byte(nil), encoded...), 0),
			want: "trailing bytes",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeProduceRecord(tc.data)
			if err == nil {
				t.Fatal("DecodeProduceRecord() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("DecodeProduceRecord() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestManagerAcceptProducePersistsAndReplays(t *testing.T) {
	dir := t.TempDir()
	manager, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}

	accepted, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", 2, []byte(`{"id":1}`))
	if err != nil {
		t.Fatalf("AcceptProduce() error = %v", err)
	}
	if got := manager.DurableProduceNext(); got != 1 {
		t.Fatalf("DurableProduceNext() = %d, want 1", got)
	}
	if accepted.Topic != "orders" || accepted.TargetPartition != 2 {
		t.Fatalf("AcceptProduce() = %+v, want topic orders partition 2", accepted)
	}
	if accepted.WAL.Seq != 0 {
		t.Fatalf("accepted WAL seq = %d, want 0", accepted.WAL.Seq)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("reopen manager error = %v", err)
	}
	defer reopened.Close()
	if got := reopened.DurableProduceNext(); got != 1 {
		t.Fatalf("reopened DurableProduceNext() = %d, want 1", got)
	}

	var got []ProduceRecord
	if err := reopened.ReplayProduce(0, func(record ProduceRecord) error {
		got = append(got, record)
		return nil
	}); err != nil {
		t.Fatalf("ReplayProduce() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("replayed records = %d, want 1", len(got))
	}
	if got[0].Topic != "orders" ||
		got[0].Key != "customer-1" ||
		got[0].TargetPartition != 2 ||
		string(got[0].Payload) != `{"id":1}` ||
		got[0].WAL.Seq != 0 {
		t.Fatalf("replayed record = %+v", got[0])
	}
}

func TestManagerAcceptProduceAppendsSequentialWALRecords(t *testing.T) {
	dir := t.TempDir()
	manager, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	defer manager.Close()

	acceptedByWAL := make(map[wal.RecordID]AcceptedProduce)
	for i := range 16 {
		partition := i % 4
		accepted, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", partition, []byte(`{"id":1}`))
		if err != nil {
			t.Fatalf("AcceptProduce(%d) error = %v", i, err)
		}
		if accepted.WAL.Seq != uint64(i) {
			t.Fatalf("accepted WAL seq = %d, want %d", accepted.WAL.Seq, i)
		}
		if accepted.TargetPartition != partition {
			t.Fatalf("accepted partition = %d, want %d", accepted.TargetPartition, partition)
		}
		acceptedByWAL[accepted.WAL] = accepted
	}

	var replayed int
	if err := manager.ReplayProduce(0, func(record ProduceRecord) error {
		accepted, ok := acceptedByWAL[record.WAL]
		if !ok {
			t.Fatalf("replayed unknown WAL record %+v", record.WAL)
		}
		if record.TargetPartition != accepted.TargetPartition {
			t.Fatalf("replayed partition = %d, want %d", record.TargetPartition, accepted.TargetPartition)
		}
		replayed++
		return nil
	}); err != nil {
		t.Fatalf("ReplayProduce() error = %v", err)
	}
	if replayed != len(acceptedByWAL) {
		t.Fatalf("replayed records = %d, want %d", replayed, len(acceptedByWAL))
	}

	got := manager.DurableProduceNext()
	if got != uint64(len(acceptedByWAL)) {
		t.Fatalf("DurableProduceNext() = %d, want %d", got, len(acceptedByWAL))
	}
	if err := manager.StoreProduceCheckpoint(got); err != nil {
		t.Fatalf("StoreProduceCheckpoint() error = %v", err)
	}
	checkpoint, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if checkpoint != got {
		t.Fatalf("LoadProduceCheckpoint() = %d, want %d", checkpoint, got)
	}
}

func TestManagerReplayFromSequence(t *testing.T) {
	dir := t.TempDir()
	manager, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	if _, err := manager.AcceptProduce(context.Background(), "orders", "k1", 0, []byte(`{"id":1}`)); err != nil {
		t.Fatalf("AcceptProduce(first) error = %v", err)
	}
	second, err := manager.AcceptProduce(context.Background(), "orders", "k2", 0, []byte(`{"id":2}`))
	if err != nil {
		t.Fatalf("AcceptProduce(second) error = %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var got []ProduceRecord
	if err := ReplayProduce(produceWALDir(dir), second.WAL.Seq, func(record ProduceRecord) error {
		got = append(got, record)
		return nil
	}); err != nil {
		t.Fatalf("ReplayProduce() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("records from seq %d = %d, want 1", second.WAL.Seq, len(got))
	}
	if got[0].WAL.Seq != second.WAL.Seq || got[0].Key != "k2" || string(got[0].Payload) != `{"id":2}` {
		t.Fatalf("replayed record = %+v, want second %+v", got[0], second)
	}
}

func TestManagerReplayFromCursorStartsAfterRecord(t *testing.T) {
	dir := t.TempDir()
	manager, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	if _, err := manager.AcceptProduce(context.Background(), "orders", "k1", 0, []byte(`{"id":1}`)); err != nil {
		t.Fatalf("AcceptProduce(first) error = %v", err)
	}
	second, err := manager.AcceptProduce(context.Background(), "orders", "k2", 0, []byte(`{"id":2}`))
	if err != nil {
		t.Fatalf("AcceptProduce(second) error = %v", err)
	}
	if _, err := manager.AcceptProduce(context.Background(), "orders", "k3", 0, []byte(`{"id":3}`)); err != nil {
		t.Fatalf("AcceptProduce(third) error = %v", err)
	}

	var cursor wal.Cursor
	if err := manager.ReplayProduceFromCursor(wal.Cursor{}, func(record ProduceRecord, next wal.Cursor) error {
		if record.WAL.Seq == 0 {
			cursor = next
		}
		return nil
	}); err != nil {
		t.Fatalf("ReplayProduceFromCursor(initial) error = %v", err)
	}
	if cursor.Seq != 1 || cursor.Offset <= 0 {
		t.Fatalf("cursor after first = %+v, want seq 1 and positive offset", cursor)
	}

	var got []ProduceRecord
	if err := manager.ReplayProduceFromCursor(cursor, func(record ProduceRecord, _ wal.Cursor) error {
		got = append(got, record)
		return nil
	}); err != nil {
		t.Fatalf("ReplayProduceFromCursor(cursor) error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("records from cursor = %d, want 2", len(got))
	}
	if got[0].WAL.Seq != second.WAL.Seq || got[0].Key != "k2" || string(got[0].Payload) != `{"id":2}` {
		t.Fatalf("first cursor replay = %+v, want second %+v", got[0], second)
	}
}

func TestManagerProduceCheckpointPersists(t *testing.T) {
	dir := t.TempDir()
	manager, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	nextSeq, err := manager.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 0 {
		t.Fatalf("initial checkpoint = %d, want 0", nextSeq)
	}
	if err := manager.StoreProduceCheckpoint(42); err != nil {
		t.Fatalf("StoreProduceCheckpoint() error = %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("reopen manager error = %v", err)
	}
	defer reopened.Close()

	nextSeq, err = reopened.LoadProduceCheckpoint()
	if err != nil {
		t.Fatalf("reopened LoadProduceCheckpoint() error = %v", err)
	}
	if nextSeq != 42 {
		t.Fatalf("checkpoint = %d, want 42", nextSeq)
	}
}

func testWALOptions() wal.Options {
	return wal.Options{
		SegmentBytes: 1024,
		SyncInterval: time.Hour,
		MaxRecord:    1024,
	}
}

func produceWALDir(dataDir string) string {
	return filepath.Join(dataDir, "ingress", "produce")
}

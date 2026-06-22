package ingress

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/wal"
)

func TestProduceRecordEncodeDecodeRoundTrip(t *testing.T) {
	want := ProduceRecord{
		MessageID:       "message-1",
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
	if got.MessageID != want.MessageID ||
		got.Topic != want.Topic ||
		got.Key != want.Key ||
		got.TargetPartition != want.TargetPartition ||
		string(got.Payload) != string(want.Payload) ||
		got.CreatedAtUnixMs != want.CreatedAtUnixMs {
		t.Fatalf("roundtrip = %+v, want %+v", got, want)
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
	if accepted.MessageID == "" {
		t.Fatal("AcceptProduce() returned empty message id")
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
	if got[0].MessageID != accepted.MessageID ||
		got[0].Topic != "orders" ||
		got[0].Key != "customer-1" ||
		got[0].TargetPartition != 2 ||
		string(got[0].Payload) != `{"id":1}` ||
		got[0].WAL.Seq != 0 {
		t.Fatalf("replayed record = %+v", got[0])
	}
}

func TestManagerAcceptProduceDistributesPartitionsAcrossShards(t *testing.T) {
	dir := t.TempDir()
	manager, err := OpenManagerWithOptions(dir, Options{
		WAL:    testWALOptions(),
		Shards: 4,
	})
	if err != nil {
		t.Fatalf("OpenManagerWithOptions() error = %v", err)
	}
	defer manager.Close()

	acceptedByID := make(map[string]AcceptedProduce)
	recordsPerShard := make(map[int]int)
	for i := range 16 {
		partition := i % 4
		accepted, err := manager.AcceptProduce(context.Background(), "orders", "customer-1", partition, []byte(`{"id":1}`))
		if err != nil {
			t.Fatalf("AcceptProduce(%d) error = %v", i, err)
		}
		if accepted.WALShard < 0 || accepted.WALShard >= 4 {
			t.Fatalf("accepted WAL shard = %d, want [0,4)", accepted.WALShard)
		}
		if accepted.WALShard != partition {
			t.Fatalf("accepted WAL shard = %d, want partition shard %d", accepted.WALShard, partition)
		}
		acceptedByID[accepted.MessageID] = accepted
		recordsPerShard[accepted.WALShard]++
	}
	if len(recordsPerShard) != 4 {
		t.Fatalf("records used %d WAL shard(s), want 4", len(recordsPerShard))
	}
	for shard := range 4 {
		if got := recordsPerShard[shard]; got != 4 {
			t.Fatalf("records on shard %d = %d, want 4", shard, got)
		}
	}

	var replayed int
	if err := manager.ReplayProduce(0, func(record ProduceRecord) error {
		accepted, ok := acceptedByID[record.MessageID]
		if !ok {
			t.Fatalf("replayed unknown message id %q", record.MessageID)
		}
		if record.WALShard != accepted.WALShard {
			t.Fatalf("replayed shard = %d, want %d", record.WALShard, accepted.WALShard)
		}
		replayed++
		return nil
	}); err != nil {
		t.Fatalf("ReplayProduce() error = %v", err)
	}
	if replayed != len(acceptedByID) {
		t.Fatalf("replayed records = %d, want %d", replayed, len(acceptedByID))
	}

	for shard, want := range recordsPerShard {
		got, err := manager.DurableProduceNextForShard(shard)
		if err != nil {
			t.Fatalf("DurableProduceNextForShard(%d) error = %v", shard, err)
		}
		if got != uint64(want) {
			t.Fatalf("DurableProduceNextForShard(%d) = %d, want %d", shard, got, want)
		}
		if err := manager.StoreProduceCheckpointForShard(shard, got); err != nil {
			t.Fatalf("StoreProduceCheckpointForShard(%d) error = %v", shard, err)
		}
		checkpoint, err := manager.LoadProduceCheckpointForShard(shard)
		if err != nil {
			t.Fatalf("LoadProduceCheckpointForShard(%d) error = %v", shard, err)
		}
		if checkpoint != got {
			t.Fatalf("LoadProduceCheckpointForShard(%d) = %d, want %d", shard, checkpoint, got)
		}
	}
}

func TestManagerReplayFromSequence(t *testing.T) {
	dir := t.TempDir()
	manager, err := OpenManager(dir, testWALOptions())
	if err != nil {
		t.Fatalf("OpenManager() error = %v", err)
	}
	first, err := manager.AcceptProduce(context.Background(), "orders", "k1", 0, []byte(`{"id":1}`))
	if err != nil {
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
	if got[0].MessageID != second.MessageID {
		t.Fatalf("replayed message id = %q, want %q; first was %q", got[0].MessageID, second.MessageID, first.MessageID)
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
	if got[0].MessageID != second.MessageID || got[0].WAL.Seq != 1 {
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
		SyncBytes:    1,
		MaxRecord:    1024,
	}
}

func produceWALDir(dataDir string) string {
	return filepath.Join(dataDir, "ingress", "produce")
}

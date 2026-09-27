package messaging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP20Engine is an Engine over a fake metastore holding one topic "t"
// with parts partitions, all owned here and none opened yet.
func zzWP20Engine(t *testing.T, parts int) (*Engine, string) {
	t.Helper()
	dir := t.TempDir()
	ms := newMessagingFakeMetastore()
	ms.topics["t"] = topic.Topic{Name: "t", ID: "t-id", Partitions: parts, VisibilityTimeoutMs: 30000}
	ms.topicVersions["t"] = 7
	logs := runtime.NewLogs(dir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 1 << 20, MaxAckedAhead: 1 << 20}, nil
	}, nil)
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })
	return e, dir
}

// zzWP20Append makes n records visible on partition p.
func zzWP20Append(t *testing.T, e *Engine, p, n int) {
	t.Helper()
	l, err := e.logs.Get("t", p)
	if err != nil {
		t.Fatal(err)
	}
	batch := make([][]byte, n)
	for i := range batch {
		batch[i] = storage.EncodeKeyedRecord("", 1, []byte(`{"n":1}`))
	}
	_, last, err := l.AppendBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.AdvanceHighWatermark(last + 1); err != nil {
		t.Fatal(err)
	}
}

// TestZZWP20ScanPastFirstPartition checks a consume whose scan starts on
// an empty partition takes the record a later one holds, from every
// start, with the rest of the scan's logs resolved together.
func TestZZWP20ScanPastFirstPartition(t *testing.T) {
	for _, parts := range []int{2, 8, 20} {
		e, _ := zzWP20Engine(t, parts)
		zzWP20Append(t, e, parts-1, parts)
		for start := range parts {
			msg, found, err := e.Consume(context.Background(), "t", ConsumeOpts{ScanStart: &start})
			if err != nil || !found || msg.Partition != parts-1 {
				t.Fatalf("P=%d start %d: consume = partition %d found=%v err=%v, want the record on partition %d", parts, start, msg.Partition, found, err, parts-1)
			}
		}
		if _, found, err := e.Consume(context.Background(), "t", ConsumeOpts{}); err != nil || found {
			t.Fatalf("P=%d: drained topic consume found=%v err=%v, want empty", parts, found, err)
		}
	}
}

// TestZZWP20ScanUnopenablePartitionDoesNotHideEarlierRecords checks that
// a partition whose log cannot be opened fails a scan only once the
// scan reaches it, as it did when each log was resolved as the scan got
// there: resolving the rest of the scan's logs together must not let it
// hide a record on a partition the scan reaches first.
func TestZZWP20ScanUnopenablePartitionDoesNotHideEarlierRecords(t *testing.T) {
	e, dir := zzWP20Engine(t, 4)
	for p := range 3 {
		if _, err := e.logs.Get("t", p); err != nil {
			t.Fatal(err)
		}
	}
	// Partition 3's directory cannot be created: a file sits in its place.
	bad := storage.TopicPartitionDir(dir, "t", 3)
	if err := os.MkdirAll(filepath.Dir(bad), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bad, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := e.logs.Get("t", 3); err == nil {
		t.Fatal("partition 3 opened; the test needs it unopenable")
	}
	zzWP20Append(t, e, 2, 1)

	start := 0
	msg, found, err := e.Consume(context.Background(), "t", ConsumeOpts{ScanStart: &start})
	if err != nil || !found || msg.Partition != 2 {
		t.Fatalf("consume = partition %d found=%v err=%v, want partition 2's record ahead of the broken partition 3", msg.Partition, found, err)
	}
	// Past the record, the scan reaches partition 3 and reports it.
	if _, _, err := e.Consume(context.Background(), "t", ConsumeOpts{ScanStart: &start}); err == nil {
		t.Fatal("a scan that reached the unopenable partition reported no error")
	}
}

package messaging

import (
	"context"
	"fmt"
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

// A transient read error after a successful reserve must release the
// reservation immediately; otherwise the message hides for the whole
// visibility timeout after a 500 the client retries at once.
func TestQueueConsumeReleasesReservationOnTransientReadError(t *testing.T) {
	dataDir := t.TempDir()
	const topicName = "orders"
	ms := newMessagingFakeMetastore()
	ms.topics[topicName] = topic.Topic{Name: topicName, Partitions: 1, VisibilityTimeoutMs: 60_000}

	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	engine := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs, nil, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	if _, _, err := engine.Produce(context.Background(), topicName, "", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("Produce: %v", err)
	}
	log, err := logs.Get(topicName, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	// Close the log underneath the engine (it stays in the Logs map): the
	// read path now fails with a closed-file error, which is transient,
	// not corruption.
	if err := log.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, found, err := engine.Consume(context.Background(), topicName, ConsumeOpts{})
	if err == nil || found {
		t.Fatalf("Consume() = found=%v err=%v, want a transient error", found, err)
	}
	if inFlight, _ := offsets.Snapshot(topicName, 0); inFlight != 0 {
		t.Fatalf("in-flight after transient read error = %d, want 0 (reservation released)", inFlight)
	}
	if next := offsets.Next(topicName, 0); next != 0 {
		t.Fatalf("frontier moved to %d after a transient error; must stay at 0", next)
	}
}

// A consumer whose frontier is below the oldest retained offset (its
// segment was reaped) is moved to the oldest retained offset in one
// step and then served the first retained record, instead of walking
// the gap one poison offset per request.
func TestQueueConsumeSkipsFrontierToOldestRetainedOffset(t *testing.T) {
	dataDir := t.TempDir()
	const topicName = "orders"

	// Phase 1: write records across two segments, then delete the first
	// segment file to simulate retention having reaped it.
	logs1 := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond, SegmentBytes: 256}, nil, nil)
	log1, err := logs1.Get(topicName, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	const records = 12
	for i := range records {
		payload := storage.EncodeKeyedRecord("", 1, []byte(`{"id":`+string(rune('a'+i))+`,"pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`))
		if _, err := log1.Append(payload); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if err := log1.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
	}
	if err := log1.AdvanceHighWatermark(records); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	if err := logs1.CloseAll(); err != nil {
		t.Fatalf("CloseAll: %v", err)
	}
	partitionDir := storage.TopicPartitionDir(dataDir, topicName, 0)
	segs, err := storage.ListPartitionSegments(partitionDir)
	if err != nil {
		t.Fatalf("ListPartitionSegments: %v", err)
	}
	if len(segs) < 2 {
		t.Fatalf("need at least 2 segments to reap one, got %d", len(segs))
	}
	oldest := segs[1].BaseOffset
	first := filepath.Join(partitionDir, fmt.Sprintf("%020d.log", segs[0].BaseOffset))
	if err := os.Remove(first); err != nil {
		t.Fatalf("remove first segment: %v", err)
	}

	// Phase 2: consume with a fresh frontier (nothing ever committed).
	ms := newMessagingFakeMetastore()
	ms.topics[topicName] = topic.Topic{Name: topicName, Partitions: 1, VisibilityTimeoutMs: 60_000}
	logs2 := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond, SegmentBytes: 256}, ms, nil)
	t.Cleanup(func() { _ = logs2.CloseAll() })
	persisted := int64(-1)
	offsets := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, func(_ string, _ int, offset int64) { persisted = offset })
	engine := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, offsets, logs2, nil, nil,
		slog.New(slog.NewTextHandler(io.Discard, nil)), "")

	msg, found, err := engine.Consume(context.Background(), topicName, ConsumeOpts{})
	if err != nil || !found {
		t.Fatalf("Consume() = found=%v err=%v, want the first retained record", found, err)
	}
	if msg.Offset != oldest {
		t.Fatalf("delivered offset = %d, want oldest retained %d", msg.Offset, oldest)
	}
	if persisted != oldest-1 {
		t.Fatalf("persisted frontier = %d, want %d (oldest-1)", persisted, oldest-1)
	}
	if next := offsets.Next(topicName, 0); next != oldest {
		t.Fatalf("Next() = %d, want %d", next, oldest)
	}
}

package messaging

// Replay-mode error contracts: a negative offset is a malformed request
// (ErrInvalid → 400), and an offset that retention has already reaped is
// a fact about the request, not a server fault (ErrHandleStale → 410).
// Both used to fall through to opaque 500s.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/errs"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

func TestReplayRejectsNegativeOffset(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil)

	_, _, err := engine.Consume(context.Background(), "orders", ConsumeOpts{
		Partition: new(0),
		Offset:    new(int64(-1)),
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Consume(offset=-1) error = %v, want %v", err, ErrInvalid)
	}
}

func TestReplayReapedOffsetIsGoneNotServerError(t *testing.T) {
	dataDir := t.TempDir()
	const topicName = "orders"

	// Phase 1: tiny segments so every synced frame rolls, giving one
	// record per segment file (offsets 0..4), then close durably.
	logs1 := runtime.NewLogs(dataDir, storage.Options{
		FlushInterval: 5 * time.Millisecond,
		SegmentBytes:  1,
	}, nil, nil)
	log1, err := logs1.Get(topicName, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	for i := range 5 {
		if _, err := log1.Append(storage.EncodeKeyedRecord("", 1, []byte{'a' + byte(i)})); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if err := log1.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
	}
	if err := log1.AdvanceHighWatermark(5); err != nil {
		t.Fatalf("AdvanceHighWatermark: %v", err)
	}
	if err := logs1.CloseAll(); err != nil {
		t.Fatalf("CloseAll: %v", err)
	}

	// Simulate the retention reaper: remove the two oldest sealed
	// segment files, exactly as deleteSegmentLocked does on disk.
	partitionDir := storage.TopicPartitionDir(dataDir, topicName, 0)
	segs, err := filepath.Glob(filepath.Join(partitionDir, "*.log"))
	if err != nil || len(segs) < 3 {
		t.Fatalf("segment files = %v (err %v), want >= 3 so two can be reaped", segs, err)
	}
	sort.Strings(segs)
	for _, path := range segs[:2] {
		if err := os.Remove(path); err != nil {
			t.Fatalf("Remove(%s): %v", path, err)
		}
	}

	// Phase 2: reopen through the engine and replay.
	ms := newMessagingFakeMetastore()
	ms.topics[topicName] = topic.Topic{Name: topicName, Partitions: 1}
	engine := newTestEngineWithDir(t, dataDir, ms, nil, nil)
	ctx := context.Background()

	// A reaped offset existed and is gone: stale, not a server fault.
	_, _, err = engine.Consume(ctx, topicName, ConsumeOpts{Partition: new(0), Offset: new(int64(0))})
	if !errors.Is(err, errs.ErrHandleStale) {
		t.Fatalf("Consume(reaped offset 0) error = %v, want %v", err, errs.ErrHandleStale)
	}

	// The oldest retained offset must still replay, receipt-handle-free.
	msg, found, err := engine.Consume(ctx, topicName, ConsumeOpts{Partition: new(0), Offset: new(int64(2))})
	if err != nil || !found {
		t.Fatalf("Consume(retained offset 2) = found %v, err %v; want a message", found, err)
	}
	if msg.Offset != 2 || string(msg.Payload) != "c" {
		t.Fatalf("replayed offset %d payload %q, want 2 %q", msg.Offset, msg.Payload, "c")
	}
	if msg.ReceiptHandle != "" {
		t.Fatalf("replay returned receipt handle %q, want none", msg.ReceiptHandle)
	}
}

package messaging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/broker/ingress"
	"github.com/debanganthakuria/narad/internal/broker/runtime"
	"github.com/debanganthakuria/narad/internal/consumer"
	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// TestPrepareHandoffCarriesAckedAhead pins that acks landing out of
// order above the frontier travel with the handoff: the new owner must
// not redeliver them, so the transfer info carries the live shard's
// acked-ahead set next to the in-memory frontier.
func TestPrepareHandoffCarriesAckedAhead(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 30000}
	e := newTestEngine(t, ms, nil, nil)
	ctx := context.Background()

	recs := make([]ingress.ProduceRecord, 0, 3)
	for i := range 3 {
		recs = append(recs, ingress.ProduceRecord{Topic: "orders", TargetPartition: 0, Key: "k", Payload: []byte{byte('a' + i)}})
	}
	if _, err := e.CommitAcceptedProduceBatch(ctx, recs); err != nil {
		t.Fatalf("commit: %v", err)
	}
	handles := make([]consumer.Handle, 0, 3)
	for range 3 {
		msg, found, err := e.Consume(ctx, "orders", ConsumeOpts{})
		if err != nil || !found {
			t.Fatalf("consume: found=%v err=%v", found, err)
		}
		handles = append(handles, decodeHandleForTest(t, msg.ReceiptHandle))
	}
	// Ack 1 and 2 while 0 stays leased: the frontier is still -1.
	for _, h := range handles[1:] {
		if err := e.Ack(ctx, "orders", h); err != nil {
			t.Fatalf("ack %d: %v", h.Offset, err)
		}
	}

	info, err := e.PrepareHandoff(ctx, "orders", 0, 40*time.Millisecond)
	if err != nil {
		t.Fatalf("PrepareHandoff: %v", err)
	}
	if !info.HasCommitted || info.CommittedOffset != -1 {
		t.Fatalf("handoff committed = (%v, %d), want (true, -1): offset 0 is still leased", info.HasCommitted, info.CommittedOffset)
	}
	if !slices.Equal(info.AckedAhead, []int64{1, 2}) {
		t.Fatalf("handoff AckedAhead = %v, want [1 2]", info.AckedAhead)
	}
}

// TestPartitionTransferInfoFallsBackToAheadFile pins the branch a node
// without an in-flight tracker takes: the acked-ahead set and its
// frontier come from consumer.ahead on disk, and the file's frontier
// wins over an older consumer.offset.
func TestPartitionTransferInfoFallsBackToAheadFile(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	dataDir := t.TempDir()
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteConsumerOffset(dir, 2); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteConsumerAhead(dir, 0, 1, 4, []int64{6, 7}); err != nil {
		t.Fatal(err)
	}
	logs := runtime.NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })
	e := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, nil, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { e.dispatch.close() })

	info, err := e.PartitionTransferInfo(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("PartitionTransferInfo: %v", err)
	}
	if !info.HasCommitted || info.CommittedOffset != 4 {
		t.Fatalf("committed = (%v, %d), want (true, 4): the ahead record's frontier is newer than consumer.offset", info.HasCommitted, info.CommittedOffset)
	}
	if !slices.Equal(info.AckedAhead, []int64{6, 7}) {
		t.Fatalf("AckedAhead = %v, want [6 7] from consumer.ahead", info.AckedAhead)
	}

	// The production shape: a tracker exists but holds no shard for the
	// partition yet (restarted owner, nobody consumed since). The file
	// must still be consulted, or every such move would strip the set.
	tracker := consumer.NewInFlight(func(context.Context, string) (consumer.Caps, error) {
		return consumer.Caps{MaxInFlight: 10, MaxAckedAhead: 10}, nil
	}, nil)
	live := NewEngine(ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, tracker, logs, nil, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), "")
	t.Cleanup(func() { live.dispatch.close() })
	info, err = live.PartitionTransferInfo(context.Background(), "orders", 0)
	if err != nil {
		t.Fatalf("PartitionTransferInfo (tracker, no shard): %v", err)
	}
	if info.CommittedOffset != 4 || !slices.Equal(info.AckedAhead, []int64{6, 7}) {
		t.Fatalf("with a tracker but no live shard: committed %d acked-ahead %v, want 4 [6 7] from disk", info.CommittedOffset, info.AckedAhead)
	}
}

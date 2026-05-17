package messaging

import (
	"context"
	"os"
	"testing"

	"github.com/debanganthakuria/narad/internal/domain/topic"
)

func TestProduceCommittedVisibilityPersistsAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	replicator := &fakeReplicator{}
	engine := newTestEngineWithDir(t, dataDir, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, replicator)

	if _, _, err := engine.Produce(context.Background(), "orders", "", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if err := engine.logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}

	restarted := newTestEngineWithDir(t, dataDir, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, replicator)
	partitionIdx := 0
	msg, found, err := restarted.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partitionIdx, Wait: 0})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if !found {
		t.Fatal("Consume() found = false, want true")
	}
	if msg.Partition != 0 || msg.Offset != 0 {
		t.Fatalf("Consume() message = %+v, want partition 0 offset 0", msg)
	}
	if string(msg.Payload) != `{"id":1}` {
		t.Fatalf("Consume() payload = %q, want %q", string(msg.Payload), `{"id":1}`)
	}
	if msg.ReceiptHandle == "" {
		t.Fatal("Consume() receipt handle = empty, want non-empty")
	}
}

func TestProduceUncommittedVisibilityStaysHiddenAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngineWithDir(t, dataDir, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, &fakeReplicator{})
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	hwmPath := partitionHWMPath(dataDir, "orders", 0)
	if err := os.WriteFile(hwmPath, []byte{0, 0, 0, 0, 0, 0, 0, 0}, 0o644); err != nil {
		t.Fatalf("WriteFile(hwm): %v", err)
	}
	if err := engine.logs.CloseAll(); err != nil {
		t.Fatalf("CloseAll() error = %v", err)
	}

	restarted := newTestEngineWithDir(t, dataDir, ms, &fakeSchemas{}, fixedPartitioner{picked: 0}, &fakeReplicator{})
	partitionIdx := 0
	_, found, err := restarted.Consume(context.Background(), "orders", ConsumeOpts{Partition: &partitionIdx, Wait: 0})
	if err != nil {
		t.Fatalf("Consume() error = %v", err)
	}
	if found {
		t.Fatal("Consume() found = true, want false")
	}
}

func TestReplayReadUsesHighWatermark(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1}
	engine := newTestEngine(t, ms, nil, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, found, err := engine.replayRead("orders", 0, 0, 1); err != nil {
		t.Fatalf("replayRead() error = %v", err)
	} else if found {
		t.Fatal("replayRead() found = true before high watermark advance")
	}
	if err := log.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}
	if _, found, err := engine.replayRead("orders", 0, 0, 1); err != nil {
		t.Fatalf("replayRead() error = %v", err)
	} else if !found {
		t.Fatal("replayRead() found = false after high watermark advance")
	}
}

func TestConsumeUsesHighWatermark(t *testing.T) {
	ms := newMessagingFakeMetastore()
	ms.topics["orders"] = topic.Topic{Name: "orders", Partitions: 1, VisibilityTimeoutMs: 1000}
	engine := newTestEngine(t, ms, nil, nil, nil)
	log, err := engine.logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if _, err := log.Append([]byte(`{"id":1}`)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if _, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: 0}); err != nil {
		t.Fatalf("Consume() error = %v", err)
	} else if found {
		t.Fatal("Consume() found = true before high watermark advance")
	}
	if err := log.AdvanceHighWatermark(1); err != nil {
		t.Fatalf("AdvanceHighWatermark() error = %v", err)
	}
	if msg, found, err := engine.Consume(context.Background(), "orders", ConsumeOpts{Wait: 0}); err != nil {
		t.Fatalf("Consume() error = %v", err)
	} else if !found {
		t.Fatal("Consume() found = false after high watermark advance")
	} else if msg.Offset != 0 {
		t.Fatalf("Consume() offset = %d, want 0", msg.Offset)
	}
}

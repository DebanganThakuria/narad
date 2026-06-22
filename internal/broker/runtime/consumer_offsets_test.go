package runtime

import (
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

func TestConsumerOffsetCommitterFlushesLatestOffsetOnClose(t *testing.T) {
	dataDir := t.TempDir()
	committer := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)

	committer.Commit("orders", 0, 1)
	committer.Commit("orders", 0, 3)
	committer.Commit("orders", 0, 2)

	if err := committer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	got, ok, err := storage.ReadConsumerOffset(storage.TopicPartitionDir(dataDir, "orders", 0))
	if err != nil {
		t.Fatalf("ReadConsumerOffset() error = %v", err)
	}
	if !ok {
		t.Fatal("consumer offset was not persisted")
	}
	if got != 3 {
		t.Fatalf("consumer offset = %d, want 3", got)
	}
}

func TestConsumerOffsetCommitterCanPersistOffsetZero(t *testing.T) {
	dataDir := t.TempDir()
	committer := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)

	committer.Commit("orders", 0, 0)

	if err := committer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	got, ok, err := storage.ReadConsumerOffset(storage.TopicPartitionDir(dataDir, "orders", 0))
	if err != nil {
		t.Fatalf("ReadConsumerOffset() error = %v", err)
	}
	if !ok {
		t.Fatal("consumer offset was not persisted")
	}
	if got != 0 {
		t.Fatalf("consumer offset = %d, want 0", got)
	}
}

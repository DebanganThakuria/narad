package runtime

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

func TestConsumerOffsetCommitterFlushesLatestOffsetOnClose(t *testing.T) {
	dataDir := t.TempDir()
	mustCreatePartitionDir(t, dataDir, "orders", 0)
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
	mustCreatePartitionDir(t, dataDir, "orders", 0)
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

func TestConsumerOffsetCommitterDoesNotRecreatePurgedPartitionDir(t *testing.T) {
	dataDir := t.TempDir()
	partitionDir := mustCreatePartitionDir(t, dataDir, "orders", 0)
	committer := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)

	committer.Commit("orders", 0, 7)
	if err := os.RemoveAll(partitionDir); err != nil {
		t.Fatalf("RemoveAll() error = %v", err)
	}

	if err := committer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := os.Stat(partitionDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partition dir stat error = %v, want not exist", err)
	}
}

func mustCreatePartitionDir(t *testing.T, dataDir, topic string, partition int) string {
	t.Helper()
	partitionDir := storage.TopicPartitionDir(dataDir, topic, partition)
	if err := os.MkdirAll(partitionDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	return partitionDir
}

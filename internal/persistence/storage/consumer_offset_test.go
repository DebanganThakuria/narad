package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReadConsumerOffsetMissingFile(t *testing.T) {
	offset, ok, err := ReadConsumerOffset(t.TempDir())
	if err != nil {
		t.Fatalf("ReadConsumerOffset() error = %v", err)
	}
	if ok {
		t.Fatal("ReadConsumerOffset() ok = true, want false")
	}
	if offset != 0 {
		t.Fatalf("ReadConsumerOffset() offset = %d, want 0", offset)
	}
}

func TestConsumerOffsetRoundTrip(t *testing.T) {
	partitionDir := t.TempDir()
	if err := WriteConsumerOffset(partitionDir, 42); err != nil {
		t.Fatalf("WriteConsumerOffset() error = %v", err)
	}

	offset, ok, err := ReadConsumerOffset(partitionDir)
	if err != nil {
		t.Fatalf("ReadConsumerOffset() error = %v", err)
	}
	if !ok {
		t.Fatal("ReadConsumerOffset() ok = false, want true")
	}
	if offset != 42 {
		t.Fatalf("ReadConsumerOffset() offset = %d, want 42", offset)
	}
}

func TestReadConsumerOffsetCorruptFile(t *testing.T) {
	partitionDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(partitionDir, consumerOffsetFileName), []byte{1, 2, 3}, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, ok, err := ReadConsumerOffset(partitionDir)
	if err == nil {
		t.Fatal("ReadConsumerOffset() error = nil, want error")
	}
	if ok {
		t.Fatal("ReadConsumerOffset() ok = true, want false")
	}
}

func TestWriteConsumerOffsetReplacesExistingValue(t *testing.T) {
	partitionDir := t.TempDir()
	if err := WriteConsumerOffset(partitionDir, 7); err != nil {
		t.Fatalf("WriteConsumerOffset(first) error = %v", err)
	}
	if err := WriteConsumerOffset(partitionDir, 9); err != nil {
		t.Fatalf("WriteConsumerOffset(second) error = %v", err)
	}

	offset, ok, err := ReadConsumerOffset(partitionDir)
	if err != nil {
		t.Fatalf("ReadConsumerOffset() error = %v", err)
	}
	if !ok {
		t.Fatal("ReadConsumerOffset() ok = false, want true")
	}
	if offset != 9 {
		t.Fatalf("ReadConsumerOffset() offset = %d, want 9", offset)
	}
}

func TestWriteConsumerOffsetCreatesPartitionDir(t *testing.T) {
	partitionDir := filepath.Join(t.TempDir(), "topics", "orders", "p00000")
	if err := WriteConsumerOffset(partitionDir, 3); err != nil {
		t.Fatalf("WriteConsumerOffset() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(partitionDir, consumerOffsetFileName)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Fatal("consumer offset file not created")
		}
		t.Fatalf("Stat() error = %v", err)
	}
}

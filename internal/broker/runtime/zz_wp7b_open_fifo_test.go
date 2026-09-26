//go:build darwin || linux

package runtime

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// storage.NewLog itself, not only the lookups before it, runs outside
// the log map lock. The partition's high-watermark file is a FIFO here,
// so NewLog blocks reading it (after recovering the segments) until the
// test lets it go: a stand-in for recovering a partition with gigabytes
// retained, which used to stall every Get on the node for the whole
// scan.
func TestZZWP7bRecoveringAPartitionDoesNotStallOtherTopics(t *testing.T) {
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "slow", ID: "00000000000000a5", Partitions: 1})
	ms.put(topic.Topic{Name: "b", ID: "000000000000000b", Partitions: 1})
	dataDir := t.TempDir()
	g := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	if _, err := g.Get("b", 0); err != nil {
		t.Fatalf("Get(b): %v", err)
	}

	partitionDir := storage.TopicPartitionDir(dataDir, "slow", 0)
	if err := os.MkdirAll(partitionDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(partitionDir, "hwm")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}

	slowDone := make(chan error, 1)
	go func() {
		_, err := g.Get("slow", 0)
		slowDone <- err
	}()
	// A non-blocking open for writing succeeds only once NewLog has the
	// FIFO open for reading; NewLog then blocks in the read until the
	// writer closes.
	var w *os.File
	deadline := time.Now().Add(5 * time.Second)
	for {
		f, err := os.OpenFile(fifo, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err == nil {
			w = f
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("NewLog never opened the high-watermark file: %v", err)
		}
		time.Sleep(time.Millisecond)
	}

	done, inTime := zzWP7bGetWithin(t, g, "b", 0, 2*time.Second)
	_ = w.Close()
	if err := <-done; err != nil {
		t.Fatalf("Get(b): %v", err)
	}
	if !inTime {
		t.Fatal("Get(b) waited for another topic's storage.NewLog")
	}
	if err := <-slowDone; err != nil {
		t.Fatalf("Get(slow): %v", err)
	}
}

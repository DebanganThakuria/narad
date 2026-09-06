package runtime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// A move installs a copied partition by renaming the staged directory
// over the partition's directory. If this node still had that
// partition's log open (it sourced the partition moments earlier), the
// swap under the open handle served the stale files and lost every
// later write. ReplacePartitionDir closes the log first and holds the
// open guard, so the next Get serves the installed copy.
func TestReplacePartitionDirClosesOpenLogAndServesTheCopy(t *testing.T) {
	dataDir := t.TempDir()
	logs := NewLogs(dataDir, storage.Options{}, nil, nil)
	t.Cleanup(func() { _ = logs.CloseAll() })

	commit := func(l *storage.Log, payloads ...string) {
		t.Helper()
		encoded := make([][]byte, 0, len(payloads))
		for _, p := range payloads {
			encoded = append(encoded, storage.EncodeKeyedRecord("k", 1, []byte(p)))
		}
		first, last, err := l.AppendBatchOwned(encoded)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := l.CommitDurable(first, last); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}

	old, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	commit(old, "stale-1", "stale-2")

	// The staged copy: what the transient owner had, one record ahead.
	staging := filepath.Join(dataDir, ".moves", "orders-0")
	staged, err := storage.NewLog(staging, storage.Options{})
	if err != nil {
		t.Fatalf("staged NewLog: %v", err)
	}
	commit(staged, "copy-1", "copy-2", "copy-3")
	if err := staged.Close(); err != nil {
		t.Fatalf("close staged: %v", err)
	}

	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	if err := logs.ReplacePartitionDir("orders", 0, func() error {
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
		return os.Rename(staging, dir)
	}); err != nil {
		t.Fatalf("ReplacePartitionDir: %v", err)
	}

	// The old handle is closed: it must not accept writes any more.
	if _, _, err := old.AppendBatchOwned([][]byte{storage.EncodeKeyedRecord("k", 1, []byte("late"))}); err == nil {
		t.Fatal("the pre-install log handle still accepted a write after the directory was replaced")
	}
	// A fresh Get serves the installed copy, not the stale files.
	cur, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatalf("Get after install: %v", err)
	}
	if cur == old {
		t.Fatal("Get returned the pre-install handle")
	}
	if got := cur.HighWatermark(); got != 3 {
		t.Fatalf("installed copy HWM = %d, want 3", got)
	}
	_, _, payload, err := cur.ReadKeyed(2)
	if err != nil || string(payload) != "copy-3" {
		t.Fatalf("record 2 = %q, %v; want the copy's third record", payload, err)
	}
	// And a write after the install lands in the installed directory.
	commit(cur, "after-install")
	if got := cur.HighWatermark(); got != 4 {
		t.Fatalf("HWM after a post-install commit = %d, want 4", got)
	}
}

package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP23RemoveAllWithTick removes topics/<name> the way os.RemoveAll
// does for a topic with one partition directory, in its syscall order:
// the partition directory's entries and the topic directory's marker are
// unlinked, then (the window) tick runs, then the two directories are
// rmdir'ed. os.RemoveAll lists before it unlinks and does not list
// again before the rmdir, so a file created in the window fails the
// rmdir with ENOTEMPTY, which it returns (removeall_at.go).
func zzWP23RemoveAllWithTick(t *testing.T, partDir string, tick func()) func(string) error {
	return func(topicDir string) error {
		entries, err := os.ReadDir(partDir)
		if err != nil {
			t.Errorf("list %s: %v", partDir, err)
		}
		for _, e := range entries {
			if err := os.Remove(filepath.Join(partDir, e.Name())); err != nil {
				t.Errorf("unlink %s: %v", e.Name(), err)
			}
		}
		if err := os.Remove(filepath.Join(topicDir, storage.IncarnationMarkerFileName)); err != nil {
			t.Errorf("unlink marker: %v", err)
		}
		tick()
		if err := syscall.Rmdir(partDir); err != nil {
			return &os.PathError{Op: "unlinkat", Path: partDir, Err: err}
		}
		if err := syscall.Rmdir(topicDir); err != nil {
			return &os.PathError{Op: "unlinkat", Path: topicDir, Err: err}
		}
		return nil
	}
}

// A topic is purged while a commit of its shard is pending: a real
// InFlight shard handed out and acked 0..19, and the committer never
// primed it. A committer tick lands inside the removal, after the
// unlinks and before the rmdirs. The purge must remove the directory,
// and a same-named successor must deliver its own offset 0. When the
// retired hook ran only after the removal, the live shard's tick
// created consumer.ahead (and levelled consumer.offset) in the partition
// directory being removed, the rmdir failed with ENOTEMPTY, and the
// successor adopted the unmarked leftover and recovered frontier 19.
func TestZZWP23PurgeRetiresBeforeRemoving(t *testing.T) {
	ctx := context.Background()
	store := newIncarnationStore(t)
	dataDir := t.TempDir()
	logs := NewLogs(dataDir, storage.Options{FlushInterval: time.Millisecond}, store, nil)
	defer logs.CloseAll()
	committer := zzWP23ManualCommitter(dataDir)
	defer func() { _ = committer.Close() }()
	offsets := zzWP23WireInFlight(dataDir, committer)
	logs.SetTopicRetiredHook(func(name string) { offsets.DropTopic(name) })

	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "1111111111111111", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	l, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	appendOld(t, l, 30, "old-incarnation")
	for range 20 {
		res, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, l.HighWatermark())
		if err != nil || !res.Reserved {
			t.Fatalf("reserve: %+v %v", res, err)
		}
		if err := offsets.CommitHandle("orders", 0, res.Offset, res.Nonce); err != nil {
			t.Fatal(err)
		}
	}

	if err := store.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	topicDir := storage.TopicDir(dataDir, "orders")
	partDir := storage.TopicPartitionDir(dataDir, "orders", 0)
	ticked := false
	logs.removeAll = zzWP23RemoveAllWithTick(t, partDir, func() {
		ticked = true
		if err := committer.flush(); err != nil {
			t.Logf("tick inside the removal: %v", err)
		}
	})
	purged, err := logs.PurgeTopic("orders", "1111111111111111")
	if !purged || !ticked {
		t.Fatalf("setup: purged %v, ticked %v", purged, ticked)
	}
	if err != nil {
		entries, _ := os.ReadDir(partDir)
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("purge: %v; the partition directory is left holding %v", err, names)
	}
	if _, err := os.Stat(topicDir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the purged topic's directory survives the purge (stat err %v)", err)
	}

	// The name is recreated. A leftover directory has no marker, so the
	// successor would adopt it.
	if err := store.CreateTopic(ctx, topic.Topic{Name: "orders", ID: "2222222222222222", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	l2, err := logs.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	appendOld(t, l2, 10, "new-incarnation")
	res, err := offsets.ReserveNext(ctx, "orders", 0, time.Minute, l2.HighWatermark())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Reserved || res.Offset != 0 {
		t.Fatalf("LOSS: the recreated topic's first reserve is %+v, want offset 0: it recovered the purged incarnation's frontier %d",
			res, zzWP23RecoverNew(t, partDir).frontier)
	}
}

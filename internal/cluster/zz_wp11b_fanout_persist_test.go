package cluster

// The per-slab fan-out cursor persist overwrites the cursor file in
// place instead of writing a temp file and renaming it over the old one.

import (
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile/faulttest"
)

// The per-slab cursor persist overwrites the cursor file in place: no
// temp file, no rename, the same inode across advances.
func TestZZWP11BFanoutPersistCursorOverwritesInPlace(t *testing.T) {
	env := zzWP11BSetup(t, 1, "node-self", nil, nil, true)
	dir := storage.TopicPartitionDir(env.dataDir, "parent", 0)
	if err := storage.WriteFanoutCursorCreating(dir, env.key.child, storage.FanoutCursor{Epoch: env.key.epoch, NextOffset: 0}); err != nil {
		t.Fatal(err)
	}
	if !env.runner.persistCursor(env.key, dir, 1) {
		t.Fatal("persistCursor failed")
	}
	path := dir + string(os.PathSeparator) + "fanout-child.offset"
	inode := func() uint64 {
		t.Helper()
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		return uint64(info.Sys().(*syscall.Stat_t).Ino)
	}
	before := inode()

	inj := faulttest.New(t)
	renames := inj.FailNth(syncfile.OpRename, "fanout-child.offset", 1, syscall.EIO)
	for next := int64(2); next <= 20; next++ {
		if !env.runner.persistCursor(env.key, dir, next) {
			t.Fatalf("persistCursor(%d) failed", next)
		}
	}
	if renames.Fired() != 0 {
		t.Fatalf("persistCursor renamed the cursor file %d times, want an in-place overwrite", renames.Fired())
	}
	if after := inode(); after != before {
		t.Fatalf("cursor file inode changed %d -> %d: persisted through a new file", before, after)
	}
	cur, ok, err := storage.ReadFanoutCursor(dir, env.key.child)
	if err != nil || !ok || cur.NextOffset != 20 || cur.Epoch != env.key.epoch {
		t.Fatalf("ReadFanoutCursor = (%+v, %v, %v), want next 20 epoch %q", cur, ok, err, env.key.epoch)
	}
}

// BenchmarkZZWP11BFanoutPersistCursor measures the per-slab cursor
// persist. The nosync variant makes every data and directory sync
// report success without reaching the disk, which leaves the file
// system work around the sync (temp file, rename, unlink versus one
// overwrite): on this macOS box F_FULLFSYNC swamps it, and other
// processes' syncs make the synced variant noisy.
func BenchmarkZZWP11BFanoutPersistCursor(b *testing.B) {
	for _, nosync := range []bool{false, true} {
		b.Run(fmt.Sprintf("nosync=%v", nosync), func(b *testing.B) {
			env := zzWP11BSetup(b, 1, "node-self", nil, &zzWP11BBroker{}, true)
			dir := storage.TopicPartitionDir(env.dataDir, "parent", 0)
			if err := storage.WriteFanoutCursorCreating(dir, env.key.child, storage.FanoutCursor{Epoch: env.key.epoch}); err != nil {
				b.Fatal(err)
			}
			if nosync {
				restore := syncfile.SetFaultHook(func(op syncfile.Op, _ string) error {
					if op == syncfile.OpSyncData || op == syncfile.OpSync {
						return syncfile.ErrLie
					}
					return nil
				})
				b.Cleanup(restore)
			}
			b.ReportAllocs()
			b.ResetTimer()
			next := int64(0)
			for b.Loop() {
				next += 4096
				if !env.runner.persistCursor(env.key, dir, next) {
					b.Fatal("persistCursor failed")
				}
			}
		})
	}
}

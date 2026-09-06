package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/debanganthakuria/narad/internal/persistence/storage/codec"
)

// assertPrivate fails when path is readable by anyone but its owner.
// The modes are subject to the umask, which can only clear bits, so
// "no group/other bits" is the invariant that holds everywhere.
func assertPrivate(t *testing.T, path string) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("%s has mode %o, want no group/other bits", path, perm)
	}
}

// Every file and directory the partition log creates is private to the
// broker's user: segments, the high-watermark file, the consumer
// offset, fan-out cursors, transferred segments and the directories
// that hold them (audit finding 2.7).
func TestDataFilesAndDirectoriesArePrivate(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "topics", "orders", "p00000")

	l, err := NewLog(dir, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("NewLog: %v", err)
	}
	off, err := l.Append([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.CommitDurable(off, off); err != nil {
		t.Fatalf("CommitDurable: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertPrivate(t, filepath.Join(root, "topics"))
	assertPrivate(t, filepath.Join(root, "topics", "orders"))
	assertPrivate(t, dir)
	assertPrivate(t, filepath.Join(dir, segmentFileName(0)))
	assertPrivate(t, hwmFilePath(dir))

	if err := WriteConsumerOffset(dir, 1); err != nil {
		t.Fatalf("WriteConsumerOffset: %v", err)
	}
	assertPrivate(t, filepath.Join(dir, consumerOffsetFileName))

	if err := WriteFanoutCursorCreating(dir, "child", FanoutCursor{Epoch: "e", NextOffset: 1}); err != nil {
		t.Fatalf("WriteFanoutCursorCreating: %v", err)
	}
	assertPrivate(t, filepath.Join(dir, fanoutCursorFileName("child")))

	fresh := filepath.Join(root, "topics", "orders", "p00001")
	if err := WriteConsumerOffset(fresh, 1); err != nil {
		t.Fatalf("WriteConsumerOffset(fresh): %v", err)
	}
	assertPrivate(t, fresh)

	staging := filepath.Join(root, "staging", "p00000")
	if err := WriteSegmentFile(staging, 0, []byte("segment")); err != nil {
		t.Fatalf("WriteSegmentFile: %v", err)
	}
	if err := AppendToSegmentFile(staging, 7, []byte("tail")); err != nil {
		t.Fatalf("AppendToSegmentFile: %v", err)
	}
	if err := WritePersistedHighWatermark(staging, 1); err != nil {
		t.Fatalf("WritePersistedHighWatermark: %v", err)
	}
	assertPrivate(t, staging)
	assertPrivate(t, filepath.Join(staging, segmentFileName(0)))
	assertPrivate(t, filepath.Join(staging, segmentFileName(7)))
	assertPrivate(t, hwmFilePath(staging))

	topicDir := filepath.Join(root, "topics", "events")
	if err := WriteTopicIncarnation(topicDir, "0123456789abcdef"); err != nil {
		t.Fatalf("WriteTopicIncarnation: %v", err)
	}
	assertPrivate(t, topicDir)
	assertPrivate(t, filepath.Join(topicDir, IncarnationMarkerFileName))
}

// Existing files keep their mode: the log never chmods what it finds.
func TestExistingDataFilesKeepTheirMode(t *testing.T) {
	dir := testLogPath(t)
	mustWriteAndClose(t, dir, slowFlushOpts(t, codec.NewNoopCodec()), func(l *Log) {
		if _, err := l.Append([]byte("x")); err != nil {
			t.Fatal(err)
		}
	})
	seg := filepath.Join(dir, segmentFileName(0))
	if err := os.Chmod(seg, 0o644); err != nil {
		t.Fatal(err)
	}
	l, err := NewLog(dir, slowFlushOpts(t, codec.NewNoopCodec()))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l.Close()
	st, err := os.Stat(seg)
	if err != nil {
		t.Fatal(err)
	}
	if perm := st.Mode().Perm(); perm != 0o644 {
		t.Fatalf("existing segment mode changed to %o", perm)
	}
}

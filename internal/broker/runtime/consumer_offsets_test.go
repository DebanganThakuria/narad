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

// fakeAheadSource is a scripted AheadSource: tests set the snapshot a
// partition reports.
type fakeAheadSource struct {
	committed int64
	offsets   []int64
	version   uint64
}

func (s *fakeAheadSource) source(string, int) (int64, []int64, uint64, bool) {
	return s.committed, s.offsets, s.version, true
}

func TestConsumerOffsetCommitterPersistsAckedAheadWithTheFrontier(t *testing.T) {
	dataDir := t.TempDir()
	mustCreatePartitionDir(t, dataDir, "orders", 0)
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	committer := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)
	src := &fakeAheadSource{committed: 4, offsets: []int64{6, 9}, version: 1}
	committer.SetAheadSource(src.source)

	committer.Commit("orders", 0, 4)
	if err := committer.flush(); err != nil {
		t.Fatalf("flush() error = %v", err)
	}
	rec, ok, err := storage.ReadConsumerAhead(dir)
	if err != nil || !ok || rec.Committed != 4 || len(rec.Offsets) != 2 || rec.Offsets[0] != 6 || rec.Offsets[1] != 9 {
		t.Fatalf("consumer.ahead after flush = %+v ok %v err %v, want committed 4 offsets [6 9]", rec, ok, err)
	}
	firstSeq := rec.Seq

	// Same frontier, same set version: the flush writes neither file.
	before := mustModTime(t, dir, "consumer.offset")
	committer.Commit("orders", 0, 4)
	if err := committer.flush(); err != nil {
		t.Fatalf("flush() error = %v", err)
	}
	if rec2, _, _ := storage.ReadConsumerAhead(dir); rec2.Seq != firstSeq {
		t.Fatalf("consumer.ahead rewritten (seq %d -> %d) although the set did not change", firstSeq, rec2.Seq)
	}
	if after := mustModTime(t, dir, "consumer.offset"); !after.Equal(before) {
		t.Fatal("consumer.offset rewritten although the frontier did not change")
	}

	// The set drained (frontier walked over it): the next write is an
	// empty record in the other slot, with a higher seq, and the reader
	// takes it over the older non-empty one.
	src.committed, src.offsets, src.version = 9, nil, 2
	committer.Commit("orders", 0, 9)
	if err := committer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	rec, ok, err = storage.ReadConsumerAhead(dir)
	if err != nil || !ok || rec.Committed != 9 || len(rec.Offsets) != 0 || rec.Seq <= firstSeq {
		t.Fatalf("consumer.ahead after drain = %+v ok %v err %v, want an empty record at 9 with a higher seq", rec, ok, err)
	}
	if got, _, _ := storage.ReadConsumerOffset(dir); got != 9 {
		t.Fatalf("consumer.offset = %d, want 9", got)
	}
}

// TestConsumerOffsetCommitterSkipsEmptyAheadOnFreshPartition pins that
// a partition whose acks are all in order never gets a consumer.ahead
// file at all: the missing file already means "nothing acked ahead".
func TestConsumerOffsetCommitterSkipsEmptyAheadOnFreshPartition(t *testing.T) {
	dataDir := t.TempDir()
	mustCreatePartitionDir(t, dataDir, "orders", 0)
	committer := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)
	src := &fakeAheadSource{committed: 2, version: 7}
	committer.SetAheadSource(src.source)
	committer.Commit("orders", 0, 2)
	if err := committer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := os.Stat(storage.TopicPartitionDir(dataDir, "orders", 0) + "/consumer.ahead"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("consumer.ahead exists (stat err %v) although nothing was ever acked ahead", err)
	}
}

// TestConsumerOffsetCommitterForgetRewritesAfterReplacement pins the
// move story: after the partition directory was replaced under the
// committer, Forget makes the next flush write both files even when
// the values match what this process last wrote.
func TestConsumerOffsetCommitterForgetRewritesAfterReplacement(t *testing.T) {
	dataDir := t.TempDir()
	mustCreatePartitionDir(t, dataDir, "orders", 0)
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	committer := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)
	src := &fakeAheadSource{committed: 3, offsets: []int64{5}, version: 1}
	committer.SetAheadSource(src.source)
	committer.Commit("orders", 0, 3)
	if err := committer.flush(); err != nil {
		t.Fatal(err)
	}
	// Simulate an installed copy carrying an older frontier.
	if err := storage.WriteConsumerOffset(dir, 1); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dir + "/consumer.ahead"); err != nil {
		t.Fatal(err)
	}
	committer.Forget("orders", 0)
	committer.Commit("orders", 0, 3)
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	if got, _, _ := storage.ReadConsumerOffset(dir); got != 3 {
		t.Fatalf("consumer.offset after Forget = %d, want 3 (rewritten)", got)
	}
	if _, ok, _ := storage.ReadConsumerAhead(dir); !ok {
		t.Fatal("consumer.ahead not rewritten after Forget")
	}
}

func mustModTime(t *testing.T, dir, name string) time.Time {
	t.Helper()
	info, err := os.Stat(dir + "/" + name)
	if err != nil {
		t.Fatalf("stat %s: %v", name, err)
	}
	return info.ModTime()
}

// TestConsumerOffsetCommitterResumesFromTheFileAfterRestart pins the
// first write after a restart: it lands in the other slot from the
// newest record on disk and carries a higher seq, so a torn first write
// cannot destroy that record and a clock stepping back cannot make the
// reader prefer it.
func TestConsumerOffsetCommitterResumesFromTheFileAfterRestart(t *testing.T) {
	dataDir := t.TempDir()
	mustCreatePartitionDir(t, dataDir, "orders", 0)
	dir := storage.TopicPartitionDir(dataDir, "orders", 0)
	future := uint64(time.Now().Add(time.Hour).UnixNano()) // a previous process with a faster clock
	if err := storage.WriteConsumerAhead(dir, 1, future, 2, []int64{5}); err != nil {
		t.Fatal(err)
	}
	committer := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)
	src := &fakeAheadSource{committed: 2, offsets: []int64{5, 6}, version: 1}
	committer.SetAheadSource(src.source)
	committer.Commit("orders", 0, 2)
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
	rec, ok, err := storage.ReadConsumerAhead(dir)
	if err != nil || !ok {
		t.Fatalf("ReadConsumerAhead: ok %v err %v", ok, err)
	}
	if rec.Slot != 0 || rec.Seq <= future || len(rec.Offsets) != 2 {
		t.Fatalf("record after restart = slot %d seq %d offsets %v, want slot 0, seq > %d, [5 6]", rec.Slot, rec.Seq, rec.Offsets, future)
	}
}

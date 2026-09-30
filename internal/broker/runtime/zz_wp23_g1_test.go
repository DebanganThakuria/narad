package runtime

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// zzWP23NoShard is an AheadSource for a node that holds no shard for
// any partition: every shard was dropped.
func zzWP23NoShard(string, int) (int64, []int64, uint64, bool) { return 0, nil, 0, false }

// A commit that reaches the committer after its shard was dropped (acks
// call Commit after the shard lock is released, and DropPartition or
// DropTopic can run in between) belongs to state this node no longer
// holds. The committer used to write its offset to consumer.offset by
// path all the same, into whatever directory now sits there.
func TestZZWP23LateCommitOfADroppedShardWritesNothing(t *testing.T) {
	dataDir := t.TempDir()
	dir := mustCreatePartitionDir(t, dataDir, "t", 0)
	c := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)
	c.SetAheadSource(zzWP23NoShard)
	c.Commit("t", 0, 9)
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"consumer.offset", "consumer.ahead"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s written for a partition with no shard (stat err %v)", name, err)
		}
	}
}

// The move-back story behind G1: this node owned the partition, moved
// it away, and gets it back. The installed copy carries the new
// lineage's frontier (3). An ack on the old shard queued a commit of 9
// before the shard was dropped. Writing that 9 into the installed
// directory would make the next shard skip offsets 4 to 9 of the new
// lineage, which were never acked there: loss.
func TestZZWP23LateCommitNeverLandsInAnInstalledCopy(t *testing.T) {
	dataDir := t.TempDir()
	dir := mustCreatePartitionDir(t, dataDir, "t", 0)
	c := NewConsumerOffsetCommitter(dataDir, time.Hour, nil)
	c.SetAheadSource(zzWP23NoShard)
	c.Commit("t", 0, 9)

	staged := filepath.Join(dataDir, "staged")
	if err := os.MkdirAll(staged, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteConsumerOffset(staged, 3); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(staged, dir); err != nil {
		t.Fatal(err)
	}

	if err := c.flush(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	got, ok, err := storage.ReadConsumerOffset(dir)
	if err != nil || !ok || got != 3 {
		t.Fatalf("installed consumer.offset = %d (ok %v err %v), want the copy's 3", got, ok, err)
	}
	if rec, ok, _ := storage.ReadConsumerAhead(dir); ok && rec.Committed > 3 {
		t.Fatalf("installed consumer.ahead carries %d, above the copy's 3", rec.Committed)
	}
}

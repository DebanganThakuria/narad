package runtime

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// A stat that fails for a reason other than the path being gone (EACCES
// here, standing in for EIO or ESTALE) tells nothing about the partition
// directory. It must fail the tick and retry, not strand the partition:
// a strand stops persisting its acks until the next Forget, which a
// live, never-moved partition never gets, so every later ack would be
// redelivered after a restart with nothing logged.
func TestZZShipTransientStatErrorDoesNotStrand(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dataDir := t.TempDir()
	dir := storage.TopicPartitionDir(dataDir, "t", 0)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	c := newConsumerOffsetCommitter(dataDir, time.Second, nil, committerOptions{manual: true})
	shards := &zzWP23Shards{}
	c.SetAheadSource(shards.source)
	sh := newZZWP23Shard(-1)
	shards.set(0, sh)
	for off := range int64(20) {
		sh.ack(off)
	}
	c.Commit("t", 0, sh.state().frontier)
	if err := c.flush(); err != nil {
		t.Fatal(err)
	}

	// The topic directory loses its search bit for one tick: the held
	// descriptor still writes, but every stat under it fails EACCES.
	parent := filepath.Dir(dir)
	if err := os.Chmod(parent, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o755) })
	sh.ack(20)
	c.Commit("t", 0, sh.state().frontier)
	faulted := c.flush()
	if err := os.Chmod(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if faulted == nil {
		t.Fatal("a tick whose writeout could not check its file reported no error")
	}

	for off := int64(21); off < 40; off++ {
		sh.ack(off)
		c.Commit("t", 0, sh.state().frontier)
		if err := c.flush(); err != nil {
			t.Fatalf("tick after the directory recovered: %v", err)
		}
	}
	c.ioMu.Lock()
	_, stranded := c.stranded[offsetCommitKey{"t", 0}]
	c.ioMu.Unlock()
	if stranded {
		t.Fatal("one transient stat error stranded a live partition")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	want := sh.state().frontier
	rec, ok, err := storage.ReadConsumerAhead(dir)
	if err != nil || !ok || rec.Committed != want {
		t.Fatalf("persisted consumer.ahead frontier = %d (ok=%v err=%v), want %d", rec.Committed, ok, err, want)
	}
	if off, ok, err := storage.ReadConsumerOffset(dir); err != nil || !ok || off != want {
		t.Fatalf("persisted consumer.offset = %d (ok=%v err=%v), want %d", off, ok, err, want)
	}
}

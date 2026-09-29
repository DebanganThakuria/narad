package runtime

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
	"github.com/debanganthakuria/narad/internal/persistence/syncfile"
)

// zzShipLogs is a Logs over one fresh topic "orders" of n partitions.
func zzShipLogs(t *testing.T, n int) (*Logs, string) {
	t.Helper()
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: n})
	dataDir := t.TempDir()
	g := NewLogs(dataDir, storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	return g, dataDir
}

// zzShipClaim claims partition idx's open entry, as a close does, and
// returns it for closeClaimed. Caller holds the topic's guard.
func zzShipClaim(t *testing.T, g *Logs, idx int) claimedEntry {
	t.Helper()
	key := keyOf("orders", idx)
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.logs[key]
	if e == nil {
		t.Fatalf("partition %d is not open", idx)
	}
	claimLocked(e)
	return claimedEntry{key: key, entry: e}
}

// A GetMany whose slow path fails on one partition returns nil and that
// error, not a slice with a hole in it; the partitions it opened before
// the failure stay open.
func TestZZShipGetManyFailsOnAnUnopenablePartition(t *testing.T) {
	g, dataDir := zzShipLogs(t, 3)
	if _, err := g.Get("orders", 0); err != nil {
		t.Fatal(err)
	}
	// A file where partition 1's directory belongs: its open fails.
	if err := os.WriteFile(storage.TopicPartitionDir(dataDir, "orders", 1), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	logs, err := g.GetMany("orders", []int{2, 1, 0}, nil)
	if err == nil || logs != nil {
		t.Fatalf("GetMany = (%d logs, %v), want nil and partition 1's open error", len(logs), err)
	}
	if _, ok := g.Peek("orders", 2); !ok {
		t.Fatal("partition 2, opened before the failure, was not left open")
	}
	if _, ok := g.Peek("orders", 1); ok {
		t.Fatal("the unopenable partition is registered as open")
	}
}

// A close that fails still drops the entry and releases every Peek
// waiting on it; otherwise Peek hangs on a channel nothing closes, and
// the next Get finds a dead log. The next Get opens the partition fresh,
// with its committed records visible (the failed close left the hwm file
// empty, so the open takes the boundary from the record tail).
func TestZZShipFailedCloseDropsTheEntryAndReleasesPeek(t *testing.T) {
	g, dataDir := zzShipLogs(t, 1)
	l, err := g.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	appendOld(t, l, 3, "committed")

	unlock := g.lockTopic("orders")
	claimed := zzShipClaim(t, g, 0)
	peeked := make(chan bool, 1)
	go func() {
		_, ok := g.Peek("orders", 0)
		peeked <- ok
	}()
	select {
	case ok := <-peeked:
		unlock()
		t.Fatalf("Peek returned (ok=%v) while the log was being closed", ok)
	case <-time.After(50 * time.Millisecond):
	}

	hwmFile := filepath.Join(storage.TopicPartitionDir(dataDir, "orders", 0), "hwm")
	restore := syncfile.SetFaultHook(func(op syncfile.Op, path string) error {
		if op == syncfile.OpWrite && path == hwmFile {
			return syscall.EIO
		}
		return nil
	})
	closeErr := g.closeClaimed([]claimedEntry{claimed})
	restore()
	unlock()
	if closeErr == nil {
		t.Fatal("the close whose hwm write failed reported success")
	}
	select {
	case ok := <-peeked:
		if ok {
			t.Fatal("Peek served the log whose close failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Peek still waiting after a failed close")
	}
	g.mu.RLock()
	_, present := g.logs[keyOf("orders", 0)]
	g.mu.RUnlock()
	if present {
		t.Fatal("a failed close left its entry in the log map")
	}

	reopened, err := g.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if reopened == l {
		t.Fatal("Get served the log whose close failed")
	}
	if hwm := reopened.HighWatermark(); hwm != 3 {
		t.Fatalf("reopened HighWatermark = %d, want 3", hwm)
	}
}

// Current is false for a log whose entry is being closed, even before
// the close drops it: a combined produce commit that asks must not append
// into a log on its way out.
func TestZZShipCurrentIsFalseForAClosingEntry(t *testing.T) {
	g, _ := zzShipLogs(t, 1)
	l, err := g.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if !g.Current("orders", 0, l) {
		t.Fatal("an open, unchanged log is not current")
	}
	unlock := g.lockTopic("orders")
	claimed := zzShipClaim(t, g, 0)
	if g.Current("orders", 0, l) {
		unlock()
		t.Fatal("a log being closed reads as current")
	}
	if err := g.closeClaimed([]claimedEntry{claimed}); err != nil {
		unlock()
		t.Fatal(err)
	}
	unlock()
	reopened, err := g.Get("orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if g.Current("orders", 0, l) || !g.Current("orders", 0, reopened) {
		t.Fatal("after the reopen, the closed log reads as current or the reopened one does not")
	}
}

// The eviction scan skips an entry already being closed: it neither
// counts it nor waits on the topic guard its closer holds, so an idle
// partition of another topic is evicted while that close is in flight.
func TestZZShipEvictionSkipsAClosingEntry(t *testing.T) {
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "orders", ID: "0000000000000001", Partitions: 1})
	ms.put(topic.Topic{Name: "other", ID: "0000000000000002", Partitions: 1})
	g := NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	for _, name := range []string{"orders", "other"} {
		if _, err := g.Get(name, 0); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-time.Hour).UnixNano()
	g.mu.RLock()
	for _, name := range []string{"orders", "other"} {
		g.logs[keyOf(name, 0)].lastAccess.Store(old)
	}
	g.mu.RUnlock()

	unlock := g.lockTopic("orders")
	claimed := zzShipClaim(t, g, 0)
	evicted := make(chan int, 1)
	go func() { evicted <- g.EvictIdleOnce(time.Minute) }()
	select {
	case n := <-evicted:
		if n != 1 {
			t.Errorf("EvictIdleOnce closed %d logs, want 1 (other/0 only)", n)
		}
	case <-time.After(5 * time.Second):
		t.Error("EvictIdleOnce waited on the guard of a partition already being closed")
	}
	if err := g.closeClaimed([]claimedEntry{claimed}); err != nil {
		t.Error(err)
	}
	unlock()
	if t.Failed() {
		<-evicted
		return
	}
	if _, ok := g.Peek("other", 0); ok {
		t.Fatal("idle partition other/0 was not evicted")
	}
}

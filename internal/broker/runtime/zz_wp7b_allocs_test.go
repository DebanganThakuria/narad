package runtime

import (
	"testing"
	"time"

	"github.com/debanganthakuria/narad/internal/domain/topic"
	"github.com/debanganthakuria/narad/internal/persistence/storage"
)

// The Get fast path runs once per local partition per consume scan and
// once per produce commit batch. Keyed by a "topic/idx" string it
// allocated that string on every call (7% of all objects the broker
// allocated under load); the produce lock allocated a second one plus
// an unlock closure per commit batch.
func TestZZWP7bOpenLogLookupsDoNotAllocate(t *testing.T) {
	ms := newZZWP7bMetastore()
	ms.put(topic.Topic{Name: "orders-events-v2", ID: "0000000000000001", Partitions: 4})
	g := NewLogs(t.TempDir(), storage.Options{FlushInterval: 5 * time.Millisecond}, ms, nil)
	t.Cleanup(func() { _ = g.CloseAll() })
	for p := range 4 {
		if _, err := g.Get("orders-events-v2", p); err != nil {
			t.Fatalf("Get(%d): %v", p, err)
		}
	}

	p := 0
	if allocs := testing.AllocsPerRun(200, func() {
		if _, err := g.Get("orders-events-v2", p); err != nil {
			t.Fatalf("Get: %v", err)
		}
		p = (p + 1) & 3
	}); allocs != 0 {
		t.Errorf("Get on an open log allocated %.1f objects per call, want 0", allocs)
	}

	noop := func(*storage.Log) error { return nil }
	if allocs := testing.AllocsPerRun(200, func() {
		if err := g.WithProduceLock("orders-events-v2", p, noop); err != nil {
			t.Fatalf("WithProduceLock: %v", err)
		}
		p = (p + 1) & 3
	}); allocs != 0 {
		t.Errorf("WithProduceLock on an open log allocated %.1f objects per call, want 0", allocs)
	}
}

// stamp skips the store while lastAccess is under a second old, and
// still refreshes an older one: a Get after an eviction scan must keep
// the log open (evict.go invariant 5).
func TestZZWP7bStampRefreshesOnlyStaleAccess(t *testing.T) {
	e := &logEntry{}
	e.walkOwned.Store(true)
	e.stamp()
	first := e.lastAccess.Load()
	if first == 0 || e.walkOwned.Load() {
		t.Fatalf("first stamp: lastAccess=%d walkOwned=%v, want set and cleared", first, e.walkOwned.Load())
	}
	time.Sleep(5 * time.Millisecond)
	e.stamp()
	if got := e.lastAccess.Load(); got != first {
		t.Fatalf("a stamp within a second rewrote lastAccess (%d -> %d)", first, got)
	}
	old := time.Now().Add(-2 * time.Second).UnixNano()
	e.lastAccess.Store(old)
	e.stamp()
	if got := e.lastAccess.Load(); got <= old {
		t.Fatalf("a stamp over a second after the last one kept lastAccess at %d", got)
	}
}
